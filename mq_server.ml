open Printf
open Lwt

(** Simple STOMP message queue. *)

(** Message store. *)
module type PERSISTENCE =
sig
  type t

  val initialize : t -> unit Lwt.t

  val save_msg : t -> ?low_priority:bool -> Mq_types.message -> unit Lwt.t
  val register_ack_pending_new_msg : t -> Mq_types.message -> unit Lwt.t

  (** Returns [false] if the msg was already in the ACK-pending set. *)
  val register_ack_pending_msg : t -> string -> bool Lwt.t
  val get_ack_pending_msg : t -> string -> Mq_types.message option Lwt.t
  val ack_msg : t -> string -> unit Lwt.t
  val unack_msg : t -> string -> unit Lwt.t
  val get_msg_for_delivery : t -> string -> Mq_types.message option Lwt.t
  val count_queue_msgs : t -> string -> Int64.t Lwt.t
  val crash_recovery : t -> unit Lwt.t
end

module Make(P : PERSISTENCE) =
struct

open Mq_types
module STOMP = Mq_stomp
module SSET = Set.Make(String)

type message_kind = Saved | Ack_pending

type subscription = {
  qs_prefetch : int;
  mutable qs_pending_acks : int;
}

let dummy_subscription = { qs_prefetch = 0; qs_pending_acks = 0 }

type connection = {
  conn_id : int;
  conn_ich : Lwt_io.input_channel;
  conn_och : Lwt_io.output_channel;
  mutable conn_pending_acks : (string, unit Lwt.u) Hashtbl.t;
  conn_queues : (string, subscription) Hashtbl.t;
  conn_topics : (string, unit) Hashtbl.t; (* set of topics *)
  mutable conn_closed : bool;
}


module CONNS = ExtSet.Make_lean(struct
                                  type t = connection
                                  let compare t1 t2 = t2.conn_id - t1.conn_id
                                end)

module SUBS = ExtSet.Make(struct
                            type t = (connection * subscription)
                            let compare (t1, _) (t2, _) = t2.conn_id - t1.conn_id
                          end)

type listeners = {
  mutable l_ready : SUBS.t;
  mutable l_blocked : SUBS.t;
  mutable l_last_sent : SUBS.elt option;
}

type broker = {
  mutable b_connections : CONNS.t;
  b_queues : (string, listeners) Hashtbl.t;
  b_topics : (string, CONNS.t) Hashtbl.t;
  b_socket : Lwt_unix.file_descr;
  b_frame_eol : bool;
  b_msg_store : P.t;
  b_force_async : bool;
  b_debug : bool;
  b_async_maxmem : int;
  mutable b_async_usedmem : int;
  b_login : string option;
  b_passcode : string option;
}

let ignore_result ?(exn_handler = fun _ -> return ()) f x =
  ignore_result (try_lwt f x with e -> exn_handler e)

let remove_topic_subscription broker topic conn =
  try
    let conns = Hashtbl.find broker.b_topics topic in
      match CONNS.remove conn conns with
          s when CONNS.is_empty s -> Hashtbl.remove broker.b_topics topic
        | s -> Hashtbl.replace broker.b_topics topic s
  with Not_found -> ()

let remove_queue_subscription broker queue conn =
  try
    let ls = Hashtbl.find broker.b_queues queue in
      ls.l_ready <- SUBS.remove (conn, dummy_subscription) ls.l_ready;
      ls.l_blocked <- SUBS.remove (conn, dummy_subscription) ls.l_blocked;
      if SUBS.is_empty ls.l_ready && SUBS.is_empty ls.l_blocked then
        Hashtbl.remove broker.b_queues queue
  with Not_found -> ()

let terminate_connection broker conn =
  let wakeners =
    Hashtbl.fold (fun _ u l -> u :: l) conn.conn_pending_acks [] in

  conn.conn_closed <- true;

  if broker.b_debug then
    eprintf "Connection %d terminated with %d pending ACKs\n%!"
      conn.conn_id (List.length wakeners);

  (* remove from connection set and subscription lists *)
  broker.b_connections <- CONNS.remove conn broker.b_connections;
  Hashtbl.iter
    (fun topic _ -> remove_topic_subscription broker topic conn) conn.conn_topics;
  Hashtbl.iter
    (fun queue _ -> remove_queue_subscription broker queue conn) conn.conn_queues;
  (* cancel all the waiters: they will re-queue the corresponding messages *)
  List.iter (fun w -> wakeup_exn w Lwt.Canceled) wakeners;
  return ()

let send_error broker conn fmt =
  STOMP.send_error ~eol:broker.b_frame_eol conn.conn_och fmt

let send_to_topic broker msg =
  Lwt_unix.yield () >>
  try
    let s = Hashtbl.find broker.b_topics (destination_name msg.msg_destination) in
      CONNS.iter
        (fun conn ->
           ignore_result
             (STOMP.send_message ~eol:broker.b_frame_eol conn.conn_och) msg)
        s;
      return ()
  with Not_found -> return ()

let subs_wanted_msgs (conn, subs) =
  if subs.qs_prefetch > 0 then subs.qs_prefetch - subs.qs_pending_acks
  else max_int

let is_subs_blocked_locally subs =
  subs.qs_prefetch > 0 && subs.qs_pending_acks >= subs.qs_prefetch

let is_subs_blocked (conn, subs) =
  conn.conn_closed || is_subs_blocked_locally subs

let select_blocked_subs s = SUBS.filter is_subs_blocked s

let select_unblocked_subs s = SUBS.filter (fun x -> not (is_subs_blocked x)) s

let block_subscription listeners ((conn, subs) as c) =
  listeners.l_ready <- SUBS.remove c listeners.l_ready;
  listeners.l_blocked <- SUBS.add c listeners.l_blocked

let unblock_some_listeners listeners =
  let unblocked = select_unblocked_subs listeners.l_blocked in
    listeners.l_ready <- SUBS.union listeners.l_ready unblocked;
    listeners.l_blocked <- SUBS.diff listeners.l_blocked unblocked

let find_recipient broker name =
  try
    let ls = Hashtbl.find broker.b_queues name in
      match ls.l_last_sent with
          None -> (* first msg sent, there can be no blocked client *)
            Some (ls, SUBS.min_elt ls.l_ready)
        | Some cursor ->
            if SUBS.is_empty ls.l_ready then unblock_some_listeners ls;
            match SUBS.next cursor ls.l_ready with
              | (conn, _) when conn == fst (SUBS.min_elt ls.l_ready) ->
                  (* went through all ready subscriptions, try to unblock some &
                   * give it another try *)
                  unblock_some_listeners ls;
                  Some (ls, SUBS.next cursor ls.l_ready)
              | c -> Some (ls, c)
  with Not_found -> None

let have_recipient broker name = Option.is_some (find_recipient broker name)

let rec send_to_recipient ~kind broker listeners conn subs queue msg =
  if broker.b_debug then
    eprintf "Sending %S to conn %d.\n%!" msg.msg_id conn.conn_id;
  let sleep, wakeup = Lwt.task () in
  let msg_id = msg.msg_id in
    subs.qs_pending_acks <- subs.qs_pending_acks + 1;
    Hashtbl.replace conn.conn_pending_acks msg_id wakeup;
    if is_subs_blocked (conn, subs) then block_subscription listeners (conn, subs);
    (* we check after doing block_subscription so that the next find_recipient
     * won't get this connection *)
    if conn.conn_closed then fail Lwt.Canceled else let () = () in
    listeners.l_last_sent <- Some (conn, subs);

    (* if kind is Saved, the msg is believed not to be in the ACK-pending set;
     * if it actually is, this means it was already sent to some other conn,
     * so we don't try to send it again *)
    lwt must_send = (match kind with
         Ack_pending -> (* the message was already in ACK-pending set *) return true
       | Saved -> (* just move to ACK *)
           P.register_ack_pending_msg broker.b_msg_store msg_id) in
    if not must_send then return () else

    STOMP.send_message ~eol:broker.b_frame_eol conn.conn_och msg >>
    let threads = match msg.msg_ack_timeout with
        dt when dt > 0. -> [ Lwt_unix.timeout dt; sleep ]
      | _ -> [ sleep ] in
    begin try_lwt
      Lwt.select threads
    finally
      (* either ACKed or Timeout/Cancel, at any rate, no longer want the ACK *)
      Hashtbl.remove conn.conn_pending_acks msg_id;
      subs.qs_pending_acks <- subs.qs_pending_acks - 1;
      return ()
    end >>
    begin
      if broker.b_debug then
        eprintf "ACKed %S by conn %d\n%!" msg_id conn.conn_id;
      P.ack_msg broker.b_msg_store msg_id >>
      (* try to send older messages for the subscription whose message
       * we just ACKed *)
      (ignore_result (send_saved_messages broker) queue;
       return ())
    end

and send_saved_messages ?(only_once = false) broker queue =
  if not (have_recipient broker queue) then return () else
  P.get_msg_for_delivery broker.b_msg_store queue >>= function
      None -> return ()
    | Some msg ->
        let msg_id = msg.msg_id in
        match find_recipient broker queue with
            None -> P.unack_msg broker.b_msg_store msg_id >>
                    send_saved_messages ~only_once:true broker queue
          | Some (listeners, (conn, subs)) ->
              ignore_result
                ~exn_handler:(handle_send_msg_exn broker conn ~queue ~msg_id)
                (send_to_recipient ~kind:Ack_pending broker listeners conn subs queue)
                msg;
              if only_once then return () else send_saved_messages broker queue

and handle_send_msg_exn broker ~queue conn ~msg_id = function
  | Lwt_unix.Timeout | Lwt.Canceled ->
      if broker.b_debug then
        eprintf "Timeout/Canceled on message %S.\n%!" msg_id;
      enqueue_after_timeout broker ~queue ~msg_id
  | _ -> terminate_connection broker conn >>
         enqueue_after_timeout broker ~queue ~msg_id

and enqueue_after_timeout broker ~queue ~msg_id =
  if not (have_recipient broker queue) then
    P.unack_msg broker.b_msg_store msg_id >>
    send_saved_messages ~only_once:true broker queue else
  P.get_ack_pending_msg broker.b_msg_store msg_id >>= function
      None -> return ()
    | Some msg ->
        let msg_id = msg.msg_id in
        match find_recipient broker queue with
          | None -> begin (* move to main table *)
              if broker.b_debug then
                eprintf "No recipient for unACKed message %S, saving.\n%!" msg_id;
              P.unack_msg broker.b_msg_store msg_id >>
              send_saved_messages ~only_once:true broker queue
            end
          | Some (listeners, (conn, subs)) ->
              eprintf "Found a recipient for unACKed message %S.\n%!" msg_id;
              try_lwt
                send_to_recipient ~kind:Ack_pending broker listeners conn subs queue msg
              with Lwt_unix.Timeout | Lwt.Canceled ->
                if broker.b_debug then
                  eprintf "Trying to enqueue unACKed message %S again.\n%!" msg_id;
                enqueue_after_timeout broker ~queue ~msg_id

let rec send_to_queue broker msg =
  let queue = destination_name msg.msg_destination in
  match find_recipient broker queue with
      None -> return ()
    | Some (listeners, (conn, subs)) ->
        let msg_id = msg.msg_id in
          try_lwt
            send_to_recipient ~kind:Saved broker listeners conn subs queue msg
          with e -> handle_send_msg_exn broker conn ~queue ~msg_id e

let new_msg_id =
  let cnt = ref 0 in fun () ->
    incr cnt;
    String.concat "-"
      ["msg"; string_of_float (Unix.gettimeofday ()); string_of_int !cnt]

let new_conn_id = let n = ref 0 in fun () -> incr n; !n

let cmd_subscribe broker conn frame =
  try_lwt
    match STOMP.get_destination frame with
        Topic name -> begin
          Hashtbl.replace conn.conn_topics name ();
          try
            let conns = Hashtbl.find broker.b_topics name in
              Hashtbl.replace broker.b_topics name (CONNS.add conn conns);
              return ()
          with Not_found ->
            Hashtbl.add broker.b_topics name (CONNS.singleton conn);
            return ()
        end
      | Queue name -> begin
          let subscription =
            {
              qs_prefetch =
                (try
                   int_of_string (STOMP.get_header frame "prefetch")
                 with _ -> -1);
              qs_pending_acks = 0;
            }
          in Hashtbl.replace conn.conn_queues name subscription;
             begin
               try
                 let ls = Hashtbl.find broker.b_queues name in
                   ls.l_ready <- SUBS.add (conn, subscription) ls.l_ready;
               with Not_found ->
                 let ls = { l_ready = SUBS.singleton (conn, subscription);
                            l_blocked = SUBS.empty;
                            l_last_sent = None }
                 in Hashtbl.add broker.b_queues name ls
             end;
             ignore_result (send_saved_messages broker) name;
             return ()
        end
  with Not_found ->
    STOMP.send_error ~eol:broker.b_frame_eol conn.conn_och
      "Invalid or missing destination: must be of the form /queue/xxx or /topic/xxx."

let cmd_unsubscribe broker conn frame =
  try
    match STOMP.get_destination frame with
        Topic topic -> remove_topic_subscription broker topic conn; return ()
      | Queue queue -> remove_queue_subscription broker queue conn; return ()
  with Not_found ->
    STOMP.send_error ~eol:broker.b_frame_eol conn.conn_och
      "Invalid or missing destination: must be of the form /queue/xxx or /topic/xxx."

let cmd_disconnect broker conn frame =
  Lwt_io.abort conn.conn_och >>
  fail End_of_file

let cmd_send broker conn frame =
  try_lwt
    let destination = STOMP.get_destination frame in
    let msg =
      {
        msg_id = String.concat "-" ["conn"; string_of_int conn.conn_id; new_msg_id ()];
        msg_destination = destination;
        msg_priority = 0;
        msg_timestamp = Unix.gettimeofday ();
        msg_body = frame.STOMP.fr_body;
        msg_ack_timeout =
          (try
             float_of_string (STOMP.get_header frame "ack-timeout")
           with _ -> 0.)
      }
    in match destination with
        Topic topic -> send_to_topic broker msg
      | Queue queue ->
          let save ?low_priority x =
            P.save_msg ?low_priority broker.b_msg_store x in
          let len = String.length msg.msg_body in
            if broker.b_async_maxmem - len <= broker.b_async_usedmem ||
              (not broker.b_force_async &&
                List.mem_assoc "receipt" frame.STOMP.fr_headers)
            then begin
              lwt () = save msg in
                ignore_result (send_to_queue broker) msg;
                return ()
            end else begin
              broker.b_async_usedmem <- broker.b_async_usedmem + len;
              ignore_result
                (fun x ->
                   try_lwt
                     save ~low_priority:true x >> send_to_queue broker x
                   finally
                     broker.b_async_usedmem <- broker.b_async_usedmem - len;
                     return ())
                msg;
              return ()
            end
  with Not_found ->
    STOMP.send_error ~eol:broker.b_frame_eol conn.conn_och
      "Invalid or missing destination: must be of the form /queue/xxx or /topic/xxx."

let cmd_ack broker conn frame =
  try_lwt
    let msg_id = STOMP.get_header frame "message-id" in
      wakeup (Hashtbl.find conn.conn_pending_acks msg_id) ();
      return ()
  with Not_found -> return ()

let ignore_command broker conn frame = return ()

let command_handlers = Hashtbl.create 13
let register_command (name, f) = Hashtbl.add command_handlers name f

let with_receipt f broker conn frame =
  f broker conn frame >>
  STOMP.handle_receipt ~eol:broker.b_frame_eol conn.conn_och frame

let () =
  List.iter register_command
    [
      "SUBSCRIBE", with_receipt cmd_subscribe;
      "UNSUBSCRIBE", with_receipt cmd_unsubscribe;
      "SEND", with_receipt cmd_send;
      "DISCONNECT", cmd_disconnect;
      "ACK", with_receipt cmd_ack;
      "BEGIN", with_receipt ignore_command;
      "COMMIT", with_receipt ignore_command;
      "ABORT", with_receipt ignore_command;
    ]

let handle_frame broker conn frame =
  try
    Hashtbl.find command_handlers frame.STOMP.fr_command
      broker conn frame
  with Not_found ->
    send_error broker conn "Unknown command %S." frame.STOMP.fr_command

let handle_connection broker conn =
  let rec loop () =
    lwt frame = STOMP.read_stomp_frame ~eol:broker.b_frame_eol conn.conn_ich in
    handle_frame broker conn frame >>
    loop ()
  in loop ()

let connect_error msg ich och =
  Lwt_io.write och msg >> Lwt_io.flush och >> Lwt_io.abort ich

let valid_credentials broker frame =
  try
    let check_value name v =
      if STOMP.get_header frame name <> v then raise Exit
    in Option.may (check_value "login") broker.b_login;
       Option.may (check_value "passcode") broker.b_passcode;
       true
  with Not_found | Exit -> false

let establish_connection broker fd addr =
  let ich = Lwt_io.of_fd Lwt_io.input fd in
  let och = Lwt_io.of_fd Lwt_io.output fd in
  lwt frame = STOMP.read_stomp_frame ~eol:broker.b_frame_eol ich in
    match frame.STOMP.fr_command with
        "CONNECT" when not (valid_credentials broker frame) ->
          connect_error "Invalid credentials." ich och
      | "CONNECT" ->
          let conn =
            {
              conn_id = new_conn_id ();
              conn_ich = ich;
              conn_och = och;
              conn_pending_acks = Hashtbl.create 13;
              conn_queues = Hashtbl.create 13;
              conn_topics = Hashtbl.create 13;
              conn_closed = false;
            }
          in begin
            try_lwt
              STOMP.write_stomp_frame ~eol:broker.b_frame_eol och
                {
                  STOMP.fr_command = "CONNECTED";
                  fr_headers = ["session", string_of_int conn.conn_id];
                  fr_body = "";
                } >>
              handle_connection broker conn
            with
              | Lwt_io.Channel_closed _ | End_of_file | Sys_error _ | Unix.Unix_error _ ->
                  (* give it time to process the last few acks *)
                  Lwt_unix.sleep 2. >> terminate_connection broker conn
              | e ->
                  if broker.b_debug then begin
                      eprintf "GOT EXCEPTION for conn %d: %s\n%!"
                        conn.conn_id (Printexc.to_string e);
                      eprintf "backtrace:\n%s" (Printexc.get_backtrace ());
                      Printexc.print_backtrace stderr
                  end;
                  Lwt_io.abort och >> terminate_connection broker conn
          end
      | _ -> connect_error "ERROR\n\nExpected CONNECT frame.\000\n" ich och

let make_broker
      ?(frame_eol = true) ?(force_send_async = false)
      ?(send_async_max_mem = 32 * 1024 * 1024)
      ?login ?passcode
      msg_store address =
  let sock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
  Lwt_unix.bind sock address;
  Lwt_unix.listen sock 1024;
  return {
    b_connections = CONNS.empty;
    b_queues = Hashtbl.create 13;
    b_topics = Hashtbl.create 13;
    b_socket = sock;
    b_frame_eol = frame_eol;
    b_msg_store = msg_store;
    b_force_async = force_send_async;
    b_async_maxmem = send_async_max_mem;
    b_async_usedmem = 0;
    b_debug = false;
    b_login = login;
    b_passcode = passcode;
  }

let server_loop ?(debug = false) broker =
  let broker = { broker with b_debug = debug } in
  let rec loop () =
    lwt (fd, addr) = Lwt_unix.accept broker.b_socket in
      ignore_result (establish_connection broker fd) addr;
      loop ()
  in
    P.crash_recovery broker.b_msg_store >> loop ()
end (* Make functor *)
