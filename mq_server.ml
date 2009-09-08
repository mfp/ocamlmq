open Printf
open Lwt

(** Simple STOMP message queue. *)

(** Message store. *)
module type PERSISTENCE =
sig
  type t

  val initialize : t -> unit Lwt.t

  val save_msg : t -> Mq_types.message -> unit Lwt.t
  val register_ack_pending_new_msg : t -> Mq_types.message -> unit Lwt.t

  (** Returns [false] if the msg was already in the ACK-pending set. *)
  val register_ack_pending_msg : t -> string -> bool Lwt.t
  val get_ack_pending_msg : t -> string -> Mq_types.message option Lwt.t
  val ack_msg : t -> string -> unit Lwt.t
  val unack_msg : t -> string -> unit Lwt.t
  val get_queue_msgs : t -> string -> int -> Mq_types.message list Lwt.t
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
  qs_name : string;
  qs_prefetch : int;
  mutable qs_pending_acks : int;
}

let dummy_subscription = { qs_name = ""; qs_prefetch = 0; qs_pending_acks = 0 }

type 'listeners poly_connection = {
  conn_id : int;
  conn_ich : Lwt_io.input_channel;
  conn_och : Lwt_io.output_channel;
  conn_prefetch : int;
  mutable conn_pending_acks : (string, unit Lwt.u) Hashtbl.t;
  conn_queues : (string, subscription) Hashtbl.t;
  conn_topics : (string, subscription) Hashtbl.t;
  conn_blocked_subs : ('listeners * subscription) Queue.t;
}

(* untying the recursive knot: connection depends on listeners which depends
 * on sets of connections *)
module rec M : sig
  type connection = M.listeners poly_connection
  module SUBS : ExtSet.S with type elt = (connection * subscription)
  module CONNS : Set.S with type elt = connection
  type listeners = {
    mutable l_ready : SUBS.t;
    mutable l_blocked : SUBS.t;
    mutable l_last_sent : SUBS.elt option;
  }
end =
struct
  type connection = M.listeners poly_connection
  module CONNS = Set.Make(struct
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
end

include M

type broker = {
  mutable b_connections : CONNS.t;
  b_queues : (string, listeners) Hashtbl.t;
  b_topics : (string, CONNS.t ref) Hashtbl.t;
  b_socket : Lwt_unix.file_descr;
  b_frame_eol : bool;
  b_msg_store : P.t;
  b_async_send : bool;
  b_debug : bool;
}

let send_error broker conn fmt =
  STOMP.send_error ~eol:broker.b_frame_eol conn.conn_och fmt

let send_to_topic broker msg =
  try
    let s = Hashtbl.find broker.b_topics (destination_name msg.msg_destination) in
      Lwt.ignore_result
        (Lwt_util.iter
           (fun conn -> STOMP.send_message ~eol:broker.b_frame_eol conn.conn_och msg)
           (CONNS.elements !s));
      return ()
  with Not_found -> return ()

let subs_wanted_msgs (conn, subs) =
  let local = subs.qs_prefetch - subs.qs_pending_acks in
  let global = conn.conn_prefetch - Hashtbl.length conn.conn_pending_acks in
    match subs.qs_prefetch > 0, conn.conn_prefetch > 0 with
        false, false -> max_int
      | true, false -> local
      | false, true -> global
      | true, true -> min local global

let is_subs_blocked_locally subs =
  subs.qs_prefetch > 0 && subs.qs_pending_acks >= subs.qs_prefetch

let is_subs_blocked (conn, subs) =
  is_subs_blocked_locally subs ||
  conn.conn_prefetch > 0 && Hashtbl.length conn.conn_pending_acks >= conn.conn_prefetch

let select_blocked_subs s = SUBS.filter is_subs_blocked s

let select_unblocked_subs s = SUBS.filter (fun x -> not (is_subs_blocked x)) s

let block_subscription listeners ((conn, subs) as c) =
  listeners.l_ready <- SUBS.remove c listeners.l_ready;
  listeners.l_blocked <- SUBS.add c listeners.l_blocked;
  (* if the subscription is blocked because of the global prefetch limit,
   * add it to the queue of "globally (per-conn) blocked" subs *)
  if is_subs_blocked_locally subs then
    Queue.add (listeners, subs) conn.conn_blocked_subs

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

let rec send_to_recipient ~kind broker listeners conn subs msg =
  if broker.b_debug then
    eprintf "Sending %S to conn %d.\n%!" msg.msg_id conn.conn_id;
  let sleep, wakeup = Lwt.task () in
  let msg_id = msg.msg_id in
    subs.qs_pending_acks <- subs.qs_pending_acks + 1;
    Hashtbl.replace conn.conn_pending_acks msg_id wakeup;
    listeners.l_last_sent <- Some (conn, subs);
    if is_subs_blocked (conn, subs) then block_subscription listeners (conn, subs);

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
      (* first, try to send messages for subscriptions that were blocked
       * because of the global prefetch limit *)
      let q = Queue.create () in
        (* we get the elements and clear the queue "atomically" (no Lwt thread
         * switches) because other msgs could arrive and cause new
         * subscriptions to be added to conn.conn_blocked_subs while we are
         * trying to send messages to subs that were globally blocked before
         * this ACK *)
        Queue.transfer conn.conn_blocked_subs q;
        Queue.iter
          (fun (listeners, subs) ->
             ignore_result (send_saved_messages ~max_msgs:1 broker listeners conn subs))
          q;
        (* then, try to send older messages for the subscription whose message
         * we just ACKed *)
        send_saved_messages broker listeners conn subs
    end

and send_saved_messages ?(max_msgs = max_int) broker listeners conn subs =

  let do_send msg =
    let msg_id = msg.msg_id in
    if not (is_subs_blocked (conn, subs)) then
      try_lwt
        send_to_recipient ~kind:Saved broker listeners conn subs msg
      with Lwt_unix.Timeout | Lwt.Canceled ->
        if broker.b_debug then
          eprintf "Timeout/Canceled on old message %S.\n%!" msg_id;
        enqueue_after_timeout broker msg_id
    else return () in

  let wanted = min max_msgs (subs_wanted_msgs (conn, subs)) in
  lwt msgs = P.get_queue_msgs broker.b_msg_store subs.qs_name wanted in
    if broker.b_debug then
      eprintf "Sending old messages: wanted %d, got %d.\n%!"
        wanted (List.length msgs);
    Lwt_util.iter do_send msgs

and enqueue_after_timeout broker msg_id =
  P.get_ack_pending_msg broker.b_msg_store msg_id >>= function
      None -> return ()
    | Some msg ->
        let queue = destination_name msg.msg_destination in
        let msg_id = msg.msg_id in
        match find_recipient broker queue with
          | None -> begin (* move to main table *)
              if broker.b_debug then
                eprintf "No recipient for unACKed message %S, saving.\n%!" msg_id;
              P.unack_msg broker.b_msg_store msg_id
            end
          | Some (listeners, (conn, subs)) ->
              eprintf "Found a recipient for unACKed message %S.\n%!" msg_id;
              try_lwt
                send_to_recipient ~kind:Ack_pending broker listeners conn subs msg
              with Lwt_unix.Timeout | Lwt.Canceled ->
                if broker.b_debug then
                  eprintf "Trying to enqueue unACKed message %S again.\n%!" msg_id;
                enqueue_after_timeout broker msg_id

let rec send_to_queue broker msg =
  match find_recipient broker (destination_name msg.msg_destination) with
      None -> return ()
    | Some (listeners, (conn, subs)) ->
        let msg_id = msg.msg_id in
          try_lwt
            send_to_recipient ~kind:Saved broker listeners conn subs msg
          with Lwt_unix.Timeout | Lwt.Canceled ->
            if broker.b_debug then
              eprintf "Timeout/Canceled on new message %S.\n%!" msg_id;
            enqueue_after_timeout broker msg_id

let new_id prefix =
  let cnt = ref 0 in
    fun () ->
      incr cnt;
      sprintf "%s-%f-%d" prefix (Unix.gettimeofday ()) !cnt

let new_msg_id = new_id "msg"

let new_conn_id = let n = ref 0 in fun () -> incr n; !n

let cmd_subscribe broker conn frame =
  try_lwt
    let destination = STOMP.get_destination frame in
    let subscription =
      {
        qs_name = (match destination with Topic n | Queue n -> n);
        qs_prefetch =
          (try
             int_of_string (STOMP.get_header frame "prefetch")
           with _ -> -1);
        qs_pending_acks = 0;
      }
    in match destination with
        Topic name -> begin
          Hashtbl.replace conn.conn_topics name subscription;
          try
            let s = Hashtbl.find broker.b_topics name in
              s := CONNS.add conn !s;
              return ()
          with Not_found ->
            Hashtbl.add broker.b_topics name (ref (CONNS.singleton conn));
            return ()
        end
      | Queue name -> begin
          Hashtbl.replace conn.conn_queues name subscription;
          let listeners =
            try
              let ls = Hashtbl.find broker.b_queues name in
                ls.l_ready <- SUBS.add (conn, subscription) ls.l_ready;
                ls
            with Not_found ->
              let ls = { l_ready = SUBS.singleton (conn, subscription);
                         l_blocked = SUBS.empty;
                         l_last_sent = None }
              in Hashtbl.add broker.b_queues name ls;
                 ls
          in Lwt.ignore_result
               (send_saved_messages broker listeners conn subscription);
             return ()
        end
  with Not_found ->
    STOMP.send_error ~eol:broker.b_frame_eol conn.conn_och
      "Invalid or missing destination: must be of the form /queue/xxx or /topic/xxx."

let cmd_unsubscribe broker conn frame =
  try
    match STOMP.get_destination frame with
        Topic name -> begin
          try
            let s = Hashtbl.find broker.b_topics name in
              s := CONNS.remove conn !s;
              if CONNS.is_empty !s then
                Hashtbl.remove broker.b_topics name;
              return ()
          with Not_found -> return ()
        end
      | Queue name -> begin
          try
            let ls = Hashtbl.find broker.b_queues name in
              ls.l_ready <- SUBS.remove (conn, dummy_subscription) ls.l_ready;
              ls.l_blocked <- SUBS.remove (conn, dummy_subscription) ls.l_blocked;
              if SUBS.is_empty ls.l_ready && SUBS.is_empty ls.l_blocked then
                Hashtbl.remove broker.b_queues name;
              return ()
          with Not_found -> return ()
        end
  with Not_found ->
    STOMP.send_error ~eol:broker.b_frame_eol conn.conn_och
      "Invalid or missing destination: must be of the form /queue/xxx or /topic/xxx."

let cmd_disconnect broker conn frame =
  Lwt_io.abort conn.conn_och >>
  Lwt_io.abort conn.conn_ich >>
  fail End_of_file

let cmd_send broker conn frame =
  try_lwt
    let destination = STOMP.get_destination frame in
    let msg =
      {
        msg_id = sprintf "conn-%d:%s" conn.conn_id (new_msg_id ());
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
          let save = P.save_msg broker.b_msg_store msg in
            if broker.b_async_send then begin
              Lwt.ignore_result (save >> send_to_queue broker msg);
              return ()
            end else begin
              lwt () = save in
                Lwt.ignore_result (send_to_queue broker msg);
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
let register_command (name, f) =
  Hashtbl.add command_handlers (String.uppercase name) f

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
    Hashtbl.find command_handlers (String.uppercase frame.STOMP.fr_command)
      broker conn frame
  with Not_found ->
    send_error broker conn "Unknown command %S." frame.STOMP.fr_command

let handle_connection broker conn =
  let rec loop () =
    lwt frame = STOMP.read_stomp_frame ~eol:broker.b_frame_eol conn.conn_ich in
    handle_frame broker conn frame >>
    loop ()
  in loop ()

let terminate_connection broker conn =
  let wakeners =
    Hashtbl.fold (fun _ u l -> u :: l) conn.conn_pending_acks [] in

  if broker.b_debug then
    eprintf "Connection %d terminated with %d pending ACKs\n%!"
      conn.conn_id (List.length wakeners);

  (* remove from connection set and subscription lists *)
  broker.b_connections <- CONNS.remove conn broker.b_connections;
  Hashtbl.iter
    (fun k _ ->
       try
         let s = Hashtbl.find broker.b_topics k in
           s := CONNS.remove conn !s
       with Not_found -> ())
    conn.conn_topics;
  Hashtbl.iter
    (fun k _ ->
       let rm s = SUBS.remove (conn, dummy_subscription) s in
       try
         let ls = Hashtbl.find broker.b_queues k in
           ls.l_ready <- rm ls.l_ready;
           ls.l_blocked <- rm ls.l_blocked;
       with Not_found -> ())
    conn.conn_queues;
  (* cancel all the waiters: they will re-queue the
   * corresponding messages *)
  List.iter (fun w -> wakeup_exn w Lwt.Canceled) wakeners;
  return ()

let establish_connection broker fd addr =
  let ich = Lwt_io.of_fd Lwt_io.input fd in
  let och = Lwt_io.of_fd Lwt_io.output fd in
  lwt frame = STOMP.read_stomp_frame ~eol:broker.b_frame_eol ich in
    match String.uppercase frame.STOMP.fr_command with
        "CONNECT" ->
          let conn =
            {
              conn_id = new_conn_id ();
              conn_ich = ich;
              conn_och = och;
              conn_prefetch =
                (try int_of_string (STOMP.get_header frame "prefetch")
                 with _ -> -1);
              conn_pending_acks = Hashtbl.create 13;
              conn_queues = Hashtbl.create 13;
              conn_topics = Hashtbl.create 13;
              conn_blocked_subs = Queue.create ();
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
              | End_of_file | Sys_error _ | Unix.Unix_error _ ->
                terminate_connection broker conn
              | e ->
                  if broker.b_debug then begin
                      eprintf "GOT EXCEPTION for conn %d: %s\n%!"
                        conn.conn_id (Printexc.to_string e);
                      eprintf "backtrace:\n%s" (Printexc.get_backtrace ());
                      Printexc.print_backtrace stderr
                  end;
                  Lwt_io.abort och >> terminate_connection broker conn
          end
      | _ -> Lwt_io.write och "ERROR\n\nExcepted CONNECT frame.\000\n" >>
             Lwt_io.flush och >>
             Lwt_io.abort ich

let make_broker ?(frame_eol = true) ?(send_async = false) msg_store address =
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
    b_async_send = send_async;
    b_debug = false;
  }

let server_loop ?(debug = false) broker =
  let broker = { broker with b_debug = debug } in
  let rec loop () =
    lwt (fd, addr) = Lwt_unix.accept broker.b_socket in
      ignore_result
        (try_lwt establish_connection broker fd addr with _ -> return ());
      loop ()
  in
    P.crash_recovery broker.b_msg_store >> loop ()
end (* Make functor *)
