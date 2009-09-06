open Printf
open Lwt
open ExtString

module ACKS = Set.Make(struct
                         type t = string * unit Lwt.t * unit Lwt.u
                         let compare (s1, _, _) (s2, _, _) = String.compare s1 s2
                       end)

type subscription = {
  qs_name : string;
  qs_prefetch : int;
  mutable qs_pending_acks : ACKS.t;
}

let dummy_subscription =
  { qs_name = ""; qs_prefetch = 0; qs_pending_acks = ACKS.empty }

type connection = {
  conn_id : int;
  conn_ich : Lwt_io.input_channel;
  conn_och : Lwt_io.output_channel;
  conn_default_prefetch : int;
  mutable conn_pending_acks : int;
  conn_queues : (string, subscription) Hashtbl.t;
  conn_topics : (string, subscription) Hashtbl.t;
}

module CONNS = Set.Make(struct
                          type t = connection
                          let compare t1 t2 = t2.conn_id - t1.conn_id
                        end)

module SUBS = ExtSet.Make(struct
                            type t = (connection * subscription)
                            let compare (t1, _) (t2, _) = t2.conn_id - t1.conn_id
                          end)

INCLUDE "mq_schema.ml"

module PGOCaml = PGOCaml_generic.Make(struct include Lwt include Lwt_chan end)

type listeners = {
  mutable l_ready : SUBS.t;
  mutable l_blocked : SUBS.t;
  mutable l_last_sent : SUBS.elt option;
}

type 'a broker = {
  mutable b_connections : CONNS.t;
  b_queues : (string, listeners) Hashtbl.t;
  b_topics : (string, CONNS.t ref) Hashtbl.t;
  b_socket : Lwt_unix.file_descr;
  b_frame_eol : bool;
  b_dbh : 'a PGOCaml.t;
}

type destination = Queue of string | Topic of string

type message = {
  msg_id : string;
  msg_destination : destination;
  msg_priority : int;
  msg_timestamp : float;
  msg_body : string;
  msg_ack_timeout : float;
}

type stomp_frame = {
  fr_command : string;
  fr_headers : (string * string) list;
  fr_body : string;
}

let string_of_destination = function
    Topic n -> "/topic/" ^ n
  | Queue n -> "/queue/" ^ n

let destination_name = function Topic n | Queue n -> n

let write_stomp_frame ~eol och frame =
  let b = Buffer.create
            (80 * List.length frame.fr_headers + String.length frame.fr_body)
  in
    bprintf b "%s\n" frame.fr_command;
    List.iter
      (fun (k, v) -> if k <> "content-length" then bprintf b "%s: %s\n" k v)
      frame.fr_headers;
    bprintf b "content-length: %d\n" (String.length frame.fr_body);
    bprintf b "\n";
    Buffer.add_string b frame.fr_body;
    if eol then
      Buffer.add_string b "\000\n"
    else
      Buffer.add_string b "\000";
    Lwt_io.write och (Buffer.contents b) >> Lwt_io.flush och

let handle_receipt ~eol och frame =
  try
    let receipt = List.assoc "receipt" frame.fr_headers in
      write_stomp_frame ~eol och
        { fr_command = "RECEIPT";
          fr_headers = ["receipt-id", receipt];
          fr_body = "" }
  with Not_found -> return ()

let send_message broker msg conn =
  write_stomp_frame ~eol:broker.b_frame_eol conn.conn_och
    {
      fr_command = "MESSAGE";
      fr_headers = [
        "message-id", msg.msg_id;
        "destination", string_of_destination msg.msg_destination;
        "content-length", string_of_int (String.length msg.msg_body);
      ];
      fr_body = msg.msg_body
    }

let send_to_topic broker name msg =
  try
    let s = Hashtbl.find broker.b_topics name in
      Lwt.ignore_result
        (Lwt_util.iter (send_message broker msg) (CONNS.elements !s));
      return ()
  with Not_found -> return ()

let save_message broker queue msg =
  let body = msg.msg_body in
  let t = CalendarLib.Calendar.from_unixfloat msg.msg_timestamp in
  let msg_id = msg.msg_id in
  let priority = Int32.of_int msg.msg_priority in
  let ack_timeout = msg.msg_ack_timeout in
    (* eprintf "Saving message %S.\n%!" msg_id; *)
    PGSQL(broker.b_dbh)
      "INSERT INTO mq_server_msgs(msg_id, priority, destination, timestamp,
                                  ack_timeout, body)
              VALUES ($msg_id, $priority, $queue, $t, $ack_timeout, $body)"

let subs_wanted_msgs subs =
  if subs.qs_prefetch <= 0 then max_int
  else subs.qs_prefetch - ACKS.cardinal subs.qs_pending_acks

let is_subs_blocked subs =
  subs.qs_prefetch >= 0 && ACKS.cardinal subs.qs_pending_acks >= subs.qs_prefetch

let select_blocked_subs s = SUBS.filter (fun (_, x) -> is_subs_blocked x) s

let select_unblocked_subs s = SUBS.filter (fun (_, x) -> not (is_subs_blocked x)) s

let block_subscription listeners ((conn, subs) as c) =
  listeners.l_ready <- SUBS.remove c listeners.l_ready;
  listeners.l_blocked <- SUBS.add c listeners.l_blocked

let unblock_some_listeners listeners =
  let unblocked = select_unblocked_subs listeners.l_blocked in
    listeners.l_ready <- SUBS.union listeners.l_ready unblocked;
    listeners.l_blocked <- SUBS.diff listeners.l_blocked unblocked

let enqueue_non_acked_msg broker msg =
  let dbh = broker.b_dbh in
  let id = msg.msg_id in
  (* eprintf "enqueueing non-ACKed msg %S\n%!" id; *)
  begin try_lwt
    PGSQL(dbh)
      "INSERT INTO mq_server_ack_msgs
         (SELECT * FROM mq_server_msgs WHERE msg_id = $id)"
  with PGOCaml.PostgreSQL_Error _ -> return ()
    (* msg_id is not unique: happens if we had an ACK timeout and
     * requeued the message *)
  end >>
  PGSQL(dbh) "DELETE FROM mq_server_msgs WHERE msg_id = $id"

let find_recipient broker name =
  try
    let ls = Hashtbl.find broker.b_queues name in
      match ls.l_last_sent with
          None -> (* first msg sent, there can be no blocked client *)
            Some (ls, SUBS.min_elt ls.l_ready)
        | Some cursor ->
            if SUBS.is_empty ls.l_ready then unblock_some_listeners ls;
            match SUBS.next cursor ls.l_ready with
              | c when c = SUBS.min_elt ls.l_ready ->
                  (* went through all ready subscriptions, try to unblock some &
                   * give it another try *)
                  unblock_some_listeners ls;
                  Some (ls, SUBS.next cursor ls.l_ready)
              | c -> Some (ls, c)
  with Not_found -> None

let send_to_recipient broker listeners conn subs msg =
  let sleep, wakeup = Lwt.task () in
  let msg_id = msg.msg_id in
    subs.qs_pending_acks <- ACKS.add (msg.msg_id, sleep, wakeup)
                                     subs.qs_pending_acks;
    listeners.l_last_sent <- Some (conn, subs);
    if is_subs_blocked subs then block_subscription listeners (conn, subs);
    send_message broker msg conn >>
    enqueue_non_acked_msg broker msg >>
    Lwt.select [ Lwt_unix.timeout msg.msg_ack_timeout; sleep; ] >>
    PGSQL(broker.b_dbh)
      "DELETE FROM mq_server_ack_msgs WHERE msg_id = $msg_id"

let rec send_to_queue broker name msg = match find_recipient broker name with
    None -> save_message broker name msg
  | Some (listeners, (conn, subs)) ->
      try_lwt
        send_to_recipient broker listeners conn subs msg
        (* with Lwt_unix.Timeout | Lwt.Canceled -> *)
      with _ ->
        (* eprintf "TIMEOUT or CANCELED\n%!"; *)
        (* didn't get ACK within msg_ack_timeout, enqueue msg again *)
        (* We don't delete from the mq_server_ack_msgs tbl since a crash
         * before calling send_to_queue would mean the msg is LOST! *)
        send_to_queue broker name msg

let send_saved_messages broker listeners (conn, subs) =
  let num_msgs = Int64.of_int (subs_wanted_msgs subs) in
  let dest = subs.qs_name in
  lwt msgs = PGSQL(broker.b_dbh)
      "SELECT msg_id, destination, timestamp, priority, ack_timeout, body FROM mq_server_msgs
        WHERE destination = $dest ORDER BY priority, timestamp LIMIT $num_msgs" in
  let new_pending =
    List.fold_left
      (fun s (id, _, _, _, _, _) ->
         let sleep, wakeup = Lwt.task () in
           ACKS.add (id, sleep, wakeup) s)
      subs.qs_pending_acks
      msgs
  in
    subs.qs_pending_acks <- new_pending;
    if is_subs_blocked subs then block_subscription listeners (conn, subs);
    Lwt_util.iter_serial
      (fun (msg_id, dst, timestamp, priority, ack_timeout, body) ->
         let msg =
           { msg_id = msg_id;
             msg_destination = Queue dst;
             msg_priority = Int32.to_int priority;
             msg_timestamp = CalendarLib.Calendar.to_unixfloat timestamp;
             msg_ack_timeout = ack_timeout;
             msg_body = body;
           }
         in send_message broker msg conn >>
            enqueue_non_acked_msg broker msg)
      msgs

let send_message broker msg = match msg.msg_destination with
    Queue name -> send_to_queue broker name msg
  | Topic name -> send_to_topic broker name msg

let new_id prefix =
  let cnt = ref 0 in
    fun () ->
      incr cnt;
      sprintf "%s-%f-%d" prefix (Unix.gettimeofday ()) !cnt

let new_msg_id = new_id "msg"

let new_conn_id = let n = ref 0 in fun () -> incr n; !n

let topic_re = Str.regexp "/topic/"
let queue_re = Str.regexp "/queue/"

let send_error ~eol conn fmt =
  ksprintf
    (fun msg ->
       write_stomp_frame ~eol conn.conn_och
         { fr_command = "ERROR"; fr_headers = []; fr_body = msg; })
    fmt

let get_destination frame =
  let destination = List.assoc "destination" frame.fr_headers in
    if Str.string_match topic_re destination 0 then
      Topic (String.slice ~first:7 destination)
    else if Str.string_match queue_re destination 0 then
      Queue (String.slice ~first:7 destination)
    else raise Not_found

let cmd_subscribe broker conn frame =
  try
    let destination = get_destination frame in
    let subscription =
      {
        qs_name = (match destination with Topic n | Queue n -> n);
        qs_prefetch =
          (try
             int_of_string (List.assoc "prefetch" frame.fr_headers)
           with _ -> -1);
        qs_pending_acks = ACKS.empty;
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
          in
            send_saved_messages broker listeners (conn, subscription)
        end
  with Not_found ->
    send_error ~eol:broker.b_frame_eol conn
      "Invalid or missing destination: must be of the form /queue/xxx or /topic/xxx."

let cmd_unsubscribe broker conn frame =
  try
    match get_destination frame with
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
    send_error ~eol:broker.b_frame_eol conn
      "Invalid or missing destination: must be of the form /queue/xxx or /topic/xxx."

let cmd_disconnect broker conn frame =
  Lwt_io.abort conn.conn_och >>
  Lwt_io.abort conn.conn_ich >>
  fail End_of_file

let cmd_send broker conn frame =
  try
    send_message broker
      {
        msg_id = sprintf "conn-%d:%s" conn.conn_id (new_msg_id ());
        msg_destination = get_destination frame;
        msg_priority = 0;
        msg_timestamp = Unix.gettimeofday ();
        msg_body = frame.fr_body;
        msg_ack_timeout =
          (try
             float_of_string (List.assoc "ack-timeout" frame.fr_headers)
           with _ -> 0.)
      }
  with Not_found ->
    send_error ~eol:broker.b_frame_eol conn
      "Invalid or missing destination: must be of the form /queue/xxx or /topic/xxx."

let ignore_command broker conn frame = return ()

let command_handlers = Hashtbl.create 13
let register_command (name, f) =
  Hashtbl.add command_handlers (String.uppercase name) f

let with_receipt f broker conn frame =
  f broker conn frame >>
  handle_receipt ~eol:broker.b_frame_eol conn.conn_och frame

let () =
  List.iter register_command
    [
      "SUBSCRIBE", with_receipt cmd_subscribe;
      "UNSUBSCRIBE", with_receipt cmd_unsubscribe;
      "SEND", with_receipt cmd_send;
      "DISCONNECT", cmd_disconnect;
      "BEGIN", with_receipt ignore_command;
      "COMMIT", with_receipt ignore_command;
      "ABORT", with_receipt ignore_command;
    ]

let read_stomp_headers ch =
  let rec loop acc =
    Lwt_io.read_line ch >>= function
        "" -> return acc
      | s ->
          let k, v = String.split s ":" in
            loop ((String.lowercase k, String.strip v) :: acc)
  in loop []

let rec read_stomp_command ch =
  Lwt_io.read_line ch >>= function
      "" -> read_stomp_command ch
    | l -> return l

let read_until_zero ?(eol = true) ich =
  let b = Buffer.create 80 in
    if eol then begin
      let rec loop () =
        Lwt_io.read_line ich >>= function
            "" -> Buffer.add_char b '\n'; loop ()
          | l when l.[String.length l - 1] = '\000' ->
              Buffer.add_substring b l 0 (String.length l - 1);
              return (Buffer.contents b)
          | l -> Buffer.add_string b l;
                 Buffer.add_char b '\n';
                 loop ()
      in loop ()
    end else begin
      let rec loop () =
        Lwt_io.read_char ich >>= function
            '\000' -> return (Buffer.contents b)
          | c -> Buffer.add_char b c; loop ()
      in loop ()
    end

let read_stomp_frame ?(eol = true) ich =
  try_lwt
    lwt cmd = read_stomp_command ich in
    lwt headers = read_stomp_headers ich in
    lwt body =
      try
        let len = int_of_string (List.assoc "content-length" headers) in
        let body = String.create len in
        lwt () = Lwt_io.read_into_exactly ich body 0 len in
          lwt (_ : char) = Lwt_io.read_char ich in
            (* FIXME: check that it's \0 ? *)
            return body
      with Not_found -> read_until_zero ~eol ich
    in return { fr_command = cmd; fr_headers = headers; fr_body = body }


let handle_frame broker conn frame =
  try
    Hashtbl.find command_handlers (String.uppercase frame.fr_command)
      broker conn frame
  with Not_found ->
    send_error ~eol:broker.b_frame_eol conn "Unknown command %S." frame.fr_command

let handle_connection broker conn =
  let rec loop () =
    lwt frame = read_stomp_frame ~eol:broker.b_frame_eol conn.conn_ich in
    handle_frame broker conn frame >>
    loop ()
  in loop ()

let establish_connection broker fd addr =
  let ich = Lwt_io.of_fd Lwt_io.input fd in
  let och = Lwt_io.of_fd Lwt_io.output fd in
  lwt frame = read_stomp_frame ~eol:broker.b_frame_eol ich in
    match String.uppercase frame.fr_command with
        "CONNECT" ->
            (* TODO: prefetch *)
          let conn =
            {
              conn_id = new_conn_id ();
              conn_ich = ich;
              conn_och = och;
              conn_default_prefetch = -1;
              conn_pending_acks = 0;
              conn_queues = Hashtbl.create 13;
              conn_topics = Hashtbl.create 13;
            }
          in begin
            try_lwt
              write_stomp_frame ~eol:broker.b_frame_eol och
                {
                  fr_command = "CONNECTED";
                  fr_headers = ["session", string_of_int conn.conn_id];
                  fr_body = "";
                } >>
              handle_connection broker conn
            with End_of_file | Unix.Unix_error _ ->
                let pending_acks =
                  ACKS.elements
                    (Hashtbl.fold
                       (fun _ subs s -> ACKS.union subs.qs_pending_acks s)
                       conn.conn_queues
                       ACKS.empty) in
                (* eprintf "CONNECTION %d DONE with %d pending ACKS\n%!" *)
                  (* conn.conn_id *)
                  (* (List.length pending_acks); *)
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
                List.iter
                  (fun (id, sleep, w) ->
                     (* eprintf "have to re-queue %S\n%!" id; *)
                     wakeup w ())
                     (* wakeup_exn wakeup Lwt.Canceled) *)
                  pending_acks;
                return ()
             | e ->
                 (* eprintf "GOT EXCEPTION: %s" (Printexc.to_string e); *)
                 Lwt_io.abort och
          end
      | _ -> Lwt_io.write och "ERROR\n\nExcepted CONNECT frame.\000\n" >>
             Lwt_io.flush och >>
             Lwt_io.abort ich

let recover_unacked_messages dbh =
  eprintf "Recovering from crash...\n%!";
  PGOCaml.begin_work dbh >>
  try_lwt
    lwt n = PGSQL(dbh) "SELECT COUNT(*) FROM mq_server_ack_msgs" in
    PGSQL(dbh) "INSERT INTO mq_server_msgs (SELECT * from mq_server_ack_msgs)" >>
    PGSQL(dbh) "DELETE FROM mq_server_ack_msgs" >>
    ((match n with
         Some nmsgs :: _ -> eprintf "Recovered %Ld messages.\n%!" nmsgs;
       | _ -> eprintf "No messages found.\n%!");
    PGOCaml.commit dbh)
  with _ ->
    eprintf "Couldn't recover messages.\n%!";
    PGOCaml.rollback dbh >>
    fail (Failure "Couldn't recover non-ACKed messages from earlier crash.")

let make_broker ?(frame_eol = true) dbh address =
  let sock = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
  Lwt_unix.bind sock address;
  Lwt_unix.listen sock 1024;
  {
    b_connections = CONNS.empty;
    b_queues = Hashtbl.create 13;
    b_topics = Hashtbl.create 13;
    b_socket = sock;
    b_frame_eol = frame_eol;
    b_dbh = dbh;
  }

let serve_broker broker =
  let rec loop () =
    lwt (fd, addr) = Lwt_unix.accept broker.b_socket in
      ignore_result (establish_connection broker fd addr);
      loop ()
  in
    recover_unacked_messages broker.b_dbh >> loop ()

let set_some_string r = Arg.String (fun s -> r := Some s)
let set_some_int r = Arg.Int (fun n -> r := Some n)

let db_host = ref None
let db_port = ref None
let db_database = ref None
let db_user = ref None
let db_password = ref None
let db_unix_sock_dir = ref None
let port = ref 44444

let params =
  Arg.align
    [
      "-dbhost", set_some_string db_host, "HOST Database server host.";
      "-dbport", set_some_int db_port, "HOST Database server port.";
      "-dbdatabase", set_some_string db_database, "DATABASE Database name.";
      "-dbsockdir", set_some_string db_password, "DIR Database UNIX domain socket dir.";
      "-dbuser", set_some_string db_user, "USER Database user.";
      "-dbpassword", set_some_string db_password, "PASSWORD Database password.";
      "-port", Arg.Set_int port, "PORT Port to listen at.";
    ]

let usage_message = "Usage: mq_server [options]"

let _ = Sys.set_signal Sys.sigpipe Sys.Signal_ignore

let () =
  Arg.parse
    params
    (fun s -> eprintf "Unknown argument: %S\n%!" s;
              Arg.usage params usage_message;
              exit 1)
    usage_message;
  let addr = Unix.ADDR_INET (Unix.inet_addr_any, !port) in
    Lwt_unix.run begin
      lwt dbh = PGOCaml.connect
                  ?host:!db_host
                  ?port:!db_port
                  ?database:!db_database
                  ?unix_domain_socket_dir:!db_unix_sock_dir
                  ?user:!db_user
                  ?password:!db_password
                  ()
      in serve_broker (make_broker dbh addr)
    end

