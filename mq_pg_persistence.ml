open Printf
open Mq_types
open Lwt

INCLUDE "mq_schema.ml"

module PGOCaml = PGOCaml_generic.Make(struct include Lwt include Lwt_chan end)

type t = { dbh : PGOCaml.pa_pg_data PGOCaml.t; debug : bool }

let connect
      ?host ?port ?unix_domain_socket_dir ?database ?user ?password
      ?(debug = false) () =
  lwt dbh = PGOCaml.connect ?host ?port ?database ?unix_domain_socket_dir
              ?user ?password ()
  in return { dbh = dbh; debug = debug }

let msg_of_tuple (msg_id, dst, timestamp, priority, ack_timeout, body) =
  { 
    msg_id = msg_id;
    msg_destination = Queue dst;
    msg_priority = Int32.to_int priority;
    msg_timestamp = CalendarLib.Calendar.to_unixfloat timestamp;
    msg_ack_timeout = ack_timeout;
    msg_body = body;
  }

let save_msg t msg = match msg.msg_destination with
    Topic _ -> return ()
  | Queue queue ->
      let body = msg.msg_body in
      let time = CalendarLib.Calendar.from_unixfloat msg.msg_timestamp in
      let msg_id = msg.msg_id in
      let priority = Int32.of_int msg.msg_priority in
      let ack_timeout = msg.msg_ack_timeout in
        if t.debug then eprintf "Saving message %S.\n%!" msg_id;
        PGSQL(t.dbh)
          "INSERT INTO mq_server_msgs(msg_id, priority, destination, timestamp,
                                      ack_timeout, body)
                  VALUES ($msg_id, $priority, $queue, $time, $ack_timeout, $body)"

let get_ack_pending_msg t msg_id =
  PGSQL(t.dbh)
    "SELECT priority, destination, timestamp, ack_timeout, body
       FROM mq_server_ack_msgs
      WHERE msg_id = $msg_id"
  >>= function
    | (priority, destination, timestamp, ack_timeout, body) :: _ ->
        return
          (Some { msg_id = msg_id;
                  msg_priority = Int32.to_int priority;
                  msg_destination = Queue destination;
                  msg_timestamp = CalendarLib.Calendar.to_unixfloat timestamp;
                  msg_ack_timeout = ack_timeout;
                  msg_body = body })
    | [] -> return None

let register_ack_pending_new_msg t msg =
  print_endline "register_ack_pending_new_msg";
  let dbh = t.dbh in
  let body = msg.msg_body in
  let time = CalendarLib.Calendar.from_unixfloat msg.msg_timestamp in
  let msg_id = msg.msg_id in
  let priority = Int32.of_int msg.msg_priority in
  let ack_timeout = msg.msg_ack_timeout in
  let queue = destination_name msg.msg_destination in
  if t.debug then eprintf "Saving non-ACKed message %S.\n%!" msg_id;
  begin
    try_lwt
      print_endline "SAVING";
      PGSQL(dbh)
        "INSERT INTO mq_server_ack_msgs(msg_id, priority, destination, timestamp,
                                    ack_timeout, body)
                VALUES ($msg_id, $priority, $queue, $time, $ack_timeout, $body)" >>
      (print_endline "SAVED"; return ())
    with PGOCaml.PostgreSQL_Error (s, _) -> 
      print_endline "GOT PostgreSQL_Error";
      print_endline s;
      return ()
    (* msg_id is not unique: happens if we had an ACK timeout and
     * requeued the message *)
    | e ->
        print_endline "GOT EXCEPTION";
        print_endline (Printexc.to_string e);
        print_endline (Printexc.get_backtrace ());
        return ()
  end >>
  PGSQL(dbh) "DELETE FROM mq_server_msgs WHERE msg_id = $msg_id"

let transaction t f = f t.dbh
  (* PGOCaml.begin_work t.dbh >> *)
  (* try_lwt *)
    (* f t.dbh >> (print_endline "finished"; PGOCaml.commit t.dbh) *)
  (* with e -> PGOCaml.rollback t.dbh >> fail e *)

let register_ack_pending_msg t msg_id =
  print_endline "register_ack_pending_msg";
  transaction t
    (fun dbh ->
       PGSQL(dbh)
         "INSERT INTO
           mq_server_ack_msgs(msg_id, priority, destination, timestamp, ack_timeout, body)
           (SELECT msg_id, priority, destination, timestamp, ack_timeout, body
            FROM mq_server_msgs
            WHERE msg_id = $msg_id)" >>
       PGSQL(dbh) "DELETE FROM mq_server_msgs WHERE msg_id = $msg_id")

let ack_msg t msg_id =
  PGSQL(t.dbh) "DELETE FROM mq_server_ack_msgs WHERE msg_id = $msg_id"

let unack_msg t msg_id =
  transaction t
    (fun dbh ->
       PGSQL(dbh)
         "INSERT INTO
           mq_server_msgs(msg_id, priority, destination, timestamp, ack_timeout, body)
           (SELECT msg_id, priority, destination, timestamp, ack_timeout, body
            FROM mq_server_ack_msgs
            WHERE msg_id = $msg_id)" >>
       PGSQL(dbh) "DELETE FROM mq_server_ack_msgs WHERE msg_id = $msg_id")

let get_queue_msgs t queue maxmsgs =
  let maxmsgs = Int64.of_int maxmsgs in
  lwt msgs =
    PGSQL(t.dbh)
        "SELECT msg_id, destination, timestamp, priority, ack_timeout, body
           FROM mq_server_msgs
          WHERE destination = $queue
       ORDER BY priority, timestamp
          LIMIT $maxmsgs"
  in return (List.map msg_of_tuple msgs)

let crash_recovery t =
  let dbh = t.dbh in
  if t.debug then eprintf "Recovering from crash...\n%!";
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
