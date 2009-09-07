open Printf
open Mq_types
open Lwt

INCLUDE "mq_schema.ml"

module PGOCaml = PGOCaml_generic.Make(struct include Lwt include Lwt_chan end)

type t = {
  dbconns : PGOCaml.pa_pg_data PGOCaml.t Lwt_pool.t;
  debug : bool
}

let connect
      ?host ?port ?unix_domain_socket_dir ?database ?user ?password
      ?(debug = false) ?(max_conns = 1) () =
  let create_conn () = PGOCaml.connect ?host ?port ?database ?unix_domain_socket_dir
                         ?user ?password () in
  return { dbconns = Lwt_pool.create max_conns create_conn; debug = debug }

let msg_of_tuple (msg_id, dst, timestamp, priority, ack_timeout, body) =
  {
    msg_id = msg_id;
    msg_destination = Queue dst;
    msg_priority = Int32.to_int priority;
    msg_timestamp = CalendarLib.Calendar.to_unixfloat timestamp;
    msg_ack_timeout = ack_timeout;
    msg_body = body;
  }

let with_db t f = Lwt_pool.use t.dbconns f

DEFINE WithDB(x) = with_db t (fun dbh -> x)

let save_msg t msg = match msg.msg_destination with
    Topic _ -> return ()
  | Queue queue ->
      let body = msg.msg_body in
      let time = CalendarLib.Calendar.from_unixfloat msg.msg_timestamp in
      let msg_id = msg.msg_id in
      let priority = Int32.of_int msg.msg_priority in
      let ack_timeout = msg.msg_ack_timeout in
        if t.debug then eprintf "Saving message %S.\n%!" msg_id;
        WithDB begin
          PGSQL(dbh)
           "INSERT INTO mq_server_msgs(msg_id, priority, destination,
                                       timestamp, ack_timeout, body)
                   VALUES ($msg_id, $priority, $queue, $time, $ack_timeout, $body)"
        end

let get_ack_pending_msg t msg_id =
  WithDB begin
    PGSQL(dbh)
       "SELECT priority, destination, timestamp, ack_timeout, body
          FROM mq_server_ack_msgs
         WHERE msg_id = $msg_id"
  end
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
  let body = msg.msg_body in
  let time = CalendarLib.Calendar.from_unixfloat msg.msg_timestamp in
  let msg_id = msg.msg_id in
  let priority = Int32.of_int msg.msg_priority in
  let ack_timeout = msg.msg_ack_timeout in
  let queue = destination_name msg.msg_destination in
  if t.debug then eprintf "Saving non-ACKed message %S.\n%!" msg_id;
  WithDB begin
    begin
      try_lwt
        PGSQL(dbh)
          "INSERT INTO mq_server_ack_msgs(msg_id, priority, destination,
                                          timestamp, ack_timeout, body)
           VALUES ($msg_id, $priority, $queue, $time, $ack_timeout, $body)"
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
  end

let transaction t f =
  try_lwt
    with_db t f
  with e ->
    print_endline "got exception in SQL transaction";
    print_endline (Printexc.to_string e);
    return ()
  (* PGOCaml.begin_work t.dbh >> *)
  (* try_lwt *)
    (* f t.dbh >> (print_endline "finished"; PGOCaml.commit t.dbh) *)
  (* with e -> PGOCaml.rollback t.dbh >> fail e *)

let register_ack_pending_msg t msg_id =
  WithDB begin
    PGSQL(dbh)
      "INSERT INTO
        mq_server_ack_msgs(msg_id, priority, destination, timestamp, ack_timeout, body)
        (SELECT msg_id, priority, destination, timestamp, ack_timeout, body
         FROM mq_server_msgs
         WHERE msg_id = $msg_id)" >>
    PGSQL(dbh) "DELETE FROM mq_server_msgs WHERE msg_id = $msg_id"
  end

let ack_msg t msg_id =
  WithDB(PGSQL(dbh) "DELETE FROM mq_server_ack_msgs WHERE msg_id = $msg_id")

let unack_msg t msg_id =
  WithDB begin
    PGSQL(dbh)
      "INSERT INTO
        mq_server_msgs(msg_id, priority, destination, timestamp, ack_timeout, body)
        (SELECT msg_id, priority, destination, timestamp, ack_timeout, body
         FROM mq_server_ack_msgs
         WHERE msg_id = $msg_id)" >>
    PGSQL(dbh) "DELETE FROM mq_server_ack_msgs WHERE msg_id = $msg_id"
  end

let get_queue_msgs t queue maxmsgs =
  let maxmsgs = Int64.of_int maxmsgs in
  lwt msgs =
    WithDB begin
      PGSQL(dbh)
          "SELECT msg_id, destination, timestamp, priority, ack_timeout, body
             FROM mq_server_msgs
            WHERE destination = $queue
         ORDER BY priority, timestamp
            LIMIT $maxmsgs"
    end
  in return (List.map msg_of_tuple msgs)

let crash_recovery t =
  WithDB begin
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
  end
