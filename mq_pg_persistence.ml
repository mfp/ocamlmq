open Printf
open Mq_types
open Lwt

module PGOCaml = struct
  include PGOCaml_generic.Make(struct include Lwt include Lwt_chan end)

  let buf = Buffer.create 64000

  let string_of_bytea b =
    Buffer.clear buf;
    let len = String.length b in
    for i = 0 to len - 1 do
      let c = String.unsafe_get b i in
      let cc = Char.code c in
      if cc < 0x20 || cc > 0x7e then begin
        Buffer.add_char buf '\\';
        Buffer.add_char buf (Char.unsafe_chr (48 + ((cc lsr 6) land 0x7))); (* '0' + higher bits *)
        Buffer.add_char buf (Char.unsafe_chr (48 + ((cc lsr 3) land 0x7)));
        Buffer.add_char buf (Char.unsafe_chr (48 + (cc land 0x7)));
        (* Buffer.add_string buf (sprintf "\\%03o" cc) [> non-print -> \ooo <] *)
      end else if c = '\\' then
        Buffer.add_string buf "\\\\" (* \ -> \\ *)
      else
        Buffer.add_char buf c
    done;
    Buffer.contents buf
end

INCLUDE "mq_schema.ml"

type t = {
  dbconns : PGOCaml.pa_pg_data PGOCaml.t Lwt_pool.t;
  debug : bool
}

let initialize t = Lwt_pool.use t.dbconns create_db

let connect
      ?host ?port ?unix_domain_socket_dir ?database ?user ?password
      ?(debug = false) ?(max_conns = 1) () =
  let create_conn () = PGOCaml.connect ?host ?port ?database ?unix_domain_socket_dir
                         ?user ?password () in
  let pool = Lwt_pool.create max_conns create_conn in
    (* try to connect so we raise the exception as early as possible if
     * something's wrong *)
  Lwt_pool.use pool (fun _ -> return ()) >>
  return { dbconns = pool; debug = debug }

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
DEFINE WithDB_trans(x) =
  with_db t
    (fun dbh ->
       PGOCaml.begin_work dbh >>
       try_lwt
         lwt y = x in
         PGOCaml.commit dbh >> return y
       with e -> PGOCaml.rollback dbh >> fail e)

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
  WithDB_trans begin
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

let register_ack_pending_msg t msg_id =
  WithDB_trans begin
    lwt count =
      PGSQL(dbh) "SELECT COUNT(*) FROM mq_server_msgs WHERE msg_id = $msg_id" in
    PGSQL(dbh)
      "INSERT INTO
        mq_server_ack_msgs(msg_id, priority, destination, timestamp, ack_timeout, body)
        (SELECT msg_id, priority, destination, timestamp, ack_timeout, body
         FROM mq_server_msgs
         WHERE msg_id = $msg_id)" >>
    PGSQL(dbh) "DELETE FROM mq_server_msgs WHERE msg_id = $msg_id" >>
   (* if count was 0 / undefined, we didn't actually instead it in the ACK set *)
    match count with
        Some c :: _ -> return (c = 1L)
      | _ -> return false
  end

let get_msg_for_delivery t queue =
  WithDB_trans begin
    lwt tuples =
      PGSQL(dbh)
        "SELECT msg_id, destination, timestamp, priority, ack_timeout, body
           FROM mq_server_msgs
          WHERE destination = $queue
       ORDER BY priority, timestamp
          LIMIT 1"
    in match tuples with
        (id, dest, time, prio, timeout, body) as tuple :: _ ->
          let msg = msg_of_tuple tuple in
            PGSQL(dbh)
              "INSERT INTO mq_server_ack_msgs(msg_id, destination, timestamp,
                                              priority, ack_timeout, body)
               VALUES($id, $dest, $time, $prio, $timeout, $body)" >>
            PGSQL(dbh) "DELETE FROM mq_server_msgs WHERE msg_id = $id" >>
            return (Some msg)
      | [] -> return None
  end

let ack_msg t msg_id =
  WithDB(PGSQL(dbh) "DELETE FROM mq_server_ack_msgs WHERE msg_id = $msg_id")

let unack_msg t msg_id =
  WithDB_trans begin
    PGSQL(dbh)
      "INSERT INTO
        mq_server_msgs(msg_id, priority, destination, timestamp, ack_timeout, body)
        (SELECT msg_id, priority, destination, timestamp, ack_timeout, body
         FROM mq_server_ack_msgs
         WHERE msg_id = $msg_id)" >>
    PGSQL(dbh) "DELETE FROM mq_server_ack_msgs WHERE msg_id = $msg_id"
  end

let count_queue_msgs t queue =
  WithDB(PGSQL(dbh) "SELECT COUNT(*) FROM mq_server_msgs WHERE destination = $queue")
    >>= function
        Some count :: _ -> return count
      | _ -> return 0L

let crash_recovery t =
  WithDB_trans begin
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
