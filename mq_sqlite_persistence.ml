
open Lwt
open Printf
open Mq_types

module Sqlexpr = Sqlexpr_sqlite.Make(Sqlexpr_concurrency.Id)
open Sqlexpr

module MSET = Set.Make(struct
                         type t = int * message
                         let compare ((p1, m1) : t) (p2, m2) =
                           if p2 = p1 then
                             String.compare m1.msg_id m2.msg_id
                           else p2 - p1
                       end)
module SSET = Set.Make(String)

type t = {
  db : Sqlexpr_sqlite.db;
  in_mem : (string, MSET.t * SSET.t) Hashtbl.t;
  in_mem_msgs : (string, message) Hashtbl.t;
  mutable ack_pending : SSET.t;
  mutable flush_alarm : unit Lwt.u;
  max_msgs_in_mem : int;
  mutable unacks : SSET.t;
}

let count_unmaterialized_pending_acks db =
  select_one db sqlc"SELECT @L{COUNT(*)} FROM pending_acks"

let count_acked_messages db =
  select_one db sqlc"SELECT @L{COUNT(*)} FROM acked_msgs"

let flush_acked_msgs ?(verbose = false) db =
  if verbose then
    printf "Flushing %Ld ACKs\n%!" (count_acked_messages db);
  execute db
    sqlc"DELETE FROM ocamlmq_msgs WHERE msg_id IN (SELECT * FROM acked_msgs)";
  execute db sqlc"DELETE FROM acked_msgs"

let materialize_pending_acks ?(verbose = false) db =
  if verbose then
    printf "Materializing %Ld pending ACKs in DB\n%!"
      (count_unmaterialized_pending_acks db);
  execute db sqlc"UPDATE ocamlmq_msgs SET ack_pending = 1
                   WHERE msg_id IN (SELECT msg_id FROM pending_acks)";
  execute db sqlc"DELETE FROM pending_acks"

let flush t =
  let t0 = Unix.gettimeofday () in
  transaction t.db begin fun db ->
    printf "Flushing to disk: %d msgs, %d + %Ld pending ACKS, %Ld ACKS%!"
      (Hashtbl.length t.in_mem_msgs)
      (SSET.cardinal t.ack_pending)
      (count_unmaterialized_pending_acks db)
      (count_acked_messages db);
    Hashtbl.iter
      (fun _ msg ->
         execute db
           sqlc"INSERT INTO ocamlmq_msgs
                 (ack_pending, msg_id, priority, destination, timestamp,
                  ack_timeout, body)
               VALUES(0, %s, %d, %s, %f, %f, %S)"
           msg.msg_id msg.msg_priority (destination_name msg.msg_destination)
           msg.msg_timestamp msg.msg_ack_timeout msg.msg_body)
      t.in_mem_msgs;
    SSET.iter
      (execute db sqlc"INSERT INTO pending_acks(msg_id) VALUES(%s)")
      t.ack_pending;
    flush_acked_msgs db;
    materialize_pending_acks db;
    Hashtbl.clear t.in_mem;
    Hashtbl.clear t.in_mem_msgs;
    t.ack_pending <- SSET.empty;
  end;
  printf " (%8.5fs)\n%!" (Unix.gettimeofday () -. t0)

let msg_materialized_as_ack_pending db msg_id =
  select_one db
    sqlc"SELECT @b{EXISTS (SELECT 1 FROM ocamlmq_msgs
                            WHERE msg_id = %s AND ack_pending)}"
    msg_id

let unack db msg_id =
  execute db sqlc"DELETE FROM pending_acks WHERE msg_id = %s" msg_id;
  if msg_materialized_as_ack_pending db msg_id then
    execute db
      sqlc"UPDATE ocamlmq_msgs SET ack_pending = 0 WHERE msg_id = %s"
      msg_id

let make ?(max_msgs_in_mem = max_int) ?(flush_period = 1.0) file =
  let wait_flush, awaken_flush = Lwt.wait () in
  let t =
    { db = Sqlexpr_sqlite.open_db file; in_mem = Hashtbl.create 13;
      in_mem_msgs = Hashtbl.create 13; ack_pending = SSET.empty;
      flush_alarm = awaken_flush;
      max_msgs_in_mem = max_msgs_in_mem;
      unacks = SSET.empty;
    } in
  let flush_period = max flush_period 0.005 in
  let rec loop_flush wait_flush =
    Lwt.choose [Lwt_unix.sleep flush_period; wait_flush] >>
    begin
      let wait, awaken = Lwt.wait () in
        flush t;
        t.flush_alarm <- awaken;
        loop_flush wait
    end in
  let rec loop_flush_unacks () =
    lwt () = Lwt_unix.sleep 0.1 in
      if not (SSET.is_empty t.unacks) then
        transaction t.db
          (fun db ->
             printf "UnACKing %d messages in DB\n%!" (SSET.cardinal t.unacks);
             SSET.iter (unack db) t.unacks);
      t.unacks <- SSET.empty;
      loop_flush_unacks ()
  in
    ignore
      (try_lwt loop_flush wait_flush
       with e -> printf "EXCEPTION IN FLUSHER: %s\n%!" (Printexc.to_string e);
                 return ());
    ignore
      (try_lwt loop_flush_unacks ()
       with e -> printf "EXCEPTION IN UNACK FLUSHER: %s\n%!" (Printexc.to_string e);
                 return ());
    t

let initialize t =
  execute t.db sqlinit"ATTACH \":memory:\" AS mem";
  execute t.db
    sqlinit"CREATE TABLE IF NOT EXISTS ocamlmq_msgs(
              ack_pending BOOL NOT NULL,
              msg_id VARCHAR(255) NOT NULL PRIMARY KEY,
              priority INT NOT NULL,
              destination VARCHAR(255) NOT NULL,
              timestamp DOUBLE NOT NULL,
              ack_timeout DOUBLE NOT NULL,
              body BLOB NOT NULL
            )";
  execute t.db
    sqlinit"CREATE INDEX IF NOT EXISTS
            ocamlmq_msgs_destination_priority_timestamp
            ON ocamlmq_msgs(destination, ack_pending, priority, timestamp)";
  execute t.db
    sqlinit"CREATE TABLE mem.pending_acks(msg_id VARCHAR(255) NOT NULL PRIMARY KEY)";
  execute t.db
    sqlinit"CREATE TABLE mem.acked_msgs(msg_id VARCHAR(255) NOT NULL PRIMARY KEY)";
  return ()

let do_save_msg t sent msg =
  let dest = destination_name msg.msg_destination in
  let v = (msg.msg_priority, msg) in
  begin
    try
      let unsent, want_ack = Hashtbl.find t.in_mem dest in
      let p =
        if sent then (unsent, SSET.add msg.msg_id want_ack)
        else (MSET.add v unsent, want_ack)
      in Hashtbl.replace t.in_mem dest p
    with Not_found ->
      let p =
        if sent then (MSET.empty, SSET.singleton msg.msg_id)
        else (MSET.singleton v, SSET.empty)
      in Hashtbl.add t.in_mem dest p
  end;
  Hashtbl.add t.in_mem_msgs msg.msg_id msg;
  if Hashtbl.length t.in_mem_msgs > t.max_msgs_in_mem then
    Lwt.wakeup t.flush_alarm ();
  return ()

let save_msg t ?low_priority msg =
  do_save_msg t false msg

let register_ack_pending_msg t msg_id =
  if Hashtbl.mem t.in_mem_msgs msg_id then
    let r = SSET.mem msg_id t.ack_pending in
      if not r then begin
        let msg = Hashtbl.find t.in_mem_msgs msg_id in
        let dest = destination_name msg.msg_destination in
        let unsent, want_ack = Hashtbl.find t.in_mem dest in
        let v = (msg.msg_priority, msg) in
          t.ack_pending <- SSET.add msg_id t.ack_pending;
          Hashtbl.replace t.in_mem dest (MSET.remove v unsent, SSET.add msg_id want_ack)
      end;
      return (not r)
  else
    match
      select_one_maybe t.db
        sqlc"SELECT @b{1} FROM pending_acks WHERE msg_id = %s" msg_id
    with
        Some _ -> return false
      | None ->
          if msg_materialized_as_ack_pending t.db msg_id then
            return false
          else begin
            execute t.db sqlc"INSERT INTO pending_acks(msg_id) VALUES(%s)" msg_id;
            if count_unmaterialized_pending_acks t.db > 100L then
              transaction t.db (materialize_pending_acks ~verbose:true);
            return true
          end

let msg_of_tuple (msg_id, dst, timestamp, priority, ack_timeout, body) =
  {
    msg_id = msg_id;
    msg_destination = Queue dst;
    msg_priority = priority;
    msg_timestamp = timestamp;
    msg_ack_timeout = ack_timeout;
    msg_body = body;
  }

let get_ack_pending_msg t msg_id =
  return begin try
    let msg = Hashtbl.find t.in_mem_msgs msg_id in
      if SSET.mem msg_id t.ack_pending then Some msg else None
  with Not_found ->
    select_one_f_maybe t.db msg_of_tuple
      sqlc"SELECT @s{msg_id}, @s{destination}, @f{timestamp},
                 @d{priority}, @f{ack_timeout}, @S{body}
            FROM ocamlmq_msgs AS msg
           WHERE msg_id = %s
             AND (msg.ack_pending OR
                  EXISTS (SELECT 1 FROM pending_acks WHERE msg_id = msg.msg_id))
             AND NOT EXISTS (SELECT 1 FROM acked_msgs WHERE msg_id = msg.msg_id)"
      msg_id
  end

let ack_msg t msg_id =
  if SSET.mem msg_id t.ack_pending then begin
    let msg = Hashtbl.find t.in_mem_msgs msg_id in
      Hashtbl.remove t.in_mem_msgs msg_id;
      t.ack_pending <- SSET.remove msg_id t.ack_pending;
      let dst = destination_name msg.msg_destination in
      let unsent, want_ack = Hashtbl.find t.in_mem dst in
        Hashtbl.replace t.in_mem dst (unsent, SSET.remove msg_id want_ack)
  end else begin
    execute t.db sqlc"INSERT INTO acked_msgs(msg_id) VALUES(%s)" msg_id;
    t.ack_pending <- SSET.remove msg_id t.ack_pending;
    if count_acked_messages t.db > 100L then
      transaction t.db (flush_acked_msgs ~verbose:true);
  end;
  return ()

let unack_msg t msg_id =
  if SSET.mem msg_id t.ack_pending then begin
    t.ack_pending <- SSET.remove msg_id t.ack_pending;
    let msg = Hashtbl.find t.in_mem_msgs msg_id in
    let dst = destination_name msg.msg_destination in
    let msg = Hashtbl.find t.in_mem_msgs msg_id in
    let unsent, want_ack = Hashtbl.find t.in_mem dst in
    let v = (msg.msg_priority, msg) in
      Hashtbl.replace t.in_mem dst (MSET.add v unsent, SSET.remove msg_id want_ack)
  end else begin
    t.unacks <- SSET.add msg_id t.unacks;
  end;
  return ()

exception Msg of message

let get_msg_for_delivery t dest =
  try
    let unsent, want_ack = Hashtbl.find t.in_mem dest in
    let ((prio, msg) as v) = MSET.min_elt unsent in
      t.ack_pending <- SSET.add msg.msg_id t.ack_pending;
      Hashtbl.replace t.in_mem dest
        (MSET.remove v unsent, SSET.add msg.msg_id want_ack);
      return (Some msg)
  with Not_found ->
    match
      select_one_f_maybe t.db msg_of_tuple
        sqlc"SELECT @s{msg_id}, @s{destination}, @f{timestamp},
                   @d{priority}, @f{ack_timeout}, @S{body}
              FROM ocamlmq_msgs as msg
             WHERE destination = %s
               AND msg.ack_pending = 0
               AND NOT EXISTS (SELECT 1 FROM pending_acks WHERE msg_id = msg.msg_id)
               AND NOT EXISTS (SELECT 1 FROM acked_msgs WHERE msg_id = msg.msg_id)
          ORDER BY priority, timestamp"
        dest
    with
        None -> return None
      | Some msg ->
          execute t.db sqlc"INSERT INTO pending_acks VALUES(%s)" msg.msg_id;
          if count_unmaterialized_pending_acks t.db > 100L then
            transaction t.db (materialize_pending_acks ~verbose:true);
          return (Some msg)

let count_queue_msgs t dst =
  let in_mem =
    try
      let unsent, want_ack = Hashtbl.find t.in_mem dst in
        MSET.cardinal unsent + SSET.cardinal want_ack
    with Not_found -> 0 in
  let in_db =
    select_one t.db
      sqlc"SELECT @L{COUNT(*)} FROM ocamlmq_msgs as msg
            WHERE destination=%s
              AND NOT EXISTS (SELECT 1 FROM acked_msgs WHERE msg_id = msg.msg_id)"
      dst
  in
    return (Int64.add (Int64.of_int in_mem) in_db)

let crash_recovery t =
  execute t.db sqlc"UPDATE ocamlmq_msgs SET ack_pending = 0";
  return ()

let init_db, check_db, auto_check_db = sql_check"sqlite"
