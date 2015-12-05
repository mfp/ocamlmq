open Lwt
open Mq_types
open Printf

type t = { fd : Unix.file_descr; och : Lwt_io.output_channel }

type record =
    Add of message
  | Del of string
  | Nothing

let truncate t =
  Unix.ftruncate t.fd 0;
  ignore (Unix.lseek t.fd 0 Unix.SEEK_SET)

module LE = Lwt_io.LE

let read_exactly ch n =
  if n < 0 then fail End_of_file else
  let s = String.create n in
    Lwt_io.read_into_exactly ch s 0 n >>
    return s

let read_string ich =
  lwt len = LE.read_int ich in
    read_exactly ich len

let read_record ich =
  try_lwt
    lwt kind = Lwt_io.read_char ich in
      begin match kind with
          'A' ->
            lwt id = read_string ich in
            lwt dest = read_string ich in
            lwt prio = LE.read_int ich in
            lwt timestamp = LE.read_float64 ich in
            lwt body = read_string ich in
            lwt timeout = LE.read_float64 ich in
            let r =
              Add
                {
                  msg_id = id; msg_destination = Queue dest;
                  msg_priority = prio; msg_timestamp = timestamp;
                  msg_body = body; msg_ack_timeout = timeout
                }
            in return r
        | 'B' ->
            lwt id = read_string ich in return (Del id)
        | _ -> raise End_of_file
      end
  with End_of_file -> return Nothing

let write_string och s =
  LE.write_int och (String.length s) >>
  Lwt_io.write och s

let write_record och = function
    Nothing -> return ()
  | Del msg_id ->
      Lwt_io.write_char och 'D' >> write_string och msg_id
  | Add msg ->
      Lwt_io.write_char och 'A' >>
      write_string och msg.msg_id >>
      write_string och (destination_name msg.msg_destination) >>
      LE.write_int och msg.msg_priority >>
      LE.write_float64 och msg.msg_timestamp >>
      write_string och msg.msg_body >>
      LE.write_float64 och msg.msg_ack_timeout

let write_record ?(flush = true) och r =
  Lwt_io.atomic (fun och -> write_record och r) och >>
  if flush then Lwt_io.flush och else return ()

let cancel t msg = write_record t.och (Del msg.msg_id)
let add t msg = write_record t.och (Add msg)

(* Creates [dst] even if [src] cannot be read *)
let copy src dst =
  Lwt_io.with_file
    ~flags:[Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC]
    ~mode:Lwt_io.output dst (fun _ -> return ()) >>
  Lwt_io.with_file ~mode:Lwt_io.input src
    (fun ich ->
       Lwt_io.with_file
         ~flags:[Unix.O_SYNC; Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC]
         ~buffer:(Lwt_bytes.create (1024 * 1024))
         ~mode:Lwt_io.output dst
         (fun och ->
            let h = Hashtbl.create 13 in
            let rec copy_loop () =
              read_record ich >>= function
                  Add msg as x ->
                    Hashtbl.add h msg.msg_id msg;
                    write_record ~flush:false och x >> copy_loop ()
                | Del msg_id as x ->
                    Hashtbl.remove h msg_id;
                    write_record ~flush:false och x >> copy_loop ()
                | Nothing -> return ()
            in copy_loop () >>
               return (Hashtbl.fold (fun _ msg l -> msg :: l) h [])))

let copy src dst =
  try_lwt
    copy src dst
  with Unix.Unix_error (Unix.ENOENT, _, _) -> return []

let make ?(sync = false) file =
  let tmp = sprintf "%s.%d.%d" file (Unix.getpid ()) (Random.int 0x3FFFFFFF) in
  lwt msgs = copy file tmp in
    Sys.rename tmp file;
    let sync = if sync then [ Unix.O_SYNC ] else [] in
    let fd = Unix.openfile file ([ Unix.O_WRONLY; ] @ sync) 0o640 in
    let och = Lwt_io.of_unix_fd ~mode:Lwt_io.output fd in
      return ({ fd = fd; och = och; }, msgs)
