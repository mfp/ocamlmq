(* Copyright (c) 2010 Mauricio Fernández <mfp@acm.org> *)
open Printf
open Lwt

let set_some_string r = Arg.String (fun s -> r := Some s)
let set_some_int r = Arg.Int (fun n -> r := Some n)

let port = ref 61613
let debug = ref false
let login = ref None
let passcode = ref None
let db = ref None
let max_in_mem = ref 100000
let flush_period = ref 1.
let binlog = ref ""
let sync_binlog = ref false

let params =
  Arg.align
    [
      "-port", Arg.Set_int port, "PORT Port to listen at (default: 61613).";
      "-login", set_some_string login, "LOGIN Login expected in CONNECT.";
      "-passcode", set_some_string passcode, "PASSCODE Passcode expected in CONNECT.";
      "-maxmsgs", Arg.Set_int max_in_mem,
        "N Flush to disk when there are more than N msgs in mem (default: 100000)";
      "-flush-period", Arg.Set_float flush_period,
        "DT Flush period in seconds (default: 1.0)";
      "-binlog", Arg.Set_string binlog,
        "FILE Use FILE as the binlog for msgs in mem (default: none).";
      "-sync-binlog", Arg.Set sync_binlog,
        " fsync the binlog on each write (default: no)";
      "-debug", Arg.Set debug, " Write debug info to stderr.";
    ]

let usage_message = "Usage: ocamlmq [options] [sqlite3 database (default: ocamlmq.db)]"

let _ = Sys.set_signal Sys.sigpipe Sys.Signal_ignore
let _ = Sys.set_signal Sys.sigint (Sys.Signal_handle (fun _ -> exit 0))

module SERVER = Mq_server.Make(Mq_sqlite_persistence)

let () =
  Arg.parse
    params
    (function
         s when !db = None && s <> "" && s.[0] <> '-' -> db := Some s
       | s -> eprintf "Unknown argument: %S\n%!" s;
              Arg.usage params usage_message;
              exit 1)
    usage_message;
  let addr = Unix.ADDR_INET (Unix.inet_addr_any, !port) in
    Lwt_unix.run begin
      let msg_store =
        Mq_sqlite_persistence.make
          ~max_msgs_in_mem:!max_in_mem
          ~flush_period:!flush_period
          ?binlog:(match !binlog with "" -> None | s -> Some s)
          ~sync_binlog:!sync_binlog
          (Option.default "ocamlmq.db" !db)
      in
        if !debug then eprintf "Connected to database.\n%!";
        eprintf "Initializing database... %!";
        Mq_sqlite_persistence.initialize msg_store >>
        let () = eprintf "DONE\n%!" in
        lwt broker = SERVER.make_broker
                       ?login:!login ?passcode:!passcode msg_store addr
        in SERVER.server_loop ~debug:!debug broker
    end


