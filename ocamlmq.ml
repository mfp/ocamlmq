open Printf
open Lwt

let set_some_string r = Arg.String (fun s -> r := Some s)
let set_some_int r = Arg.Int (fun n -> r := Some n)

let db_host = ref None
let db_port = ref None
let db_database = ref None
let db_user = ref None
let db_password = ref None
let db_unix_sock_dir = ref None
let port = ref 44444
let debug = ref false
let initdb = ref false

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
      "-initdb", Arg.Set initdb, " Initialize the database (create required tables).";
      "-debug", Arg.Set debug, " Write debug info to stderr.";
    ]

let usage_message = "Usage: ocamlmq [options]"

let _ = Sys.set_signal Sys.sigpipe Sys.Signal_ignore

module SERVER = Mq_server.Make(Mq_pg_persistence)

let () =
  Arg.parse
    params
    (fun s -> eprintf "Unknown argument: %S\n%!" s;
              Arg.usage params usage_message;
              exit 1)
    usage_message;
  let addr = Unix.ADDR_INET (Unix.inet_addr_any, !port) in
    Lwt_unix.run begin
      lwt msg_store = Mq_pg_persistence.connect
                        ?host:!db_host
                        ?port:!db_port
                        ?database:!db_database
                        ?unix_domain_socket_dir:!db_unix_sock_dir
                        ?user:!db_user
                        ?password:!db_password
                        ~debug:!debug
                        () in
      (if !initdb then Mq_pg_persistence.initialize msg_store
       else return ()) >>
      lwt broker = SERVER.make_broker msg_store addr in
        SERVER.server_loop ~debug:!debug broker
    end


