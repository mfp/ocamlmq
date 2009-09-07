
include Mq_server.PERSISTENCE

val connect :
  ?host:string -> ?port:int -> ?unix_domain_socket_dir:string ->
  ?database:string -> ?user:string -> ?password:string ->
  ?debug:bool -> ?max_conns:int -> unit -> t Lwt.t
