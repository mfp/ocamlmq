
include Mq_server.PERSISTENCE

val make : ?max_msgs_in_mem:int -> ?flush_period:float ->
  ?sync:bool -> ?unsafe_db:bool ->
  ?binlog:string -> ?sync_binlog:bool -> string -> t

(* Used for testing *)
val auto_check_db : Format.formatter -> bool

