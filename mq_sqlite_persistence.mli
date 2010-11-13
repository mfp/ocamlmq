
include Mq_server.PERSISTENCE

val make :
  ?max_msgs_in_mem:int ->
  ?flush_period:float ->
  ?flush_wait_time:float -> string -> t

(* Used for testing *)
val auto_check_db : Format.formatter -> bool

