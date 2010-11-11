
include Mq_server.PERSISTENCE

val make : ?max_msgs_in_mem:int -> ?flush_period:float -> string -> t
