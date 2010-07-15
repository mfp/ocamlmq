
include Mq_server.PERSISTENCE
  
val make : ?max_msgs_in_mem:int -> string -> t
