
type t

val make : ?sync:bool -> string -> (t * Mq_types.message list) Lwt.t
val truncate : t -> unit
val add : t -> Mq_types.message -> unit Lwt.t
val cancel : t -> Mq_types.message -> unit Lwt.t

