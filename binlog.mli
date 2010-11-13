
type t

val read : string -> Mq_types.message list Lwt.t
val make : string -> t
val truncate : t -> unit
val add : t -> Mq_types.message -> unit Lwt.t
val cancel : t -> Mq_types.message -> unit Lwt.t

