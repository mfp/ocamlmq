(** Finite map over strings using ternary search trees (TSTs). *)

type 'a t
val empty : 'a t
val length : 'a t -> int
val is_empty : 'a t -> bool
val find : string -> 'a t -> 'a

(** [find_prefixes k t] returns all the values whose keys are a prefix of [k]
  * (including [k] itself), in longest to shortest order (i.e., the value for
  * [k] would come first). *)
val find_prefixes : string -> 'a t -> 'a list
val mem : string -> 'a t -> bool
val add : string -> 'a -> 'a t -> 'a t
val remove : string -> 'a t -> 'a t
