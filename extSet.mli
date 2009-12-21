(** Sets with extra functionality *)

module type S =
sig
  include Set.S

  (** Return the "next" element when considering the set as a circular list.
    * [next e t] returns either the first element greater than [e], or
    * [min_elt t] if there is none.
    * @raise Not_found if the set is empty.
    * *)
  val next : elt -> t -> elt
end

module Make : functor (Ord : Set.OrderedType) -> S with type elt = Ord.t

module Make_lean : functor (Ord : Set.OrderedType) ->
sig
  type t
  val empty : t
  val cardinal : t -> int
  val is_empty : t -> bool
  val singleton : Ord.t -> t
  val iter : (Ord.t -> unit) -> t -> unit
  val remove : Ord.t -> t -> t
  val add : Ord.t -> t -> t
  val union : t -> t -> t
end
