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
