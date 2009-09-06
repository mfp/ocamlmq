
(** Sets with extra functionality *)
module Make : functor (Ord : Set.OrderedType) -> 
sig
  include Set.S with type elt = Ord.t

  (** Return the "next" element when considering the set as a circular list.
    * [next e t] returns either the first element greater than [e], or
    * [min_elt t] if there is none.
    * @raise Not_found if the set is empty.
    * *)
  val next : elt -> t -> elt
end
