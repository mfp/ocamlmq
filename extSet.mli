
(** Sets with extra functionality *)
module Make : functor (Ord : Set.OrderedType) -> 
sig
  include Set.S with type elt = Ord.t

  (** Return the "next" element when considering the set as a circular list.
    * [next e t] returns either the first greater element greater than 
  * *)
  val next : elt -> t -> elt option
end
