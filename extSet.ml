
module Make(Ord : Set.OrderedType) =
struct
  include Set.Make(Ord)

  let next elt t =
    let lt, mem, gt = split elt t in
      match is_empty lt, is_empty gt with
          true, true -> if mem then Some elt else None
        | _, false -> Some (min_elt gt)
        | false, true -> Some (min_elt lt)
end
