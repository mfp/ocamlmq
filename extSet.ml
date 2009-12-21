open ExtArray

module type S =
sig
  include Set.S
  val next : elt -> t -> elt
end

module Make(Ord : Set.OrderedType) =
struct
  include Set.Make(Ord)

  let next elt t =
    let lt, mem, gt = split elt t in
      match is_empty lt, is_empty gt with
          true, true -> if mem then elt else raise Not_found
        | _, false -> min_elt gt
        | false, true -> min_elt lt
end

module Make_lean(Ord : Set.OrderedType) =
struct
  module S = Set.Make(Ord)
  type t = Empty | Single of Ord.t | Array of Ord.t array | Set of S.t

  let max_arr_size = 16

  let empty = Empty

  let cardinal = function
      Empty -> 0
    | Single _ -> 1
    | Array x -> Array.length x
    | Set s -> S.cardinal s

  let is_empty s = match s with
      Empty -> true
    | Single _ -> false
    | Array x -> Array.length x = 0
    | Set s -> S.is_empty s

  let singleton x = Single x

  let iter f = function
      Empty -> ()
    | Single x -> f x
    | Array a -> Array.iter f a
    | Set s -> S.iter f s

  let remove x = function
      Empty -> Empty
    | Single y as orig -> if Ord.compare x y = 0 then Empty else orig
    | Array a -> begin match Array.filter (fun y -> Ord.compare x y <> 0) a with
          [||] -> Empty
        | [|x|] -> Single x
        | a -> Array a
      end
    | Set s -> match S.remove x s with
          s when S.is_empty s -> Empty
        | s when S.cardinal s <= max_arr_size -> Array (Array.of_list (S.elements s))
        | s -> Set s

  let add x = function
      Empty -> Single x
    | Single y -> Array [|x; y|]
    | Array a ->
        if Array.length a + 1 <= max_arr_size then Array (Array.append a [|x|])
        else Set (Array.fold_right S.add a (S.singleton x))
    | Set s -> Set (S.add x s)

  let to_set =  function
      Empty -> S.empty
    | Single x -> S.singleton x
    | Array a -> Array.fold_right S.add a S.empty
    | Set s -> s

  let union t1 t2 = match t1, t2 with
      Empty, t | t, Empty -> t
    | _ -> Set (S.union (to_set t1) (to_set t2))
end
