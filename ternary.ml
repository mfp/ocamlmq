(* Copyright (c) 2010 Mauricio Fernández <mfp@acm.org> *)

type 'a t = 
    E (* empty *)
  | NV of char * 'a t * 'a t * 'a t (* no value *)
  | V of char * 'a t * 'a t * 'a t * 'a (* value *)

type key = string

let empty = E

let rec length = function
    E -> 0
  | NV (_, l, m, r) | V (_, l, m, r, _) -> length l + length m + length r

let rec is_empty = function
    E -> true
  | V _ -> false
  | NV (_, l, m, r) -> is_empty l && is_empty m && is_empty r

let find k t =
  let rec loop s n maxn = function
      E -> raise Not_found
    | NV (c, l, m, r) | V (c, l, m, r, _) as node when n <= maxn ->
        let c' = s.[n] in
          if c' < c then loop s n maxn l
          else if c' > c then loop s n maxn r
          else begin match node with
              NV _ -> loop s (n + 1) maxn m
            | V (_, _, _, _, v) -> if n = maxn then v else loop s (n + 1) maxn m
            | _ -> assert false
          end
    | _ -> raise Not_found in 
  let len = String.length k in
    if len <> 0 then loop k 0 (len - 1) t
    else match t with
        E | NV _ -> raise Not_found
      | V (_, _, _, _, v) -> v

let find_prefixes k t =
  let rec loop acc s n maxn = function
      E -> acc
    | NV (c, l, m, r) | V (c, l, m, r, _) as node when n <= maxn ->
        let c' = s.[n] in
          if c' < c then loop acc s n maxn l
          else if c' > c then loop acc s n maxn r
          else begin match node with
              NV _ -> loop acc s (n + 1) maxn m
            | V (_, _, _, _, v) -> if n = maxn then (v :: acc) else loop (v :: acc) s (n + 1) maxn m
            | _ -> assert false
          end
    | _ -> acc in
  let len = String.length k in
    if len <> 0 then loop [] k 0 (len - 1) t
    else match t with
        E | NV _ -> []
      | V (_, _, _, _, v) -> [v]

let mem k t = try ignore (find k t); true with Not_found -> false

let add k v t = 
  let rec add k v off maxn = function
      E -> 
        if off = maxn then V(k.[off], E, E, E, v)
        (* maxn = -1 when k = "" *)
        else if maxn = -1 then V (Char.chr 127, E, E, E, v)
        else NV (k.[off], E, add k v (off + 1) maxn E, E)
    | NV (c, l, m, r) ->
        let cmp = Char.compare k.[off] c in
          if cmp < 0 then NV (c, add k v off maxn l, m, r)
          else if cmp > 0 then NV (c, l, m, add k v off maxn r)
          else if off = maxn then V (c, l, m, r, v)
          else NV (c, l, add k v (off + 1) maxn m, r)
    | V (c, l, m, r, v') ->
        let cmp = Char.compare k.[off] c in
          if cmp < 0 then V (c, add k v off maxn l, m, r, v')
          else if cmp > 0 then V (c, l, m, add k v off maxn r, v')
          else if off = maxn then V (c, l, m, r, v)
          else V (c, l, add k v (off + 1) maxn m, r, v')
  in add k v 0 (String.length k - 1) t

let prune = function
    NV (_, E, E, E) -> E
  | e -> e

let remove k t =
  let rec del k off maxn = function
      E -> E
    | NV (c, l, m, r) as t ->
        let cmp = Char.compare k.[off] c in
        let t = 
          if cmp < 0 then NV (c, del k off maxn l, m, r)
          else if cmp > 0 then NV (c, l, m, del k off maxn r)
          else if off = maxn then t
          else NV (c, l, del k (off + 1) maxn m, r)
        in prune t
    | V (c, l, m, r, v) ->
        let cmp = Char.compare k.[off] c in
          if cmp < 0 then V (c, del k off maxn l, m, r, v)
          else if cmp > 0 then V (c, l, m, del k off maxn r, v)
          else if off = maxn then prune (NV (c, l, m, r))
          else V (c, l, del k (off + 1) maxn m, r, v) in 
  let len = String.length k in
    if len <> 0 then del k 0 (len - 1) t
    else match t with
        E | NV _ as t -> t (* nothing associated to "" *)
      | V (c, l, m, r, _) -> NV (c, l, m, r)
