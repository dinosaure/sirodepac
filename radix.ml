module Compare =
struct
  type t =
    | Eq
    | Prefix
    | Contain
    | Inf of int
    | Sup of int

  let rec compare value1 off1 len1 value2 off2 len2 =
    if off1 = len1
    then if off2 = len2
         then Eq
         else Prefix
    else if off2 = len2
         then Contain
         else
           let c1 = value1.[off1] in
           let c2 = value2.[off2] in

           if c1 = c2
           then compare value1 (off1 + 1) len1 value2 (off2 + 1) len2
           else if c1 < c2
           then Inf off1
           else (* c1 > c2 *) Sup off1
end

let critbit c1 c2 =
  let rec aux p c1 c2 =
    if (c1 land 128) <> (c2 land 128)
    then p
    else aux (p - 1) (c1 lsl 1) (c2 lsl 1)
  in

  if c1 = c2
  then raise Not_found
  else aux 7 (Char.code c1) (Char.code c2)
       (* XXX(dinosaure): a XOR and find first bit should be more efficient. *)

type 'a node =
  | L of string * 'a
  | T of 'a node * string * 'a
  | B of 'a node * 'a node * int * int

type 'a t = 'a node option

let empty = None

let is_empty = function
  | None -> true
  | Some _ -> false

let rec first_key = function
  | L (k, _) -> k
  | T (_, k, _) -> k
  | B (l, r, _, _) ->
    first_key l (* XXX(dinosaure): could take the shortest path if [B] node
                                   embed he size of tree. *)

let rec bind key off keylen value tree = match tree with
  | L (k, v) ->
    (let kl = String.length k in
     match Compare.compare key off keylen k off kl with
     | Compare.Eq -> L (k, value) (* replace *)
     | Compare.Prefix -> T (tree, key, value)
     | Compare.Contain -> T (L (key, value), k, v)
     | Compare.Inf p ->
       let b = critbit key.[p] k.[p] in
       B (L (key, value), tree, p, b)
     | Compare.Sup p ->
       let b = critbit key.[p] k.[p] in
       B (tree, L (key, value), p, b))
  | T (m, k, v) ->
    (let kl = String.length k in
     match Compare.compare key off keylen k off kl with
     | Compare.Eq -> T (m, k, value) (* replace *)
     | Compare.Prefix -> T (tree, key, value)
     | Compare.Contain -> T (bind key kl keylen value m, k, v)
     | Compare.Inf p ->
       let b = critbit key.[p] k.[p] in
       B (L (key, value), tree, p, b)
     | Compare.Sup p ->
       let b = critbit key.[p] k.[p] in
       B (tree, L (key, value), p, b))
  | B (l, r, i, b) ->
    if keylen > i
    then if ((Char.code key.[i]) land (1 lsl b)) = 0
         then B (bind key i keylen value l, r, i, b)
         else B (l, bind key i keylen value r, i, b)
    else let k = first_key l in
         match Compare.compare key off keylen k off keylen with
         | Compare.Eq | Compare.Prefix -> T (tree, key, value)
         | Compare.Contain -> B (bind key i keylen value l, r, i, b)
         | Compare.Inf p ->
           if p = i
           then B (bind key i keylen value l, r, i, b)
           else let bn = critbit key.[p] k.[p] in
                B (L (key, value), tree, p, bn)
         | Compare.Sup p ->
           if p = i
           then B (l, bind key i keylen value r, i, b)
           else let bn = critbit key.[p] k.[p] in
                B (tree, L (key, value), p, bn)

let bind tree key value =
  match tree with
  | None -> Some (L (key, value))
  | Some tree ->
    let keylen = String.length key in
    Some (bind key 0 keylen value tree)

let rec lookup key off keylen tree = match tree with
  | L (k, v) ->
    if key = k then Some v else None
  | T (m, k, v) ->
    (let kl = String.length k in
     match Compare.compare key off keylen k off kl with
     | Compare.Eq | Compare.Prefix -> Some v
     | Compare.Contain -> lookup key kl keylen m
     | _ -> None)
  | B (l, r, i, b) ->
    if keylen > i
    then let dir = if ((Char.code key.[i]) land (1 lsl b)) =0
                   then l else r in
         lookup key i keylen dir
    else None

let lookup tree key =
  match tree with
  | None -> None
  | Some tree ->
    let keylen = String.length key in
    lookup key 0 keylen tree

let rec fold f acc tree = match tree with
  | L (k, v) -> f (k, v) acc
  | T (m, k, v) ->
    let acc' = f (k, v) acc in
    fold f acc' m
  | B (l, r, i, b) ->
    let acc' = fold f acc l in
    fold f acc' r

let fold f acc = function
  | None -> acc
  | Some tree -> fold f acc tree
