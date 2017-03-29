(* linear search between [lo] and [hi] of value in the container, which respects the predicate. *)
let rec linear t ~get ~lo ~hi ~predicate =
  if lo > hi
  then None
  else if predicate (get t lo)
  then Some lo
  else linear t ~get ~lo:(lo + 1) ~hi ~predicate

(* return a closest range for a linear search where it is possible to find a value which respects the predicate. *)
let rec find t ~get ~lo ~hi ~predicate =
  if hi - lo <= 8 (* must upper than 1. *)
  then (lo, hi)
  else let mid = lo + ((hi - lo) / 2) in
    if predicate (get t mid)
    then find t ~get ~lo ~hi:mid ~predicate (* lower part *)
    else find t ~get ~lo:(mid + 1) ~hi ~predicate (* higher part *)

let find_first ?(pos = 0) ?len t ~get ~length ~predicate =
  let pos, len =
    let len = match len with Some len -> len | None -> (length t) in
    pos, len in
    (* [len] is relative to [pos], that means [pos + len <= (length t)]. *)
  let lo = pos in
  let hi = pos + len - 1 in
  let (lo, hi) = find t ~get ~lo ~hi ~predicate in
  linear t ~get ~lo ~hi ~predicate

(* [`FE] means the first value stricly equal to [value].
   [`FGE] means the first value when [compare x v >= 0] (Greater or Equal).
   [`FSE] means the first value when [compare x v > 0] (Strictly Equal).
*)
let search ?pos ?len t ~get ~length ~compare how v =
  match how with
  | `FE ->
    (match find_first ?pos ?len t ~get ~length ~predicate:(fun x -> compare x v >= 0) with
      | Some x when compare (get t x) v = 0 -> Some x
      | _ -> None)
  | `FGE ->
    find_first ?pos ?len t ~get ~length ~predicate:(fun x -> compare x v >= 0)
  | `FSG ->
    find_first ?pos ?len t ~get ~length ~predicate:(fun x -> compare x v > 0)
