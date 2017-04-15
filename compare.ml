(* compare [x] and [y] lexicographically using functions in the list [fs]. *)
let lexicographic fs x y =
  let rec loop = function
    | cmp :: rest ->
      let res = cmp x y in
      if res = 0
      then loop rest
      else res
    | [] -> 0
  in loop fs

(* specialization with [int]. *)
let int : int -> int -> int = Pervasives.compare

(* specialization with [int64]. *)
let int64 : int64 -> int64 -> int = Int64.compare

let char : char -> char -> int = Char.compare
