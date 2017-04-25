type +'a t

let none : _ t =
  Obj.magic `x6e8ee3478e1d7449 (* HACK! *)
let none_substitute : _ t = Obj.obj (Obj.new_block Obj.abstract_tag 1)

let physical_equality = (==)
let physical_same (type a) (type b) (a : a) (b : b) = physical_equality a (Obj.magic b : a)

let is_none x = physical_equality x none
let is_some x = not (physical_equality x none)

let some (type a) (x : a) : a t =
  if physical_same x none
  then none_substitute
  else (Obj.magic x)

let value_unsafe (type a) (x : a t) : a =
  if physical_equality x none_substitute
  then Obj.magic none
  else Obj.magic x

let value_exn x =
  if is_some x
  then value_unsafe x
  else raise (Invalid_argument "Cheap.value_exn: the value is none")

let of_option = function
  | None -> none
  | Some x -> some x

let to_option x =
  if is_some x
  then Some (value_unsafe x)
  else None
