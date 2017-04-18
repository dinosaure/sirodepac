module Cheap =
struct
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
end

module ObjArray =
struct
  type t = Obj.t array

  let length = Array.length
  let zero = Obj.repr (0 : int)
  let create len = Array.make len zero
  let empty = [||]

  type nofloat = NoFloat0 | NoFloat1 of int

  let get t i =
    Obj.repr (Array.get (Obj.magic (t : t) : nofloat array) i : nofloat)

  let set t i x =
    let old = get t i in
    if Obj.is_int old && Obj.is_int x
    then Array.unsafe_set (Obj.magic (t : t) : int array) i (Obj.obj x : int)
    else if not (old == x)
    then Array.unsafe_set t i x

  let blit src src_pos dst dst_pos len =
    if dst_pos < src_pos
    then for i = 0 to len - 1
      do set dst (dst_pos + i) (get src (src_pos + i)) done
    else for i = len - 1 downto 0
      do set dst (dst_pos + i) (get src (src_pos + i)) done

  let copy src =
    let dst = create (length src) in
    blit src 0 dst 0 (length src);
    dst
end

module UniformArray =
struct
  type 'a t = ObjArray.t

  let empty = ObjArray.empty
  let get arr i = Obj.obj (ObjArray.get arr i)
  let set arr i x = ObjArray.set arr i (Obj.repr x)
  let length = ObjArray.length
  let blit = ObjArray.blit
  let copy = ObjArray.copy
  let create len v =
    let res = ObjArray.create len in
    for i = 0 to len - 1
    do set res i v done;
    res
  let iter a f =
    for i = 0 to length a - 1
    do f (get a i) done
end

module CheapArray =
struct
  type 'a t = 'a Cheap.t UniformArray.t

  let empty = UniformArray.empty
  let create len = UniformArray.create len Cheap.none
  let is_none t i = Cheap.is_none (UniformArray.get t i)
  let is_some t i = Cheap.is_some (UniformArray.get t i)
  let set t i x = UniformArray.set t i (Cheap.of_option x)
  let get t i = Cheap.to_option (UniformArray.get t i)
end

module Option =
struct
  let iter f = function
    | Some a -> f a
    | None -> ()
end

type 'a t =
  { mutable a : int
  ; mutable b : int
  ; len       : int
  ; buf       : 'a CheapArray.t }

let make len =
  { a = 0
  ; b = 0
  ; len = len + 1
  ; buf = CheapArray.create (len + 1) }

let drop t n =
  t.a <- if t.a + n < t.len then t.a + n else t.a + n - t.len

let move t n =
  t.b <- if t.b + n < t.len then t.b + n else t.b + n - t.len

let available t =
  if t.b >= t.a
  then t.len - (t.b - t.a) - 1
  else (t.a - t.b) - 1

let have t =
  if t.b >= t.a
  then t.b - t.a
  else t.len - (t.a - t.b)

let of_array t arr off len =
  if len > available t then drop t (len - (available t));

  let pre = t.len - t.b in
  let extra = len - pre in

  if extra > 0
  then begin
    for i = 0 to pre - 1
    do CheapArray.set t.buf (t.b + i) (Array.get arr (off + i)) done;
    for i = 0 to extra - 1
    do CheapArray.set t.buf i (Array.get arr (off + pre + i)) done
  end else
    for i = 0 to len - 1
    do CheapArray.set t.buf (t.b + i) (Array.get arr (off + i)) done;

  move t len

(* XXX(dinosaure): need to be fixed. the log is wrong. TODO! *)
let push t x = of_array t [| Some x |] 0 1

let iteri t f =
  let idx = ref 0 in
  let pre = t.len - t.a in
  let extra = (have t) - pre in

  if extra > 0
  then begin
    for i = 0 to pre - 1
    do Option.iter (f !idx) (CheapArray.get t.buf (t.a + i)); incr idx; done;
    for i = 0 to extra - 1
    do Option.iter (f !idx) (CheapArray.get t.buf i); incr idx; done;
  end else
    for i = 0 to (have t) - 1
    do Option.iter (f !idx) (CheapArray.get t.buf (t.a + i)); incr idx; done

let iter t f = iteri t (fun i x -> f x)

let foldi t f a =
  let acc = ref a in
  iteri t (fun idx x -> acc := f !acc idx x);
  !acc

let fold t f a =
  foldi t (fun acc idx x -> f acc x) a

let nth t i =
  let pre = t.len - t.a in
  let extra = i - pre in

  let value_opt =
    if extra >= 0
    then CheapArray.get t.buf extra
    else CheapArray.get t.buf (t.a + i)
  in

  match value_opt with
  | Some x -> x
  | None -> raise (Invalid_argument "Window.nth")
