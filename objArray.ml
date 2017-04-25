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
