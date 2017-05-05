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
  (* XXX(dinosaure): replace to 'a option array? TODO! *)

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
