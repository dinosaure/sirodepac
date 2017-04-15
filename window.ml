type 'a t =
  { mutable a   : int
  ; mutable b   : int
  ; mutable buf : 'a array
  ; len         : int }

let make len =
  { a = 0
  ; b = 0
  ; len
  ; buf = Array.of_list [] }

let capacity t =
  let len = Array.length t.buf in
  match len with 0 -> 0 | l -> l - 1

let max t = t.len

let length t =
  if t.b >= t.a
  then t.b - t.a
  else (Array.length t.buf - t.a) + t.b

let resize t c e =
  assert (c >= Array.length t.buf);

  let buf' = Array.make c e in

  if t.b >= t.a
  then Array.blit t.buf t.a buf' 0 (t.b - t.a)
  else begin
    let extra = Array.length t.buf - t.a in
    Array.blit t.buf t.a buf' 0 extra;
    Array.blit t.buf 0 buf' extra t.b;
  end;

  t.buf <- buf'

let blit_from t src o len =
  if Array.length src = 0
  then ()
  else
    let c = capacity t - length t in

    if c < len
    then begin
      let len' =
        let desired = Array.length t.buf + len + 24 in
        min (t.len + 1) desired
      in
      resize t len' (Array.get src 0);
      let good = capacity t = t.len || capacity t - length t >= len in
      assert good;
    end;

    let sub = Array.sub src o len in
    let iter x =
      let c = Array.length t.buf in

      Array.set t.buf t.b x;

      if t.b = c - 1
      then t.b <- 0
      else t.b <- t.b + 1;

      if t.a = t.b
      then if t.a = c - 1
        then t.a <- 0
        else t.a <- t.a + 1
    in

    Array.iter iter sub

let clear t =
  t.a <- 0;
  t.b <- 0

let reset t =
  clear t;
  t.buf <- Array.of_list []

exception Empty

let take_front_exn t =
  if t.a = t.b then raise Empty;

  let c = Array.get t.buf t.a in

  if t.a + 1 = Array.length t.buf
  then t.a <- 0
  else t.a <- t.a + 1;

  c

let junk_front_exn t =
  if t.a = t.b then raise Empty;

  if t.a + 1 = Array.length t.buf
  then t.a <- 0
  else t.a <- t.a + 1

let junk_front t =
  try junk_front_exn t
  with Empty -> ()

let iter t f =
  if t.b >= t.a
  then for i = t.a to t.b - 1 do f (Array.get t.buf i) done
  else begin
    for i = t.a to Array.length t.buf - 1 do f (Array.get t.buf i) done;
    for i = 0 to t.b - 1 do f (Array.get t.buf i) done;
  end

let iteri t f =
  let idx = ref 0 in

  if t.b >= t.a
  then for i = t.a to t.b - 1 do f !idx (Array.get t.buf i); incr idx done
  else begin
    for i = t.a to Array.length t.buf - 1 do f !idx (Array.get t.buf i); incr idx done;
    for i = 0 to t.b - 1 do f !idx (Array.get t.buf i); incr idx done;
  end

let fold t f a =
  let a = ref a in

  iter t (fun x -> a := f !a x);
  !a

let foldi t f a =
  let a = ref a in

  iteri t (fun i x -> a := f !a i x);
  !a

let push_back t e = blit_from t (Array.make 1 e) 0 1

let nth_exn t i =
  if t.b >= t.a
  then Array.get t.buf (t.a + i)
  else
    if t.a + i < Array.length t.buf
    then Array.get t.buf (t.a + i)
    else Array.get t.buf (i - (Array.length t.buf - t.a))
