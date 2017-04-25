type t =
  { mutable buf : Cstruct.t
  ; mutable pos : int
  ; mutable len : int
  ; init        : Cstruct.t }

let create n =
  let n = if n < 1 then 1 else n in
  let n = if n > Sys.max_string_length then Sys.max_string_length else n in
  let s = Cstruct.create n in

  { buf  = s
  ; pos  = 0
  ; len  = n
  ; init = s }

let contents b =
  Cstruct.sub b.buf 0 b.pos

let length b = b.pos

let resize ({ len; pos; buf; } as b) m =
  let len' = ref len in

  while pos + m > !len' do len' := 2 * !len' done;

  if !len' > Sys.max_string_length
  then if pos + m <= Sys.max_string_length
        then len' := Sys.max_string_length
        else raise (Failure "BBuffer.add: cannot grow buffer");

  let buf' = Cstruct.create !len' in

  Cstruct.blit buf 0 buf' 0 pos;
  b.buf <- buf';
  b.len <- !len'

(* XXX(dinosaure): we can factorize by a first module, but too big to be
                    useful. *)
let add tmp ?(off = 0) ?(len = Cstruct.len tmp) buf =
  if off < 0 || len < 0 || off + len > Cstruct.len tmp
  then raise (Invalid_argument "BBuffer.add");

  let pos' = buf.pos + len in

  if pos' > buf.len
  then resize buf len;

  Cstruct.blit tmp off buf.buf buf.pos len;

  buf.pos <- pos'

let add_string tmp ?(off = 0) ?(len = String.length tmp) buf =
  if off < 0 || len < 0 || off + len > String.length tmp
  then raise (Invalid_argument "BBuffer.add_string");

  let pos' = buf.pos + len in

  if pos' > buf.len
  then resize buf len;

  Cstruct.blit_from_string tmp off buf.buf buf.pos len;
  buf.pos <- pos'

let add_bytes tmp ?(off = 0) ?(len = Bytes.length tmp) buf =
  if off < 0 || len < 0 || off + len > Bytes.length tmp
  then raise (Invalid_argument "BBuffer.add_bytes");

  let pos' = buf.pos + len in

  if pos' > buf.len
  then resize buf len;

  Cstruct.blit_from_bytes tmp off buf.buf buf.pos len;
  buf.pos <- pos'

let add_bigstring tmp ?(off = 0) ?(len = Decompress.B.Bigstring.length tmp) buf =
  if off < 0 || len < 0 || off + len > Decompress.B.Bigstring.length tmp
  then raise (Invalid_argument "BBuffer.add_bigstring");

  let pos' = buf.pos + len in

  if pos' > buf.len
  then resize buf len;

  Cstruct.blit (Cstruct.of_bigarray tmp) off buf.buf buf.pos len;
  buf.pos <- pos'

let clear b = b.pos <- 0
