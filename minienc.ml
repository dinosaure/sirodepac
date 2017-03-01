module B :
sig
  include module type of Decompress.B
    with type st = Decompress.B.st
     and type bs = Decompress.B.bs
     and type 'a t = 'a Decompress.B.t

  type p =
    [ `String of string
    | `Bytes of Bytes.t
    | `Bigstring of Bigstring.t ]

  val blit_string : string -> int -> 'a t -> int -> int -> unit
  val blit_bytes : Bytes.t -> int -> 'a t -> int -> int -> unit
  val blit_bigstring : Bigstring.t -> int -> 'a t -> int -> int -> unit

  val to_poly : ?safe:bool -> 'a t -> p
end = struct
  include Decompress.B

  type p =
    [ `String of string
    | `Bytes of Bytes.t
    | `Bigstring of Bigstring.t ]

  let blit_string src src_off dst dst_off len =
    for i = 0 to len - 1
    do set dst (dst_off + i) (String.get src (src_off + i)) done

  let blit_bytes src src_off dst dst_off len =
    for i = 0 to len - 1
    do set dst (dst_off + i) (Bytes.get src (src_off + i)) done

  let blit_bigstring src src_off dst dst_off len =
    for i = 0 to len - 1
    do set dst (dst_off + i) (Bigstring.get src (src_off + i)) done

  let to_poly
    : type a. ?safe:bool -> a t -> p
    = fun ?(safe = false) -> function
      | Bytes v when safe -> `String (Bytes.unsafe_to_string v)
      | Bytes v -> `Bytes v
      | Bigstring v -> `Bigstring v
end

external swap16 : int -> int = "%bswap16"
external swap32 : int32 -> int32 = "%bswap_int32"
external swap64 : int64 -> int64 = "%bswap_int64"

module type E =
sig
  type _ t

  val set_u16 : _ t -> int -> int -> unit
  val set_u32 : _ t -> int -> int32 -> unit
  val set_u64 : _ t -> int -> int64 -> unit
end

module BE' (E : E) : E with type 'a t = 'a E.t =
struct
  type 'a t = 'a E.t

  let set_u16 s off v =
    if not Sys.big_endian
    then E.set_u16 s off (swap16 v)
    else E.set_u16 s off v

  let set_u32 s off v =
    if not Sys.big_endian
    then E.set_u32 s off (swap32 v)
    else E.set_u32 s off v

  let set_u64 s off v =
    if not Sys.big_endian
    then E.set_u64 s off (swap64 v)
    else E.set_u64 s off v
end

module LE' (E : E) : E with type 'a t = 'a E.t =
struct
  type 'a t = 'a E.t

  let set_u16 s off v =
    if Sys.big_endian
    then E.set_u16 s off (swap16 v)
    else E.set_u16 s off v

  let set_u32 s off v =
    if Sys.big_endian
    then E.set_u32 s off (swap32 v)
    else E.set_u32 s off v

  let set_u64 s off v =
    if Sys.big_endian
    then E.set_u64 s off (swap64 v)
    else E.set_u64 s off v
end

module NE' (E : E) : E with type 'a t = 'a E.t =
struct
  include E
end

type buffer = B.p

module Vec =
struct
  type t = { off : int
           ; len : int
           ; buf : buffer }

  let length t = t.len
end

type 'i t =
  { mutable buffer    : 'i B.t
  ; sched             : Vec.t Dequeue.t
  ; flush             : (int * (unit -> unit)) Dequeue.t
  ; mutable sched_pos : int
  ; mutable write_pos : int
  ; mutable close     : bool
  ; mutable yield     : bool
  ; mutable receive   : int
  ; mutable written   : int }

let from : type i. i B.t -> i t = fun buffer ->
  { buffer
  ; sched = Dequeue.create ()
  ; flush = Dequeue.create ()
  ; sched_pos = 0
  ; write_pos = 0
  ; close = false
  ; yield = false
  ; receive  = 0
  ; written  = 0 }

let writable t =
  if t.close then raise (Failure "Closed writer")

let schedule_vector t ?(off = 0) ~len buf =
  t.receive <- t.receive + len;
  Dequeue.push_back t.sched Vec.{ off; len; buf; }

let flush_buffer t =
  let len = t.write_pos in

  if len > 0
  then begin
    schedule_vector t ~off:0 ~len (B.to_poly t.buffer);

    t.write_pos <- 0;
    t.buffer <- B.from ~proof:t.buffer (B.length t.buffer);
  end

let flush t f =
  flush_buffer t;

  if Dequeue.is_empty t.sched
  then f ()
  else Dequeue.push_back t.flush (t.receive, f)

let available t =
  let len = B.length t.buffer in
  len - t.write_pos

let pending t =
  not (Dequeue.is_empty t.sched)

let schedule_gen t ~to_poly ~length ?(off = 0) ?len a =
  writable t;
  flush_buffer t;

  let len =
    match len with
    | None -> length a - off
    | Some len -> len
  in

  schedule_vector t ~off ~len (to_poly a)

let to_string x    = `String x
let to_bytes x     = `Bytes x
let to_bigstring x = `Bigstring x

let schedule_string =
  fun t ?(off = 0) ?len s ->
    schedule_gen t ~to_poly:to_string ~length:String.length ~off ?len s

let schedule_bytes =
  fun t ?(off = 0) ?len s ->
    schedule_gen t ~to_poly:to_bytes ~length:Bytes.length ~off ?len s

let schedule_bigstring =
  fun t ?(off = 0) ?len s ->
    schedule_gen t ~to_poly:to_bigstring ~length:B.Bigstring.length ~off ?len s

let schedule =
  fun t ?(off = 0) ?len s ->
    schedule_gen t ~to_poly:B.to_poly ~length:B.length ~off ?len s

let ensure t len =
  if available t < len
  then flush_buffer t

let write_gen t ~blit ~length ?(off = 0) ?len a =
  writable t;

  let len =
    match len with
    | None -> length a - off
    | Some len -> len
  in

  ensure t len;
  blit a off t.buffer t.write_pos len;
  t.write_pos <- t.write_pos + len

let write_string =
  fun t ?(off = 0) ?len s ->
    write_gen t ~blit:B.blit_string ~length:String.length ~off ?len s

let write_bytes =
  fun t ?(off = 0) ?len s ->
    write_gen t ~blit:B.blit_bytes ~length:Bytes.length ~off ?len s

let write_bigstring =
  fun t ?(off = 0) ?len s ->
    write_gen t ~blit:B.blit_bigstring ~length:B.Bigstring.length ~off ?len s

let write =
  fun t ?(off = 0) ?len s ->
    write_gen t ~blit:B.blit ~length:B.length ~off ?len s

let write_char =
  fun t chr ->
    let length a = assert false in

    let blit src src_off dst dst_off len =
      assert (src_off = 0);
      assert (len = 1);

      B.set dst dst_off src
    in
    write_gen t ~blit ~length ~off:0 ~len:1 chr

module type ENDIAN =
sig
  val write_u16 : 'a t -> int -> unit
  val write_u32 : 'a t -> int32 -> unit
  val write_u64 : 'a t -> int64 -> unit
end

module Endian (E : E with type 'a t = 'a B.t) : ENDIAN =
struct
  let length a = assert false

  let write_u16 =
    let blit src src_off dst dst_off len =
      assert (src_off = 0);
      assert (len = 2);

      E.set_u16 dst dst_off src
    in
    fun t a -> write_gen t ~blit ~length ~off:0 ~len:2 a

  let write_u32 =
    let blit src src_off dst dst_off len =
      assert (src_off = 0);
      assert (len = 4);

      E.set_u32 dst dst_off src
    in
    fun t a -> write_gen t ~blit ~length ~off:0 ~len:4 a

  let write_u64 =
    let blit src src_off dst dst_off len =
      assert (src_off = 0);
      assert (len = 8);

      E.set_u64 dst dst_off src
    in
    fun t a -> write_gen t ~blit ~length ~off:0 ~len:8 a
end

module BE = Endian(BE'(B))
module LE = Endian(LE'(B))
module NE = Endian(NE'(B))

let write_int8 =
  fun t u ->
    let length a = assert false in

    let blit src src_off dst dst_off len =
      assert (src_off = 0);
      assert (len = 1);

      B.set dst dst_off src
    in
    write_gen t ~blit ~length ~off:0 ~len:1 u

let close t =
  t.close <- true;
  flush_buffer t

let is_close t = t.close

let yield t =
  t.yield <- true

let rec shift_buffers t written =
  match Dequeue.take_front t.sched with
  | { Vec.len; _ } as vec ->
    if len <= written
    then shift_buffers t (written - len)
    else Dequeue.push_front
           t.sched
           Vec.{ vec with off = vec.off + written
                        ; len = vec.len - written }
  | exception Dequeue.Empty ->
    t.sched_pos <- 0;
    t.write_pos <- 0

let rec shift_flushes t =
  match Dequeue.take_front t.flush with
  | (threshold, f) as flush ->
    if t.written >= threshold
    then begin f (); shift_flushes t end
    else Dequeue.push_front t.flush flush
  | exception Dequeue.Empty -> ()

let shift t written =
  shift_buffers t written;
  t.written <- t.written + written;
  shift_flushes t

type operation =
  [ `Write of Vec.t list
  | `Yield
  | `Close ]

let operation t =
  if t.close then t.yield <- false;

  flush_buffer t;

  let nothing_to_do = Dequeue.is_empty t.sched in

  if t.close && nothing_to_do
  then `Close
  else if t.yield || nothing_to_do
  then `Yield
  else
    let l = Dequeue.to_list t.sched in
    `Write l

let rec serialize t write =
  match operation t with
  | (`Close | `Yield) as next -> next
  | `Write l ->
    let () = match write l with
      | `Ok n ->
        shift t n;
        if not (Dequeue.is_empty t.sched)
        then yield t
      | `Close -> close t
    in serialize t write

let rec serialize_to_string t =
  close t;

  match operation t with
  | `Write l ->
    let len = List.fold_left (+) 0 (List.map Vec.length l) in
    let tmp = Bytes.create len in
    let pos = ref 0 in

    List.iter
      (function
       | { Vec.buf = `String s; off; len } ->
         Bytes.blit_string s off tmp !pos len;
         pos := !pos + len
       | { Vec.buf = `Bytes s; off; len } ->
         Bytes.blit s off tmp !pos len;
         pos := !pos + len
       | { Vec.buf = `Bigstring s; off; len } ->
         for i = 0 to len - 1
         do Bytes.set tmp (!pos + i) (B.Bigstring.get s (off + i)) done;
         pos := !pos + len)
      l;
    shift t len;
    assert (operation t = `Close);
    Bytes.unsafe_to_string tmp
  | `Close -> ""
  | `Yield -> assert false

let drain =
  let rec loop t acc =
    match operation t with
    | `Write l ->
      List.fold_left (+) 0 @@ List.map Vec.length l
    | `Close -> acc
    | `Yield -> loop t acc
  in

  fun t -> loop t 0
