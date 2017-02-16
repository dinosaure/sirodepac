module Bigstring =
struct
  open Bigarray

  type t = (char, int8_unsigned_elt, c_layout) Array1.t

  let length = Array1.dim
  let create = Array1.create Char c_layout
  let get    = Array1.get
  let set    = Array1.set
  let sub    = Array1.sub
  let fill   = Array1.fill
  let blit   = Array1.blit
  let copy v =
    let v' = create (length v) in
    Array1.blit v v'; v'

  external get_u16 : t -> int -> int     = "%caml_bigstring_get16u"
  external get_u32 : t -> int -> Int32.t = "%caml_bigstring_get32u"
  external get_u64 : t -> int -> Int64.t = "%caml_bigstring_get64u"

  let to_string v =
    let buf = Bytes.create (length v) in
    for i = 0 to length v - 1
    do Bytes.set buf i (get v i) done;
    Bytes.unsafe_to_string buf

  (** See [bs.c]. *)
  external blit
    : t -> int -> t -> int -> int -> unit
    = "bigstring_memcpy" [@@noalloc]

  let pp fmt ba =
    for i = 0 to length ba - 1
    do match get ba i with
       | '\000' .. '\031' | '\127' -> Format.pp_print_char fmt '.'
       | chr -> Format.pp_print_char fmt chr
    done

  let tpp fmt ba =
    for i = 0 to length ba - 1
    do Format.pp_print_char fmt (get ba i) done

  let rec _index bs len off chr =
    if off >= len
    then raise Not_found
    else if get bs off = chr then off
    else _index bs len (off + 1) chr

  let index_from bs off len chr =
    if off < 0 || off > len
    then raise (Invalid_argument "Bigstring.index_from")
    else _index bs len off chr

  let empty = create 0
end

module Bytes =
struct
  include Bytes

  external get_u16 : t -> int -> int     = "%caml_string_get16u"
  external get_u32 : t -> int -> Int32.t = "%caml_string_get32u"
  external get_u64 : t -> int -> Int64.t = "%caml_string_get64u"

  external blit
    : t -> int -> t -> int -> int -> unit
    = "bytes_memcpy" [@@noalloc]
  (** See [bs.c]. *)

  let pp fmt bs =
    for i = 0 to length bs - 1
    do match get bs i with
       | '\000' .. '\031' | '\127' -> Format.pp_print_char fmt '.'
       | chr -> Format.pp_print_char fmt chr
    done

  let tpp fmt bs =
    for i = 0 to length bs - 1
    do Format.pp_print_char fmt (get bs i) done

  let blit src src_off dst dst_off len =
    if len < 0 || src_off < 0 || src_off > Bytes.length src - len
               || dst_off < 0 || dst_off > Bytes.length dst - len
    then raise (Invalid_argument (Format.sprintf "Bytes.blit (src: %d:%d, \
                                                              dst: %d:%d, \
                                                              len: %d)"
                                    src_off (Bytes.length src)
                                    dst_off (Bytes.length dst)
                                    len))
    else blit src src_off dst dst_off len

  let rec _index bs len off chr =
    if off >= len
    then raise Not_found
    else if get bs off = chr then off
    else _index bs len (off + 1) chr

  let index_from bs off len chr =
    if off < 0 || off > len
    then raise (Invalid_argument "Bigstring.index_from")
    else _index bs len off chr
end

(* mandatory for a GADT *)
type st = St
type bs = Bs

type 'a t =
  | Bytes : Bytes.t -> st t
  | Bigstring : Bigstring.t -> bs t

let from_bytes v = Bytes v
let from_bigstring v = Bigstring v
let from
  : type a. proof:a t -> int -> a t
  = fun ~proof len -> match proof with
  | Bytes v -> Bytes (Bytes.create len)
  | Bigstring v -> Bigstring (Bigstring.create len)

let length : type a. a t -> int = function
  | Bytes v -> Bytes.length v
  | Bigstring v -> Bigstring.length v

let get : type a. a t -> int -> char = fun v idx -> match v with
  | Bytes v -> Bytes.get v idx
  | Bigstring v -> Bigstring.get v idx

let set : type a. a t -> int -> char -> unit = fun v idx chr -> match v with
  | Bytes v -> Bytes.set v idx chr
  | Bigstring v -> Bigstring.set v idx chr

let get_u16 : type a. a t -> int -> int = fun v idx -> match v with
  | Bytes v -> Bytes.get_u16 v idx
  | Bigstring v -> Bigstring.get_u16 v idx

let get_u32 : type a. a t -> int -> Int32.t = fun v idx -> match v with
  | Bytes v -> Bytes.get_u32 v idx
  | Bigstring v -> Bigstring.get_u32 v idx

let get_u64 : type a. a t -> int -> Int64.t = fun v idx -> match v with
  | Bytes v -> Bytes.get_u64 v idx
  | Bigstring v -> Bigstring.get_u64 v idx

let sub : type a. a t -> int -> int -> a t = fun v off len -> match v with
  | Bytes v -> Bytes.sub v off len |> from_bytes
  | Bigstring v -> Bigstring.sub v off len |> from_bigstring

let fill
  : type a. a t -> int -> int -> char -> unit
  = fun v off len chr -> match v with
  | Bytes v -> Bytes.fill v off len chr
  | Bigstring v -> Bigstring.fill (Bigstring.sub v off len) chr

let blit : type a. a t -> int -> a t -> int -> int -> unit =
  fun src src_idx dst dst_idx len -> match src, dst with
  | Bytes src, Bytes dst ->
    Bytes.blit src src_idx dst dst_idx len
  | Bigstring src, Bigstring dst ->
    Bigstring.blit src src_idx dst dst_idx len

let pp : type a. Format.formatter -> a t -> unit = fun fmt -> function
  | Bytes v -> Format.fprintf fmt "%a" Bytes.pp v
  | Bigstring v -> Format.fprintf fmt "%a" Bigstring.pp v

let tpp : type a. Format.formatter -> a t -> unit = fun fmt -> function
  | Bytes v -> Format.fprintf fmt "%a" Bytes.tpp v
  | Bigstring v -> Format.fprintf fmt "%a" Bigstring.tpp v

let to_string : type a. a t -> string = function
  | Bytes v -> Bytes.unsafe_to_string v
  | Bigstring v -> Bigstring.to_string v

external st_adler32 : int32 -> Bytes.t -> int -> int -> int32 =
  "bytes_adler32"
external bs_adler32 : int32 -> Bigstring.t -> int -> int -> int32 =
  "bigstring_adler32"

let adler32 : type a. a t -> int32 -> int -> int -> int32 = function
  | Bytes v -> fun i off len -> st_adler32 i v off len
  | Bigstring v -> fun i off len -> bs_adler32 i v off len

let empty : type a. a t -> a t = function
  | Bytes v -> Bytes Bytes.empty
  | Bigstring v -> Bigstring Bigstring.empty

let index_from : type a. a t -> int -> ?len:int -> char -> int = function
  | Bytes v ->
    fun off ?(len = Bytes.length v) chr ->
      Bytes.index_from v off len chr
  | Bigstring v ->
    fun off ?(len = Bigstring.length v) chr ->
      Bigstring.index_from v off len chr

let count_while v ?(init = 0) predicate =
  let i = ref init in
  let l = length v in

  while !i < l && predicate (get v !i) do incr i done;

  !i - init
