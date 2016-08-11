let () = Printexc.record_backtrace true

module B =
struct
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

    external get_u16 : t -> int -> int = "%caml_bigstring_get16u"
    external get_u32 : t -> int -> Int32.t = "%caml_bigstring_get32u"
    external get_u64 : t -> int -> Int64.t = "%caml_bigstring_get64u"

    let to_string v =
      let buf = Bytes.create (length v) in
      for i = 0 to length v - 1
      do Bytes.set buf i (get v i) done;
      Bytes.unsafe_to_string buf

    external blit : t -> int -> t -> int -> int -> unit = "bigstring_memcpy" [@@noalloc]
  end

  module Bytes =
  struct
    include Bytes

    external get_u16 : t -> int -> int = "%caml_string_get16u"
    external get_u32 : t -> int -> Int32.t = "%caml_string_get32u"
    external get_u64 : t -> int -> Int64.t = "%caml_string_get64u"
    external blit : t -> int -> t -> int -> int -> unit = "bytes_memcpy" [@@noalloc]
  end

  type st = St
  type bs = Bs

  type 'a t =
    | Bytes : Bytes.t -> st t
    | Bigstring : Bigstring.t -> bs t

  let from_bytes v = Bytes v
  let from_bigstring v = Bigstring v
  let from : type a. proof:a t -> int -> a t = fun ~proof len -> match proof with
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

  let fill : type a. a t -> int -> int -> char -> unit = fun v off len chr -> match v with
    | Bytes v -> Bytes.fill v off len chr
    | Bigstring v -> Bigstring.fill (Bigstring.sub v off len) chr

  let blit : type a. a t -> int -> a t -> int -> int -> unit =
    fun src src_idx dst dst_idx len -> match src, dst with
    | Bytes src, Bytes dst ->
      Bytes.blit src src_idx dst dst_idx len
    | Bigstring src, Bigstring dst ->
      Bigstring.blit src src_idx dst dst_idx len

  let pp : type a. Format.formatter -> a t -> unit = fun fmt -> function
    | Bytes v -> Format.fprintf fmt "%S" (Bytes.unsafe_to_string v)
    | Bigstring v -> Format.fprintf fmt "%S" (Bigstring.to_string v)
end

module Z =
struct
  let _extra_lbits =
    [| 0; 0; 0; 0; 0; 0; 0; 0; 1; 1; 1; 1; 2; 2; 2; 2; 3; 3; 3; 3; 4; 4; 4; 4;
       5; 5; 5; 5; 0 |]

  let _extra_dbits =
    [|  0; 0; 0; 0; 1; 1; 2; 2; 3; 3; 4; 4; 5; 5; 6; 6; 7; 7; 8; 8; 9; 9; 10;
       10; 11; 11; 12; 12; 13; 13 |]

  let _base_length =
    [|  0;  1;  2;  3;  4;  5;   6;   7;   8;  10;  12; 14; 16; 20; 24; 28; 32;
       40; 48; 56; 64; 80; 96; 112; 128; 160; 192; 224; 255 |]

  let _base_dist =
    [|    0;    1;    2;     3;     4;     6;   8;  12;   16;   24;   32;   48;
         64;   96;  128;   192;   256;   384; 512; 768; 1024; 1536; 2048; 3072;
       4096; 6144; 8192; 12288; 16384; 24576 |]

  let _length =
    [|  0;  1;  2;  3;  4;  5;  6;  7;  8;  8;  9;  9; 10; 10; 11; 11; 12; 12;
       12; 12; 13; 13; 13; 13; 14; 14; 14; 14; 15; 15; 15; 15; 16; 16; 16; 16;
       16; 16; 16; 16; 17; 17; 17; 17; 17; 17; 17; 17; 18; 18; 18; 18; 18; 18;
       18; 18; 19; 19; 19; 19; 19; 19; 19; 19; 20; 20; 20; 20; 20; 20; 20; 20;
       20; 20; 20; 20; 20; 20; 20; 20; 21; 21; 21; 21; 21; 21; 21; 21; 21; 21;
       21; 21; 21; 21; 21; 21; 22; 22; 22; 22; 22; 22; 22; 22; 22; 22; 22; 22;
       22; 22; 22; 22; 23; 23; 23; 23; 23; 23; 23; 23; 23; 23; 23; 23; 23; 23;
       23; 23; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24;
       24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 25; 25;
       25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 25;
       25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 26; 26; 26; 26; 26; 26;
       26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26;
       26; 26; 26; 26; 26; 26; 26; 26; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27;
       27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27;
       27; 27; 27; 28 |]

  let _distance =
    let t =
    [|  0;  1;  2;  3;  4;  4;  5;  5;  6;  6;  6;  6;  7;  7;  7;  7;  8;  8;
        8;  8;  8;  8;  8;  8;  9;  9;  9;  9;  9;  9;  9;  9; 10; 10; 10; 10;
       10; 10; 10; 10; 10; 10; 10; 10; 10; 10; 10; 10; 11; 11; 11; 11; 11; 11;
       11; 11; 11; 11; 11; 11; 11; 11; 11; 11; 12; 12; 12; 12; 12; 12; 12; 12;
       12; 12; 12; 12; 12; 12; 12; 12; 12; 12; 12; 12; 12; 12; 12; 12; 12; 12;
       12; 12; 12; 12; 12; 12; 13; 13; 13; 13; 13; 13; 13; 13; 13; 13; 13; 13;
       13; 13; 13; 13; 13; 13; 13; 13; 13; 13; 13; 13; 13; 13; 13; 13; 13; 13;
       13; 13; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14;
       14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14;
       14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14;
       14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 14; 15; 15; 15; 15; 15; 15;
       15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15;
       15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15;
       15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15; 15;
       15; 15; 15; 15;  0;  0; 16; 17; 18; 18; 19; 19; 20; 20; 20; 20; 21; 21;
       21; 21; 22; 22; 22; 22; 22; 22; 22; 22; 23; 23; 23; 23; 23; 23; 23; 23;
       24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 24; 25; 25;
       25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 25; 26; 26; 26; 26;
       26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 26;
       26; 26; 26; 26; 26; 26; 26; 26; 26; 26; 27; 27; 27; 27; 27; 27; 27; 27;
       27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27; 27;
       27; 27; 27; 27; 27; 27; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28;
       28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28;
       28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28;
       28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 28; 29; 29;
       29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29;
       29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29;
       29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29; 29;
       29; 29; 29; 29; 29; 29; 29; 29 |]
    in
    fun code ->
      if code < 256
      then Array.get t code
      else Array.get t (256 + (code lsr 7))

  let _static_ltree =
    [| ( 12,  8); (140,  8); ( 76,  8); (204,  8); ( 44,  8);
       (172,  8); (108,  8); (236,  8); ( 28,  8); (156,  8);
       ( 92,  8); (220,  8); ( 60,  8); (188,  8); (124,  8);
       (252,  8); (  2,  8); (130,  8); ( 66,  8); (194,  8);
       ( 34,  8); (162,  8); ( 98,  8); (226,  8); ( 18,  8);
       (146,  8); ( 82,  8); (210,  8); ( 50,  8); (178,  8);
       (114,  8); (242,  8); ( 10,  8); (138,  8); ( 74,  8);
       (202,  8); ( 42,  8); (170,  8); (106,  8); (234,  8);
       ( 26,  8); (154,  8); ( 90,  8); (218,  8); ( 58,  8);
       (186,  8); (122,  8); (250,  8); (  6,  8); (134,  8);
       ( 70,  8); (198,  8); ( 38,  8); (166,  8); (102,  8);
       (230,  8); ( 22,  8); (150,  8); ( 86,  8); (214,  8);
       ( 54,  8); (182,  8); (118,  8); (246,  8); ( 14,  8);
       (142,  8); ( 78,  8); (206,  8); ( 46,  8); (174,  8);
       (110,  8); (238,  8); ( 30,  8); (158,  8); ( 94,  8);
       (222,  8); ( 62,  8); (190,  8); (126,  8); (254,  8);
       (  1,  8); (129,  8); ( 65,  8); (193,  8); ( 33,  8);
       (161,  8); ( 97,  8); (225,  8); ( 17,  8); (145,  8);
       ( 81,  8); (209,  8); ( 49,  8); (177,  8); (113,  8);
       (241,  8); (  9,  8); (137,  8); ( 73,  8); (201,  8);
       ( 41,  8); (169,  8); (105,  8); (233,  8); ( 25,  8);
       (153,  8); ( 89,  8); (217,  8); ( 57,  8); (185,  8);
       (121,  8); (249,  8); (  5,  8); (133,  8); ( 69,  8);
       (197,  8); ( 37,  8); (165,  8); (101,  8); (229,  8);
       ( 21,  8); (149,  8); ( 85,  8); (213,  8); ( 53,  8);
       (181,  8); (117,  8); (245,  8); ( 13,  8); (141,  8);
       ( 77,  8); (205,  8); ( 45,  8); (173,  8); (109,  8);
       (237,  8); ( 29,  8); (157,  8); ( 93,  8); (221,  8);
       ( 61,  8); (189,  8); (125,  8); (253,  8); ( 19,  9);
       (275,  9); (147,  9); (403,  9); ( 83,  9); (339,  9);
       (211,  9); (467,  9); ( 51,  9); (307,  9); (179,  9);
       (435,  9); (115,  9); (371,  9); (243,  9); (499,  9);
       ( 11,  9); (267,  9); (139,  9); (395,  9); ( 75,  9);
       (331,  9); (203,  9); (459,  9); ( 43,  9); (299,  9);
       (171,  9); (427,  9); (107,  9); (363,  9); (235,  9);
       (491,  9); ( 27,  9); (283,  9); (155,  9); (411,  9);
       ( 91,  9); (347,  9); (219,  9); (475,  9); ( 59,  9);
       (315,  9); (187,  9); (443,  9); (123,  9); (379,  9);
       (251,  9); (507,  9); (  7,  9); (263,  9); (135,  9);
       (391,  9); ( 71,  9); (327,  9); (199,  9); (455,  9);
       ( 39,  9); (295,  9); (167,  9); (423,  9); (103,  9);
       (359,  9); (231,  9); (487,  9); ( 23,  9); (279,  9);
       (151,  9); (407,  9); ( 87,  9); (343,  9); (215,  9);
       (471,  9); ( 55,  9); (311,  9); (183,  9); (439,  9);
       (119,  9); (375,  9); (247,  9); (503,  9); ( 15,  9);
       (271,  9); (143,  9); (399,  9); ( 79,  9); (335,  9);
       (207,  9); (463,  9); ( 47,  9); (303,  9); (175,  9);
       (431,  9); (111,  9); (367,  9); (239,  9); (495,  9);
       ( 31,  9); (287,  9); (159,  9); (415,  9); ( 95,  9);
       (351,  9); (223,  9); (479,  9); ( 63,  9); (319,  9);
       (191,  9); (447,  9); (127,  9); (383,  9); (255,  9);
       (511,  9); (  0,  7); ( 64,  7); ( 32,  7); ( 96,  7);
       ( 16,  7); ( 80,  7); ( 48,  7); (112,  7); (  8,  7);
       ( 72,  7); ( 40,  7); (104,  7); ( 24,  7); ( 88,  7);
       ( 56,  7); (120,  7); (  4,  7); ( 68,  7); ( 36,  7);
       (100,  7); ( 20,  7); ( 84,  7); ( 52,  7); (116,  7);
       (  3,  8); (131,  8); ( 67,  8); (195,  8); ( 35,  8);
       (163,  8); ( 99,  8); (227,  8) |]

  let _static_dtree =
    [| ( 0, 5); (16, 5); ( 8, 5); (24, 5); ( 4, 5);
       (20, 5); (12, 5); (28, 5); ( 2, 5); (18, 5);
       (10, 5); (26, 5); ( 6, 5); (22, 5); (14, 5);
       (30, 5); ( 1, 5); (17, 5); ( 9, 5); (25, 5);
       ( 5, 5); (21, 5); (13, 5); (29, 5); ( 3, 5);
       (19, 5); (11, 5); (27, 5); ( 7, 5); (23, 5) |]

  let hclen_order =
    [| 16; 17; 18; 0; 8; 7; 9; 6; 10; 5; 11; 4; 12; 3; 13; 2; 14; 1; 15 |]

  module Adler32 =
  struct
    type t = Int32.t

    let default = 1l
    let update buf off len crc = crc
    let atom chr crc = crc
    let fill chr len crc = crc
    let make a b = 1l
    let eq a b = a = b
    let neq a b = not (eq a b)
  end

  module Heap =
  struct
    type priority = int
    type 'a queue = Empty | Node of priority * 'a * 'a queue * 'a queue

    let empty = Empty

    let rec push queue priority elt =
      match queue with
      | Empty -> Node (priority, elt, Empty, Empty)
      | Node (p, e, left, right) ->
        if priority <= p
        then Node (priority, elt, push right p e, left)
        else Node (p, e, push right priority elt, left)

    exception Empty_heap

    let rec remove = function
      | Empty -> raise Empty_heap
      | Node (p, e, left, Empty)  -> left
      | Node (p, e, Empty, right) -> right
      | Node (p, e, (Node (lp, le, _, _) as left),
                    (Node (rp, re, _, _) as right)) ->
        if lp <= rp
        then Node (lp, le, remove left, right)
        else Node (rp, re, left, remove right)

    let take = function
      | Empty -> raise Empty_heap
      | Node (p, e, _, _) as queue -> (p, e, remove queue)
  end

  module Huffman =
  struct
    exception Invalid_huffman

    let prefix heap max =
      let tbl = Array.make (1 lsl max) (0, 0) in

      let rec backward huff incr =
        if huff land incr <> 0
        then backward huff (incr lsr 1)
        else incr
      in

      let rec aux huff heap = match Heap.take heap with
        | bits, (len, value), heap ->
          let rec loop decr fill =
            Array.set tbl (huff + fill) (len, value);
            if fill <> 0 then loop decr (fill - decr)
          in

          let decr = 1 lsl len in
          loop decr ((1 lsl max) - decr);

          let incr = backward huff (1 lsl (len - 1)) in

          aux (if incr <> 0 then (huff land (incr - 1)) + incr else 0) heap
        | exception Heap.Empty_heap -> ()
      in

      aux 0 heap; tbl

    let make table position size max_bits =
      let bl_count = Array.make (max_bits + 1) 0 in

      for i = 0 to size - 1 do
        let p = Array.get table (i + position) in

        if p >= (max_bits + 1) then raise Invalid_huffman;

        Array.set bl_count p (Array.get bl_count p + 1);
      done;

      let code = ref 0 in
      let next_code = Array.make (max_bits + 1) 0 in

      for i = 1 to max_bits - 1 do
        code := (!code + Array.get bl_count i) lsl 1;
        Array.set next_code i !code;
      done;

      let ordered = ref Heap.Empty in
      let max  = ref 0 in

      for i = 0 to size - 1 do
        let l = Array.get table (i + position) in

        if l <> 0 then begin
          let n = Array.get next_code (l - 1) in
          Array.set next_code (l - 1) (n + 1);
          ordered := Heap.push !ordered n (l, i);
          max     := if l > !max then l else !max;
        end;
      done;

      prefix !ordered !max, !max
  end

  module Window =
  struct
    type 'a t =
      { rpos   : int
      ; wpos   : int
      ; size   : int
      ; buffer : 'a B.t
      ; crc    : Adler32.t }

    let make_by ~proof size =
      { rpos   = 0
      ; wpos   = 0
      ; size   = size + 1
      ; buffer = B.from ~proof (size + 1)
      ; crc    = Adler32.default }

    let available_to_write { wpos; rpos; size; _ } =
      if wpos >= rpos then size - (wpos - rpos) - 1
      else rpos - wpos - 1

    let drop n ({ rpos; size; _ } as t) =
      { t with rpos = if rpos + n < size then rpos + n
                      else rpos + n - size }

    let move n ({ wpos; size; _ } as t) =
      { t with wpos = if wpos + n < size then wpos + n
                      else wpos + n - size }

    let write_ro buf off len t =
      let t = if len > available_to_write t
              then drop (len - (available_to_write t)) t
              else t in

      let pre = t.size - t.wpos in
      let extra = len - pre in

      if extra > 0 then begin
        B.blit buf off t.buffer t.wpos pre;
        B.blit buf (off + pre) t.buffer 0 extra;
      end else
        B.blit buf off t.buffer t.wpos len;

      move len { t with crc = Adler32.update buf off len t.crc }

    let write_rw buf off len t =
      let t = if len > available_to_write t
              then drop (len - (available_to_write t)) t
              else t in

      let pre = t.size - t.wpos in
      let extra = len - pre in

      if extra > 0 then begin
        B.blit buf off t.buffer t.wpos pre;
        B.blit buf (off + pre) t.buffer 0 extra;
      end else
        B.blit buf off t.buffer t.wpos len;

      move len t

    let write_char chr t =
      let t = if 1 > available_to_write t
              then drop (1 - (available_to_write t)) t
              else t in

      B.set t.buffer t.wpos chr;

      move 1 { t with crc = Adler32.atom chr t.crc }

    let fill_char chr len t =
      let t = if len > available_to_write t
              then drop (len - (available_to_write t)) t
              else t in

      let pre = t.size - t.wpos in
      let extra = len - pre in

      if extra > 0 then begin
        B.fill t.buffer t.wpos pre chr;
        B.fill t.buffer 0 extra chr;
      end else
        B.fill t.buffer t.wpos len chr;

      move len { t with crc = Adler32.fill chr len t.crc }

    let rec sanitize n ({ size; _ } as t) =
      if n < 0 then sanitize (size + n) t
      else if n >= 0 && n < size then n
      else sanitize (n - size) t

    let ( % ) n t = sanitize n t

    let checksum { crc; _ } = crc
  end

  type error = ..
  type error += Invalid_kind_of_block
  type error += Invalid_complement_of_length
  type error += Invalid_dictionary
  type error += Invalid_crc

  let pp = Format.fprintf
  let pp_error fmt = function
    | Invalid_kind_of_block -> pp fmt "Invalid_kind_of_block"
    | Invalid_complement_of_length -> pp fmt "Invalid_complete_of_length"
    | Invalid_dictionary -> pp fmt "Invalid_dictionary"
    | Invalid_crc -> pp fmt "Invalid_crc"
    | _ -> pp fmt "<error>"

  let reverse_bits =
    let t =
      [| 0x00; 0x80; 0x40; 0xC0; 0x20; 0xA0; 0x60; 0xE0; 0x10; 0x90; 0x50; 0xD0;
         0x30; 0xB0; 0x70; 0xF0; 0x08; 0x88; 0x48; 0xC8; 0x28; 0xA8; 0x68; 0xE8;
         0x18; 0x98; 0x58; 0xD8; 0x38; 0xB8; 0x78; 0xF8; 0x04; 0x84; 0x44; 0xC4;
         0x24; 0xA4; 0x64; 0xE4; 0x14; 0x94; 0x54; 0xD4; 0x34; 0xB4; 0x74; 0xF4;
         0x0C; 0x8C; 0x4C; 0xCC; 0x2C; 0xAC; 0x6C; 0xEC; 0x1C; 0x9C; 0x5C; 0xDC;
         0x3C; 0xBC; 0x7C; 0xFC; 0x02; 0x82; 0x42; 0xC2; 0x22; 0xA2; 0x62; 0xE2;
         0x12; 0x92; 0x52; 0xD2; 0x32; 0xB2; 0x72; 0xF2; 0x0A; 0x8A; 0x4A; 0xCA;
         0x2A; 0xAA; 0x6A; 0xEA; 0x1A; 0x9A; 0x5A; 0xDA; 0x3A; 0xBA; 0x7A; 0xFA;
         0x06; 0x86; 0x46; 0xC6; 0x26; 0xA6; 0x66; 0xE6; 0x16; 0x96; 0x56; 0xD6;
         0x36; 0xB6; 0x76; 0xF6; 0x0E; 0x8E; 0x4E; 0xCE; 0x2E; 0xAE; 0x6E; 0xEE;
         0x1E; 0x9E; 0x5E; 0xDE; 0x3E; 0xBE; 0x7E; 0xFE; 0x01; 0x81; 0x41; 0xC1;
         0x21; 0xA1; 0x61; 0xE1; 0x11; 0x91; 0x51; 0xD1; 0x31; 0xB1; 0x71; 0xF1;
         0x09; 0x89; 0x49; 0xC9; 0x29; 0xA9; 0x69; 0xE9; 0x19; 0x99; 0x59; 0xD9;
         0x39; 0xB9; 0x79; 0xF9; 0x05; 0x85; 0x45; 0xC5; 0x25; 0xA5; 0x65; 0xE5;
         0x15; 0x95; 0x55; 0xD5; 0x35; 0xB5; 0x75; 0xF5; 0x0D; 0x8D; 0x4D; 0xCD;
         0x2D; 0xAD; 0x6D; 0xED; 0x1D; 0x9D; 0x5D; 0xDD; 0x3D; 0xBD; 0x7D; 0xFD;
         0x03; 0x83; 0x43; 0xC3; 0x23; 0xA3; 0x63; 0xE3; 0x13; 0x93; 0x53; 0xD3;
         0x33; 0xB3; 0x73; 0xF3; 0x0B; 0x8B; 0x4B; 0xCB; 0x2B; 0xAB; 0x6B; 0xEB;
         0x1B; 0x9B; 0x5B; 0xDB; 0x3B; 0xBB; 0x7B; 0xFB; 0x07; 0x87; 0x47; 0xC7;
         0x27; 0xA7; 0x67; 0xE7; 0x17; 0x97; 0x57; 0xD7; 0x37; 0xB7; 0x77; 0xF7;
         0x0F; 0x8F; 0x4F; 0xCF; 0x2F; 0xAF; 0x6F; 0xEF; 0x1F; 0x9F; 0x5F; 0xDF;
         0x3F; 0xBF; 0x7F; 0xFF |]
    in
    fun bits -> t.(bits)

  module Lookup =
  struct
    type t =
      { table : (int * int) array
      ; max   : int
      ; mask  : int }

    let make table max =
      { table; max; mask = (1 lsl max) - 1; }

    let fixed_chr =
      let tbl =
        Array.init 288
          (fun n -> if n < 144 then 8
                    else if n < 256 then 9
                    else if n < 280 then 7
                    else 8)
      in
      let tbl, max = Huffman.make tbl 0 288 9 in
      make tbl max

    let fixed_dst =
      let tbl = Array.make (1 lsl 5) (0, 0) in
      Array.iteri (fun i _ -> Array.set tbl i (5, reverse_bits (i lsl 3))) tbl;
      make tbl 5
  end

  type ('i, 'o) t =
    { last  : bool
    ; hold  : int
    ; bits  : int
    ; o_off : int
    ; o_pos : int
    ; o_len : int
    ; i_off : int
    ; i_pos : int
    ; i_len : int
    ; write : int
    ; state : ('i, 'o) state }
  and ('i, 'o) state =
    | Header
    | Last       of 'o Window.t
    | Block      of 'o Window.t
    | Flat       of ('i B.t -> 'o B.t -> ('i, 'o) t -> ('i, 'o) res)
    | Fixed      of 'o Window.t
    | Dictionary of ('i B.t -> 'o B.t -> ('i, 'o) t -> ('i, 'o) res)
    | Inffast    of ('o Window.t * Lookup.t * Lookup.t * code)
    | Inflate    of ('i B.t -> 'o B.t -> ('i, 'o) t -> ('i, 'o) res)
    | Switch     of 'o Window.t
    | Crc        of ('i B.t -> 'o B.t -> ('i, 'o) t -> ('i, 'o) res)
    | Exception  of error
  and ('i, 'o) res =
    | Cont  of ('i, 'o) t
    | Wait  of ('i, 'o) t
    | Flush of ('i, 'o) t
    | Ok    of ('i, 'o) t
    | Error of ('i, 'o) t * error
  and code =
    | Length
    | ExtLength of int
    | Dist      of int
    | ExtDist   of int * int
    | Write     of int * int

  let bin_of_int d =
    if d < 0 then invalid_arg "bin_of_int" else
    if d = 0 then "0" else
    let rec aux acc d =
      if d = 0 then acc else
      aux (string_of_int (d land 1) :: acc) (d lsr 1)
    in
    String.concat "" (aux [] d)

  let pp fmt { last; hold; bits
             ; o_off; o_pos; o_len
             ; i_off; i_pos; i_len; write
             ; state } =
    pp fmt "{@[<hov>last = %b;@ \
                    hold = %s;@ \
                    bits = %d;@ \
                    o_off = %d;@ \
                    o_pos = %d;@ \
                    o_len = %d;@ \
                    i_off = %d;@ \
                    i_pos = %d;@ \
                    i_len = %d;@ \
                    write = %d;@]}"
      last (bin_of_int hold) bits
      o_off o_pos o_len i_off i_pos i_len write

  let error t exn =
    Error ({ t with state = Exception exn }, exn)

  (* Continuation passing-style stored in [Dictionary] *)
  module KDictionary =
  struct
    let rec get_byte k src dst t =
      if t.i_len - t.i_pos > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src dst
             { t with i_pos = t.i_pos + 1 }
      else Wait { t with state = Dictionary (get_byte k) }

    let peek_bits n k src dst t =
      let rec loop src dst t =
        if t.bits < n
        then get_byte (fun byte src dst t ->
                         (loop[@tailcall])
                         src dst
                         { t with hold = t.hold lor (byte lsl t.bits)
                                ; bits = t.bits + 8 })
                      src dst t
        else k src dst t
      in (loop[@tailcall]) src dst t

    let drop_bits n k src dst t =
      k src dst
        { t with hold = t.hold lsr n
               ; bits = t.bits - n }

    let get_bits n k src dst t =
      let catch src dst t =
        let value = t.hold land ((1 lsl n) - 1) in

        k value src dst { t with hold = t.hold lsr n
                               ; bits = t.bits - n }
      in
      let rec loop src dst t =
        if t.bits < n
        then get_byte (fun byte src dst t ->
                         (loop[@tailcall])
                         src dst
                         { t with hold = t.hold lor (byte lsl t.bits)
                                ; bits = t.bits + 8 })
                      src dst t
        else catch src dst t
      in (loop[@tailcall]) src dst t
  end

  (* Continuation passing-style stored in [Flat] *)
  module KFlat =
  struct
    let rec get_byte k src dst t =
      if t.i_len - t.i_pos > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src dst
             { t with i_pos = t.i_pos + 1 }
      else Wait { t with state = Flat (get_byte k) }

    let get_ui16 k =
      get_byte
      @@ fun byte0 -> get_byte
      @@ fun byte1 -> k (byte0 lor (byte1 lsl 8))
  end

  (* Continuation passing-style stored in [Inflate] *)
  module KInflate =
  struct
    let rec get lookup k src dst t =
      if t.bits < lookup.Lookup.max
      then
        if t.i_len - t.i_pos > 0
        then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
        (get[@tailcall]) lookup k src dst
          { t with i_pos = t.i_pos + 1
                 ; hold  = t.hold lor (byte lsl t.bits)
                 ; bits  = t.bits + 8 }
        else Wait { t with state = Inflate (get lookup k) }
      else let (len, v) = Array.get
             lookup.Lookup.table (t.hold land lookup.Lookup.mask) in
           k v src dst { t with hold = t.hold lsr len
                              ; bits = t.bits - len }

    let rec put_chr window chr k src dst t =
      if t.o_len - t.o_pos > 0
      then begin
        let window = Window.write_char chr window in
        B.set dst (t.o_off + t.o_pos) chr;

        k window src dst { t with o_pos = t.o_pos + 1 }
      end else Flush { t with state = Inflate (put_chr window chr k) }

    let rec fill_chr window length chr k src dst t =
      if t.o_len - t.o_pos > 0
      then begin
        let len = min length (t.o_len - t.o_pos) in

        let window = Window.fill_char chr len window in
        B.fill dst (t.o_off + t.o_pos) len chr;

        if length - len > 0
        then Flush
          { t with o_pos = t.o_pos + len
                 ; state = Inflate (fill_chr window (length - len) chr k) }
        else k window src dst { t with o_pos = t.o_pos + len }
      end else Flush { t with state = Inflate (fill_chr window length chr k) }

    let rec write window lookup_chr lookup_dst length distance k src dst t =
      match distance with
      | 1 ->
        let chr = B.get window.Window.buffer
          Window.((window.wpos - 1) % window) in

        fill_chr window length chr k src dst t
      | distance ->
        let len = min (t.o_len - t.o_pos) length in
        let off = Window.((window.wpos - distance) % window) in
        let sze = window.Window.size in

        let pre = sze - off in
        let ext = len - pre in

        let window =
          if ext > 0
          then begin
            let window0 = Window.write_rw window.Window.buffer off pre window in
            B.blit   window0.Window.buffer off dst (t.o_off + t.o_pos) pre;
            let window1 = Window.write_rw window0.Window.buffer 0 ext window0 in
            B.blit   window1.Window.buffer 0 dst (t.o_off + t.o_pos + pre) ext;
            window1
          end else begin
            let window0 = Window.write_rw window.Window.buffer off len window in
            B.blit   window0.Window.buffer off dst (t.o_off + t.o_pos) len;
            window0
          end
        in

        if length - len > 0
        then Flush
          { t with o_pos = t.o_pos + len
                 ; write = t.write + len
                 ; state = Inflate (write window lookup_chr lookup_dst (length - len) distance k) }
        else Cont
          { t with o_pos = t.o_pos + len
                 ; write = t.write + len
                 ; state = Inffast (window, lookup_chr, lookup_dst, Length) }

    let rec read_extra_dist distance k src dst t =
      let len = Array.get _extra_dbits distance in

      if t.bits < len
      then if t.i_len - t.i_pos > 0
           then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
                read_extra_dist
                  distance k
                  src dst
                  { t with hold = t.hold lor (byte lsl t.bits)
                         ; bits = t.bits + 8
                         ; i_pos = t.i_pos + 1 }
           else Wait
             { t with state = Inflate (read_extra_dist distance k) }
      else let extra = t.hold land ((1 lsl len) - 1) in
           k (Array.get _base_dist distance + 1 + extra) src dst
             { t with hold = t.hold lsr len
                    ; bits = t.bits - len }

    let rec read_extra_length length k src dst t =
      let len = Array.get _extra_lbits length in

      if t.bits < len
      then if t.i_len - t.i_pos > 0
           then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
                read_extra_length
                  length k
                  src dst
                  { t with hold = t.hold lor (byte lsl t.bits)
                         ; bits = t.bits + 8
                         ; i_pos = t.i_pos + 1 }
           else Wait
             { t with state = Inflate (read_extra_length length k) }
      else let extra = t.hold land ((1 lsl len) - 1) in
           k ((Array.get _base_length length) + 3 + extra) src dst
             { t with hold = t.hold lsr len
                    ; bits = t.bits - len }
  end

  (* Continuation passing-style stored in [Crc] *)
  module KCrc =
  struct
    let drop_bits n k src dst t =
      k src dst { t with hold = t.hold lsr n
                       ; bits = t.bits - n }

    let rec get_byte k src dst t =
      if t.bits / 8 > 0
      then let byte = t.hold land 255 in
           k byte src dst { t with hold = t.hold lsr 8
                                 ; bits = t.bits - 8 }
      else if t.i_len - t.i_pos > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
            k byte src dst
              { t with i_pos = t.i_pos + 1 }
      else Wait { t with state = Crc (get_byte k) }
  end

  (* Dictionary *)
  module Dictionary =
  struct
    type t =
      { idx        : int
      ; prv        : int
      ; max        : int
      ; dictionary : int array }

    let make max =
      { idx = 0
      ; prv = 0
      ; max
      ; dictionary = Array.make max 0 }

    let inflate (tbl, max_bits, max) k src dst t =
      let mask_bits = (1 lsl max_bits) - 1 in

      let rec get k src dst t =
        if t.bits < max_bits
        then KDictionary.peek_bits max_bits
               (fun src dst t -> (get[@tailcall]) k src dst t) src dst t
        else let (len, v) = Array.get tbl (t.hold land mask_bits) in
             KDictionary.drop_bits len (k v) src dst t
      in

      let rec loop state value src dst t = match value with
        | n when n <= 15 ->
          Array.set state.dictionary state.idx n;

          if state.idx + 1 < state.max
          then get (fun src dst t -> (loop[@tailcall])
                     { state with idx = state.idx + 1
                                ; prv = n }
                     src dst t) src dst t
          else k state.dictionary src dst t
        | 16 ->
          let aux n src dst t =
            if state.idx + n + 3 > state.max
            then error t Invalid_dictionary
            else begin
              for j = 0 to n + 3 - 1
              do Array.set state.dictionary (state.idx + j) state.prv done;

              if state.idx + n + 3 < state.max
              then get (fun src dst t -> (loop[@tailcall])
                         { state with idx = state.idx + n + 3 }
                         src dst t) src dst t
              else k state.dictionary src dst t
            end
          in

          KDictionary.get_bits 2 aux src dst t
        | 17 ->
          let aux n src dst t =
            if state.idx + n + 3 > state.max
            then error t Invalid_dictionary
            else begin
              if state.idx + n + 3 < state.max
              then get (fun src dst t -> (loop[@tailcall])
                         { state with idx = state.idx + n + 3 }
                         src dst t) src dst t
              else k state.dictionary src dst t
            end
          in

          KDictionary.get_bits 3 aux src dst t
        | 18 ->
          let aux n src dst t =
            if state.idx + n + 11 > state.max
            then error t Invalid_dictionary
            else begin
              if state.idx + n + 11 < state.max
              then get ((loop[@tailclal])
                        { state with idx = state.idx + n + 11 }) src dst t
              else k state.dictionary src dst t
            end
          in

          KDictionary.get_bits 7 aux src dst t
        | _ -> error t Invalid_dictionary
      in

      get (fun src dst t -> (loop[@tailcall]) (make max) src dst t) src dst t
  end

  let fixed src dst t window =
    Cont { t with state = Inffast (window, Lookup.fixed_chr, Lookup.fixed_dst, Length) }

  let dictionary window src dst t =
    let make_table hlit hdist hclen buf src dst t =
      let tbl, max = Huffman.make buf 0 19 7 in

      Dictionary.inflate (tbl, max, hlit + hdist)
        (fun dict src dst t ->
         let tbl_chr, max_chr = Huffman.make dict 0 hlit 15 in
         let tbl_dst, max_dst = Huffman.make dict hlit hdist 15 in

         Cont { t with state = Inffast (window,
                                        Lookup.make tbl_chr max_chr,
                                        Lookup.make tbl_dst max_dst,
                                        Length) })
        src dst t
    in

    let read_table hlit hdist hclen src dst t =
      let buf = Array.make 19 0 in

      let rec loop idx code src dst t =
        Array.set buf (Array.get hclen_order idx) code;

        if idx + 1 = hclen
        then begin
          for i = hclen to 18
          do Array.set buf (Array.get hclen_order i) 0 done;

          make_table hlit hdist hclen buf src dst t
        end else
          KDictionary.get_bits 3
            (fun src dst t -> (loop[@tailcall]) (idx + 1) src dst t) src dst t
      in

      KDictionary.get_bits 3
        (fun src dst t -> (loop[@tailcall]) 0 src dst t)
        src dst t
    in

    let read_hclen hlit hdist = KDictionary.get_bits 4
      (fun hclen -> read_table hlit hdist (hclen + 4)) in
    let read_hdist hlit       = KDictionary.get_bits 5
      (fun hdist -> read_hclen hlit (hdist + 1)) in
    let read_hlit             = KDictionary.get_bits 5
      (fun hlit  -> read_hdist (hlit + 257)) in

    read_hlit src dst t

  let rec ok src dst t =
    Ok { t with state = Crc ok }

  let crc window src dst t =
    let crc = Window.checksum window in

    (KCrc.drop_bits (t.bits mod 8)
     @@ KCrc.get_byte
     @@ fun a1 -> KCrc.get_byte
     @@ fun a2 -> KCrc.get_byte
     @@ fun b1 -> KCrc.get_byte
     @@ fun b2 src dst t ->
       if Adler32.neq (Adler32.make ((a1 lsl 8) lor a2) ((b1 lsl 8) lor b2)) crc
       then ok src dst t
       else ok src dst t) src dst t

  let switch src dst t window =
    if t.last
    then Cont { t with state = Crc (crc window) }
    else Cont { t with state = Last window }

  let flat window src dst t =
    let rec loop window length src dst t =
      let n = min length (min (t.i_len - t.i_pos) (t.o_len - t.o_pos)) in

      let window = Window.write_ro src (t.i_off + t.i_pos) n window in
      B.blit src (t.i_off + t.i_pos) dst (t.o_off + t.o_pos) n;

      if length - n = 0
      then Cont  { t with i_pos = t.i_pos + n
                        ; o_pos = t.o_pos + n
                        ; state = Switch window }
      else match t.i_len - (t.i_pos + n), t.o_len - (t.o_pos + n) with
      | 0, b ->
        Wait  { t with i_pos = t.i_pos + n
                     ; o_pos = t.o_pos + n
                     ; state = Flat (loop window (length - n)) }
      | a, 0 ->
        Flush { t with i_pos = t.i_pos + n
                     ; o_pos = t.o_pos + n
                     ; state = Flat (loop window (length - n)) }
      | a, b ->
        Cont { t with i_pos = t.i_pos + n
                    ; o_pos = t.o_pos + n
                    ; state = Flat (loop window (length - n)) }
    in

    let header window len nlen src dst t =
      if nlen <> 0xFFFF - len
      then Cont { t with state = Exception Invalid_complement_of_length }
      else Cont { t with hold  = 0
                       ; bits  = 0
                       ; state = Flat (loop window len) }
    in

    (KFlat.get_ui16
     @@ fun len -> KFlat.get_ui16
     @@ fun nlen -> header window len nlen)
    src dst t

  let rec inflate window lookup_chr lookup_dst src dst t =
    let rec loop window length src dst t = match length with
      | literal when literal < 256 ->
        KInflate.put_chr window (Char.chr literal)
          (fun window src dst t -> KInflate.get lookup_chr
            (fun length src dst t -> (loop[@tailcall]) window length src dst t)
            src dst t)
          src dst t
      | 256 ->
        Cont { t with state = Switch window }
      | length ->
        (* Party-hard *)
        KInflate.read_extra_length (length - 257)
          (fun length src dst t -> KInflate.get lookup_dst
            (fun distance src dst t -> KInflate.read_extra_dist distance
              (fun distance src dst t -> KInflate.write
                window lookup_chr lookup_dst length distance
                (fun window src dst t -> (inflate[@tailcall])
                  window lookup_chr lookup_dst src dst t)
                src dst t)
              src dst t)
            src dst t)
          src dst t
    in

    KInflate.get
      lookup_chr
      (fun length src dst t -> (loop[@tailcall]) window length src dst t)
      src dst t

  exception End

  let inffast src dst t window lookup_chr lookup_dst goto =
    let hold = ref t.hold in
    let bits = ref t.bits in

    let goto = ref goto   in

    let i_pos = ref t.i_pos in

    let o_pos = ref t.o_pos in
    let write = ref t.write in

    let window = ref window in

    try
      while t.i_len - !i_pos > 1 && t.o_len - !o_pos > 0
      do match !goto with
         | Length ->
           if !bits < lookup_chr.Lookup.max
           then begin
             hold := !hold lor ((Char.code @@ B.get src (t.i_off + !i_pos)) lsl !bits);
             bits := !bits + 8;
             incr i_pos;
             hold := !hold lor ((Char.code @@ B.get src (t.i_off + !i_pos)) lsl !bits);
             bits := !bits + 8;
             incr i_pos;
           end;

           let (len, value) = Array.get lookup_chr.Lookup.table (!hold land lookup_chr.Lookup.mask) in

           hold := !hold lsr len;
           bits := !bits - len;

           if value < 256
           then begin
             B.set dst (t.o_off + !o_pos) (Char.chr value);
             window := Window.write_char (Char.chr value) !window;
             incr o_pos;
             incr write;

             goto := Length;
           end else if value = 256 then begin raise End
           end else begin
             goto := ExtLength (value - 257)
           end
         | ExtLength length ->
           let len = Array.get _extra_lbits length in

           if !bits < len
           then begin
             hold := !hold lor ((Char.code @@ B.get src (t.i_off + !i_pos)) lsl !bits);
             bits := !bits + 8;
             incr i_pos;
           end;

           let extra = !hold land ((1 lsl len) - 1) in

           hold := !hold lsr len;
           bits := !bits - len;
           goto := Dist ((Array.get _base_length length) + 3 + extra)
         | Dist length ->
           if !bits < lookup_dst.Lookup.max
           then begin
             hold := !hold lor ((Char.code @@ B.get src (t.i_off + !i_pos)) lsl !bits);
             bits := !bits + 8;
             incr i_pos;
             hold := !hold lor ((Char.code @@ B.get src (t.i_off + !i_pos)) lsl !bits);
             bits := !bits + 8;
             incr i_pos;
           end;

           let (len, value) = Array.get lookup_dst.Lookup.table (!hold land lookup_dst.Lookup.mask) in

           hold := !hold lsr len;
           bits := !bits - len;
           goto := ExtDist (length, value)
         | ExtDist (length, dist) ->
           let len = Array.get _extra_dbits dist in

           if !bits < len
           then begin
             hold := !hold lor ((Char.code @@ B.get src (t.i_off + !i_pos)) lsl !bits);
             bits := !bits + 8;
             incr i_pos;
             hold := !hold lor ((Char.code @@ B.get src (t.i_off + !i_pos)) lsl !bits);
             bits := !bits + 8;
             incr i_pos;
           end;

           let extra = !hold land ((1 lsl len) - 1) in

           hold := !hold lsr len;
           bits := !bits - len;
           goto := Write (length, (Array.get _base_dist dist) + 1 + extra)
         | Write (length, 1) ->

           let chr = B.get !window.Window.buffer
             Window.((!window.wpos - 1) % !window) in

           let n = min length (t.o_len - !o_pos) in

           window := Window.fill_char chr n !window;
           B.fill dst (t.o_off + !o_pos) n chr;

           o_pos := !o_pos + n;
           write := !write + n;
           goto  := if length - n = 0 then Length else Write (length - n, 1)
         | Write (length, dist) ->
           let n = min length (t.o_len - !o_pos) in

           let off = Window.((!window.Window.wpos - dist) % !window) in
           let len = !window.Window.size in

           let pre = len - off in
           let ext = n - pre in

           window := if ext > 0
             then begin
               let window0 = Window.write_rw !window.Window.buffer off pre !window in
               B.blit   window0.Window.buffer off dst (t.o_off + !o_pos) pre;
               let window1 = Window.write_rw window0.Window.buffer 0 ext window0 in
               B.blit   window1.Window.buffer 0 dst (t.o_off + !o_pos + pre) ext;
               window1
             end else begin
               let window0 = Window.write_rw !window.Window.buffer off n !window in
               B.blit   window0.Window.buffer off dst (t.o_off + !o_pos) n;
               window0
             end;

           o_pos := !o_pos + n;
           write := !write + n;
           goto  := if length - n = 0 then Length else Write (length - n, dist)
      done;

      let write_fn length distance src dst t =
        KInflate.write !window lookup_chr lookup_dst length distance
          (fun window src dst t -> inflate window lookup_chr lookup_dst src dst t)
          src dst t
      in

      let state = match !goto with
        | Length ->
          Inflate (inflate !window lookup_chr lookup_dst)
        | ExtLength length ->
          let fn length src dst t =
            KInflate.read_extra_length length
              (fun length src dst t -> KInflate.get lookup_dst
                (fun distance src dst t -> KInflate.read_extra_dist distance
                   (fun distance src dst t -> write_fn length distance src dst t)
                   src dst t)
                src dst t)
              src dst t
          in

          Inflate (fn length)
        | Dist length ->
          let fn length src dst t =
            KInflate.get lookup_dst
              (fun distance src dst t -> KInflate.read_extra_dist distance
                (fun distance src dst t -> write_fn length distance src dst t)
                src dst t)
              src dst t
          in

          Inflate (fn length)
        | ExtDist (length, distance) ->
          let fn length distance src dst t =
            KInflate.read_extra_dist distance
              (fun distance src dst t -> write_fn length distance src dst t)
              src dst t
          in

          Inflate (fn length distance)
        | Write (length, distance) ->
          let fn length distance src dst t = write_fn length distance src dst t in

          Inflate (fn length distance)
      in

      Cont { t with hold = !hold
                  ; bits = !bits
                  ; i_pos = !i_pos
                  ; o_pos = !o_pos
                  ; write = !write
                  ; state = state }
    with End ->
      Cont { t with hold = !hold
                  ; bits = !bits
                  ; i_pos = !i_pos
                  ; o_pos = !o_pos
                  ; write = !write
                  ; state = Switch !window }

  let block src dst t window =
    if t.bits > 1
    then let state = match t.hold land 0x3 with
           | 0 -> Flat (flat window)
           | 1 -> Fixed window
           | 2 -> Dictionary (dictionary window)
           | _ -> Exception Invalid_kind_of_block
         in

         Cont { t with hold = t.hold lsr 2
                     ; bits = t.bits - 2
                     ; state }
    else if (t.i_len - t.i_pos) > 0
    then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in

         Cont { t with i_pos = t.i_pos + 1
                     ; hold  = (t.hold lor (byte lsl t.bits))
                     ; bits  = t.bits + 8 }
    else Wait t

  let last src dst t window =
    if t.bits > 0
    then let last = t.hold land 1 = 1 in

         Cont { t with last  = last
                     ; hold  = t.hold lsr 1
                     ; bits  = t.bits - 1
                     ; state = Block window }
    else if (t.i_len - t.i_pos) > 0
    then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in

         Cont { t with i_pos = t.i_pos + 1
                     ; hold  = (t.hold lor (byte lsl t.bits))
                     ; bits  = t.bits + 8 }
    else Wait t

  let header src dst t =
    if (t.i_len - t.i_pos) > 1
    then let byte0 = Char.code @@ B.get src (t.i_off + t.i_pos) in
         let _     = Char.code @@ B.get src (t.i_off + t.i_pos + 1) in

         let window = Window.make_by ~proof:dst (1 lsl (byte0 lsr 4 + 8)) in

         Cont { t with i_pos = t.i_pos + 2
                     ; state = Last window }
    else Wait t

  let eval src dst t =
    let eval0 t = match t.state with
      | Header -> header src dst t
      | Last window -> last src dst t window
      | Block window -> block src dst t window
      | Flat k -> k src dst t
      | Fixed window -> fixed src dst t window
      | Dictionary k -> k src dst t
      | Inffast (window, lookup_chr, lookup_dst, code) ->
        inffast src dst t window lookup_chr lookup_dst code
      | Inflate k -> k src dst t
      | Switch window -> switch src dst t window
      | Crc k -> k src dst t
      | Exception exn -> error t exn
    in

    let rec loop t =
      match eval0 t with
      | Cont t -> loop t
      | Wait t -> `Await t
      | Flush t -> `Flush t
      | Ok t -> `End t
      | Error (t, exn) -> `Error (t, exn)
    in

    loop t

  let default =
    { last  = false
    ; hold  = 0
    ; bits  = 0
    ; i_off = 0
    ; i_pos = 0
    ; i_len = 0
    ; o_off = 0
    ; o_pos = 0
    ; o_len = 0
    ; write = 0
    ; state = Header }

  let sp = Format.sprintf

  let refill off len t =
    if t.i_pos = t.i_len
    then { t with i_off = off
                ; i_len = len
                ; i_pos = 0 }
    else raise (Invalid_argument (sp "Z.refill: you lost something (pos: %d, len: %d)" t.i_pos t.i_len))

  let flush off len t =
    { t with o_off = off
           ; o_len = len
           ; o_pos = 0 }

  let available_in t  = t.i_len - t.i_pos
  let available_out t = t.o_len - t.i_pos

  let used_in t  = t.i_pos
  let used_out t = t.o_pos

  let write t = t.write

  let extract_error { state; _ } = match state with
    | Exception exn -> Some exn
    | _ -> None
end

module H =
struct
  type error = ..
  type error += Reserved_opcode of int
  type error += Wrong_insert_hunk of int * int * int

  type 'i t =
    { i_off  : int
    ; i_pos  : int
    ; i_len  : int
    ; read   : int
    ; length   : int
    ; backward : int
    ; source_length : int
    ; target_length : int
    ; hunks         : 'i obj list
    ; state  : 'i state }
  and 'i state =
    | Header    of ('i B.t -> 'i t -> 'i res)
    | List      of ('i B.t -> 'i t -> 'i res)
    | Is_insert of ('i B.t * int * int)
    | Is_copy   of ('i B.t -> 'i t -> 'i res)
    | End
    | Exception of error
  and 'i res =
    | Wait  of 'i t
    | Error of 'i t * error
    | Cont  of 'i t
    | Ok    of 'i t * 'i obj list
  and 'i obj =
    | Insert of 'i B.t
    | Copy of int * int

  let pp = Format.fprintf

  let pp_lst ~sep pp_data fmt lst =
    let rec aux = function
      | [] -> ()
      | [ x ] -> pp_data fmt x
      | x :: r -> pp fmt "%a%a" pp_data x sep (); aux r
    in aux lst

  let pp_obj fmt = function
    | Insert buf ->
      pp fmt "(Insert %a)" B.pp buf
    | Copy (off, len) ->
      pp fmt "(Copy (%d, %d))" off len

  let await t      = Wait t
  let error t exn  = Error ({ t with state = Exception exn }, exn)
  let ok t         = Ok ({ t with state = End }, t.hunks)

  module KHeader =
  struct
    let rec get_byte k src t =
      if t.i_len - t.i_pos > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1
                             ; read = t.read + 1 }
      else await { t with state = Header ((get_byte[@tailcall]) k) }

    let rec length msb (len, bit) k src t = match msb with
      | true ->
        get_byte
          (fun byte src t ->
             let msb = byte land 0x80 <> 0 in
             (length[@tailcall]) msb (len lor ((byte land 0x7F) lsl bit), bit + 7)
             k src t)
          src t
      | false -> k len src t

    let length k src t =
      get_byte
        (fun byte src t ->
           let msb = byte land 0x80 <> 0 in
           length msb ((byte land 0x7F), 7) k src t)
        src t
  end

  module KList =
  struct
    let rec get_byte k src t =
      if t.i_len > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1
                             ; read = t.read + 1 }
      else await { t with state = List ((get_byte[@tailcall]) k) }
  end

  let rec copy opcode =
    let rec get_byte flag k src t =
      if not flag
      then k 0 src t
      else if t.i_len > 0
           then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
                k byte src { t with i_pos = t.i_pos + 1
                                  ; read = t.read + 1 }
           else await { t with state = Is_copy ((get_byte[@tailcall]) flag k)}
    in

    get_byte ((opcode lsr 0) land 1 <> 0)
    @@ fun o0 -> get_byte ((opcode lsr 1) land 1 <> 0)
    @@ fun o1 -> get_byte ((opcode lsr 2) land 1 <> 0)
    @@ fun o2 -> get_byte ((opcode lsr 3) land 1 <> 0)
    @@ fun o3 -> get_byte ((opcode lsr 4) land 1 <> 0)
    @@ fun l0 -> get_byte ((opcode lsr 5) land 1 <> 0)
    @@ fun l1 -> get_byte ((opcode lsr 6) land 1 <> 0)
    @@ fun l2 src t ->
      let copy_offset = o0 lor (o1 lsl 8) lor (o2 lsl 16) lor (o3 lsl 24) in
      let copy_length = l0 lor (l1 lsl 8) lor (l2 lsl 16) in

      if copy_offset + copy_length > t.source_length
      then error t (Wrong_insert_hunk (copy_offset, copy_length, t.source_length))
      else Cont { t with hunks = (Copy (copy_offset, copy_length)) :: t.hunks
                       ; state = List list }

  and list src t =
    if t.read < t.length
    then KList.get_byte
           (fun opcode src t ->
             if opcode = 0 then error t (Reserved_opcode opcode)
             else match opcode land 0x80 with
             | 0 -> Cont { t with state = Is_insert (B.from ~proof:src opcode, 0, opcode) }
             | _ -> Cont { t with state = Is_copy (copy opcode) })
           src t
    else ok t

  let insert src t buffer (off, rest) =
    let n = min (t.i_len - t.i_pos) rest in
    B.blit src (t.i_off + t.i_pos) buffer off n;
    if rest - n = 0
    then Cont { t with hunks = (Insert buffer) :: t.hunks
                     ; i_pos = t.i_pos + n
                     ; read = t.read + n
                     ; state = List list }
    else await { t with i_pos = t.i_pos + n
                      ; read = t.read + n
                      ; state = Is_insert (buffer, off + n, rest - n) }

  let header =
    KHeader.length
    @@ fun source_length -> KHeader.length
    @@ fun target_length src t ->
       Cont { t with state = List list
                   ; source_length
                   ; target_length }

  let eval src t =
    let eval0 t = match t.state with
      | Header k -> k src t
      | List k -> k src t
      | Is_insert (buffer, off, rest) -> insert src t buffer (off, rest)
      | Is_copy k -> k src t
      | End -> ok t
      | Exception exn -> error t exn
    in

    let rec loop t =
      match eval0 t with
      | Cont t -> loop t
      | Wait t -> `Await t
      | Error (t, exn) -> `Error (t, exn)
      | Ok (t, objs) -> `Ok (t, objs)
    in

    loop t

  let default length backward =
    { i_off = 0
    ; i_pos = 0
    ; i_len = 0
    ; read  = 0
    ; length
    ; backward
    ; source_length = 0
    ; target_length = 0
    ; hunks = []
    ; state = Header header }

  let sp = Format.sprintf

  let refill off len t =
    if t.i_pos = t.i_len 
    then { t with i_off = off
                ; i_len = len }
    else raise (Invalid_argument (sp "Hunks.refill: you lost something"))

  let available_in t = t.i_len
  let used_in t = t.i_pos
end

module Unpack =
struct
  type error = ..
  type error += Invalid_byte of int
  type error += Reserved_kind of int
  type error += Invalid_kind of int
  type error += Inflate_error of Z.error
  type error += Hunk_error of H.error
  type error += Hunk_input
  type error += Invalid_length of int * int

  let pp = Format.fprintf
  let pp_error fmt = function
    | Invalid_byte byte              -> pp fmt "(Invalid_byte %02x)" byte
    | Reserved_kind byte             -> pp fmt "(Reserved_byte %02x)" byte
    | Invalid_kind byte              -> pp fmt "(Invalid_kind %02x)" byte
    | Inflate_error err              -> pp fmt "(Inflate_error %a)" Z.pp_error err
    | Invalid_length (expected, has) -> pp fmt "(Invalid_length (%d <> %d))" expected has

  type ('i, 'o) t =
    { i_off   : int
    ; i_pos   : int
    ; i_len   : int
    ; o_buf   : 'o B.t
    ; version : Int32.t
    ; objects : Int32.t
    ; state   : ('i, 'o) state }
  and ('i, 'o) state =
    | Header    of ('i B.t -> ('i, 'o) t -> ('i, 'o) res)
    | Object    of ('i B.t -> ('i, 'o) t -> ('i, 'o) res)
    | Length    of ('i B.t -> ('i, 'o) t -> ('i, 'o) res)
    | Unzip     of { length : int; kind : 'i kind; z : ('i, 'o) Z.t; }
    | Hunks     of { length : int; z : ('i, 'o) Z.t; h : 'i H.t; }
    | Next      of { length : int; count : int; kind : 'i kind; }
    | Exception of error
  and ('i, 'o) res =
    | Wait  of ('i, 'o) t
    | Flush of ('i, 'o) t
    | Error of ('i, 'o) t * error
    | Cont  of ('i, 'o) t
    | Ok    of ('i, 'o) t
  and 'i kind =
    | Commit
    | Tree
    | Blob
    | Tag
    | Hunk of 'i H.obj list

  let pp_kind fmt = function
    | Commit -> pp fmt "Commit"
    | Tree -> pp fmt "Tree"
    | Blob -> pp fmt "Blob"
    | Tag -> pp fmt "Tag"
    | Hunk lst -> pp fmt "(Hunks [@[<hov>%a@]])"
      (H.pp_lst ~sep:(fun fmt () -> pp fmt ";@ ") H.pp_obj) lst

  let pp fmt { i_off; i_pos; i_len; o_buf; version; objects; state; } =
    match state with
    | Unzip { z; _ } ->
      pp fmt "{@[<hov>i_off = %d;@ \
                      i_pos = %d;@ \
                      i_len = %d;@ \
                      version = %ld;@ \
                      objects = %ld;@ \
                      z = %a;@]}"
        i_off i_pos i_len version objects Z.pp z
    | _ ->
      pp fmt "{@[<hov>i_off = %d;@ \
                      i_pos = %d;@ \
                      i_len = %d;@ \
                      version = %ld;@ \
                      objects = %ld;@]}"
        i_off i_pos i_len version objects

  let await t      = Wait t
  let flush t      = Flush t
  let error t exn  = Error ({ t with state = Exception exn }, exn)
  let continue t   = Cont t
  let rec ok src t = Ok { t with state = Header ok }

  module KHeader =
  struct
    let rec check_byte chr k src t =
      if (t.i_len - t.i_pos) > 0 && B.get src (t.i_off + t.i_pos) = chr
      then k src { t with i_pos = t.i_pos + 1 }
      else if t.i_len = 0
      then await { t with state = Header ((check_byte[@tailcall]) chr k) }
      else error { t with i_pos = t.i_pos + 1 }
                 (Invalid_byte (Char.code @@ B.get src (t.i_off + t.i_pos)))

    let rec get_byte k src t =
      if t.i_len > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1 }
      else await { t with state = Header ((get_byte[@tailcall]) k) }

    let rec get_u32 k src t =
      if t.i_len > 3
      then k (B.get_u32 src (t.i_off + t.i_pos)) src
             { t with i_pos = t.i_pos + 4 }
      else await { t with state = Header ((get_u32[@tailcall]) k) }
  end

  module KLength =
  struct
    let rec get_byte k src t =
      if t.i_len > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1 }
      else await { t with state = Length ((get_byte[@tailcall]) k) }
  end

  module KObject =
  struct
    let rec get_byte k src t =
      if t.i_len > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1 }
      else await { t with state = Object ((get_byte[@tailcall]) k) }
  end

  let rec length msb (len, bit) k src t = match msb with
    | true ->
      KLength.get_byte
        (fun byte src t ->
           let msb = byte land 0x80 <> 0 in
           (length[@tailcall]) msb (len lor ((byte land 0x7F) lsl bit), bit + 7)
           k src t)
        src t
    | false -> k len src t

  let hunks src t length z h =
    match Z.eval src t.o_buf z with
    | `Await z ->
      await { t with state = Hunks { length; z; h; }
                   ; i_pos = t.i_pos + Z.used_in z }
    | `Error (z, exn) -> error t (Inflate_error exn)
    | `End z ->
      let ret = if Z.used_out z <> 0
                then H.eval t.o_buf (H.refill 0 (Z.used_out z) h)
                else H.eval t.o_buf h
      in
      (match ret with
       | `Ok (h, lst) ->
         Cont { t with state = Next { length
                                    ; count = Z.write z
                                    ; kind = Hunk lst }
                     ; i_pos = t.i_pos + Z.used_in z }
       | `Await h -> error t Hunk_input
       | `Error (h, exn) -> error t (Hunk_error exn))
    | `Flush z ->
      match H.eval t.o_buf (H.refill 0 (Z.used_out z) h) with
      | `Await h ->
        Cont { t with state = Hunks { length
                                    ; z = (Z.flush 0 (B.length t.o_buf) z)
                                    ; h } }
      | `Error (h, exn) -> error t (Hunk_error exn)
      | `Ok (h, objs) ->
        Cont { t with state = Hunks { length
                                    ; z = (Z.flush 0 (B.length t.o_buf) z)
                                    ; h } }

  let switch typ len src t =
    match typ with
    | 0b000 | 0b101 -> error t (Reserved_kind typ)
    | 0b001 ->
      Cont { t with state = Unzip { length = len
                                  ; kind = Commit
                                  ; z = Z.flush 0 (B.length t.o_buf)
                                        @@ Z.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Z.default; } }
    | 0b010 ->
      Cont { t with state = Unzip { length = len
                                  ; kind = Tree
                                  ; z = Z.flush 0 (B.length t.o_buf)
                                        @@ Z.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Z.default; } }
    | 0b011 ->
      Cont { t with state = Unzip { length = len
                                  ; kind = Blob
                                  ; z = Z.flush 0 (B.length t.o_buf)
                                        @@ Z.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Z.default; } }
    | 0b100 ->
      Cont { t with state = Unzip { length = len
                                  ; kind = Tag
                                  ; z = Z.flush 0 (B.length t.o_buf)
                                        @@ Z.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Z.default; } }
    | 0b110 ->
      KObject.get_byte
        (fun byte src t ->
           let msb = byte land 0x80 <> 0 in
           length msb (byte land 0x7F, 7)
             (fun backward src t ->
               Cont { t with state = Hunks { length = len
                                           ; z = Z.flush 0 (B.length t.o_buf)
                                                 @@ Z.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                                 @@ Z.default
                                           ; h = H.default len backward } })
             src t)
        src t
    | _  -> error t (Invalid_kind typ)

  let kind src t =
    KObject.get_byte
      (fun byte src t ->
         let msb = byte land 0x80 <> 0 in
         let typ = (byte land 0x70) lsr 4 in
         length msb (byte land 0x0F, 4) (switch typ) src t)
      src t

  let unzip src t length kind z =
    match Z.eval src t.o_buf z with
    | `Await z ->
      await { t with state = Unzip { length
                                   ; kind
                                   ; z }
                   ; i_pos = t.i_pos + Z.used_in z }
    | `Flush z ->
      flush { t with state = Unzip { length
                                   ; kind
                                   ; z } }
    | `End z ->
      if Z.used_out z <> 0
      then flush { t with state = Unzip { length
                                        ; kind
                                        ; z } }
      else Cont { t with state = Next { length; count = Z.write z; kind; }
                       ; i_pos = t.i_pos + Z.used_in z }
    | `Error (z, exn) -> error t (Inflate_error exn)

  let next src t length count kind =
    if length = count
    then Cont t
    else error t (Invalid_length (length, count))

  let number src t =
    KHeader.get_u32
      (fun objects src t ->
         Cont { t with objects = objects
                     ; state = Object kind })
      src t

  let version src t =
    KHeader.get_u32
      (fun version src t ->
         number src { t with version = version })
      src t

  let header =
       KHeader.check_byte 'P'
    @@ KHeader.check_byte 'A'
    @@ KHeader.check_byte 'C'
    @@ KHeader.check_byte 'K'
    @@ version

  let default ~proof ?(chunk = 4096) =
    { i_off   = 0
    ; i_pos   = 0
    ; i_len   = 0
    ; o_buf   = B.from ~proof chunk
    ; version = 0l
    ; objects = 0l
    ; state   = Header header }

  let sp = Format.sprintf

  let refill off len t =
    if t.i_len - t.i_pos = 0
    then match t.state with
         | Unzip { length; kind; z; } ->
           { t with i_off = off
                  ; i_len = len
                  ; i_pos = 0
                  ; state = Unzip { length; kind; z = Z.refill off len z; } }
         | Hunks { length; z; h; } ->
           { t with i_off = off
                  ; i_len = len
                  ; i_pos = 0
                  ; state = Hunks { length; z = Z.refill off len z; h; } }
         | _ -> { t with i_off = off
                       ; i_len = len
                       ; i_pos = 0 }
    else raise (Invalid_argument (sp "Unpack.refill: you lost something (pos: %d, len: %d)" t.i_pos t.i_len))

  let flush off len t = match t.state with
    | Unzip { length; kind; z; } ->
      { t with state = Unzip { length; kind; z = Z.flush off len z; } }
    | _ -> raise (Invalid_argument "flush: bad state")

  let output t = match t.state with
    | Unzip { z; _ } ->
      t.o_buf, Z.used_out z
    | _ -> raise (Invalid_argument "output: bad state")

  let next_object t = match t.state with
    | Next _ ->
      if t.objects = 1l
      then { t with state = Header ok }
      else { t with state = Object kind }
    | _ -> raise (Invalid_argument "next_object: bad state")

  let kind t = match t.state with
    | Next { kind; _ } -> kind
    | _ -> raise (Invalid_argument "kind: bad state")

  let rec eval src t =
    let eval0 t = match t.state with
      | Header k -> k src t
      | Object k -> k src t
      | Length k -> k src t
      | Unzip { length; kind; z; } ->
        unzip src t length kind  z
      | Hunks { length; z; h; } ->
        hunks src t length z h
      | Next { length; count; kind; } -> next src t length count kind
      | Exception exn -> error t exn
    in

    let rec loop t =
      match eval0 t with
      | Cont ({ state = Next _ } as t) -> `Object t
      | Cont t -> loop t
      | Wait t -> `Await t
      | Flush t -> `Flush t
      | Ok t -> `End t
      | Error (t, exn) -> `Error (t, exn)
    in

    loop t
end

(* from ocaml-hex *)
module Hex =
struct
  let to_char x y =
    let code c = match c with
      | '0'..'9' -> Char.code c - 48 (* Char.code '0' *)
      | 'A'..'F' -> Char.code c - 55 (* Char.code 'A' + 10 *)
      | 'a'..'f' -> Char.code c - 87 (* Char.code 'a' + 10 *)
      | _ -> raise (Invalid_argument (Format.sprintf "Hex.to_char: %d is an invalid char" (Char.code c)))
    in
    Char.chr (code x lsl 4 + code y)

  let hexa = "0123456789abcdef"
  and hexa1 =
    "0000000000000000111111111111111122222222222222223333333333333333\
     4444444444444444555555555555555566666666666666667777777777777777\
     88888888888888889999999999999999aaaaaaaaaaaaaaaabbbbbbbbbbbbbbbb\
     ccccccccccccccccddddddddddddddddeeeeeeeeeeeeeeeeffffffffffffffff"
  and hexa2 =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\
     0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\
     0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\
     0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

  let of_string_fast s =
    let len = String.length s in
    let buf = Bytes.create (len * 2) in
    for i = 0 to len - 1 do
      Bytes.unsafe_set buf (i * 2)
        (String.unsafe_get hexa1 (Char.code (String.unsafe_get s i)));
      Bytes.unsafe_set buf (succ (i * 2))
        (String.unsafe_get hexa2 (Char.code (String.unsafe_get s i)));
    done;
    `Hex (Bytes.unsafe_to_string buf)

  let hexdump_s ?(print_row_numbers=true) ?(print_chars=true) (`Hex s) =
    let char_len = 16 in
    let hex_len = char_len * 2 in
    let buf = Buffer.create ((String.length s) * 4) in
    let ( <= ) buf s = Buffer.add_string buf s in
    let n = String.length s in
    let rows = (n / hex_len) + (if n mod hex_len = 0 then 0 else 1) in
    for row = 0 to rows-1 do
      let last_row = row = rows-1 in
      if print_row_numbers then
        buf <= Printf.sprintf "%.8d: " row;
      let row_len = if last_row then
          (let rem = n mod hex_len in
           if rem = 0 then hex_len else rem)
        else hex_len in
      for i = 0 to row_len-1 do
        if i mod 4 = 0 && i <> 0 then buf <= Printf.sprintf " ";
        let i = i + (row * hex_len) in
        buf <= Printf.sprintf "%c" (String.get s i)
      done;
      if last_row then
        let missed_chars = hex_len - row_len in
        let pad = missed_chars in
        (* Every four chars add spacing *)
        let pad = pad + (missed_chars / 4) in
        buf <= Printf.sprintf "%s" (String.make pad ' ')
      else ();
      if print_chars then begin
        buf <= "  ";
        let rec aux i j =
          if i > row_len - 2 then ()
          else begin
            let pos = i + (row * hex_len) in
            let pos' = pos + 1 in
            let c = to_char (String.get s pos) (String.get s pos') in
            let () = match c with
              | '\t' | '\n' -> buf <= "."
              | _ -> buf <= Printf.sprintf "%c" c
            in ();
            aux (j+1) (j+2)
          end
        in
        aux 0 1;
      end;
      buf <= "\n";
    done;
    Buffer.contents buf

  let hexdump fmt hex =
    Format.fprintf fmt "%s" (hexdump_s hex)
end

external bs_read : Unix.file_descr -> B.Bigstring.t -> int -> int -> int =
  "bigstring_read" [@@noalloc]
external bs_write : Unix.file_descr -> B.Bigstring.t -> int -> int -> int =
  "bigstring_write" [@@noalloc]

let () =
  let tmp     = B.Bigstring.create 4096 in
  let buf     = Buffer.create 4096 in

  let rec loop t =
    match Unpack.eval (B.from_bigstring tmp) t with
    | `Await t ->
      let n = bs_read Unix.stdin tmp 0 4096 in
      loop (Unpack.refill 0 n t)
    | `Flush t ->
      let o, n = Unpack.output t in
      let () = match o with B.Bigstring v -> Buffer.add_substring buf (B.Bigstring.to_string v) 0 n in
      loop (Unpack.flush 0 n t)
    | `Object t ->
      let kind = Unpack.kind t in
      Format.printf "%a:\n%a\n%!"
        Unpack.pp_kind kind
        Hex.hexdump (Hex.of_string_fast @@ Buffer.contents buf);

      let () = Buffer.clear buf in
      loop (Unpack.next_object t)
    | `End t -> ()
    | `Error (t, exn) -> Format.eprintf "%a\n%!" Unpack.pp_error exn
  in

  loop (Unpack.default ~proof:(B.from_bigstring tmp) ~chunk:4096)
