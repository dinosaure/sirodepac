module B : sig
  include module type of Decompress.B
    with type st = Decompress.B.st
     and type bs = Decompress.B.bs
     and type 'a t = 'a Decompress.B.t

  val to_cstruct : 'a t -> Cstruct.t
  val blit_string : string -> int -> 'a t -> int -> int -> unit
end = struct
  include Decompress.B

  let to_cstruct
    : type a. a t -> Cstruct.t
    = function
    | Bytes v -> Cstruct.of_bytes v
    | Bigstring v -> Cstruct.of_bigarray v

  let blit_string src src_off dst dst_off len =
    for i = 0 to len - 1
    do set dst (dst_off + i) (String.get src (src_off + i)) done
end

external swap32 : int32 -> int32 = "%bswap_int32"
external swap64 : int64 -> int64 = "%bswap_int64"

module Lazy =
struct
  type error = ..
  type error += Invalid_header of string
  type error += Invalid_version of Int32.t
  type error += Invalid_index

  let pp = Format.fprintf
  let pp_error fmt = function
    | Invalid_header header -> pp fmt "(Invalid_header %s)" header
    | Invalid_version version -> pp fmt "(Invalid_version %ld)" version
    | Invalid_index -> pp fmt "Invalid_index"

  type 'a t =
    { map           : 'a B.t
    ; fanout_offset : int
    ; hashes_offset : int
    ; crcs_offset   : int
    ; values_offset : int
    ; v64_offset    : int option
    ; cache         : Int64.t LRU.t }

  let has map off len =
    if (off < 0 || len < 0 || off + len > B.length map)
    then raise (Invalid_argument (Printf.sprintf "%d:%d:%d" off len (B.length map)))
    else true

  let check_header map =
    if has map 0 4
    then (if B.get map 0 = '\255'
          && B.get map 1 = '\116'
          && B.get map 2 = '\079'
          && B.get map 3 = '\099'
          then Ok ()
          else Error (Invalid_header (B.to_string @@ B.sub map 0 4)))
    else Error Invalid_index

  let check_version map =
    if has map 4 4
    then (if Endian.get_u32 map 4 = 2l
          then Ok ()
          else Error (Invalid_version (Endian.get_u32 map 4)))
    else Error Invalid_index

  let number_of_hashes map =
    if has map 8 (256 * 4)
    then let n = Endian.get_u32 map (8 + (255 * 4)) in
         Ok (8, n)
    else Error Invalid_index

  let bind v f = match v with Ok v -> f v | Error _ as e -> e
  let ( >>= ) = bind
  let ( *> ) u v = match u with Ok _ -> v | Error _ as e -> e

  let make map =
    check_header map
    *> check_version map
    *> number_of_hashes map
    >>= fun (fanout_offset, number_of_hashes) ->
      let number_of_hashes = Int32.to_int number_of_hashes in
      let hashes_offset = 8 + (256 * 4) in
      let crcs_offset   = 8 + (256 * 4) + (number_of_hashes * 20) in
      let values_offset = 8 + (256 * 4) + (number_of_hashes * 20) + (number_of_hashes * 4) in

      Ok { map
         ; fanout_offset
         ; hashes_offset
         ; crcs_offset
         ; values_offset
         ; v64_offset = None
         ; cache = LRU.make 1024 }

  exception Break

  let compare buf off hash =
    try for i = 0 to 19
        do if B.get buf (off + i) <> String.get hash i
           then raise Break
        done; true
    with Break -> false

  exception ReturnT
  exception ReturnF

  let lt buf off hash =
    try for i = 0 to 19
        do let a = Char.code @@ B.get buf (off + i) in
           let b = Char.code @@ String.get hash i in

           if a > b
           then raise ReturnT
           else if a <> b then raise ReturnF;
        done; false
    with ReturnT -> true
       | ReturnF -> false

  (* XXX(dinosaure): keep in your mind, it's important to implement this
                     function in the tail-rec way. We can use the [@@tailcall]
                     annotation...
   *)
  let binary_search buf hash =
    let rec aux off len buf =
      if len = 20
      then begin
        (off / 20)
      end else
        let len' = ((len / 40) * 20) in
        let off' = off + len' in

        if compare buf off' hash
        then (off' / 20)
        else if lt buf off' hash then aux off len' buf
        else aux off' (len - len') buf
    in

    aux 0 (B.length buf) buf

  let fanout_idx t hash =
    match Char.code @@ String.get hash 0 with
    | 0 ->
      let n = Endian.get_u32 t.map t.fanout_offset in
      Ok (binary_search (B.sub t.map t.hashes_offset (Int32.to_int n * 20)) hash)
    | idx ->
      if has t.map (t.fanout_offset + (4 * idx)) 4
      && has t.map (t.fanout_offset + (4 * (idx - 1))) 4
      then let off1 = Int32.to_int @@ Endian.get_u32 t.map (t.fanout_offset + (4 * idx)) in
           let off0 = Int32.to_int @@ Endian.get_u32 t.map (t.fanout_offset + (4 * (idx - 1))) in

           if has t.map (t.hashes_offset + (off0 * 20)) ((off1 - off0) * 20)
           then Ok (binary_search (B.sub t.map (t.hashes_offset + (off0 * 20)) ((off1 - off0) * 20)) hash + off0)
           else Error Invalid_index
      else Error Invalid_index

  let nth t n =
    let off = Int32.to_int @@ Int32.mul 20l n in

    B.sub t.map (t.hashes_offset + off) 20

  let find t hash =
    match LRU.find t.cache hash with
    | Some offset -> Some offset
    | None ->
      match fanout_idx t hash with
      | Ok idx ->
        let off = Endian.get_u32 t.map (t.values_offset + (idx * 4)) in
        Some (Int64.of_int32 off)
      | Error _ -> None
end

module Decoder =
struct
  type error = ..
  type error += Invalid_byte of int
  type error += Invalid_version of Int32.t
  type error += Invalid_index_of_bigoffset of int
  type error += Expected_bigoffset_table

  let pp = Format.fprintf
  let pp_error fmt = function
    | Invalid_byte byte -> pp fmt "(Invalid_byte %02x)" byte
    | Invalid_version version -> pp fmt "(Invalid_version %ld)" version
    | Invalid_index_of_bigoffset idx -> pp fmt "(Invalid_index_of_bigoffset %d)" idx
    | Expected_bigoffset_table -> pp fmt "Expected_bigoffset_table"

  type 'i t =
    { i_off   : int
    ; i_pos   : int
    ; i_len   : int
    ; fanout  : Int32.t array
    ; hashes  : string Queue.t
    ; crcs    : Int32.t Queue.t
    ; offsets : (Int32.t * bool) Queue.t
    ; state   : 'i state }
  and 'i state =
    | Header    of ('i B.t -> 'i t -> 'i res)
    | Fanout    of ('i B.t -> 'i t -> 'i res)
    | Hashes    of ('i B.t -> 'i t -> 'i res)
    | Crcs      of ('i B.t -> 'i t -> 'i res)
    | Offsets   of ('i B.t -> 'i t -> 'i res)
    | Ret
    | End
    | Exception of error
  and 'i res =
    | Wait   of 'i t
    | Error  of 'i t * error
    | Cont   of 'i t
    | Result of 'i t * (string * Int32.t * Int64.t)
    | Ok     of 'i t

  let await t     = Wait t
  let error t exn = Error ({ t with state = Exception exn }, exn)
  let ok t        = Ok { t with state = End }

  let to_int32 b0 b1 b2 b3 =
    let (<<) = Int32.shift_left in
    let (||) = Int32.logor in
    (Int32.of_int b0 << 24)
    || (Int32.of_int b1 << 16)
    || (Int32.of_int b2 << 8)
    || (Int32.of_int b3)

  let to_int64 b0 b1 b2 b3 b4 b5 b6 b7 =
    let (<<) = Int64.shift_left in
    let (||) = Int64.logor in
    (Int64.of_int b0 << 56)
    || (Int64.of_int b1 << 48)
    || (Int64.of_int b2 << 40)
    || (Int64.of_int b3 << 32)
    || (Int64.of_int b4 << 24)
    || (Int64.of_int b5 << 16)
    || (Int64.of_int b6 << 8)
    || (Int64.of_int b7)

  module KHeader =
  struct
    let rec check_byte chr k src t =
      if (t.i_len - t.i_pos) > 0 && B.get src (t.i_off + t.i_pos) = chr
      then k src { t with i_pos = t.i_pos + 1 }
      else if (t.i_len - t.i_pos) = 0
      then await { t with state = Header (fun src t -> (check_byte[@tailcall]) chr k src t) }
      else error { t with i_pos = t.i_pos + 1 }
                 (Invalid_byte (Char.code @@ B.get src (t.i_off + t.i_pos)))

    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1 }
      else await { t with state = Header (fun src t -> (get_byte[@tailcall]) k src t) }

    let rec get_u32 k src t =
      if (t.i_len - t.i_pos) > 3
      then let num = Endian.get_u32 src (t.i_off + t.i_pos) in
           k num src
             { t with i_pos = t.i_pos + 4 }
      else if (t.i_len - t.i_pos) > 0
      then (get_byte
            @@ fun byte0 -> get_byte
            @@ fun byte1 -> get_byte
            @@ fun byte2 -> get_byte
            @@ fun byte3 src t ->
               k (to_int32 byte0 byte1 byte2 byte3) src t)
            src t
      else await { t with state = Header (fun src t -> (get_u32[@tailcall]) k src t) }
  end

  module KFanoutTable =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1 }
      else await { t with state = Fanout (fun src t -> (get_byte[@tailcall]) k src t) }

    let rec get_u32 k src t =
      if (t.i_len - t.i_pos) > 3
      then let num = Endian.get_u32 src (t.i_off + t.i_pos) in
           k num src
             { t with i_pos = t.i_pos + 4 }
      else if (t.i_len - t.i_pos) > 0
      then (get_byte
            @@ fun byte0 -> get_byte
            @@ fun byte1 -> get_byte
            @@ fun byte2 -> get_byte
            @@ fun byte3 src t ->
               k (to_int32 byte0 byte1 byte2 byte3) src t)
            src t
      else await { t with state = Fanout (fun src t -> (get_u32[@tailcall]) k src t) }
  end

  module KHashes =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1 }
      else await { t with state = Hashes (fun src t -> (get_byte[@tailcall]) k src t) }

    let get_hash k src t =
      let buf = Buffer.create 20 in

      let rec loop i src t =
        if i = 20
        then k (Buffer.contents buf) src t
        else
          get_byte (fun byte src t ->
                     Buffer.add_char buf (Char.chr byte);
                     (loop[@tailcall]) (i + 1) src t)
            src t
      in

      loop 0 src t
  end

  module KCrcs =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1 }
      else await { t with state = Crcs (fun src t -> (get_byte[@tailcall]) k src t) }

    let rec get_u32 k src t =
      if (t.i_len - t.i_pos) > 3
      then let num = Endian.get_u32 src (t.i_off + t.i_pos) in
           k num src
             { t with i_pos = t.i_pos + 4 }
      else if (t.i_len - t.i_pos) > 0
      then (get_byte
            @@ fun byte0 -> get_byte
            @@ fun byte1 -> get_byte
            @@ fun byte2 -> get_byte
            @@ fun byte3 src t ->
               k (to_int32 byte0 byte1 byte2 byte3) src t)
            src t
      else await { t with state = Crcs (fun src t -> (get_u32[@tailcall]) k src t) }
  end

  module KOffsets =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1 }
      else await { t with state = Offsets (fun src t -> (get_byte[@tailcall]) k src t) }

    let rec get_u32 k src t =
      if (t.i_len - t.i_pos) > 3
      then let num = Endian.get_u32 src (t.i_off + t.i_pos) in
           k num src
             { t with i_pos = t.i_pos + 4 }
      else if (t.i_len - t.i_pos) > 0
      then (get_byte
            @@ fun byte0 -> get_byte
            @@ fun byte1 -> get_byte
            @@ fun byte2 -> get_byte
            @@ fun byte3 src t ->
               k (to_int32 byte0 byte1 byte2 byte3) src t)
            src t
      else await { t with state = Offsets (fun src t -> (get_u32[@tailcall]) k src t) }

    let get_u32 k src t =
      get_u32
        (fun u32 src t ->
          k (u32, Int32.equal 0l (Int32.logand u32 0x80000000l)) src t)
        src t

    let rec get_u64 k src t =
      if (t.i_len - t.i_pos) > 7
      then let num = Endian.get_u64 src (t.i_off + t.i_pos) in
           k num src
             { t with i_pos = t.i_pos + 8 }
      else if (t.i_len - t.i_pos) > 0
      then (get_byte
            @@ fun byte0 -> get_byte
            @@ fun byte1 -> get_byte
            @@ fun byte2 -> get_byte
            @@ fun byte3 -> get_byte
            @@ fun byte4 -> get_byte
            @@ fun byte5 -> get_byte
            @@ fun byte6 -> get_byte
            @@ fun byte7 src t ->
               k (to_int64 byte0 byte1 byte2 byte3 byte4 byte5 byte6 byte7) src t)
            src t
      else await { t with state = Offsets (fun src t -> (get_u64[@tailcall]) k src t) }
  end

  let rest ?boffsets src t =
    match Queue.pop t.hashes, Queue.pop t.crcs, Queue.pop t.offsets with
    | hash, crc, (offset, true) -> Result ({ t with state = Ret }, (hash, crc, Int64.of_int32 offset))
    | exception Queue.Empty -> ok t
    | hash, crc, (offset, false) -> match boffsets with
      | None -> error t (Expected_bigoffset_table)
      | Some arr ->
        let idx = Int32.to_int (Int32.logand offset 0x7FFFFFFFl) in
        if idx >= 0 && idx < Array.length arr
        then Result ({ t with state = Ret }, (hash, crc, Array.get arr idx))
        else error t (Invalid_index_of_bigoffset idx)

  let rec boffsets arr idx max src t =
    if idx >= max
    then rest ~boffsets:arr src t
    else KOffsets.get_u64
           (fun offset src t ->
              Array.set arr idx offset;
              (boffsets[@tailcall]) arr (succ idx) max src t)
           src t

  let rec offsets idx boffs max src t =
    if Int32.compare idx max >= 0
    then (if boffs > 0 then boffsets (Array.make boffs 0L) 0 boffs src t else rest src t)
    else KOffsets.get_u32
           (fun (offset, msb) src t ->
             Queue.add (offset, msb) t.offsets;
             (offsets[@tailcall])
               (Int32.succ idx)
               (if not msb then succ boffs else boffs)
               max
               src t)
           src t

  let rec crcs idx max src t =
    if Int32.compare idx max >= 0
    then offsets 0l 0 max src t
    else KCrcs.get_u32
           (fun crc src t ->
             Queue.add crc t.crcs;
             (crcs[@tailcall]) (Int32.succ idx) max src t)
           src t

  let rec hashes idx max src t =
    if Int32.compare idx max >= 0
    then Cont { t with state = Crcs (crcs 0l max) }
    else KHashes.get_hash
           (fun hash src t ->
              Queue.add hash t.hashes;
              (hashes[@tailcall]) (Int32.succ idx) max src t)
           src t

  (* XXX(dinosaure): fix this compute. See the serialization to understand. *)
  let rec fanout idx src t =
    match idx with
    | 256 ->
      Cont { t with state = Hashes (hashes 0l (Array.fold_left max 0l t.fanout)) }
    | n ->
      KFanoutTable.get_u32
        (fun entry src t ->
         Array.set t.fanout idx entry; (fanout[@tailcall]) (idx + 1) src t)
        src t

  let header src t =
    (KHeader.check_byte '\255'
     @@ KHeader.check_byte '\116'
     @@ KHeader.check_byte '\079'
     @@ KHeader.check_byte '\099'
     @@ KHeader.get_u32
     @@ fun version src t ->
        if version = 2l
        then Cont { t with state = Fanout (fanout 0) }
        else error t (Invalid_version version))
    src t

  let make () =
    { i_off   = 0
    ; i_pos   = 0
    ; i_len   = 0
    ; fanout  = Array.make 256 0l
    ; hashes  = Queue.create ()
    ; crcs    = Queue.create ()
    ; offsets = Queue.create ()
    ; state   = Header header }

  let sp = Format.sprintf

  let refill off len t =
    if (t.i_len - t.i_pos) = 0
    then { t with i_off = off
                ; i_len = len
                ; i_pos = 0 }
    else raise (Invalid_argument (sp "I.refill: you lost something \
                                      (pos: %d, len: %d)" t.i_pos t.i_len))

  let rec eval src t =
    let eval0 t = match t.state with
      | Header k -> k src t
      | Fanout k -> k src t
      | Hashes k -> k src t
      | Crcs k -> k src t
      | Offsets k -> k src t
      | Ret -> rest src t
      | End -> ok t
      | Exception exn -> error t exn
    in

    let rec loop t =
      match eval0 t with
      | Cont t -> loop t
      | Wait t -> `Await t
      | Ok t -> `End t
      | Result (t, hash) -> `Hash (t, hash)
      | Error (t, exn) -> `Error (t, exn)
    in

    loop t
end

module Encoder =
struct
  type error = ..

  type 'o t =
    { o_off    : int
    ; o_pos    : int
    ; o_len    : int
    ; table    : (int32 * int64) Fanout.t
    ; boffsets : int64 array
    ; state    : 'o state }
  and 'o state =
    | Header     of ('o B.t -> 'o t -> 'o res)
    | Fanout     of ('o B.t -> 'o t -> 'o res)
    | Hashes     of ('o B.t -> 'o t -> 'o res)
    | Crcs       of ('o B.t -> 'o t -> 'o res)
    | Offsets    of ('o B.t -> 'o t -> 'o res)
    | BigOffsets of ('o B.t -> 'o t -> 'o res)
  and 'o res =
    | Error  of 'o t * error
    | Flush  of 'o t
    | Cont   of 'o t
    | Ok     of 'o t

  (* XXX(dinosaure): the state contains only a closure. May be we can optimize
                     the serialization with an hot loop (like Decompress). But
                     the first goal is to work!
   *)

  module Int32 =
  struct
    include Int32

    let ( >> ) = Int32.shift_right
    let ( << ) = Int32.shift_left
    let ( && ) = Int32.logand
    let ( || ) = Int32.logor
    let ( ! )  = Int32.to_int
  end

  module KHeader =
  struct
    let rec put_byte chr k dst t =
      if (t.o_len - t.o_pos) > 0
      then begin
        B.set dst (t.o_off + t.o_pos) chr;
        k dst { t with o_pos = t.o_pos + 1 }
      end else Flush { t with state = Header (put_byte chr k) }

    (* XXX(dinosaure): we can abstract the constructor of the state and provide
                       only one implementation of [put_int32] for all [K*]
                       modules.
     *)
    let rec put_int32 integer k dst t =
      if (t.o_len - t.o_pos) >= 4
      then begin
        Endian.set_u32 dst (t.o_off + t.o_pos) integer;
        k dst { t with o_pos = t.o_pos + 4 }
      end else if (t.o_len - t.o_pos) > 0
      then let i1 = Char.unsafe_chr @@ Int32.(! ((integer && 0xFF000000l) >> 24)) in
           let i2 = Char.unsafe_chr @@ Int32.(! ((integer && 0x00FF0000l) >> 16)) in
           let i3 = Char.unsafe_chr @@ Int32.(! ((integer && 0x0000FF00l) >> 8)) in
           let i4 = Char.unsafe_chr @@ Int32.(! (integer && 0x000000FFl)) in

           (put_byte i1
            @@ put_byte i2
            @@ put_byte i3
            @@ put_byte i4 k)
           dst t
      else Flush { t with state = Header (put_int32 integer k) }
  end

  module Int64 =
  struct
    include Int64

    let ( >> ) = Int64.shift_right_logical
    let ( << ) = Int64.shift_left
    let ( && ) = Int64.logand
    let ( || ) = Int64.logor
    let ( ! )  = Int64.to_int
  end

  module KFanout =
  struct
    let rec put_byte chr k dst t =
      if (t.o_len - t.o_pos) > 0
      then begin
        B.set dst (t.o_off + t.o_pos) chr;
        k dst { t with o_pos = t.o_pos + 1 }
      end else Flush { t with state = Fanout (put_byte chr k) }

    let rec put_int32 integer k dst t =
      if (t.o_len - t.o_pos) >= 4
      then begin
        Endian.set_u32 dst (t.o_off + t.o_pos) integer;
        k dst { t with o_pos = t.o_pos + 4 }
      end else if (t.o_len - t.o_pos) > 0
      then let i1 = Char.unsafe_chr @@ Int32.(! ((integer && 0xFF000000l) >> 24)) in
           let i2 = Char.unsafe_chr @@ Int32.(! ((integer && 0x00FF0000l) >> 16)) in
           let i3 = Char.unsafe_chr @@ Int32.(! ((integer && 0x0000FF00l) >> 8)) in
           let i4 = Char.unsafe_chr @@ Int32.(! (integer && 0x000000FFl)) in

           (put_byte i1
            @@ put_byte i2
            @@ put_byte i3
            @@ put_byte i4 k)
           dst t
      else Flush { t with state = Fanout (put_int32 integer k) }
  end

  module KHashes =
  struct
    let rec put_byte chr k dst t =
      if (t.o_len - t.o_pos) > 0
      then begin
        B.set dst (t.o_off + t.o_pos) chr;
        k dst { t with o_pos = t.o_pos + 1 }
      end else Flush { t with state = Hashes (put_byte chr k) }

    let rec put_hash hash k dst t =
      if (t.o_len - t.o_pos) >= 20
      then begin
        B.blit_string hash 0 dst (t.o_off + t.o_pos) 20;
        k dst { t with o_pos = t.o_pos + 20 }
      end else if (t.o_len - t.o_pos) > 0
      then let rec aux rest hash k dst t =
             if rest = 0
             then let n = min (t.o_len - t.o_pos) rest in
                  B.blit_string hash (20 - rest) dst (t.o_off + t.o_pos)  n;
                  Flush { t with state = Hashes (aux (rest - n) hash k)
                               ; o_pos = t.o_pos + n }
             else k dst t
           in aux 20 hash k dst t
      else Flush { t with state = Hashes (put_hash hash k) }
  end

  module KCrcs =
  struct
    let rec put_byte chr k dst t =
      if (t.o_len - t.o_pos) > 0
      then begin
        B.set dst (t.o_off + t.o_pos) chr;
        k dst { t with o_pos = t.o_pos + 1 }
      end else Flush { t with state = Crcs (put_byte chr k) }

    let rec put_int32 integer k dst t =
      if (t.o_len - t.o_pos) >= 4
      then begin
        Endian.set_u32 dst (t.o_off + t.o_pos) integer;
        k dst { t with o_pos = t.o_pos + 4 }
      end else if (t.o_len - t.o_pos) > 0
      then let i1 = Char.unsafe_chr @@ Int32.(! ((integer && 0xFF000000l) >> 24)) in
           let i2 = Char.unsafe_chr @@ Int32.(! ((integer && 0x00FF0000l) >> 16)) in
           let i3 = Char.unsafe_chr @@ Int32.(! ((integer && 0x0000FF00l) >> 8)) in
           let i4 = Char.unsafe_chr @@ Int32.(! (integer && 0x000000FFl)) in

           (put_byte i1
            @@ put_byte i2
            @@ put_byte i3
            @@ put_byte i4 k)
           dst t
      else Flush { t with state = Crcs (put_int32 integer k) }
  end

  module KOffsets =
  struct
    let rec put_byte chr k dst t =
      if (t.o_len - t.o_pos) > 0
      then begin
        B.set dst (t.o_off + t.o_pos) chr;
        k dst { t with o_pos = t.o_pos + 1 }
      end else Flush { t with state = Offsets (put_byte chr k) }

    let rec put_int32 integer k dst t =
      if (t.o_len - t.o_pos) >= 4
      then begin
        Endian.set_u32 dst (t.o_off + t.o_pos) integer;
        k dst { t with o_pos = t.o_pos + 4 }
      end else if (t.o_len - t.o_pos) > 0
      then let i1 = Char.unsafe_chr @@ Int32.(! ((integer && 0xFF000000l) >> 24)) in
           let i2 = Char.unsafe_chr @@ Int32.(! ((integer && 0x00FF0000l) >> 16)) in
           let i3 = Char.unsafe_chr @@ Int32.(! ((integer && 0x0000FF00l) >> 8)) in
           let i4 = Char.unsafe_chr @@ Int32.(! (integer && 0x000000FFl)) in

           (put_byte i1
            @@ put_byte i2
            @@ put_byte i3
            @@ put_byte i4 k)
           dst t
      else Flush { t with state = Offsets (put_int32 integer k) }
  end

  module KBOffsets =
  struct
    let rec put_byte chr k dst t =
      if (t.o_len - t.o_pos) > 0
      then begin
        B.set dst (t.o_off + t.o_pos) chr;
        k dst { t with o_pos = t.o_pos + 1 }
      end else Flush { t with state = BigOffsets (put_byte chr k) }

    let rec put_int64 integer k dst t =
      if (t.o_len - t.o_pos) >= 8
      then begin
        Endian.set_u64 dst (t.o_off + t.o_pos) integer;
        k dst { t with o_pos = t.o_pos + 8 }
      end else if (t.o_len - t.o_pos) > 0
      then let i1 = Char.unsafe_chr @@ Int64.(! ((integer && 0xFF00000000000000L) >> 56)) in
           let i2 = Char.unsafe_chr @@ Int64.(! ((integer && 0x00FF000000000000L) >> 48)) in
           let i3 = Char.unsafe_chr @@ Int64.(! ((integer && 0x0000FF0000000000L) >> 40)) in
           let i4 = Char.unsafe_chr @@ Int64.(! ((integer && 0x000000FF00000000L) >> 32)) in
           let i5 = Char.unsafe_chr @@ Int64.(! ((integer && 0x00000000FF000000L) >> 24)) in
           let i6 = Char.unsafe_chr @@ Int64.(! ((integer && 0x0000000000FF0000L) >> 16)) in
           let i7 = Char.unsafe_chr @@ Int64.(! ((integer && 0x000000000000FF00L) >> 8)) in
           let i8 = Char.unsafe_chr @@ Int64.(! (integer && 0x00000000000000FFL)) in

           (put_byte i1
            @@ put_byte i2
            @@ put_byte i3
            @@ put_byte i4
            @@ put_byte i5
            @@ put_byte i6
            @@ put_byte i7
            @@ put_byte i8 k)
           dst t
      else Flush { t with state = BigOffsets (put_int64 integer k) }
  end

  let is_big_offset integer =
    Int64.(integer >> 31) <> 0L

  let rec boffsets idx dst t =
    if idx = Array.length t.boffsets
    then Cont t
    else KBOffsets.put_int64 (Array.get t.boffsets idx)
           (boffsets (idx + 1)) dst t

  let rec offsets idx idx_boffs dst t =
    if idx = 256
    then Cont t
    else let rec aux acc idx_boffs dst t = match acc with
           | [] -> Cont { t with state = Offsets (offsets (idx + 1) idx_boffs) }
           | (_, (_, off)) :: r ->
               if is_big_offset off
               then let integer = Int32.(0x40000000l && (Int32.of_int idx_boffs)) in
                    KOffsets.put_int32 integer (aux r (idx_boffs + 1)) dst t
               else KOffsets.put_int32 (Int64.to_int32 off) (aux r idx_boffs) dst t
                    (* XXX(dinosaure): safe to convert the offset to an int32. *)
         in
         aux (Fanout.get idx t.table) idx_boffs dst t

  let rec crcs idx dst t =
    if idx = 256
    then Cont t
    else let rec aux acc dst t = match acc with
           | [] -> Cont { t with state = Hashes (crcs (idx + 1)) }
           | (_, (crc, _)) :: r -> KCrcs.put_int32 crc (aux r) dst t
         in
         aux (Fanout.get idx t.table) dst t

  let rec hashes idx dst t =
    if idx = 256
    then Cont t
    else let rec aux acc dst t = match acc with
           | [] -> Cont { t with state = Hashes (hashes (idx + 1)) }
           | (hash, _) :: r -> KHashes.put_hash hash (aux r) dst t
         in
         aux (Fanout.get idx t.table) dst t

  let rec fanout idx acc dst t =
    match idx with
    | 256 -> Cont t
    | n ->
      let value = Int32.of_int (Fanout.length n t.table) in
      KFanout.put_int32 (Int32.add value acc)
        (fanout (idx + 1) (Int32.add value acc))
        dst t

  let header dst t =
    (KHeader.put_byte '\255'
     @@ KHeader.put_byte '\116'
     @@ KHeader.put_byte '\079'
     @@ KHeader.put_byte '\099'
     @@ KHeader.put_int32 2l
     @@ fun dst t -> Cont t)
    dst t

  type 'a sequence = ('a -> unit) -> unit

  let default : (string * (int32 * int64)) sequence -> 'o t = fun seq ->

    let boffsets = ref [] in
    let table    = Fanout.make () in
    let f (hash, (crc, offset)) =
      if is_big_offset offset
      then let idx = Int64.of_int (List.length !boffsets) in
           boffsets := offset :: !boffsets;
           Fanout.bind hash (crc, idx) table
      else Fanout.bind hash (crc, offset) table
    in

    (* make the table and the big offsets table to ensure the order. *)
    let () = seq f in

    { o_off = 0
    ; o_pos = 0
    ; o_len = 0
    ; table
    ; boffsets = Array.of_list (List.rev !boffsets)
    ; state = Header header }
end

let idx_to_tree refiller =
  let input = B.from ~proof:B.proof_bigstring 0x8000 in

  let rec loop tree t = match Decoder.eval input t with
    | `Await t ->
      let n = refiller input in
      let t = Decoder.refill 0 n t in
      loop tree t
    | `End t -> Ok tree
    | `Hash (t, (hash, crc, offset)) ->
      loop (Radix.bind tree hash offset) t
    | `Error (t, exn) -> Error exn
  in

  loop Radix.empty (Decoder.make ())
