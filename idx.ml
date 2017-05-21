module type HASH =
sig
  type t = Cstruct.buffer
  type ctx
  type buffer = Cstruct.buffer

  val pp      : Format.formatter -> t -> unit
  val length  : int
  val feed    : ctx -> buffer -> unit
  val get     : ctx -> t
  val init    : unit -> ctx
  val compare : t -> t -> int
  val hash    : t -> int
  val equal   : t -> t -> bool
end

module Lazy (Hash : HASH) =
struct
  type error =
    | Invalid_header of string
    | Invalid_version of Int32.t
    | Invalid_index
    | Expected_bigoffset_table
    | Invalid_bigoffset_index of int

  let pp = Format.fprintf
  let pp_error fmt = function
    | Invalid_header header -> pp fmt "(Invalid_header %s)" header
    | Invalid_version version -> pp fmt "(Invalid_version %ld)" version
    | Invalid_index -> pp fmt "Invalid_index"
    | Expected_bigoffset_table -> pp fmt "Expected_bigoffset_table"
    | Invalid_bigoffset_index index -> pp fmt "(Invalid_bigoffset_index %d)" index

  module Cache = Lru.M.Make(Hash)(struct type t = Crc32.t * int64 let weight _ = 1 end)

  type t =
    { map              : Cstruct.t
    ; number_of_hashes : int
    ; fanout_offset    : int
    ; hashes_offset    : int
    ; crcs_offset      : int
    ; values_offset    : int
    ; v64_offset       : int option
    ; cache            : Cache.t }

  let has map off len =
    if (off < 0 || len < 0 || off + len > Cstruct.len map)
    then raise (Invalid_argument (Printf.sprintf "%d:%d:%d" off len (Cstruct.len map)))
    else true

  let check_header map =
    if has map 0 4
    then (if Cstruct.get_char map 0 = '\255'
          && Cstruct.get_char map 1 = '\116'
          && Cstruct.get_char map 2 = '\079'
          && Cstruct.get_char map 3 = '\099'
          then Ok ()
          else Error (Invalid_header (Cstruct.to_string @@ Cstruct.sub map 0 4)))
    else Error Invalid_index

  let check_version map =
    if has map 4 4
    then (if Cstruct.BE.get_uint32 map 4 = 2l
          then Ok ()
          else Error (Invalid_version (Cstruct.BE.get_uint32 map 4)))
    else Error Invalid_index

  let number_of_hashes map =
    if has map 8 (256 * 4)
    then let n = Cstruct.BE.get_uint32 map (8 + (255 * 4)) in
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
      let v64_offset    = 8 + (256 * 4) + (number_of_hashes * 20) + (number_of_hashes * 4) + (number_of_hashes * 4) in

      let v64_offset = if v64_offset + (Hash.length * 2) = Cstruct.len map then None else Some v64_offset in

      Ok { map
         ; number_of_hashes
         ; fanout_offset
         ; hashes_offset
         ; crcs_offset
         ; values_offset
         ; v64_offset
         ; cache = Cache.create ~random:true 1024 }

  exception Break

  let compare buf off hash =
    try for i = 0 to 19
        do if Cstruct.get_char buf (off + i) <> Cstruct.get_char hash i
           then raise Break
        done; true
    with Break -> false

  exception ReturnT
  exception ReturnF

  let lt buf off hash =
    try for i = 0 to 19
        do let a = Cstruct.get_uint8 buf (off + i) in
           let b = Cstruct.get_uint8 hash i in

           if a > b
           then raise ReturnT
           else if a <> b then raise ReturnF;
        done; false
    with ReturnT -> true
       | ReturnF -> false

  let binary_search buf hash =
    let rec aux off len buf =
      if len = 20
      then (off / 20)
      else
        let len' = ((len / 40) * 20) in
        let off' = off + len' in

        if compare buf off' hash
        then (off' / 20)
        else if lt buf off' hash
        then (aux[@tailcall]) off len' buf
        else (aux[@tailcall]) off' (len - len') buf
    in

    aux 0 (Cstruct.len buf) buf

  let fanout_idx t hash =
    match Cstruct.get_uint8 hash 0 with
    | 0 ->
      let n = Cstruct.BE.get_uint32 t.map t.fanout_offset in
      Ok (binary_search (Cstruct.sub t.map t.hashes_offset (Int32.to_int n * 20)) hash)
    | idx ->
      if has t.map (t.fanout_offset + (4 * idx)) 4
      && has t.map (t.fanout_offset + (4 * (idx - 1))) 4
      then let off1 = Int32.to_int @@ Cstruct.BE.get_uint32 t.map (t.fanout_offset + (4 * idx)) in
           let off0 = Int32.to_int @@ Cstruct.BE.get_uint32 t.map (t.fanout_offset + (4 * (idx - 1))) in

           if has t.map (t.hashes_offset + (off0 * 20)) ((off1 - off0) * 20)
           then Ok (binary_search (Cstruct.sub t.map (t.hashes_offset + (off0 * 20)) ((off1 - off0) * 20)) hash + off0)
           else Error Invalid_index
      else Error Invalid_index

  let nth t n =
    let off = Int32.to_int @@ Int32.mul 20l n in

    Cstruct.sub t.map (t.hashes_offset + off) 20

  let find t hash =
    match Cache.find hash t.cache with
    | Some (crc, offset) -> Some (crc, offset)
    | None ->
      match fanout_idx t (Cstruct.of_bigarray hash) with
      | Ok idx ->
        let crc = Cstruct.BE.get_uint32 t.map (t.crcs_offset + (idx * 4)) in
        let off = Cstruct.BE.get_uint32 t.map (t.values_offset + (idx * 4)) in

        let off =
          if Int32.equal 0l (Int32.logand off 0x80000000l)
          then Ok (Int64.of_int32 off)
          else (match t.v64_offset with
              | Some v64_offset ->
                let n = Int32.to_int (Int32.logand off 0x7FFFFFFFl) in

                if has t.map (v64_offset + (n * 8)) 8
                then Ok (Cstruct.BE.get_uint64 t.map (v64_offset + (n * 8)))
                else Error (Invalid_bigoffset_index n)
              | None -> Error Expected_bigoffset_table)
        in

        (match off with
         | Ok off ->
           Cache.add hash (Crc32.of_int32 crc, off) t.cache;
           Some (Crc32.of_int32 crc,  off)
         | Error _ -> None)
      | Error _ -> None

  let iter t f =
    for i = 0 to t.number_of_hashes - 1
    do
      let hash = Cstruct.sub t.map (t.hashes_offset + (i * Hash.length)) Hash.length in
      let crc = Crc32.of_int32 (Cstruct.BE.get_uint32 t.map (t.crcs_offset + (i * 4))) in
      let off = Cstruct.BE.get_uint32 t.map (t.values_offset + (i * 4)) in

      let off =
        if Int32.equal 0l (Int32.logand off 0x80000000l)
        then Int64.of_int32 off
        else (match t.v64_offset with
            | Some v64_offset ->
              let n = Int32.to_int (Int32.logand off 0x7FFFFFFFl) in

              Cstruct.BE.get_uint64 t.map (v64_offset + (n * 8))
            | None -> raise (Invalid_argument "Expected big offset table"))
      in

      f (Cstruct.to_bigarray hash) (crc, off)
    done

  let fold t f a =
    let a = ref a in

    iter t (fun k v -> a := f k v !a); !a
end

module Option =
struct
  let bind f = function Some x -> Some (f x) | None -> None
  let value ~default = function Some x -> x | None -> default
end

module Decoder (Hash : HASH) =
struct
  type error =
    | Invalid_byte of int
    | Invalid_version of Int32.t
    | Invalid_index_of_bigoffset of int
    | Expected_bigoffset_table
    | Invalid_hash of Hash.t * Hash.t

  let pp = Format.fprintf

  let pp_error fmt = function
    | Invalid_byte byte              -> pp fmt "(Invalid_byte %02x)" byte
    | Invalid_version version        -> pp fmt "(Invalid_version %ld)" version
    | Invalid_index_of_bigoffset idx -> pp fmt "(Invalid_index_of_bigoffset %d)" idx
    | Expected_bigoffset_table       -> pp fmt "Expected_bigoffset_table"
    | Invalid_hash (has, expect)     -> pp fmt "(Invalid_hash (%a, %a))" Hash.pp has Hash.pp expect

  type t =
    { i_off     : int
    ; i_pos     : int
    ; i_len     : int
    ; fanout    : Int32.t array
    ; hashes    : Hash.t Queue.t
    ; crcs      : Crc32.t Queue.t
    ; offsets   : (Int32.t * bool) Queue.t
    ; hash      : Hash.ctx
    ; state     : state }
  and k = Cstruct.t -> t -> res
  and state =
    | Header    of k
    | Fanout    of k
    | Hashes    of k
    | Crcs      of k
    | Offsets   of k
    | Hash      of k
    | Ret       of int64 array option * Hash.t * Hash.t
    | End       of Hash.t
    | Exception of error
  and res =
    | Wait   of t
    | Error  of t * error
    | Cont   of t
    | Result of t * (Hash.t * Crc32.t * Int64.t)
    | Ok     of t * Hash.t

  let pp_state fmt = function
    | Header _                  -> pp fmt "(Header #k)"
    | Fanout _                  -> pp fmt "(Fanout #k)"
    | Hashes _                  -> pp fmt "(Hashes #k)"
    | Crcs _                    -> pp fmt "(Crcs #k)"
    | Offsets _                 -> pp fmt "(Offsets #k)"
    | Hash _                    -> pp fmt "(Hash #k)"
    | Ret (boffs, hash_idx, hash_pack) -> pp fmt "(Ret (big offsets:%d, idx:%a, pack:%a))"
                                            (Option.value ~default:0 (Option.bind Array.length boffs))
                                            Hash.pp hash_idx Hash.pp hash_pack
    | End hash_pack             -> pp fmt "(End %a)" Hash.pp hash_pack
    | Exception exn             -> pp fmt "(Exception %a)" pp_error exn

  let pp fmt t =
    pp fmt "{ @[<hov>i_off = %d;@ \
                     i_pos = %d;@ \
                     i_len = %d;@ \
                     fanout = #table;@ \
                     hashes = #queue;@ \
                     crcs = #queue;@ \
                     offsets = #queue;@ \
                     hash = #ctx;@ \
                     state = %a;@] }"
      t.i_off t.i_pos t.i_len pp_state t.state

  let await src t =
    let () = Hash.feed t.hash (Cstruct.to_bigarray (Cstruct.sub src t.i_off t.i_pos)) in
    Wait t
  let error t exn = Error ({ t with state = Exception exn }, exn)
  let ok t hash   = Ok ({ t with state = End hash }, hash)

  let to_int32 b0 b1 b2 b3 =
    let ( << ) = Int32.shift_left in (* >> (tuareg) *)
    let ( || ) = Int32.logor in
    (Int32.of_int b0 << 24)          (* >> (tuareg) *)
    || (Int32.of_int b1 << 16)       (* >> (tuareg) *)
    || (Int32.of_int b2 << 8)        (* >> (tuareg) *)
    || (Int32.of_int b3)

  let to_int64 b0 b1 b2 b3 b4 b5 b6 b7 =
    let ( << ) = Int64.shift_left in (* >> (tuareg) *)
    let ( || ) = Int64.logor in
    (Int64.of_int b0 << 56)          (* >> (tuareg) *)
    || (Int64.of_int b1 << 48)       (* >> (tuareg) *)
    || (Int64.of_int b2 << 40)       (* >> (tuareg) *)
    || (Int64.of_int b3 << 32)       (* >> (tuareg) *)
    || (Int64.of_int b4 << 24)       (* >> (tuareg) *)
    || (Int64.of_int b5 << 16)       (* >> (tuareg) *)
    || (Int64.of_int b6 << 8)        (* >> (tuareg) *)
    || (Int64.of_int b7)

  let rec get_byte ~ctor k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Cstruct.get_uint8 src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1 }
      else await src { t with state = ctor (fun src t -> (get_byte[@tailcall]) ~ctor k src t) }

  let rec get_u32 ~ctor k src t =
    if (t.i_len - t.i_pos) > 3
    then let num = Cstruct.BE.get_uint32 src (t.i_off + t.i_pos) in
          k num src
            { t with i_pos = t.i_pos + 4 }
    else if (t.i_len - t.i_pos) > 0
    then (get_byte ~ctor
          @@ fun byte0 -> get_byte ~ctor
          @@ fun byte1 -> get_byte ~ctor
          @@ fun byte2 -> get_byte ~ctor
          @@ fun byte3 src t ->
              k (to_int32 byte0 byte1 byte2 byte3) src t)
          src t
    else await src { t with state = Header (fun src t -> (get_u32[@tailcall]) ~ctor k src t) }

  let rec get_u64 ~ctor k src t =
    if (t.i_len - t.i_pos) > 7
    then let num = Cstruct.BE.get_uint64 src (t.i_off + t.i_pos) in
          k num src
            { t with i_pos = t.i_pos + 8 }
    else if (t.i_len - t.i_pos) > 0
    then (get_byte ~ctor
          @@ fun byte0 -> get_byte ~ctor
          @@ fun byte1 -> get_byte ~ctor
          @@ fun byte2 -> get_byte ~ctor
          @@ fun byte3 -> get_byte ~ctor
          @@ fun byte4 -> get_byte ~ctor
          @@ fun byte5 -> get_byte ~ctor
          @@ fun byte6 -> get_byte ~ctor
          @@ fun byte7 src t ->
              k (to_int64 byte0 byte1 byte2 byte3 byte4 byte5 byte6 byte7) src t)
          src t
    else await src { t with state = Offsets (fun src t -> (get_u64[@tailcall]) k ~ctor src t) }


  module KHeader =
  struct
    let rec check_byte chr k src t =
      if (t.i_len - t.i_pos) > 0 && Cstruct.get_char src (t.i_off + t.i_pos) = chr
      then k src { t with i_pos = t.i_pos + 1 }
      else if (t.i_len - t.i_pos) = 0
      then await src { t with state = Header (fun src t -> (check_byte[@tailcall]) chr k src t) }
      else error { t with i_pos = t.i_pos + 1 }
                 (Invalid_byte (Cstruct.get_uint8 src (t.i_off + t.i_pos)))

    let get_byte = get_byte ~ctor:(fun k -> Header k)
    let get_u32  = get_u32  ~ctor:(fun k -> Header k)
  end

  module KFanoutTable =
  struct
    let get_byte = get_byte ~ctor:(fun k -> Fanout k)
    let get_u32  = get_u32  ~ctor:(fun k -> Fanout k)
  end

  module KHashes =
  struct
    let get_byte = get_byte ~ctor:(fun k -> Hashes k)

    let get_hash k src t =
      let res = Cstruct.create Hash.length in

      let rec loop i src t =
        if i = Hash.length
        then k (Cstruct.to_bigarray res) src t
        else
          get_byte (fun byte src t ->
              Cstruct.set_uint8 res i byte;
              (loop[@tailcall]) (i + 1) src t)
            src t
      in

      loop 0 src t
  end

  module KCrcs =
  struct
    let get_byte = get_byte ~ctor:(fun k -> Crcs k)
    let get_u32  = get_u32  ~ctor:(fun k -> Crcs k)
  end

  module KOffsets =
  struct
    let get_byte = get_byte ~ctor:(fun k -> Offsets k)

    let get_u32_and_msb k src t =
      get_u32 ~ctor:(fun k -> Offsets k)
        (fun u32 src t ->
          k (u32, Int32.equal 0l (Int32.logand u32 0x80000000l)) src t)
        src t

    let get_u64 = get_u64 ~ctor:(fun k -> Offsets k)
  end

  module KHash =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Cstruct.get_uint8 src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1 }
      else Wait { t with state = Hash (fun src t -> (get_byte[@tailcall]) k src t) }
           (* don't use [await] function. *)

    let get_hash k src t =
      let res = Cstruct.create Hash.length in

      let rec loop i src t =
        if i = Hash.length
        then k (Cstruct.to_bigarray res) src t
        else
          get_byte (fun byte src t ->
              Cstruct.set_uint8 res i byte;
              (loop[@tailcall]) (i + 1) src t)
            src t
      in

      loop 0 src t
  end

  let rest ?boffsets (hash_idx, hash_pack) src t =
    match Queue.pop t.hashes, Queue.pop t.crcs, Queue.pop t.offsets with
    | hash, crc, (offset, true) -> Result ({ t with state = Ret (boffsets, hash_idx, hash_pack) }, (hash, crc, Int64.of_int32 offset))
    | exception Queue.Empty -> ok t hash_pack
    | hash, crc, (offset, false) -> match boffsets with
      | None -> error t (Expected_bigoffset_table)
      | Some arr ->
        let idx = Int32.to_int (Int32.logand offset 0x7FFFFFFFl) in
        if idx >= 0 && idx < Array.length arr
        then Result ({ t with state = Ret (boffsets, hash_idx, hash_pack) }, (hash, crc, Array.get arr idx))
        else error t (Invalid_index_of_bigoffset idx)

  let rec hash ?boffsets src t =
    let aux k src t =
      let () = Hash.feed t.hash (Cstruct.to_bigarray (Cstruct.sub src t.i_off t.i_pos)) in
      KHash.get_hash k src t
    in

    (KHash.get_hash
     @@ fun hash_pack -> aux
     @@ fun hash_idx src t ->
     let produce = Hash.get t.hash in

     if hash_idx <> produce
     then error t (Invalid_hash (Hash.get t.hash, hash_idx))
     else rest ?boffsets (hash_idx, hash_pack) src t)
    src t

  let rec boffsets arr idx max src t =
    if idx >= max
    then hash ~boffsets:arr src t
    else KOffsets.get_u64
           (fun offset src t ->
              Array.set arr idx offset;
              (boffsets[@tailcall]) arr (succ idx) max src t)
           src t

  let rec offsets idx boffs max src t =
    if Int32.compare idx max >= 0
    then (if boffs > 0 then boffsets (Array.make boffs 0L) 0 boffs src t else hash src t)
    else KOffsets.get_u32_and_msb
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
             Queue.add (Crc32.of_int32 crc) t.crcs;
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

  (* XXX(dinosaure): Fix this compute. See the serialization to understand. *)
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
    ; hash    = Hash.init ()
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
      | Ret (boffsets, hash_idx, hash_pack) -> rest ?boffsets (hash_idx, hash_pack) src t
      | Hash k -> k src t
      | End hash -> ok t hash
      | Exception exn -> error t exn
    in

    let rec loop t =
      match eval0 t with
      | Cont t -> loop t
      | Wait t -> `Await t
      | Ok (t, hash) -> `End (t, hash)
      | Result (t, hash) -> `Hash (t, hash)
      | Error (t, exn) -> `Error (t, exn)
    in

    loop t
end

module Encoder (Hash : HASH) =
struct
  type error = unit

  let pp_error fmt () = ()

  module K =
  struct
    type t = Hash.t

    let get : t -> int -> char = Bigarray.Array1.get
    let compare = Hash.compare
  end

  module Fanout = Fanout.Make(K)

  type t =
    { o_off    : int
    ; o_pos    : int
    ; o_len    : int
    ; write    : int
    ; table    : (Crc32.t * int64) Fanout.t
    ; boffsets : int64 array
    ; hash     : Hash.ctx
    ; pack     : Hash.t
    ; state    : state }
  and k = Cstruct.t -> t -> res
  and state =
    | Header     of k
    | Fanout     of k
    | Hashes     of k
    | Crcs       of k
    | Offsets    of k
    | BigOffsets of k
    | Hash       of k
    | End
  and res =
    | Error  of t * error
    | Flush  of t
    | Cont   of t
    | Ok     of t

  (* XXX(dinosaure): the state contains only a closure. May be we can optimize
                     the serialization with an hot loop (like Decompress). But
                     the first goal is to work!
   *)

  let pp = Format.fprintf

  let pp_state fmt = function
    | Header _     -> pp fmt "(Header #k)"
    | Fanout _     -> pp fmt "(Fanout #k)"
    | Hashes _     -> pp fmt "(Hashes #k)"
    | Crcs _       -> pp fmt "(Crcs #k)"
    | Offsets _    -> pp fmt "(Offsets #k)"
    | BigOffsets _ -> pp fmt "(BigOffsets #k)"
    | Hash _       -> pp fmt "(Hash #k)"
    | End          -> pp fmt "End"

  let pp fmt { o_off; o_pos; o_len; write; table; boffsets; hash; pack; state; } =
    pp fmt "{ @[<hov>o_off = %d;@ \
                     o_pos = %d;@ \
                     o_len = %d;@ \
                     write = %d;@ \
                     table = #table;@ \
                     boffsets = #table;@ \
                     hash = #ctx;@ \
                     pack = %a;@ \
                     state = %a;@] }"
      o_off o_pos o_len write Hash.pp pack pp_state state

  let flush dst t =
    Hash.feed t.hash (Cstruct.to_bigarray (Cstruct.sub dst t.o_off t.o_pos));
    Flush t

  module Int32 =
  struct
    include Int32

    let ( >> ) = Int32.shift_right
    let ( << ) = Int32.shift_left (* >> *)
    let ( && ) = Int32.logand
    let ( || ) = Int32.logor
    let ( ! )  = Int32.to_int
  end

  let rec put_byte ~ctor chr k dst t =
    if (t.o_len - t.o_pos) > 0
    then begin
      Cstruct.set_char dst (t.o_off + t.o_pos) chr;
      k dst { t with o_pos = t.o_pos + 1
                    ; write = t.write + 1 }
    end else flush dst { t with state = ctor (fun dst t -> (put_byte[@tailcall]) ~ctor chr k dst t) }

  let rec put_int32 ~ctor integer k dst t =
    if (t.o_len - t.o_pos) >= 4
    then begin
      Cstruct.BE.set_uint32 dst (t.o_off + t.o_pos) integer;
      k dst { t with o_pos = t.o_pos + 4
                    ; write = t.write + 4 }
    end else if (t.o_len - t.o_pos) > 0
    then let i1 = Char.unsafe_chr @@ Int32.(! ((integer && 0xFF000000l) >> 24)) in
          let i2 = Char.unsafe_chr @@ Int32.(! ((integer && 0x00FF0000l) >> 16)) in
          let i3 = Char.unsafe_chr @@ Int32.(! ((integer && 0x0000FF00l) >> 8)) in
          let i4 = Char.unsafe_chr @@ Int32.(! (integer && 0x000000FFl)) in

          (put_byte ~ctor i1
          @@ put_byte ~ctor i2
          @@ put_byte ~ctor i3
          @@ put_byte ~ctor i4 k)
          dst t
    else flush dst { t with state = ctor (fun dst t -> (put_int32[@tailcall]) ~ctor integer k dst t) }

  module KHeader =
  struct
    let put_byte = put_byte ~ctor:(fun k -> Header k)
    let put_int32 = put_int32 ~ctor:(fun k -> Header k)
  end

  module Int64 =
  struct
    include Int64

    let ( >> ) = Int64.shift_right_logical
    let ( << ) = Int64.shift_left (* >> *)
    let ( && ) = Int64.logand
    let ( || ) = Int64.logor
    let ( ! )  = Int64.to_int
  end

  module KFanout =
  struct
    let put_byte = put_byte ~ctor:(fun k -> Fanout k)
    let put_int32 = put_int32 ~ctor:(fun k -> Fanout k)
  end

  module KHashes =
  struct
    let put_hash hash k dst t =
      if t.o_len - t.o_pos >= Hash.length
      then begin
        Cstruct.blit hash 0 dst (t.o_off + t.o_pos) Hash.length;
        k dst { t with o_pos = t.o_pos + Hash.length
                     ; write = t.write + Hash.length }
      end else
        let rec loop rest dst t =
          if rest = 0
          then k dst t
          else
            let n = min rest (t.o_len - t.o_pos) in

            if n = 0
            then flush dst { t with state = Hashes (loop rest) }
            else begin
              Cstruct.blit hash (Hash.length - rest) dst (t.o_off + t.o_pos) n;
              flush dst { t with state = Hashes (loop (rest - n))
                               ; o_pos = t.o_pos + n
                               ; write = t.write + n }
            end
        in

        loop Hash.length dst t
  end

  module KCrcs =
  struct
    let put_byte = put_byte ~ctor:(fun k -> Crcs k)
    let put_int32 = put_int32 ~ctor:(fun k -> Crcs k)
  end

  module KOffsets =
  struct
    let put_byte = put_byte ~ctor:(fun k -> Offsets k)
    let put_int32 = put_int32 ~ctor:(fun k -> Offsets k)
  end

  module KBOffsets =
  struct
    let put_byte = put_byte ~ctor:(fun k -> BigOffsets k)

    let rec put_int64 integer k dst t =
      if (t.o_len - t.o_pos) >= 8
      then begin
        Cstruct.BE.set_uint64 dst (t.o_off + t.o_pos) integer;
        k dst { t with o_pos = t.o_pos + 8
                     ; write = t.write + 8 }
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
      else flush dst { t with state = BigOffsets (put_int64 integer k) }
  end

  module KHash =
  struct
    let put_hash ?(digest = true) hash k dst t =
      if t.o_len - t.o_pos >= Hash.length
      then begin
        Cstruct.blit hash 0 dst (t.o_off + t.o_pos) Hash.length;
        k dst { t with o_pos = t.o_pos + Hash.length
                     ; write = t.write + Hash.length }
      end else
        let rec loop rest dst t =
          if rest = 0
          then k dst t
          else
            let n = min rest (t.o_len - t.o_pos) in

            if n = 0
            then let t = { t with state = Hash (loop rest) } in
                 if digest then flush dst t else Flush t
            else begin
              Cstruct.blit hash (Hash.length - rest) dst (t.o_off + t.o_pos) n;
              let t = { t with state = Hash (loop (rest - n))
                             ; o_pos = t.o_pos + n
                             ; write = t.write + n }
              in
              if digest then flush dst t else Flush t
            end
        in

        loop Hash.length dst t
  end

  let ok t = Ok { t with state = End }

  let is_big_offset integer =
    Int64.(integer >> 31) <> 0L

  let hash dst t =
    (KHash.put_hash (Cstruct.of_bigarray t.pack)
     @@ fun dst t ->
     Hash.feed t.hash (Cstruct.to_bigarray (Cstruct.sub dst t.o_off t.o_pos));
     let hash = Hash.get t.hash in

     KHash.put_hash ~digest:false (Cstruct.of_bigarray hash) (fun _ t -> ok t) dst t)
    dst t

  let rec boffsets idx dst t =
    if idx = Array.length t.boffsets
    then Cont { t with state = Hash hash }
    else KBOffsets.put_int64 (Array.get t.boffsets idx)
           (boffsets (idx + 1)) dst t

  let rec offsets idx idx_boffs dst t =
    if idx = 256
    then Cont { t with state = BigOffsets (boffsets 0) }
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
    then Cont { t with state = Offsets (offsets 0 0) }
    else let rec aux acc dst t = match acc with
           | [] -> Cont { t with state = Hashes (crcs (idx + 1)) }
           | (_, (crc, _)) :: r -> KCrcs.put_int32 (Crc32.to_int32 crc) (aux r) dst t
         in
         aux (Fanout.get idx t.table) dst t

  let rec hashes idx dst t =
    if idx = 256
    then Cont { t with state = Crcs (crcs 0) }
    else let rec aux acc dst t = match acc with
           | [] -> Cont { t with state = Hashes (hashes (idx + 1)) }
           | (hash, _) :: r ->
             KHashes.put_hash (Cstruct.of_bigarray hash) (aux r) dst t
         in
         aux (Fanout.get idx t.table) dst t

  let rec fanout idx acc dst t =
    match idx with
    | 256 ->
      Cont { t with state = Hashes (hashes 0) }
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
     @@ fun dst t -> Cont { t with state = Fanout (fanout 0 0l) })
    dst t

  let flush off len t =
    { t with o_off = off
           ; o_len = len
           ; o_pos = 0 }

  let used_out t = t.o_pos

  let eval dst t =
    let eval0 t = match t.state with
      | Header k     -> k dst t
      | Fanout k     -> k dst t
      | Hashes k     -> k dst t
      | Crcs   k     -> k dst t
      | Offsets k    -> k dst t
      | BigOffsets k -> k dst t
      | Hash k       -> k dst t
      | End -> ok t
    in

    let rec loop t =
      match eval0 t with
      | Cont t -> loop t
      | Flush t -> `Flush t
      | Ok t -> `End t
      | Error (t, exn) -> `Error (t, exn)
    in

    loop t

  type 'a sequence = ('a -> unit) -> unit

  (* XXX(dinosaure): The sequence type is an abstraction of the iteration of a
                     data structure. The order of the iteration is not
                     important, the Fanout module takes care about that. So, we
                     let the user to use any data structure to store the CRC and
                     the Offset for each hash.
   *)
  let default : (Hash.t * (Crc32.t * int64)) sequence -> Hash.t -> t = fun seq hash ->

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
    ; write = 0
    ; table
    ; boffsets = Array.of_list (List.rev !boffsets)
    ; hash = Hash.init ()
    ; pack = hash
    ; state = Header header }
end
