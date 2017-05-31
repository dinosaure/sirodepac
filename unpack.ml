module Window =
struct
  type t =
    { raw : Cstruct.t
    ; off : int64
    ; len : int } (* [len] must be positive. *)

  let inside offset t =
    offset >= t.off && offset < Int64.add t.off (Int64.of_int t.len)

  let pp fmt window =
    Format.fprintf fmt "{ @[<hov>raw = #raw;@ \
                                 off = %Ld;@ \
                                 len = %d;@] }"
      window.off window.len
end

module type HASH =
sig
  type t = Cstruct.buffer

  val pp        : Format.formatter -> t -> unit
  val length    : int
  val of_string : string -> t
end

(* Implementation of deserialization of a list of hunks (from a PACK file) *)
module MakeHunkDecoder (Hash : HASH) =
struct
  type error =
    | Reserved_opcode of int
    | Wrong_copy_hunk of int * int * int

  let pp_error fmt = function
    | Reserved_opcode byte ->
      Format.fprintf fmt "(Reserved_opcode %02x)" byte
    | Wrong_copy_hunk (off, len, source) ->
      Format.fprintf fmt "(Wrong_copy_hunk (off: %d, len: %d, source: %d))" off len source

  type t =
    { i_off          : int
    ; i_pos          : int
    ; i_len          : int
    ; read           : int
    (* XXX(dinosaure): consider than it's not possible to have a hunk serialized
       in bigger than [max_native_int] bytes. In practice, it's not common to
       have a Hunks object serialized in a chunk upper than [max_int32]. But
       this case can happen and, because the architecture of this
       deserialization is non-blocking, we can fix this detail but I'm lazy like
       haskell. TODO! *)
    ; _length        : int
    ; _reference     : reference
    ; _source_length : int
    ; _target_length : int
    ; _hunks         : hunk list
    (* XXX(dinosaure): about memory optimization, this deserializer returns each
       hunk and the user can accumulate or process in the same time the git
       object. so, we allocate a [Cstruct.t] for each [Insert] and keep one
       [Cstruct.t] and used for each [Insert] returned. but internally, we
       accumulate (and copy/allocate) each hunk. in others words, we can avoid
       this and use only the non-blocking memory efficient way but I'm lazy to
       re-implement some top functions like [Decoder.get] to construct in the
       same time the git object requested. TODO! *)
    ; _tmp           : Cstruct.t
    ; state          : state }
  and k = Cstruct.t -> t -> res
  and state =
    | Header    of k
    | Stop
    | List      of k
    | Is_insert of (Cstruct.t * int * int)
    | Is_copy   of k
    | End
    | Exception of error
  and res =
    | Wait   of t
    | Error  of t * error
    | Cont   of t
    | Ok     of t * hunks
  and hunk =
    | Insert of Cstruct.t
    | Copy   of int * int
    (* XXX(dinosaure): git apply a delta-ification in only [max_int32] bytes of
       the source. That means the offset and the length can't be upper than
       [max_int32]. In 32-bits architecture, we can have a problem because in
       OCaml the [native int] is stored in 31-bits but in 64-bits architecture,
       it's safe to use in any case the [native int]. TODO! *)
  and reference =
    | Offset of int64
    | Hash   of Hash.t
  and hunks =
    { reference     : reference
    ; hunks         : hunk list
    ; length        : int
    ; source_length : int
    ; target_length : int }

  let partial_hunks t =
    { reference     = t._reference
    ; hunks         = []
    ; length        = t._length
    ; source_length = t._source_length
    ; target_length = t._target_length }

  let pp_list ~sep pp_data fmt lst =
    let rec aux = function
      | [] -> ()
      | [ x ] -> pp_data fmt x
      | x :: r -> Format.fprintf fmt "%a%a" pp_data x sep (); aux r
    in aux lst

  let pp_obj fmt = function
    | Insert buf ->
      Format.fprintf fmt "(Insert #raw)"
    | Copy (off, len) ->
      Format.fprintf fmt "(Copy (%d, %d))" off len

  let pp_reference fmt = function
    | Hash hash -> Format.fprintf fmt "(Reference %a)" Hash.pp hash
    | Offset off -> Format.fprintf fmt "(Offset %Ld)" off

  let pp_state fmt = function
    | Header k ->
      Format.fprintf fmt "(Header #k)"
    | Stop ->
      Format.fprintf fmt "Stop"
    | List k ->
      Format.fprintf fmt "(List #k)"
    | Is_insert (raw, off, len) ->
      Format.fprintf fmt "(Is_insert (#raw, %d, %d))" off len
    | Is_copy k ->
      Format.fprintf fmt "(Is_copy #k)"
    | End ->
      Format.fprintf fmt "End"
    | Exception exn ->
      Format.fprintf fmt "(Exception @[<hov>%a@])" pp_error exn

  let pp_hunks fmt hunks =
    Format.fprintf fmt "{ @[<hov>reference = @[<hov>%a@];@ \
                                hunks = [ @[<hov>%a@] ];@ \
                                length = %d;@ \
                                source_length = %d;@ \
                                target_length = %d@] }"
      pp_reference hunks.reference
      (pp_list ~sep:(fun fmt () -> Format.fprintf fmt ";@ ") pp_obj) hunks.hunks
      hunks.length hunks.source_length hunks.target_length

  let pp fmt t =
    Format.fprintf fmt "{ @[<hov>i_off = %d;@ \
                                 i_pos = %d;@ \
                                 i_len = %d;@ \
                                 read = %d;@ \
                                 length = %d;@ \
                                 reference = @[<hov>%a@];@ \
                                 source_length = %d;@ \
                                 target_length = %d;@ \
                                 hunks = [ @[<hov>%a@] ];@ \
                                 state = @[<hov>%a@];@] }"
      t.i_off t.i_pos t.i_len t.read
      t._length pp_reference t._reference t._source_length t._target_length
      (pp_list ~sep:(fun fmt () -> Format.fprintf fmt ";@ ") pp_obj) t._hunks
      pp_state t.state

  let await t     = Wait t
  let error t exn = Error ({ t with state = Exception exn }, exn)
  let ok t        = Ok ({ t with state = End },
                          { reference     = t._reference
                          ; hunks         = t._hunks
                          ; length        = t._length
                          ; source_length = t._source_length
                          ; target_length = t._target_length })

  let rec get_byte ~ctor k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Cstruct.get_uint8 src (t.i_off + t.i_pos) in
          k byte src
            { t with i_pos = t.i_pos + 1
                    ; read = t.read + 1 }
      else await { t with state = ctor (fun src t -> (get_byte[@tailcall]) ~ctor k src t) }

  module KHeader =
  struct
    let get_byte = get_byte ~ctor:(fun k -> Header k)

    let rec length msb (len, bit) k src t = match msb with
      | true ->
        get_byte
          (fun byte src t ->
            let msb = byte land 0x80 <> 0 in
            (length[@tailcall]) msb (((byte land 0x7F) lsl bit) lor len, bit + 7)
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
    let get_byte = get_byte ~ctor:(fun k -> List k)
  end

  let rec copy opcode src t =
    let rec get_byte flag k src t =
      if not flag
      then k 0 src t
      else if (t.i_len - t.i_pos) > 0
      then let byte = Cstruct.get_uint8 src (t.i_off + t.i_pos) in
        k byte src
          { t with i_pos = t.i_pos + 1
                 ; read = t.read + 1 }
      else await { t with state = Is_copy (fun src t -> (get_byte[@tailcall]) flag k src t) }
    in

    (get_byte (opcode land 0x01 <> 0)
     @@ fun o0 -> get_byte (opcode land 0x02 <> 0)
     @@ fun o1 -> get_byte (opcode land 0x04 <> 0)
     @@ fun o2 -> get_byte (opcode land 0x08 <> 0)
     @@ fun o3 -> get_byte (opcode land 0x10 <> 0)
     @@ fun l0 -> get_byte (opcode land 0x20 <> 0)
     @@ fun l1 -> get_byte (opcode land 0x40 <> 0)
     @@ fun l2 src t ->

       let dst = ref 0 in
       let len = ref 0 in

       if opcode land 0x01 <> 0 then dst := o0;
       if opcode land 0x02 <> 0 then dst := !dst lor (o1 lsl 8);
       if opcode land 0x04 <> 0 then dst := !dst lor (o2 lsl 16);
       if opcode land 0x08 <> 0 then dst := !dst lor (o3 lsl 24);

       if opcode land 0x10 <> 0 then len := l0;
       if opcode land 0x20 <> 0 then len := !len lor (l1 lsl 8);
       if opcode land 0x40 <> 0 then len := !len lor (l2 lsl 16);

       let dst = !dst in
       let len = !len in

       let len = if len = 0 then 0x10000 else len in

       if dst + len > t._source_length
       then begin
         error t (Wrong_copy_hunk (dst, len, t._source_length))
         (* Cont { t with state = Stop } (* avoid error *) *)
       end else
         Cont { t with _hunks = (Copy (dst, len)) :: t._hunks
                     ; state = Stop })
    src t

  let stop src t = Cont t

  let list src t =
    if t.read < t._length
    then KList.get_byte
        (fun opcode  src t ->
            if opcode = 0 then error t (Reserved_opcode opcode)
            else match opcode land 0x80 with
              | 0 ->
                Cont { t with state = Is_insert (Cstruct.sub t._tmp 0 opcode, 0, opcode) }
              | _ ->
                Cont { t with state = Is_copy (copy opcode) })
          src t
    else begin
      ok { t with _hunks = List.rev t._hunks }
    end

  let cstruct_copy cs =
    let ln = Cstruct.len cs in
    let rs = Cstruct.create ln in

    Cstruct.blit cs 0 rs 0 ln;

    rs

  let insert src t buffer (off, rest) =
    let n = min (t.i_len - t.i_pos) rest in

    Cstruct.blit src (t.i_off + t.i_pos) buffer off n;

    if rest - n = 0
    then begin
      Cont ({ t with _hunks = (Insert (cstruct_copy buffer)) :: t._hunks
                      ; i_pos = t.i_pos + n
                      ; read = t.read + n
                      ; state = Stop })
    end else await { t with i_pos = t.i_pos + n
                      ; read = t.read + n
                      ; state = Is_insert (buffer, off + n, rest - n) }

  let header src t =
    (KHeader.length
    @@ fun _source_length -> KHeader.length
    @@ fun _target_length src t ->
        Cont ({ t with state = List list
                    ; _source_length
                    ; _target_length }))
    src t

  let eval src t =
    let eval0 t =
      match t.state with
      | Header k -> k src t
      | Stop -> stop src t
      | List k -> k src t
      | Is_insert (buffer, off, rest) -> insert src t buffer (off, rest)
      | Is_copy k -> k src t
      | End -> ok t
      | Exception exn -> error t exn
    in

    let rec loop t =
      match eval0 t with
      | Cont ({ state = Stop } as t) -> `Hunk (t, List.hd t._hunks)
      | Cont t -> loop t
      | Wait t -> `Await t
      | Error (t, exn) -> `Error (t, exn)
      | Ok (t, objs) -> `Ok (t, objs)
    in

    loop t

  let default _length _reference =
    { i_off = 0
    ; i_pos = 0
    ; i_len = 0
    ; read  = 0
    ; _length
    ; _reference
    ; _source_length = 0
    ; _target_length = 0
    ; _hunks = []
    ; _tmp = Cstruct.create 0x7F
    ; state = Header header }

  let refill off len t =
    if t.i_pos = t.i_len
    then { t with i_off = off
                ; i_len = len
                ; i_pos = 0 }
    else raise (Invalid_argument (Format.sprintf "HunkDecoder.refill: you lost something"))

  let continue t =
    match t.state with
    | Stop -> { t with state = List list }
    | _ -> raise (Invalid_argument "HunkDecoder.continue: bad state")

  let current t = match t.state with
    | Stop -> List.hd t._hunks
    | _ -> raise (Invalid_argument "HunkDecoder.current: bad state")

  let available_in t = t.i_len
  let used_in t      = t.i_pos
  let read t         = t.read
end

module Int64 =
struct
  include Int64

  let ( / ) = Int64.div
  let ( * ) = Int64.mul
  let ( + ) = Int64.add
  let ( - ) = Int64.sub
  let ( << ) = Int64.shift_left (* >> (tuareg) *)
  let ( || ) = Int64.logor
end

module Int32 =
struct
  include Int32

  let ( << ) = Int32.shift_left (* >> (tuareg) *)
  let ( || ) = Int32.logor
end

module type Z =
sig
  type t
  type error
  type window

  val pp_error : Format.formatter -> error -> unit
  val pp       : Format.formatter -> t -> unit

  val window_reset : window -> window

  val default  : window -> t
  val eval     : Cstruct.t -> Cstruct.t -> t -> [ `Await of t | `Flush of t | `Error of (t * error) | `End of t ]
  val used_in  : t -> int
  val used_out : t -> int
  val write    : t -> int
  val flush    : int -> int -> t -> t
  val refill   : int -> int -> t -> t
end

(* Implementatioon of deserialization of a PACK file *)
module MakePACKDecoder (Hash : HASH) (Inflate : Z) =
struct
  module H = MakeHunkDecoder(Hash)

  type error =
    | Invalid_byte of int
    | Reserved_kind of int
    | Invalid_kind of int
    | Inflate_error of Inflate.error
    | Hunk_error of H.error
    | Hunk_input of int * int
    | Invalid_length of int * int

  let pp_error fmt = function
    | Invalid_byte byte              -> Format.fprintf fmt "(Invalid_byte %02x)" byte
    | Reserved_kind byte             -> Format.fprintf fmt "(Reserved_byte %02x)" byte
    | Invalid_kind byte              -> Format.fprintf fmt "(Invalid_kind %02x)" byte
    | Inflate_error err              -> Format.fprintf fmt "(Inflate_error %a)" Inflate.pp_error err
    | Invalid_length (expected, has) -> Format.fprintf fmt "(Invalid_length (%d <> %d))" expected has
    | Hunk_error err                 -> Format.fprintf fmt "(Hunk_error %a)" H.pp_error err
    | Hunk_input (expected, has)     -> Format.fprintf fmt "(Hunk_input (%d <> %d))" expected has

  type process =
    [ `All
    | `One
    | `Length
    | `Metadata ]

  type t =
    { i_off   : int
    ; i_pos   : int
    ; i_len   : int
    ; process : process
    ; read    : int64
    ; o_z     : Cstruct.t
    ; o_w     : Inflate.window
    ; version : int32
    ; objects : int32
    ; counter : int32
    ; state   : state }
  and k = Cstruct.t -> t -> res
  and state =
    | Header    of k
    | Object    of k
    | VariableLength of k
    | Unzip     of { offset   : int64
                   ; consumed : int
                   ; length   : int
                   ; kind     : kind
                   ; crc      : Crc32.t
                   ; z        : Inflate.t }
    | Hunks     of hunks_state
    | StopHunks of hunks_state
    | Next      of { offset   : int64
                   ; consumed : int
                   ; length   : int
                   ; length'  : int
                   (* XXX(dinosaure): [length] is the value decoded. [length']
                      is the value returned when we inflate the raw. It must to
                      be the same. However, we can inflate a huge object (like a
                      [Blob] which is not delta-ified).

                                      The length of the object can be upper than
                      [max_native_int] (but can't be upper than [max_int64]). So
                      we need to switch these values to [int64]. However, the
                      [Inflate] algorithm provide only a [native_int]. We can
                      bypass this limit and count the length of the object by
                      ourselves with an [int64]. TODO! *)
                   ; crc      : Crc32.t
                   ; kind     : kind }
    | Checksum  of k
    | End       of Hash.t
    | Exception of error
  and hunks_state =
    { offset   : int64
    ; length   : int
    ; consumed : int
    ; crc      : Crc32.t
    ; z        : Inflate.t
    ; h        : H.t }
  and res =
    | Wait  of t
    | Flush of t
    | Error of t * error
    | Cont  of t
    | Ok    of t * Hash.t
  and kind =
    | Commit
    | Tree
    | Blob
    | Tag
    | Hunk of H.hunks

  let pp_kind fmt = function
    | Commit     -> Format.fprintf fmt "Commit"
    | Tree       -> Format.fprintf fmt "Tree"
    | Blob       -> Format.fprintf fmt "Blob"
    | Tag        -> Format.fprintf fmt "Tag"
    | Hunk hunks -> Format.fprintf fmt "(Hunks @[<hov>%a@])" H.pp_hunks hunks

  let pp_hunks_state fmt { offset; length; consumed; z; h; } =
      Format.fprintf fmt "{ @[<hov>offset = %Ld;@ \
                                   consumed = %d;@ \
                                   length = %d;@ \
                                   z = %a;@ \
                                   h = @[<hov>%a@];@] })"
        offset consumed length
        Inflate.pp z H.pp h

  let pp_state fmt = function
    | Header k ->
      Format.fprintf fmt "(Header #k)"
    | Object k ->
      Format.fprintf fmt "(Object #k)"
    | VariableLength k ->
      Format.fprintf fmt "(VariableLength #k)"
    | Unzip { offset
            ; consumed
            ; length
            ; kind
            ; z } ->
      Format.fprintf fmt "(Unzip { @[<hov>offset = %Ld;@ \
                                          consumed = %d;@ \
                                          length = %d;@ \
                                          kind = @[<hov>%a@];@ \
                                          z = @[<hov>%a@];@] })"
        offset consumed length pp_kind kind
        Inflate.pp z
    | Hunks hs ->
      Format.fprintf fmt "(Hunks %a)"
        pp_hunks_state hs
    | StopHunks hs ->
      Format.fprintf fmt "(StopHunks %a)"
        pp_hunks_state hs
    | Next { offset
          ; consumed
          ; length
          ; length' } ->
      Format.fprintf fmt "(Next { @[<hov>offset = %Ld;@ \
                                        consumed = %d;@ \
                                        length = %d;@ \
                                        length' = %d;@] })"
        offset consumed length length'
    | Checksum k ->
      Format.fprintf fmt "(Checksum #k)"
    | End hash ->
      Format.fprintf fmt "(End @[<hov>%a@])" Hash.pp hash
    | Exception err ->
      Format.fprintf fmt "(Exception @[<hov>%a@])" pp_error err

  let pp fmt t =
    Format.fprintf fmt "{ @[<hov>i_off = %d;@ \
                                i_pos = %d;@ \
                                i_len = %d;@ \
                                version = %ld;@ \
                                objects = %ld;@ \
                                counter = %ld;@ \
                                state = @[<hov>%a@];@] }"
      t.i_off t.i_pos t.i_len
      t.version
      t.objects
      t.counter
      pp_state t.state

  (* TODO: need to compute the hash of the input. *)
  let await t      = Wait t
  let flush t      = Flush t
  let error t exn  = Error ({ t with state = Exception exn }, exn)
  let continue t   = Cont t
  let ok t hash    = Ok ({ t with state = End hash }, hash)

  let rec get_byte ~ctor k src t =
    if (t.i_len - t.i_pos) > 0
    then let byte = Cstruct.get_uint8 src (t.i_off + t.i_pos) in
        k byte src { t with i_pos = t.i_pos + 1
                          ; read = Int64.add t.read 1L }
    else await { t with state = ctor (fun src t -> (get_byte[@tailcall]) ~ctor k src t) }

  let get_hash ~ctor k src t =
    let buf = Buffer.create Hash.length in

    let rec loop i src t =
      if i = Hash.length
      then k (Hash.of_string (Buffer.contents buf)) src t
      else
        get_byte ~ctor
          (fun byte src t ->
             Buffer.add_char buf (Char.chr byte);
             (loop[@tailcall]) (i + 1) src t)
          src t
    in

    loop 0 src t

  module KHeader =
  struct
    let rec check_byte chr k src t =
      if (t.i_len - t.i_pos) > 0 && Cstruct.get_char src (t.i_off + t.i_pos) = chr
      then k src { t with i_pos = t.i_pos + 1
                        ; read = Int64.add t.read 1L }
      else if (t.i_len - t.i_pos) = 0
      then await { t with state = Header (fun src t -> (check_byte[@tailcall]) chr k src t) }
      else error { t with i_pos = t.i_pos + 1
                        ; read = Int64.add t.read 1L }
                (Invalid_byte (Cstruct.get_uint8 src (t.i_off + t.i_pos)))

    let get_byte = get_byte ~ctor:(fun k -> Header k)

    let to_int32 b0 b1 b2 b3 =
      let open Int32 in
      (of_int b0 << 24)    (* >> (tuareg) *)
      || (of_int b1 << 16) (* >> (tuareg) *)
      || (of_int b2 << 8)  (* >> (tuareg) *)
      || (of_int b3)

    let rec get_u32 k src t =
      if (t.i_len - t.i_pos) > 3
      then let num = Cstruct.BE.get_uint32 src (t.i_off + t.i_pos) in
          k num src
            { t with i_pos = t.i_pos + 4
                    ; read = Int64.add t.read 4L }
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

  module KVariableLength =
  struct
    let get_byte = get_byte ~ctor:(fun k -> VariableLength k)
  end

  module KObject =
  struct
    let get_byte = get_byte ~ctor:(fun k -> Object k)
    let get_hash = get_hash ~ctor:(fun k -> Object k)
  end

  module KChecksum =
  struct
    let get_byte = get_byte ~ctor:(fun k -> Checksum k)
    let get_hash = get_hash ~ctor:(fun k -> Checksum k)
  end

  let rec length msb (len, bit) crc k src t = match msb with
    | true ->
      KVariableLength.get_byte
        (fun byte src t ->
          let msb = byte land 0x80 <> 0 in
          let crc = Crc32.digestc crc byte in
          (length[@tailcall]) msb (len lor ((byte land 0x7F) lsl bit), bit + 7) crc
          k src t)
        src t
    | false -> k len crc src t

  let rec offset msb off crc k src t = match msb with
    | true ->
      KVariableLength.get_byte
        (fun byte src t ->
          let msb = byte land 0x80 <> 0 in
          let crc = Crc32.digestc crc byte in
          (offset[@tailcall]) msb (Int64.logor (Int64.shift_left (Int64.add off 1L) 7) (Int64.of_int (byte land 0x7F))) crc
          k src t)
        src t
    | false -> k off crc src t

  let stop_hunks src t hs =
    Cont { t with state = StopHunks hs }

  (* XXX(dinosaure): Need an explanation. We must compute firstly the evaluation
     of the [H] before [Inflate] to be sure we wait something (and ask
     only when it's needed [Inflate]).

     Then, we need to be ensure than [H] consumed all bytes provided
     by the input. That means, when [H] returns [`Await], we ensure
     than [t.o_z] is totally free.

     Then, we can safely [refill] [H] and, at the same time, [flush]
     [t.o_z] because we ensure than to the next call, [Inflate.eval] appear (and
     write something inside [t.o_z]) only when [H] consumed all bytes
     available.

     So, when you change this code or [H], we need to keep these
     assertions. Otherwise, it's end of the world.

     - When [H] returns `Await, we ensure than it consumed all bytes
     available (so, you are free to do what you want with [t.o_z])
     - We [Inflate] only when it's needed (so, when [H] returns
     [`Await]) *)
  let hunks src t offset length consumed crc z h =
    match H.eval t.o_z h with
    | `Await h ->
      (match Inflate.eval src t.o_z z with
       | `Await z ->
         let consumed = consumed + Inflate.used_in z in
         let crc = Crc32.digest ~off:(t.i_off + t.i_pos) ~len:(Inflate.used_in z) crc src in

         await { t with state = Hunks { offset; length; consumed; crc; z; h; }
                      ; i_pos = t.i_pos + (Inflate.used_in z)
                      ; read  = Int64.add t.read (Int64.of_int (Inflate.used_in z)) }
       | `Flush z ->
         let h = H.refill 0 (Inflate.used_out z) h in
         let z = Inflate.flush 0 (Cstruct.len t.o_z) z in

         Cont { t with state = Hunks { offset; length; consumed; crc; z; h; } }
       | `End z ->
         (* XXX(dinosaure): In [zlib] and [decompress], it could be happen to
            return [`End] and consume a part of the input [t.o_z].

            So, we compute the CRC-32 checksum and update [consumed]. *)
         let consumed = consumed + Inflate.used_in z in
         let crc = Crc32.digest ~off:(t.i_off + t.i_pos) ~len:(Inflate.used_in z) crc src in
         let h = H.refill 0 (Inflate.used_out z) h in

         Cont { t with state = Hunks { offset; length; consumed; crc; z; h; } }
       | `Error (z, exn) -> error t (Inflate_error exn))
    | `Hunk (h, hunk) ->
      Cont { t with state = StopHunks { offset; length; consumed; crc; z; h; } }
    | `Error (h, exn) -> error t (Hunk_error exn)
    | `Ok (h, hunks) ->
      Cont { t with state = Next { length
                                 ; length' = Inflate.write z
                                 ; offset
                                 ; consumed
                                 ; crc
                                 ; kind = Hunk hunks } }

  let size_of_variable_length vl =
    let rec loop acc = function
      | 0 -> acc
      | n -> loop (acc + 1) (n lsr 7)
    in
    loop 1 (vl lsr 4) (* avoid type and msb *)

  let size_of_offset off =
    let rec loop acc = function
      | 0L -> acc
      | n -> loop (acc + 1) (Int64.shift_right n 7)
    in
    loop 1 (Int64.shift_right off 7)

  let switch typ off len crc src t =
    match typ with
    | 0b000 | 0b101 -> error t (Reserved_kind typ)
    | 0b001 ->
      Cont { t with state = Unzip { offset   = off
                                  ; length   = len
                                  ; consumed = size_of_variable_length len
                                  ; crc
                                  ; kind = Commit
                                  ; z = Inflate.flush 0 (Cstruct.len t.o_z)
                                        @@ Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Inflate.default (Inflate.window_reset t.o_w) } }
    | 0b010 ->
      Cont { t with state = Unzip { offset   = off
                                  ; length   = len
                                  ; consumed = size_of_variable_length len
                                  ; crc
                                  ; kind     = Tree
                                  ; z = Inflate.flush 0 (Cstruct.len t.o_z)
                                        @@ Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Inflate.default (Inflate.window_reset t.o_w) } }
    | 0b011 ->
      Cont { t with state = Unzip { offset   = off
                                  ; length   = len
                                  ; consumed = size_of_variable_length len
                                  ; crc
                                  ; kind     = Blob
                                  ; z = Inflate.flush 0 (Cstruct.len t.o_z)
                                        @@ Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Inflate.default (Inflate.window_reset t.o_w) } }
    | 0b100 ->
      Cont { t with state = Unzip { offset   = off
                                  ; length   = len
                                  ; consumed = size_of_variable_length len
                                  ; crc
                                  ; kind     = Tag
                                  ; z = Inflate.flush 0 (Cstruct.len t.o_z)
                                        @@ Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Inflate.default (Inflate.window_reset t.o_w) } }
    | 0b110 ->
      KObject.get_byte
        (fun byte src t ->
          let msb = byte land 0x80 <> 0 in
          let crc = Crc32.digestc crc byte in
          offset msb (Int64.of_int (byte land 0x7F)) crc
            (fun offset crc src t ->
               Cont { t with state = Hunks { offset   = off
                                           ; length   = len
                                           ; consumed = size_of_variable_length len
                                                        + size_of_offset offset
                                           ; crc
                                           ; z = Inflate.flush 0 (Cstruct.len t.o_z)
                                               @@ Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                               @@ Inflate.default (Inflate.window_reset t.o_w)
                                           ; h = H.default len (H.Offset offset) } })
            src t)
        src t
    | 0b111 ->
      KObject.get_hash
        (fun hash src t ->
          let crc = Crc32.digest crc (Cstruct.of_bigarray hash) in

          Cont { t with state = Hunks { offset   = off
                                      ; length   = len
                                      ; consumed = Hash.length + size_of_variable_length len
                                      ; crc
                                      ; z = Inflate.flush 0 (Cstruct.len t.o_z)
                                            @@ Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                            @@ Inflate.default (Inflate.window_reset t.o_w)
                                      ; h = H.default len (H.Hash hash) } })
        src t
    | _  -> error t (Invalid_kind typ)

  (* TODO: check the hash procuded and the hash noticed. *)
  let checksum src t =
    KChecksum.get_hash
      (fun hash src t -> ok t hash)
      src t

  let kind src t =
    let offset = t.read in

    KObject.get_byte
      (fun byte src t ->
        let msb = byte land 0x80 <> 0 in
        let typ = (byte land 0x70) lsr 4 in
        let crc = Crc32.digestc Crc32.default byte in
        length msb (byte land 0x0F, 4) crc (switch typ offset) src t)
      src t

  let unzip src t offset length consumed crc kind z =
    match Inflate.eval src t.o_z z with
    | `Await z ->
      let crc = Crc32.digest ~off:(t.i_off + t.i_pos) ~len:(Inflate.used_in z) crc src in
      let consumed = consumed + Inflate.used_in z in

      await { t with state = Unzip { offset
                                   ; length
                                   ; consumed
                                   ; crc
                                   ; kind
                                   ; z }
                   ; i_pos = t.i_pos + Inflate.used_in z
                   ; read = Int64.add t.read (Int64.of_int (Inflate.used_in z)) }
    | `Flush z ->
      flush { t with state = Unzip { offset; length; consumed; crc
                                   ; kind
                                   ; z } }
    | `End z ->
      if Inflate.used_out z <> 0
      then flush { t with state = Unzip { offset; length; consumed; crc
                                        ; kind
                                        ; z } }
      else
        let crc = Crc32.digest ~off:(t.i_off + t.i_pos) ~len:(Inflate.used_in z) crc src in
        let consumed = consumed + Inflate.used_in z in

        Cont { t with state = Next { length
                                   ; length' = Inflate.write z
                                   ; consumed = consumed + Inflate.used_in z
                                   ; offset
                                   ; crc
                                   ; kind }
                    ; i_pos = t.i_pos + Inflate.used_in z
                    ; read = Int64.add t.read (Int64.of_int (Inflate.used_in z)) }
    | `Error (z, exn) ->
      error t (Inflate_error exn)

  let next src t length length' kind =
    if length = length'
    then Cont t
    else error t (Invalid_length (length, length'))

  let number src t =
    KHeader.get_u32
      (fun objects src t ->
        Cont { t with objects = objects
                    ; counter = objects
                    ; state = Object kind })
      src t

  let version src t =
    KHeader.get_u32
      (fun version src t ->
        number src { t with version = version })
      src t

  let header src t =
    (KHeader.check_byte 'P'
    @@ KHeader.check_byte 'A'
    @@ KHeader.check_byte 'C'
    @@ KHeader.check_byte 'K'
    @@ version)
    src t

  let default z_tmp z_win =
    { i_off   = 0
    ; i_pos   = 0
    ; i_len   = 0
    ; process = `All
    ; o_z     = z_tmp
    ; o_w     = z_win
    ; read    = 0L
    ; version = 0l
    ; objects = 0l
    ; counter = 0l
    ; state   = Header header }

  let size_of_vl vl =
    let rec loop acc = function
      | 0 -> acc
      | n -> loop (acc + 1) (n lsr 7)
    in
    loop 1 (vl lsr 4) (* avoid type and msb *)

  let from_window window win_offset z_tmp z_win =
    { i_off   = 0
    ; i_pos   = 0
    ; i_len   = 0
    ; process = `One
    ; o_z     = z_tmp
    ; o_w     = z_win
    ; read    = Int64.add window.Window.off (Int64.of_int win_offset)
    ; version = 0l
    ; objects = 1l
    ; counter = 1l
    ; state   = Object kind }

  let process_length window win_offset z_tmp z_win =
    { i_off   = 0
    ; i_pos   = 0
    ; i_len   = 0
    ; process = `Length
    ; o_z     = z_tmp
    ; o_w     = z_win
    ; read    = Int64.add window.Window.off (Int64.of_int win_offset)
    ; version = 0l
    ; objects = 1l
    ; counter = 1l
    ; state   = Object kind }

  let process_metadata window win_offset z_tmp z_win =
    { i_off   = 0
    ; i_pos   = 0
    ; i_len   = 0
    ; process = `Metadata
    ; o_z     = z_tmp
    ; o_w     = z_win
    ; read    = Int64.add window.Window.off (Int64.of_int win_offset)
    ; version = 0l
    ; objects = 1l
    ; counter = 1l
    ; state   = Object kind }

  let refill off len t =
    if (t.i_len - t.i_pos) = 0
    then match t.state with
        | Unzip { offset; length; consumed; crc; kind; z; } ->
          { t with i_off = off
                  ; i_len = len
                  ; i_pos = 0
                  ; state = Unzip { offset
                                  ; length
                                  ; consumed
                                  ; crc
                                  ; kind
                                  ; z = Inflate.refill off len z } }
        | Hunks { offset; length; consumed; crc; z; h; } ->
          { t with i_off = off
                  ; i_len = len
                  ; i_pos = 0
                  ; state = Hunks { offset
                                  ; length
                                  ; consumed
                                  ; crc
                                  ; z = Inflate.refill off len z; h; } }
        | _ -> { t with i_off = off
                      ; i_len = len
                      ; i_pos = 0 }
    else raise (Invalid_argument (Format.sprintf "PACKDecoder.refill: you lost something (pos: %d, len: %d)" t.i_pos t.i_len))

  let flush off len t = match t.state with
    | Unzip { offset; length; consumed; crc; kind; z } ->
      { t with state = Unzip { offset
                             ; length
                             ; consumed
                             ; crc
                             ; kind
                             ; z = Inflate.flush off len z } }
    | _ -> raise (Invalid_argument "PACKDecoder.flush: bad state")

  let output t = match t.state with
    | Unzip { z; _ } ->
      t.o_z, Inflate.used_out z
    | _ -> raise (Invalid_argument "PACKDecoder.output: bad state")

  let next_object t =
    match t.state with
    | Next _ when t.process = `All ->
      if Int32.pred t.counter = 0l
      then { t with state = Checksum checksum
                  ; counter = Int32.pred t.counter }
      else { t with state = Object kind
                  ; counter = Int32.pred t.counter }
    | Next _ -> { t with state = End (Hash.of_string (String.make Hash.length '\000')) }
      (* XXX(dinosaure): in the local case, the user don't care about the hash of the PACK file. *)
    | _ -> raise (Invalid_argument "PACKDecoder.next_object: bad state")

  let kind t = match t.state with
    | Unzip { kind; _ } -> kind
    | StopHunks { h; _ }
    | Hunks { h; _ } -> Hunk (H.partial_hunks h)
    | Next { kind; _ } -> kind
    | _ -> raise (Invalid_argument "PACKDecoder.kind: bad state")

  let length t = match t.state with
    | Unzip { length; _ } -> length
    | StopHunks { length; _ } -> length
    | Hunks { length; _ } -> length
    | Next { length; _ } -> length
    | _ -> raise (Invalid_argument "PACKDecoder.length: bad state")

  (* XXX(dinosaure): The consumed value calculated in this deserialization is
     different from what git says (a diff of 1 or 2 bytes) - may be it come from
     a wrong compute of the length of the offset value (see {!size_of_offset}).
     It's not very important but FIXME! *)
  let consumed t = match t.state with
    | Next { consumed; _ } -> consumed
    | _ -> raise (Invalid_argument "PACKDecoder.consumed: bad state")

  let offset t = match t.state with
    | Unzip { offset; _ } -> offset
    | StopHunks { offset; _ } -> offset
    | Hunks { offset; _ } -> offset
    | Next { offset; _ } -> offset
    | _ -> raise (Invalid_argument "PACKDecoder.offset: bad state")

  let crc t = match t.state with
    | Next { crc; _ } -> crc
    | _ -> raise (Invalid_argument "PACKDecoder.crc: bad state")

  let continue t =
    match t.state with
    | StopHunks hs ->
      { t with state = Hunks { hs with h = H.continue hs.h } }
    | _ -> raise (Invalid_argument "PACKDecoder.continue: bad state")

  let eval0 src t =
    match t.state with
    | Header k -> k src t
    | Object k -> k src t
    | VariableLength k -> k src t
    | Unzip { offset; length; consumed; crc; kind; z; } ->
      unzip src t offset length consumed crc kind z
    | Hunks { offset; length; consumed; crc; z; h; } ->
      hunks src t offset length consumed crc z h
    | StopHunks hs -> stop_hunks src t hs
    | Next { length; length'; kind; } -> next src t length length' kind
    | Checksum k -> k src t
    | End hash -> ok t hash
    | Exception exn -> error t exn

  let rec eval src t =
    let rec loop t =
      match eval0 src t with
      | Cont ({ state = Next _ } as t) ->
        `Object t
      | Cont ({ state = StopHunks hs } as t) ->
        `Hunk (t, H.current hs.h)
      | Cont t -> loop t
      | Wait t -> `Await t
      | Flush t -> `Flush t
      | Ok (t, hash) -> `End (t, hash)
      | Error (t, exn) -> `Error (t, exn)
    in

    loop t

  let rec eval_length src t =
    let rec loop t =
      match eval0 src t with
      | Cont (({ state = Next _ } | { state = Unzip _ } | { state = Hunks { h = { H.state = H.List _ } } }) as t) -> `Length t
      | Cont ({ state = StopHunks hs } as t) -> loop (continue t)
      | Cont t -> loop t
      | Wait t -> `Await t
      | Flush t -> `Flush t
      | Ok (t, hash) -> `End (t, hash)
      | Error (t, exn) -> `Error (t, exn)
    in

    assert (t.process = `Length);
    loop t

  let rec eval_metadata src t =
    let rec loop t =
      match eval0 src t with
      | Cont (({ state = Next _ } | { state = Unzip _ } | { state = Hunks _ }) as t) -> `Metadata t
      | Cont ({ state = StopHunks hs } as t) -> loop (continue t)
      | Cont t -> loop t
      | Wait t -> `Await t
      | Flush t -> `Flush t
      | Ok (t, hash) -> `End (t, hash)
      | Error (t, exn) -> `Error (t, exn)
    in

    assert (t.process = `Metadata);
    loop t
end

module type MAPPER =
sig
  type fd

  val length : fd -> int64
  val map    : fd -> ?pos:int64 -> share:bool -> int -> Cstruct.t
end

module type DECODER =
sig
  module Hash    : HASH
  module Mapper  : MAPPER
  module Inflate : Z

  module P :
  sig
    module H :
    sig
      type error =
        | Reserved_opcode of int
        | Wrong_copy_hunk of int * int * int

      type t

      type hunk =
        | Insert of Cstruct.t
        | Copy of int * int

      type reference =
        | Offset of int64
        | Hash of Hash.t

      type hunks =
        { reference     : reference
        ; hunks         : hunk list
        ; length        : int
        ; source_length : int
        ; target_length : int
        ; }

      val partial_hunks : t -> hunks

      val pp_reference : Format.formatter -> reference -> unit
      val pp_hunks : Format.formatter -> hunks -> unit
      val pp : Format.formatter -> t -> unit

      val eval : Cstruct.t -> t -> [ `Hunk of t * hunk | `Await of t | `Error of t * error | `Ok of t * hunks ]

      val default : int -> reference -> t

      val refill : int -> int -> t -> t

      val continue : t -> t

      val current : t -> hunk

      val used_in : t -> int
      val available_in : t -> int
      val read : t -> int
    end

    type error =
      | Invalid_byte of int
      | Reserved_kind of int
      | Invalid_kind of int
      | Inflate_error of Inflate.error
      | Hunk_error of H.error
      | Hunk_input of int * int
      | Invalid_length of int * int

    val pp_error : Format.formatter -> error -> unit

    type t

    val pp : Format.formatter -> t -> unit

    type kind =
      | Commit
      | Tree
      | Blob
      | Tag
      | Hunk of H.hunks

    val default : Cstruct.t -> Inflate.window -> t
    val from_window : Window.t -> int -> Cstruct.t -> Inflate.window -> t
    val process_length : Window.t -> int -> Cstruct.t -> Inflate.window -> t
    val process_metadata : Window.t -> int -> Cstruct.t -> Inflate.window -> t

    val refill : int -> int -> t -> t
    val flush : int -> int -> t -> t

    val next_object : t -> t
    val continue : t -> t

    val kind : t -> kind
    val length : t -> int
    val offset : t -> int64
    val consumed : t -> int
    val crc : t -> Crc32.t
    val output : t -> Cstruct.t * int

    val eval : Cstruct.t -> t -> [ `Object of t | `Hunk of t * H.hunk | `Await of t | `Flush of t | `End of t * Hash.t | `Error of t * error ]
    val eval_length : Cstruct.t -> t -> [ `Length of t | `Await of t | `Flush of t | `End of (t * Hash.t) | `Error of (t * error) ]
    val eval_metadata : Cstruct.t -> t -> [ `Metadata of t | `Await of t | `Flush of t | `End of (t * Hash.t) | `Error of (t * error) ]
  end

  type error =
    | Invalid_hash of Hash.t
    | Invalid_offset of int64
    | Invalid_target of (int * int)
    | Unpack_error of P.t * Window.t * P.error

  val pp_error : Format.formatter -> error -> unit

  type t

  type kind = [ `Commit | `Blob | `Tree | `Tag ]

  module Object :
  sig
    type from =
      | Offset of { length   : int
                  ; consumed : int
                  ; offset   : int64
                  ; crc      : Crc32.t
                  ; base     : from
                  ; }
      | External of Hash.t
      | Direct of { consumed : int
                  ; offset   : int64
                  ; crc      : Crc32.t
                  ; }
    and t =
      { kind   : kind
      ; raw    : Cstruct.t
      ; length : int64
      ; from   : from
      ; }

    val pp : Format.formatter -> t -> unit

    val first_crc_exn : t -> Crc32.t
  end

  val find : t -> int64 -> (Window.t * int)

  val make : ?bucket:int -> Mapper.fd
    -> (Hash.t -> Object.t option)
    -> (Hash.t -> (Crc32.t * int64) option)
    -> (int64 -> Hash.t option)
    -> (Hash.t -> (kind * Cstruct.t) option)
    -> t

  val idx : t -> (Hash.t -> (Crc32.t * int64) option)
  val cache : t -> (Hash.t -> Object.t option)
  val revidx : t -> (int64 -> Hash.t option)
  val extern : t -> (Hash.t -> (kind * Cstruct.t) option)

  val needed : ?chunk:int -> ?cache:(Hash.t -> int option) -> t -> Hash.t -> Cstruct.t -> Inflate.window -> (int, error) result
  val optimized_get' : ?chunk:int -> t -> int64 -> (Cstruct.t * Cstruct.t * int) -> Cstruct.t -> Inflate.window -> (Object.t, error) result
  val optimized_get : ?chunk:int -> t -> Hash.t -> (Cstruct.t * Cstruct.t * int) -> Cstruct.t -> Inflate.window -> (Object.t, error) result
  val get' : ?chunk:int -> t -> int64 -> Cstruct.t -> Inflate.window -> (Cstruct.t * Cstruct.t) -> (Object.t, error) result
  val get : ?chunk:int -> t -> Hash.t -> Cstruct.t -> Inflate.window -> (Cstruct.t * Cstruct.t) -> (Object.t, error) result
  val get_with_allocation : ?chunk:int -> t -> Hash.t -> Cstruct.t -> Inflate.window -> (Object.t, error) result
end


module MakeDecoder (Hash : HASH) (Mapper : MAPPER) (Inflate : Z)
  : DECODER with module Hash = Hash
             and module Mapper = Mapper
             and module Inflate = Inflate =
struct
  module Hash   = Hash
  module Mapper = Mapper
  module Inflate = Inflate

  module P      = MakePACKDecoder(Hash)(Inflate)
  module H      = MakeHunkDecoder(Hash)

  type error =
    | Invalid_hash of Hash.t
    | Invalid_offset of int64
    | Invalid_target of (int * int)
    | Unpack_error of P.t * Window.t * P.error

  let pp = Format.fprintf

  let pp_error fmt = function
    | Invalid_hash hash ->
      Format.fprintf fmt "(Invalid_hash %a)" Hash.pp hash
    | Invalid_offset off ->
      Format.fprintf fmt "(Invalid_offset %Ld)" off
    | Invalid_target (has, expected) ->
      Format.fprintf fmt "(Invalid_target (%d, %d))"
        has expected
    | Unpack_error (state, window, exn) ->
      Format.fprintf fmt "(Unpack_error { @[<hov>state = @[<hov>%a@];@ \
                                                 window = @[<hov>%a@];@ \
                                                 exn = @[<hov>%a@];@] })"
        P.pp state Window.pp window P.pp_error exn

  type kind = [ `Commit | `Blob | `Tree | `Tag ]

  let pp_kind fmt = function
    | `Commit -> Format.fprintf fmt "Commit"
    | `Blob -> Format.fprintf fmt "Blob"
    | `Tree -> Format.fprintf fmt "Tree"
    | `Tag -> Format.fprintf fmt "Tag"

  type partial =
    { _length : int
    ; _consumed : int
    ; _offset : int64
    ; _crc : Crc32.t }

  module Object =
  struct
    type from =
      | Offset of { length   : int
                  ; consumed : int
                  ; offset   : int64 (* absolute offset *)
                  ; crc      : Crc32.t
                  ; base     : from }
      | External of Hash.t
      | Direct of { consumed : int
                  ; offset   : int64
                  ; crc      : Crc32.t }

    and t =
      { kind     : kind
      ; raw      : Cstruct.t
      ; length   : int64
      ; from     : from }

    let to_partial = function
      | { length; from = Offset { consumed; crc; offset; _ } }
      | { length; from = Direct { consumed; crc; offset; } } ->
        { _length = Int64.to_int length
        ; _consumed = consumed
        ; _offset = offset
        ; _crc = crc }
      | _ -> raise (Invalid_argument "Object.to_partial: this object is external of the current PACK file")


    let rec pp_from fmt = function
      | Offset { length; consumed; offset; crc; base; } ->
        Format.fprintf fmt "(Hunk { @[<hov>length = %d;@ \
                                           consumed = %d;@ \
                                           offset = %Lx;@ \
                                           crc = @[<hov>%a@];@ \
                                           base = @[<hov>%a@];@] })"
          length consumed offset Crc32.pp crc pp_from base
      | External hash ->
        Format.fprintf fmt "(External @[<hov>%a@])" Hash.pp hash
      | Direct { consumed; offset; crc; } ->
        Format.fprintf fmt "(Direct { @[<hov>consumed = %d;@ \
                                             offset = %Lx;@ \
                                             crc = @[<hov>%a@];@] })"
          consumed offset Crc32.pp crc

    let pp fmt t =
      Format.fprintf fmt "{ @[<hov>kind = @[<hov>%a@];@ \
                                   raw = #raw;@ \
                                   length = %Ld;@ \
                                   from = @[<hov>%a@];@] }"
        pp_kind t.kind t.length pp_from t.from

    let first_crc_exn t =
      match t.from with
      | Direct { crc; _ } -> crc
      | Offset { crc; _ } -> crc
      | External _ -> raise (Invalid_argument "Object.first_crc")

    let first_offset_exn t =
      match t.from with
      | Direct { offset; _ } -> offset
      | Offset { offset; _ } -> offset
      | External _ -> raise (Invalid_argument "Object.first_offset")

    let first_offset t =
      try Some (first_offset_exn t) with _ -> None

    let deep_crc_exn t =
      let rec aux  = function
        | Direct { crc; _ } -> crc
        | Offset { base; _ } -> aux base
        | External _ -> raise (Invalid_argument "Object.deep_crc")
      in
      aux t.from
  end

  type pack_object =
    | Hunks  of partial * H.hunks
    | Object of kind * partial * Cstruct.t
    | External of Hash.t * kind * Cstruct.t

  type t =
    { file  : Mapper.fd
    ; max   : int64
    ; win   : Window.t Bucket.t
    ; cache : Hash.t -> Object.t option
    ; idx   : Hash.t -> (Crc32.t * int64) option
    ; rev   : int64 -> Hash.t option
    ; get   : Hash.t -> (kind * Cstruct.t) option
    ; hash  : Hash.t }

  let to_kind = function
    | P.Commit -> `Commit
    | P.Blob -> `Blob
    | P.Tree -> `Tree
    | P.Tag -> `Tag
    | _ -> assert false

  let map_window t offset_requested =
    let pos = Int64.((offset_requested / 1024L) * 1024L) in (* padding *)
    let map = Mapper.map t.file ~pos ~share:false (1024 * 1024) in
    { Window.raw = map
    ; off   = pos
    ; len   = Cstruct.len map }

  let find t offset_requested =
    let predicate window = Window.inside offset_requested window in

    match Bucket.find t.win predicate with
    | Some window ->
      let relative_offset = Int64.to_int Int64.(offset_requested - window.Window.off) in
      window, relative_offset
    | None ->
      let window = map_window t offset_requested in
      let relative_offset = Int64.to_int Int64.(offset_requested - window.Window.off) in

      let () = Bucket.add t.win window in window, relative_offset

  module Metadata =
  struct
    type from =
      | Offset of int64
      | Hash of Hash.t
      | Direct
    and t =
      { kind   : P.kind
      ; offset : int64
      ; length : int64
      ; crc    : Crc32.t
      ; from   : from }

    let get ?(chunk = 0x8000) t hash z_tmp z_win =
      match t.idx hash with
      | Some (crc, absolute_offset) ->
        let window, relative_offset = find t absolute_offset in
        let state = P.process_metadata window relative_offset z_tmp z_win in

        let rec loop window consumed_in_window state =
          match P.eval_metadata window.Window.raw state with
          | `Await state ->
            let rest_in_window = min (window.Window.len - consumed_in_window) chunk in

            if rest_in_window > 0
            then
              loop
                window
                (consumed_in_window + rest_in_window)
                (P.refill consumed_in_window rest_in_window state)
            else
              let window, relative_offset = find t Int64.(window.Window.off + (of_int consumed_in_window)) in
              loop
                window
                relative_offset
                (P.refill 0 0 state)
          | `End (state, _) | `Flush state -> assert false
          | `Metadata state ->
            let from = match P.kind state with
              | P.Hunk h ->
                (match h with
                 | { P.H.reference = P.H.Offset sub } ->
                   Offset (Int64.sub (P.offset state) sub)
                 | { P.H.reference = P.H.Hash hash } ->
                   Hash hash)
              | _ -> Direct
            in

            Ok { kind = P.kind state
               ; offset = P.offset state
               ; length = Int64.of_int (P.length state)
               ; crc
               ; from = from }
          | `Error (state, exn) -> Error (Unpack_error (state, window, exn))
        in

        loop window relative_offset state
      | None -> Error (Invalid_hash hash)
  end

  let apply partial_hunks hunks base raw =
    if Cstruct.len raw < hunks.H.target_length
    then raise (Invalid_argument "Decoder.apply");

    let target_length = List.fold_left
      (fun acc -> function
        | H.Insert insert ->
          Cstruct.blit insert 0 raw acc (Cstruct.len insert); acc + Cstruct.len insert
        | H.Copy (off, len) ->
          Cstruct.blit base.Object.raw off raw acc len; acc + len)
      0 hunks.H.hunks
      in

      if (target_length = hunks.H.target_length)
      then Ok Object.{ kind     = base.Object.kind
                   ; raw      = Cstruct.sub raw 0 target_length
                   ; length   = Int64.of_int hunks.H.target_length
                   ; from     = Offset { length   = partial_hunks._length
                                       ; consumed = partial_hunks._consumed
                                       ; offset   = partial_hunks._offset
                                       ; crc      = partial_hunks._crc
                                       ; base     = base.from } }
      else Error (Invalid_target (target_length, hunks.H.target_length))

  let result_bind ~err f = function Ok a -> f a | Error exn -> err

  let get_pack_object ?(chunk = 0x8000) t reference source_length source_offset z_tmp z_win r_tmp =
    if Cstruct.len r_tmp < source_length
    then raise (Invalid_argument (Format.sprintf "Decoder.delta: expect %d and have %d" source_length (Cstruct.len r_tmp)));

    let aux = function
      | Ok absolute_offset ->
        let window, relative_offset = find t absolute_offset in
        let state    = P.from_window window relative_offset z_tmp z_win in

        let rec loop window consumed_in_window writed_in_raw git_object state =
          match P.eval window.Window.raw state with
          | `Await state ->
            let rest_in_window = min (window.Window.len - consumed_in_window) chunk in

            if rest_in_window > 0
            then
              loop window
                (consumed_in_window + rest_in_window) writed_in_raw git_object
                (P.refill consumed_in_window rest_in_window state)
            else
              let window, relative_offset = find t Int64.(window.Window.off + (of_int consumed_in_window)) in
              (* XXX(dinosaure): we try to find a new window to compute the rest of the current object. *)
              loop window
                relative_offset
                writed_in_raw
                git_object
                (P.refill 0 0 state)
          | `Hunk (state, hunk) ->
            loop window consumed_in_window writed_in_raw git_object (P.continue state)
          | `Flush state ->
            let o, n = P.output state in
            Cstruct.blit o 0 r_tmp writed_in_raw n;
            loop window consumed_in_window (writed_in_raw + n) git_object (P.flush 0 n state)
          | `Object state ->
            loop window consumed_in_window writed_in_raw
              (Some (P.kind state,
                    { _length = P.length state
                    ; _consumed = P.consumed state
                    ; _offset = P.offset state
                    ; _crc = P.crc state }))
              (P.next_object state)
          | `Error (state, exn) ->
            Error (Unpack_error (state, window, exn))
          | `End (state, _) ->
            match git_object with
            | Some (kind, partial) ->
              Ok (kind, partial)
            | None -> assert false
            (* XXX: This is not possible, the [`End] state comes only after the
               [`Object] state and this state changes [kind] to [Some x]. *)
        in

        (match loop window relative_offset 0 None state with
        | Ok (P.Hunk hunks, partial) ->
          Ok (Hunks (partial, hunks))
        | Ok (kind, partial) ->
          Ok (Object (to_kind kind, partial, Cstruct.sub r_tmp 0 partial._length))
        | Error exn -> Error exn)
      | Error exn -> Error exn
    in

    match reference with
    | H.Offset off ->
      let absolute_offset =
        if off < t.max && off >= 0L
        then Ok (Int64.sub source_offset off)
        (* XXX(dinosaure): git has an invariant, [source_offset > off]. That
           means, the source referenced is only in the past from the current
           object. git did a topological sort to produce a PACK file to ensure
           all sources are before all targets. *)
        else Error (Invalid_offset off)
      in (match result_bind ~err:None t.rev absolute_offset with
          | None -> aux absolute_offset
          | Some hash -> match t.cache hash with
            | Some base ->
              Ok (Object (base.Object.kind, Object.to_partial base, base.Object.raw))
            | None -> aux absolute_offset)
    | H.Hash hash ->
      match t.cache hash with
      | Some base ->
        Ok (Object (base.Object.kind, Object.to_partial base, base.Object.raw))
      | None -> match t.idx hash with
        | Some (crc, absolute_offset) ->
          let absolute_offset =
            if absolute_offset < t.max && absolute_offset >= 0L
            then Ok absolute_offset
            else Error (Invalid_hash hash)
          in
          aux absolute_offset
        | None -> match t.get hash with
          | Some (kind, raw) -> Ok (External (hash, kind, raw))
          | None -> Error (Invalid_hash hash)

  let make ?(bucket = 10) file cache idx rev get =
    { file
    ; max  = Mapper.length file
    ; win  = Bucket.make bucket
    ; cache
    ; idx
    ; rev
    ; get
    ; hash = (Hash.of_string (String.make Hash.length '\000')) (* TODO *) }

  let idx { idx; _ } = idx
  let cache { cache; _ } = cache
  let revidx { rev; _ } = rev
  let extern { get; _ } = get

  (* XXX(dinosaure): this function returns the max length needed to undelta-ify
     a PACK object. *)
  let needed' ?(chunk = 0x8000) ?(cache = (fun _ -> None)) t value z_tmp z_win =
    let get absolute_offset =
      let window, relative_offset = find t absolute_offset in
      let state = P.process_length window relative_offset z_tmp z_win in

      let rec loop window consumed_in_window state =
        match P.eval_length window.Window.raw state with
        | `Await state ->
          let rest_in_window = min (window.Window.len - consumed_in_window) chunk in

          if rest_in_window > 0
          then
            loop window
              (consumed_in_window + rest_in_window)
              (P.refill consumed_in_window rest_in_window state)
          else
            let window, relative_offset = find t Int64.(window.Window.off + (of_int consumed_in_window)) in

            loop window
              relative_offset
              (P.refill 0 0 state)
        | `Hunk (state, hunk) ->
          loop window consumed_in_window (P.continue state)
        | `Flush state ->
          `Direct (P.length state)
        | `Length state
        | `Object state ->
          (match P.kind state with
           | P.Hunk ({ H.reference = H.Offset off; _ } as hunks) ->
             `IndirectOff (Int64.sub (P.offset state) off, max (P.length state) @@ max hunks.H.target_length hunks.H.source_length)
           | P.Hunk ({ H.reference = H.Hash hash; _ } as hunks) ->
             `IndirectHash (hash, max (P.length state) @@ max hunks.H.target_length hunks.H.source_length)
          | _ -> `Direct (P.length state))
        | `Error (state, exn) -> `Error (Unpack_error (state, window, exn))
        | `End (state, _) -> assert false
      in

      loop window relative_offset state
    in

    let rec loop length = function
      | `IndirectHash (hash, length') ->
        (match cache hash with
         | Some length'' -> Ok (max length (max length' length''))
         | None -> (match t.idx hash with
             | Some (_, off) -> loop (max length length') (get off)
             | None -> Error (Invalid_hash hash)))
      | `IndirectOff (absolute_offset, length') ->
         loop (max length length') (get absolute_offset)
      | `Direct length' ->
        Ok (max length length')
      | `Error exn -> Error exn
    in

    loop 0 value

  let needed ?(chunk = 0x8000) ?(cache = (fun _ -> None)) t hash z_tmp z_win =
    needed' ~chunk ~cache t (`IndirectHash (hash, 0)) z_tmp z_win

  (* XXX(dinosaure): Need an explanation. This function does not allocate any
     [Cstruct.t]. The purpose of this function is to get a git object from a
     PACK file (represented by [t]). The user requests the git object by the
     [hash].

     Then, to get the git object, we need 4 buffers.

     - One to store the inflated PACK object - The Window used to inflate the
     PACK object
     - Two buffer to undelta-ified the PACK object

     We can have 2 two cases in this function: - We get directly the git object
     (so, we just need to inflate the PACK object) - We get a {!H.hunks} object.
     In this case, we need to undelta-ified the object

     So, we use 2 [Cstruct.t] and swap themselves for each undelta-ification.
     Then, we return a {!Object.t} git object and return the true [Cstruct.t].

     However, to be clear, this function allocates some buffers but in same way
     as [git]. To read a PACK file, we need to allocate a buffer which contains
     the data of the PACK file. It's the purpose of the {!MAPPER} module.

     So, we [mmap] a part of the PACK file (a part of [1024 * 1024] bytes) which
     contains the offset of the PACK object requested - it's a {!Window.t}.
     Then, we compute the deserialization of the PACK object (note: sometimes,
     the {!Window.t} is not sufficient to deserialize the PACK object requested,
     so we allocate a new {!Window.t} which contains the rest of the PACK
     object).

     Finally, we limit the number of {!Window.t} available by 10 (value by
     default) and limit the allocation. Hopefully, we amortized the allocation
     because, for one {!Window.t}, we can compute some PACK objects. *)
  let optimized_get' ?(chunk = 0x8000) t absolute_offset (raw0, raw1, length) z_tmp z_win =
    let get_free_raw = function
      | true -> raw0
      | false -> raw1
    in

    let window, relative_offset = find t absolute_offset in
    let state  = P.from_window window relative_offset z_tmp z_win in

    let rec loop window consumed_in_window writed_in_raw swap git_object state =
      match P.eval window.Window.raw state with
      | `Await state ->
        let rest_in_window = min (window.Window.len - consumed_in_window) chunk in

        if rest_in_window > 0
        then
          loop window
            (consumed_in_window + rest_in_window)
            writed_in_raw
            swap
            git_object
            (P.refill consumed_in_window rest_in_window state)
        else
          let window, relative_offset = find t Int64.(window.Window.off + (of_int consumed_in_window)) in

          loop window
            relative_offset
            writed_in_raw
            swap
            git_object
            (P.refill 0 0 state)
      | `Flush state ->
        let o, n = P.output state in
        Cstruct.blit o 0 (get_free_raw swap) writed_in_raw n;
        loop window consumed_in_window (writed_in_raw + n) swap git_object (P.flush 0 (Cstruct.len o) state)
      | `Hunk (state, hunk) ->
        loop window consumed_in_window writed_in_raw swap git_object (P.continue state)
      | `Object state ->
        (match P.kind state with
         | P.Hunk hunks ->
           let partial_hunks =
             { _length   = P.length state
             ; _consumed = P.consumed state
             ; _offset   = P.offset state
             ; _crc      = P.crc state }
           in

           let rec undelta partial hunks swap =
             match get_pack_object ~chunk t hunks.H.reference hunks.H.source_length partial._offset z_tmp z_win (get_free_raw swap) with
             | Error exn -> Error exn
             | Ok (Hunks (partial_hunks, hunks)) ->
               (match undelta partial_hunks hunks (not swap) with
                | Ok base ->
                  apply partial_hunks hunks base (get_free_raw swap)
                | Error exn -> Error exn)
             | Ok (Object (kind, partial, raw)) ->
               Ok Object.{ kind
                         ; raw
                         ; length = Int64.of_int partial._length
                         ; from   = Direct { consumed = partial._consumed
                                           ; offset   = partial._offset
                                           ; crc      = partial._crc } }
             | Ok (External (hash, kind, raw)) ->
               Ok Object.{ kind
                         ; raw
                         ; length = Int64.of_int (Cstruct.len raw)
                         ; from   = External hash }
           in

           (match undelta partial_hunks hunks swap with
            | Ok base ->
              (match apply partial_hunks hunks base (get_free_raw (not swap)) with
               | Ok obj ->
                 loop window consumed_in_window writed_in_raw swap (Some obj) (P.next_object state)
               | Error exn -> Error exn)
            | Error exn -> Error exn)
         | kind ->
           let obj =
             Object.{ kind   = to_kind kind
                    ; raw    = Cstruct.sub (get_free_raw swap) 0 (P.length state)
                    ; length = Int64.of_int (P.length state)
                    ; from   = Direct { consumed = P.consumed state
                                      ; offset   = P.offset state
                                      ; crc      = P.crc state } }
           in

           loop window consumed_in_window writed_in_raw (not swap)
             (Some obj)
             (P.next_object state))
      | `Error (state, exn) ->
        Error (Unpack_error (state, window, exn))
      | `End (t, _) -> match git_object with
        | Some obj ->
          Ok obj
        | None -> assert false
    in

    loop window relative_offset 0 true None state

  let optimized_get ?(chunk = 0x8000) t hash v_tmp z_tmp z_win =
    match t.idx hash with
    | Some (_, absolute_offset) -> optimized_get' ~chunk t absolute_offset v_tmp z_tmp z_win
    | None -> Error (Invalid_hash hash)

  let get' ?chunk t absolute_offset z_tmp z_win (raw0, raw1) =
    match needed' ?chunk t (`IndirectOff (absolute_offset, 0)) z_tmp z_win with
    | Error exn -> Error exn
    | Ok length ->
      if Cstruct.len raw0 <> Cstruct.len raw1
      || Cstruct.len raw0 < length
      || Cstruct.len raw1 < length
      then raise (Invalid_argument "Decoder.get': invalid raws");

      optimized_get' ?chunk t absolute_offset (raw0, raw1, length) z_tmp z_win

  let get ?chunk t hash z_tmp z_win (raw0, raw1) =
    match needed t hash z_tmp z_win with
    | Error exn -> Error exn
    | Ok length ->
      if Cstruct.len raw0 <> Cstruct.len raw1
      || Cstruct.len raw0 < length
      || Cstruct.len raw1 < length
      then raise (Invalid_argument "Decoder.get': invalid raws");

      optimized_get ?chunk t hash (raw0, raw1, length) z_tmp z_win

  let get_with_allocation ?chunk t hash z_tmp z_win =
    match needed t hash z_tmp z_win with
    | Error exn -> Error exn
    | Ok length ->
      let tmp = Cstruct.create length, Cstruct.create length, length in

      optimized_get ?chunk t hash tmp z_tmp z_win
end
