let () = Printexc.record_backtrace true

module BBuffer =
struct
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
end

let sp = Format.sprintf

let bin_of_int d =
  if d < 0 then invalid_arg "bin_of_int" else
  if d = 0 then "0" else
  let rec aux acc d =
    if d = 0 then acc else
    aux (string_of_int (d land 1) :: acc) (d lsr 1)
  in
  String.concat "" (aux [] d)

(* Implementation of deserialization of a list of hunks (from a PACK file) *)
module H =
struct
  type error = ..
  type error += Reserved_opcode of int
  type error += Wrong_insert_hunk of int * int * int

  type t =
    { i_off          : int
    ; i_pos          : int
    ; i_len          : int
    ; read           : int
    ; _length        : int
    ; _reference     : reference
    ; _source_length : int
    ; _target_length : int
    ; _hunks         : hunk list
    ; state          : state }
  and k = Cstruct.t -> t -> res
  and state =
    | Header    of k
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
  and reference =
    | Offset of int64
    | Hash   of string
  and hunks =
    { reference     : reference
    ; hunks         : hunk list
    ; length        : int
    ; source_length : int
    ; target_length : int }

  let pp = Format.fprintf

  let pp_lst ~sep pp_data fmt lst =
    let rec aux = function
      | [] -> ()
      | [ x ] -> pp_data fmt x
      | x :: r -> pp fmt "%a%a" pp_data x sep (); aux r
    in aux lst

  let pp_obj fmt = function
    | Insert buf ->
      pp fmt "(Insert %a)" Cstruct.hexdump_pp buf
    | Copy (off, len) ->
      pp fmt "(Copy (%d, %d))" off len

  let pp_error fmt = function
    | Reserved_opcode byte -> pp fmt "(Reserved_opcode %02x)" byte
    | Wrong_insert_hunk (off, len, source) ->
      pp fmt "(Wrong_insert_hunk (off: %d, len: %d, source: %d))" off len source

  let pp_reference fmt = function
    | Hash hash -> pp fmt "(Reference %s)" hash
    | Offset off -> pp fmt "(Offset %Ld)" off

  let pp_hunks fmt ({ reference; hunks; length; source_length; target_length; } : hunks) =
    pp fmt "{@[<hov>reference = %a;@ \
                    hunks = [@[<hov>%a@]];@ \
                    length = %d;@ \
                    source_length = %d;@ \
                    target_length = %d]}"
      pp_reference reference
      (pp_lst ~sep:(fun fmt () -> pp fmt ";@ ") pp_obj) hunks
      length source_length target_length

  let pp fmt { i_off; i_pos; i_len; read; _length; _reference;
               _source_length; _target_length; _hunks; state; } =
    pp fmt "{@[<hov>i_off = %d;@ \
                    i_pos = %d;@ \
                    i_len = %d;@ \
                    read = %d;@ \
                    length = %d;@ \
                    reference = %a;@ \
                    source_length = %d;@ \
                    target_length = %d;@ \
                    hunks = [@[<hov>%a@]]@]}"
      i_off i_pos i_len read _length pp_reference _reference
      _source_length _target_length
      (pp_lst ~sep:(fun fmt () -> pp fmt ";@ ") pp_obj) _hunks

  let await t     = Wait t
  let error t exn = Error ({ t with state = Exception exn }, exn)
  let ok t        = Ok ({ t with state = End },
                          { reference     = t._reference
                          ; hunks         = t._hunks
                          ; length        = t._length
                          ; source_length = t._source_length
                          ; target_length = t._target_length })

  module KHeader =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Cstruct.get_uint8 src (t.i_off + t.i_pos) in
           k byte src
             { t with i_pos = t.i_pos + 1
                    ; read = t.read + 1 }
      else await { t with state = Header (fun src t -> (get_byte[@tailcall]) k src t) }

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
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Cstruct.get_uint8 src (t.i_off + t.i_pos) in
           k byte src 
             { t with i_pos = t.i_pos + 1
                    ; read = t.read + 1 }
      else await { t with state = List (fun src t -> (get_byte[@tailcall]) k src t) }
  end

  let rec copy opcode =
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

    get_byte ((opcode lsr 0) land 1 <> 0)
    @@ fun o0 -> get_byte ((opcode lsr 1) land 1 <> 0)
    @@ fun o1 -> get_byte ((opcode lsr 2) land 1 <> 0)
    @@ fun o2 -> get_byte ((opcode lsr 3) land 1 <> 0)
    @@ fun o3 -> get_byte ((opcode lsr 4) land 1 <> 0)
    @@ fun l0 -> get_byte ((opcode lsr 5) land 1 <> 0)
    @@ fun l1 -> get_byte ((opcode lsr 6) land 1 <> 0)
    @@ fun l2 src t ->
      let dst = o0 lor (o1 lsl 8) lor (o2 lsl 16) lor (o3 lsl 24) in
      let len =
        let len0 = l0 lor (l1 lsl 8) lor (l2 lsl 16) in
        if len0 = 0 then 0x10000 else len0
        (* XXX: see git@07c92928f2b782330df6e78dd9d019e984d820a7
           patch-delta.c:l. 50 *)
      in

      if dst + len > t._source_length
      then error t (Wrong_insert_hunk (dst, len, t._source_length))
      else Cont { t with _hunks = (Copy (dst, len)) :: t._hunks
                       ; state = List list }

  and list src t =
    if t.read < t._length
    then KList.get_byte
           (fun opcode src t ->
             if opcode = 0 then error t (Reserved_opcode opcode)
             else match opcode land 0x80 with
             | 0 -> Cont { t with state = Is_insert (Cstruct.create opcode, 0, opcode) }
             | _ -> Cont { t with state = Is_copy (copy opcode) })
           src t
    else ok { t with _hunks = List.rev t._hunks }

  let insert src t buffer (off, rest) =
    let n = min (t.i_len - t.i_pos) rest in
    Cstruct.blit src (t.i_off + t.i_pos) buffer off n;
    if rest - n = 0
    then Cont ({ t with _hunks = (Insert buffer) :: t._hunks
                      ; i_pos = t.i_pos + n
                      ; read = t.read + n
                      ; state = List list })
    else await { t with i_pos = t.i_pos + n
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
    ; state = Header header }

  let sp = Format.sprintf

  let refill off len t =
    if t.i_pos = t.i_len 
    then { t with i_off = off
                ; i_len = len
                ; i_pos = 0 }
    else raise (Invalid_argument (sp "H.refill: you lost something"))

  let available_in t = t.i_len
  let used_in t = t.i_pos
  let read t = t.read
end

module Int64 =
struct
  include Int64

  let ( / ) = Int64.div
  let ( * ) = Int64.mul
  let ( + ) = Int64.add
  let ( - ) = Int64.sub
  let ( << ) = Int64.shift_left (* >> *)
  let ( || ) = Int64.logor
end

module Window =
struct
  type t =
    { raw : Cstruct.t
    ; off : int64
    ; len : int } (* [len] must be positive. *)

  let inside offset t =
    offset >= t.off && offset < Int64.add t.off (Int64.of_int t.len)
end

(* Implementatioon of deserialization of a PACK file *)
module P =
struct
  type error = ..
  type error += Invalid_byte of int
  type error += Reserved_kind of int
  type error += Invalid_kind of int
  type error += Inflate_error of Decompress.Inflate.error
  type error += Hunk_error of H.error
  type error += Hunk_input of int * int
  type error += Invalid_length of int * int
  type error += Invalid_state

  let pp = Format.fprintf
  let pp_error fmt = function
    | Invalid_byte byte              -> pp fmt "(Invalid_byte %02x)" byte
    | Reserved_kind byte             -> pp fmt "(Reserved_byte %02x)" byte
    | Invalid_kind byte              -> pp fmt "(Invalid_kind %02x)" byte
    | Inflate_error err              -> pp fmt "(Inflate_error %a)" Decompress.Inflate.pp_error err
    | Invalid_length (expected, has) -> pp fmt "(Invalid_length (%d <> %d))" expected has
    | Hunk_error err                 -> pp fmt "(Hunk_error %a)" H.pp_error err
    | Hunk_input (expected, has)     -> pp fmt "(Hunk_input (%d <> %d))" expected has
    | Invalid_state                  -> pp fmt "Invalid_state"

  type t =
    { i_off   : int
    ; i_pos   : int
    ; i_len   : int
    ; local   : bool
    ; read    : int64
    ; o_z     : Decompress.B.bs Decompress.B.t
    ; o_w     : Decompress.B.bs Decompress.Window.t
    ; o_h     : Decompress.B.bs Decompress.B.t
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
                   ; z        : (Decompress.B.bs, Decompress.B.bs) Decompress.Inflate.t }
    | Hunks     of { offset   : int64
                   ; length   : int
                   ; consumed : int
                   ; z        : (Decompress.B.bs, Decompress.B.bs) Decompress.Inflate.t
                   ; h        : H.t }
    | Next      of { offset   : int64
                   ; consumed : int
                   ; length   : int
                   ; length'  : int
                   (* XXX(dinosaure): we have a problem in the 32-bit architecture,
                                      the [length] can be big! TODO!
                    *)
                   ; kind     : kind }
    | Checksum  of k
    | End       of string
    | Exception of error
  and res =
    | Wait  of t
    | Flush of t
    | Error of t * error
    | Cont  of t
    | Ok    of t * string
  and kind =
    | Commit
    | Tree
    | Blob
    | Tag
    | Hunk of H.hunks

  let pp_kind fmt = function
    | Commit -> pp fmt "Commit"
    | Tree -> pp fmt "Tree"
    | Blob -> pp fmt "Blob"
    | Tag -> pp fmt "Tag"
    | Hunk hunks -> pp fmt "(Hunks %a)" H.pp_hunks hunks

  let pp_state fmt = function
    | Header _ -> pp fmt "(Header #k)"
    | Object _ -> pp fmt "(Object #k)"
    | VariableLength _ -> pp fmt "(VariableLength #k)"
    | Unzip { offset
            ; consumed
            ; length
            ; kind
            ; z } ->
      pp fmt "{ @[<hov>offset = %Ld;@ \
                       consumed = %d;@ \
                       length = %d;@ \
                       kind = %a;@ \
                       z = %a;@] }"
        offset consumed length pp_kind kind Decompress.Inflate.pp z
    | Hunks { offset
            ; length
            ; consumed
            ; z
            ; h } ->
      pp fmt "{ @[<hov>offset = %Ld;@ \
                       consumed = %d;@ \
                       length = %d;@ \
                       z = %a;@ \
                       h = %a;@] }"
        offset consumed length Decompress.Inflate.pp z H.pp h
    | Next { offset
           ; consumed
           ; length
           ; length' } ->
      pp fmt "{ @[<hov>offset = %Ld;@ \
                       consumed = %d;@ \
                       length = %d;@ \
                       length' = %d;@] @}"
        offset consumed length length'
    | Checksum _ -> pp fmt "(Checksum #k)"
    | End hash -> pp fmt "(End %a)" Hash.pp hash
    | Exception err -> pp fmt "(Exception %a)" pp_error err

  let pp fmt { i_off; i_pos; i_len; o_z; version; objects; counter; state; } =
    match state with
    | Unzip { z; _ } ->
      pp fmt "{ @[<hov>i_off = %d;@ \
                       i_pos = %d;@ \
                       i_len = %d;@ \
                       version = %ld;@ \
                       objects = %ld;@ \
                       counter = %ld;@ \
                       state = %a;@ \
                       z = %a;@] }"
        i_off i_pos i_len version objects counter pp_state state Decompress.Inflate.pp z
    | Hunks { z; h; _ } ->
      pp fmt "{ @[<hov>i_off = %d;@ \
                       i_pos = %d;@ \
                       i_len = %d;@ \
                       version = %ld;@ \
                       objects = %ld;@ \
                       counter = %ld;@ \
                       state = %a;@ \
                       z = %a;@ \
                       h = %a;@] }"
        i_off i_pos i_len version objects counter pp_state state Decompress.Inflate.pp z H.pp h
    | _ ->
      pp fmt "{ @[<hov>i_off = %d;@ \
                       i_pos = %d;@ \
                       i_len = %d;@ \
                       version = %ld;@ \
                       objects = %ld;@ \
                       counter = %ld;@ \
                       state = %a@;] }"
        i_off i_pos i_len version objects counter pp_state state

  let await t      = Wait t
  let flush t      = Flush t
  let error t exn  = Error ({ t with state = Exception exn }, exn)
  let continue t   = Cont t
  let ok t hash    = Ok ({ t with state = End hash }, hash)

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

    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Cstruct.get_uint8 src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1
                             ; read = Int64.add t.read 1L }
      else await { t with state = Header (fun src t -> (get_byte[@tailcall]) k src t) }

    let to_int32 b0 b1 b2 b3 =
      let ( << ) = Int32.shift_left in (* >> *)
      let ( || ) = Int32.logor in
      (Int32.of_int b0 << 24)    (* >> *)
      || (Int32.of_int b1 << 16) (* >> *)
      || (Int32.of_int b2 << 8)  (* >> *)
      || (Int32.of_int b3)

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
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Cstruct.get_uint8 src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1
                             ; read = Int64.add t.read 1L }
      else await { t with state = VariableLength (fun src t -> (get_byte[@tailcall]) k src t) }
  end

  module KObject =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Cstruct.get_uint8 src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1
                             ; read = Int64.add t.read 1L }
      else await { t with state = Object (fun src t -> (get_byte[@tailcall]) k src t) }

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

  module KChecksum =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Cstruct.get_uint8 src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1
                             ; read = Int64.add t.read 1L }
      else await { t with state = Checksum (fun src t -> (get_byte[@tailcall]) k src t) }

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

  let rec length msb (len, bit) k src t = match msb with
    | true ->
      KVariableLength.get_byte
        (fun byte src t ->
           let msb = byte land 0x80 <> 0 in
           (length[@tailcall]) msb (len lor ((byte land 0x7F) lsl bit), bit + 7)
           k src t)
        src t
    | false -> k len src t

  let rec offset msb off k src t = match msb with
    | true ->
      KVariableLength.get_byte
        (fun byte src t ->
           let msb = byte land 0x80 <> 0 in
           (offset[@tailcall]) msb (Int64.logor (Int64.shift_left (Int64.add off 1L) 7) (Int64.of_int (byte land 0x7F)))
           k src t)
        src t
    | false -> k off src t

  let hunks src t offset length consumed z h =
    match Decompress.Inflate.eval src t.o_z z with
    | `Await z ->
      let consumed = consumed + Decompress.Inflate.used_in z in
      await { t with state = Hunks { offset; length; consumed; z; h; }
                   ; i_pos = t.i_pos + Decompress.Inflate.used_in z
                   ; read = Int64.add t.read (Int64.of_int (Decompress.Inflate.used_in z)) }
    | `Error (z, exn) -> error t (Inflate_error exn)
    | `End z ->
      let ret = if Decompress.Inflate.used_out z <> 0
                then H.eval (match t.o_z with Decompress.B.Bigstring bs -> Cstruct.of_bigarray bs) (H.refill 0 (Decompress.Inflate.used_out z) h)
                else H.eval (match t.o_z with Decompress.B.Bigstring bs -> Cstruct.of_bigarray bs) h
      in
      (match ret with
       | `Ok (h, hunks) ->
         Cont { t with state = Next { length
                                    ; length' = Decompress.Inflate.write z
                                    ; offset
                                    ; consumed = consumed + Decompress.Inflate.used_in z
                                    ; kind = Hunk hunks }
                     ; i_pos = t.i_pos + Decompress.Inflate.used_in z
                     ; read = Int64.add t.read (Int64.of_int (Decompress.Inflate.used_in z)) }
       | `Await h ->
         error t (Hunk_input (length, H.read h))
       | `Error (h, exn) -> error t (Hunk_error exn))
    | `Flush z ->
      match H.eval (match t.o_z with Decompress.B.Bigstring bs -> Cstruct.of_bigarray bs) (H.refill 0 (Decompress.Inflate.used_out z) h) with
      | `Await h ->
        Cont { t with state = Hunks { offset; length; consumed
                                    ; z = (Decompress.Inflate.flush 0 (Decompress.B.length t.o_z) z)
                                    ; h } }
      | `Error (h, exn) -> error t (Hunk_error exn)
      | `Ok (h, objs) ->
        Cont { t with state = Hunks { offset; length; consumed
                                    ; z = (Decompress.Inflate.flush 0 (Decompress.B.length t.o_z) z)
                                    ; h } }

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

  let switch typ off len src t =
    match typ with
    | 0b000 | 0b101 -> error t (Reserved_kind typ)
    | 0b001 ->
      Cont { t with state = Unzip { offset   = off
                                  ; length   = len
                                  ; consumed = size_of_variable_length len
                                  ; kind = Commit
                                  ; z = Decompress.Inflate.flush 0 (Decompress.B.length t.o_z)
                                        @@ Decompress.Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Decompress.Inflate.default (Decompress.Window.reset t.o_w) } }
    | 0b010 ->
      Cont { t with state = Unzip { offset   = off
                                  ; length   = len
                                  ; consumed = size_of_variable_length len
                                  ; kind = Tree
                                  ; z = Decompress.Inflate.flush 0 (Decompress.B.length t.o_z)
                                        @@ Decompress.Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Decompress.Inflate.default (Decompress.Window.reset t.o_w) } }
    | 0b011 ->
      Cont { t with state = Unzip { offset   = off
                                  ; length   = len
                                  ; consumed = size_of_variable_length len
                                  ; kind = Blob
                                  ; z = Decompress.Inflate.flush 0 (Decompress.B.length t.o_z)
                                        @@ Decompress.Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Decompress.Inflate.default (Decompress.Window.reset t.o_w) } }
    | 0b100 ->
      Cont { t with state = Unzip { offset   = off
                                  ; length   = len
                                  ; consumed = size_of_variable_length len
                                  ; kind = Tag
                                  ; z = Decompress.Inflate.flush 0 (Decompress.B.length t.o_z)
                                        @@ Decompress.Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Decompress.Inflate.default (Decompress.Window.reset t.o_w) } }
    | 0b110 ->
      KObject.get_byte
        (fun byte src t ->
           let msb = byte land 0x80 <> 0 in
           offset msb (Int64.of_int (byte land 0x7F))
             (fun offset src t ->
               Cont { t with state = Hunks { offset   = off
                                           ; length   = len
                                           ; consumed = size_of_variable_length len
                                                        + size_of_offset offset
                                           ; z = Decompress.Inflate.flush 0 (Decompress.B.length t.o_z)
                                                 @@ Decompress.Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                                 @@ Decompress.Inflate.default (Decompress.Window.reset t.o_w)
                                           ; h = H.default len (H.Offset offset) } })
             src t)
        src t
    | 0b111 ->
      KObject.get_hash
        (fun hash src t ->
          Cont { t with state = Hunks { offset   = off
                                      ; length   = len
                                      ; consumed = 20 + size_of_variable_length len
                                      ; z = Decompress.Inflate.flush 0 (Decompress.B.length t.o_z)
                                            @@ Decompress.Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                            @@ Decompress.Inflate.default (Decompress.Window.reset t.o_w)
                                      ; h = H.default len (H.Hash hash) } })
        src t
    | _  -> error t (Invalid_kind typ)

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
         length msb (byte land 0x0F, 4) (switch typ offset) src t)
      src t

  let unzip src t offset length consumed kind z =
    match Decompress.Inflate.eval src t.o_z z with
    | `Await z ->
      let consumed = consumed + Decompress.Inflate.used_in z in
      await { t with state = Unzip { offset
                                   ; length
                                   ; consumed
                                   ; kind
                                   ; z }
                   ; i_pos = t.i_pos + Decompress.Inflate.used_in z
                   ; read = Int64.add t.read (Int64.of_int (Decompress.Inflate.used_in z)) }
    | `Flush z ->
      flush { t with state = Unzip { offset; length; consumed
                                   ; kind
                                   ; z } }
    | `End z ->
      if Decompress.Inflate.used_out z <> 0
      then flush { t with state = Unzip { offset; length; consumed
                                        ; kind
                                        ; z } }
      else Cont { t with state = Next { length
                                      ; length' = Decompress.Inflate.write z
                                      ; consumed = consumed + Decompress.Inflate.used_in z
                                      ; offset
                                      ; kind }
                       ; i_pos = t.i_pos + Decompress.Inflate.used_in z
                       ; read = Int64.add t.read (Int64.of_int (Decompress.Inflate.used_in z)) }
    | `Error (z, exn) -> error t (Inflate_error exn)

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

  let default z_tmp z_win h_tmp =
    { i_off   = 0
    ; i_pos   = 0
    ; i_len   = 0
    ; local   = false
    ; o_z     = z_tmp
    ; o_w     = z_win
    ; o_h     = h_tmp
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

  let from_window window win_offset z_tmp z_win h_tmp =
    { i_off   = 0
    ; i_pos   = 0
    ; i_len   = 0
    ; local   = true
    ; o_z     = z_tmp
    ; o_w     = z_win
    ; o_h     = h_tmp
    ; read    = Int64.add window.Window.off (Int64.of_int win_offset)
    ; version = 0l
    ; objects = 1l
    ; counter = 1l
    ; state   = Object kind }

  let sp = Format.sprintf

  let refill off len t =
    if (t.i_len - t.i_pos) = 0
    then match t.state with
         | Unzip { offset; length; consumed; kind; z; } ->
           { t with i_off = off
                  ; i_len = len
                  ; i_pos = 0
                  ; state = Unzip { offset
                                  ; length
                                  ; consumed
                                  ; kind
                                  ; z = Decompress.Inflate.refill off len z } }
         | Hunks { offset; length; consumed; z; h; } ->
           { t with i_off = off
                  ; i_len = len
                  ; i_pos = 0
                  ; state = Hunks { offset
                                  ; length
                                  ; consumed
                                  ; z = Decompress.Inflate.refill off len z; h; } }
         | _ -> { t with i_off = off
                       ; i_len = len
                       ; i_pos = 0 }
    else raise (Invalid_argument (sp "P.refill: you lost something (pos: %d, len: %d)" t.i_pos t.i_len))

  let flush off len t = match t.state with
    | Unzip { offset; length; consumed; kind; z } ->
      { t with state = Unzip { offset
                             ; length
                             ; consumed
                             ; kind
                             ; z = Decompress.Inflate.flush off len z } }
    | _ -> raise (Invalid_argument "P.flush: bad state")

  let output t = match t.state with
    | Unzip { z; _ } ->
      (match t.o_z with Decompress.B.Bigstring bs -> Cstruct.of_bigarray bs), Decompress.Inflate.used_out z
    | Hunks { h; _ } ->
      (match t.o_h with Decompress.B.Bigstring bs -> Cstruct.of_bigarray bs), 0
    | _ -> raise (Invalid_argument "P.output: bad state")

  let next_object t = match t.state with
    | Next _ when not t.local ->
      if Int32.pred t.counter = 0l
      then { t with state = Checksum checksum
                  ; counter = Int32.pred t.counter }
      else { t with state = Object kind
                  ; counter = Int32.pred t.counter }
    | Next _ -> { t with state = End "" }
    | _ -> raise (Invalid_argument "P.next_object: bad state")

  let kind t = match t.state with
    | Next { kind; _ } -> kind
    | _ -> raise (Invalid_argument "P.kind: bad state")

  let length t = match t.state with
    | Next { length; _ } -> length
    | _ -> raise (Invalid_argument "P.length: bad state")

  (* XXX(dinosaure): the diff with git verify-pack is different. FIXME! *)
  let consumed t = match t.state with
    | Next { consumed; _ } -> consumed
    | _ -> raise (Invalid_argument "P.consumed: bad state")

  let offset t = match t.state with
    | Next { offset; _ } -> offset
    | _ -> raise (Invalid_argument "P.offset: bad state")

  let rec eval src t =
    let eval0 t = match t.state with
      | Header k -> k src t
      | Object k -> k src t
      | VariableLength k -> k src t
      | Unzip { offset; length; consumed; kind; z; } ->
        unzip (Decompress.B.Bigstring (Cstruct.to_bigarray src)) t offset length consumed kind z
      | Hunks { offset; length; consumed; z; h; } ->
        hunks (Decompress.B.Bigstring (Cstruct.to_bigarray src)) t offset length consumed z h
      | Next { length; length'; kind; } -> next src t length length' kind
      | Checksum k -> k src t
      | End hash -> ok t hash
      | Exception exn -> error t exn
    in

    let rec loop t =
      match eval0 t with
      | Cont ({ state = Next _ } as t) -> `Object t
      | Cont t -> loop t
      | Wait t -> `Await t
      | Flush t -> `Flush t
      | Ok (t, hash) -> `End (t, hash)
      | Error (t, exn) -> `Error (t, exn)
    in

    loop t
end

module type MAPPER =
sig
  type fd

  val length : fd -> int64
  val map    : fd -> ?pos:int64 -> share:bool -> int -> Cstruct.t
end

module D (Mapper : MAPPER) =
struct
  type error = ..
  type error += Invalid_hash of string
  type error += Invalid_offset of int64
  type error += Invalid_target of (int * int)
  type error += Unpack_error of P.error

  let pp = Format.fprintf

  let pp_error fmt = function
    | Invalid_hash hash ->
      Format.fprintf fmt "(Invalid_hash %a)" Hash.pp hash
    | Invalid_offset off ->
      Format.fprintf fmt "(Invalid_offset %Ld)" off
    | Invalid_target (has, expected) ->
      Format.fprintf fmt "(Invalid_target (%d, %d))"
        has expected
    | Unpack_error exn ->
      Format.fprintf fmt "(Unpack_error %a)" P.pp_error exn

  type 'a t =
    { file  : Mapper.fd
    ; max   : int64
    ; win   : Window.t Bucket.t
    ; idx   : 'a. ('a, [ `SHA1 ]) Hash.t -> int64 option
    ; hash  : ([ `PACK ], [ `SHA1 ]) Hash.t }

  let is_hunk t = match P.kind t with
    | P.Hunk _ -> true
    | _ -> false

  module Base =
  struct
    type t =
      { kind     : P.kind
      ; raw      : Cstruct.t
      ; offset   : Int64.t
      ; length   : int
      ; consumed : int }

    let is_hunk { kind; _ } = match kind with
      | P.Hunk _ -> true
      | _ -> false
  end

  let apply hunks base =
    let raw = Cstruct.create hunks.H.target_length in

    let target_length = List.fold_left
       (fun acc -> function
        | H.Insert insert ->
          Cstruct.blit insert 0 raw acc (Cstruct.len insert); acc + Cstruct.len insert
        | H.Copy (off, len) ->
          Cstruct.blit base.Base.raw off raw acc len; acc + len)
       0 hunks.H.hunks
      in

      if (target_length = hunks.H.target_length)
      then Ok Base.{ kind     = base.kind
                   ; raw      = raw
                   ; offset   = base.offset
                   ; length   = target_length
                   ; consumed = base.consumed }
      else Error (Invalid_target (target_length, hunks.H.target_length))

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

  let delta ?(chunk_size = 0x8000) t hunks offset_of_hunks z_tmp z_win h_tmp =
    let absolute_offset = match hunks.H.reference with
      | H.Offset off ->
        if off < t.max && off >= 0L
        then Ok (Int64.sub offset_of_hunks off)
        else Error (Invalid_offset off)
      | H.Hash hash -> match t.idx hash with
        | Some off -> Ok off
        | None -> Error (Invalid_hash hash)
    in

    match absolute_offset with
    | Ok absolute_offset ->
      let window, relative_offset = find t absolute_offset in
      let state    = P.from_window window relative_offset z_tmp z_win h_tmp in
      let base_raw = Cstruct.create hunks.H.source_length in

      let rec loop window consumed_in_window writted_in_raw git_object state =
        match P.eval window.Window.raw state with
        | `Await state ->
          let rest_in_window = min (window.Window.len - consumed_in_window) chunk_size in

          if rest_in_window > 0
          then
            loop window
              (consumed_in_window + rest_in_window) writted_in_raw git_object
              (P.refill consumed_in_window rest_in_window state)
          else
            let window, relative_offset = find t Int64.(window.Window.off + (of_int consumed_in_window)) in
            (* XXX(dinosaure): we try to find a new window to compute the rest of the current object. *)
            loop window
              relative_offset
              writted_in_raw
              git_object
              (P.refill 0 0 state)
        | `Flush state ->
          let o, n = P.output state in
          Cstruct.blit o 0 base_raw writted_in_raw n;
          loop window consumed_in_window (writted_in_raw + n) git_object (P.flush 0 n state)
        | `Object state ->
          loop window consumed_in_window writted_in_raw
            (Some (P.kind state,
                    P.offset state,
                    P.length state,
                    P.consumed state))
            (P.next_object state) (* XXX *)
        | `Error (state, exn) ->
          Error (Unpack_error exn)
        | `End (state, _) ->
          match git_object with
          | Some (kind, offset, length, consumed) ->
            Ok (kind, offset, length, consumed)
          | None -> assert false
          (* XXX: This is not possible, the [`End] state comes only after the
                  [`Object] state and this state changes [kind] to [Some x].
            *)
      in

      (match loop window relative_offset 0 None state with
        | Ok (base_kind, base_offset, base_length, base_consumed) ->
          Ok Base.{ kind     = base_kind
                  ; raw      = base_raw
                  ; offset   = base_offset
                  ; length   = base_length
                  ; consumed = base_consumed }
        | Error exn -> Error exn)
    | Error exn -> Error exn

  let make ?(bulk = 10) file (idx : string -> int64 option) =
    { file
    ; max  = Mapper.length file
    ; win  = Bucket.make bulk
    ; idx  = idx
    ; hash = "" (* TODO *) }

  let get ?(chunk_size = 0x800) t hash z_tmp z_win h_tmp =
    match t.idx hash with
    | Some absolute_offset ->
      let window, relative_offset = find t absolute_offset in
      let state  = P.from_window window relative_offset z_tmp z_win h_tmp in
      let buffer = BBuffer.create chunk_size in

      let rec loop window consumed_in_window writted_in_raw git_object state =
        match P.eval window.Window.raw state with
        | `Await state ->
          let rest_in_window = min (window.Window.len - consumed_in_window) chunk_size in

          if rest_in_window > 0
          then
            loop window
              (consumed_in_window + rest_in_window)
              writted_in_raw
              git_object
              (P.refill consumed_in_window rest_in_window state)
          else
            let window, relative_offset = find t Int64.(window.Window.off + (of_int consumed_in_window)) in
            (* XXX(dinosaure): we try to find a new window to compute the rest of the current object. *)
            loop window
              relative_offset
              writted_in_raw
              git_object
              (P.refill 0 0 state)
        | `Flush t ->
          let o, n = P.output t in
          let ()   = BBuffer.add o ~len:n buffer in
          loop window consumed_in_window (writted_in_raw + n) git_object (P.flush 0 n t)
        | `Object state ->
          (match P.kind state with
           | P.Hunk hunks ->
             let (rlength, rconsumed, roffset) = P.length state, P.consumed state, P.offset state in

             let rec undelta ?(level = 1) hunks offset =
               match delta t hunks offset z_tmp z_win h_tmp with
               | Error exn -> Error exn
               | Ok { Base.kind = P.Hunk hunks; offset; _ } ->
                 (match undelta ~level:(level + 1) hunks offset with
                  | Ok (base, level) ->
                    (match apply hunks base with
                     | Ok base -> Ok (base, level)
                     | Error exn -> Error exn)
                  | Error exn -> Error exn)
               | Ok base -> Ok (base, level)
             in

             (match undelta hunks (P.offset state) with
              | Ok (base, level) ->
                (match apply hunks base with
                 | Ok base ->
                   loop window consumed_in_window writted_in_raw
                     (Some (base.Base.kind, Some base.Base.raw, roffset, rlength, rconsumed))
                     (P.next_object state)
                 | Error exn -> Error exn)
              | Error exn -> Error exn)
           | kind ->
             loop window consumed_in_window writted_in_raw
               (Some (kind, None, P.offset state, P.length state, P.consumed state))
               (P.next_object state))
        | `Error (state, exn) ->
          Error (Unpack_error exn)
        | `End (t, _) -> match git_object with
          | Some (kind, raw_opt, offset, length, consumed) ->
            Ok (kind, raw_opt, offset, length, consumed)
          | None -> assert false
      in

      (match loop window relative_offset 0 None state with
       | Ok (kind, None, offset, length, consumed) ->
         Ok Base.{ kind
                 ; raw = BBuffer.contents buffer
                 ; offset
                 ; length
                 ; consumed }
       | Ok (kind, Some raw, offset, length, consumed) ->
         Ok Base.{ kind
                 ; raw
                 ; offset
                 ; length
                 ; consumed }
       | Error exn -> Error exn)
    | None -> Error (Invalid_hash hash)
end
