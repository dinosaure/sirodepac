let () = Printexc.record_backtrace true

module B : sig
  include module type of Decompress.B
    with type st = Decompress.B.st
     and type bs = Decompress.B.bs
     and type 'a t = 'a Decompress.B.t

  val to_cstruct : 'a t -> Cstruct.t
  val blit_string : string -> int -> 'a t -> int -> int -> unit
  val blit_bytes  : bytes -> int -> 'a t -> int -> int -> unit
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

  let blit_bytes src src_off dst dst_off len =
    for i = 0 to len - 1
    do set dst (dst_off + i) (Bytes.get src (src_off + i)) done
end

module BBuffer =
struct
  type 'i t =
    { mutable buf : 'i B.t
    ; mutable pos : int
    ; mutable len : int
    ; init        : 'i B.t }

  let create ~proof n =
    let n = if n < 1 then 1 else n in
    let n = if n > Sys.max_string_length then Sys.max_string_length else n in
    let s = B.from ~proof n in

    { buf  = s
    ; pos  = 0
    ; len  = n
    ; init = s }

  let contents b =
    B.sub b.buf 0 b.pos

  let length b = b.pos

  let resize ({ len; pos; buf; } as b) m =
    let len' = ref len in

    while pos + m > !len' do len' := 2 * !len' done;

    if !len' > Sys.max_string_length
    then if pos + m <= Sys.max_string_length
         then len' := Sys.max_string_length
         else raise (Failure "BBuffer.add: cannot grow buffer");

    let buf' = B.from buf !len' in

    B.blit buf 0 buf' 0 pos;
    b.buf <- buf';
    b.len <- !len'

  let blit_string src src_off dst dst_off len =
    for i = 0 to len - 1
    do B.set dst (dst_off + i) (String.get src (src_off + i)) done

  let blit_bytes src src_off dst dst_off len =
    for i = 0 to len - 1
    do B.set dst (dst_off + i) (Bytes.get src (src_off + i)) done

  let blit_bigstring src src_off dst dst_off len =
    for i = 0 to len - 1
    do B.set dst (dst_off + i) (B.Bigstring.get src (src_off + i)) done

  (* XXX(dinosaure): we can factorize by a first module, but too big to be
                     useful. *)
  let add tmp ?(off = 0) ?(len = B.length tmp) buf =
    if off < 0 || len < 0 || off + len > B.length tmp
    then raise (Invalid_argument "BBuffer.add");

    let pos' = buf.pos + len in

    if pos' > buf.len
    then resize buf len;

    B.blit tmp off buf.buf buf.pos len;
    buf.pos <- pos'

  let add_string tmp ?(off = 0) ?(len = String.length tmp) buf =
    if off < 0 || len < 0 || off + len > String.length tmp
    then raise (Invalid_argument "BBuffer.add_string");

    let pos' = buf.pos + len in

    if pos' > buf.len
    then resize buf len;

    blit_string tmp off buf.buf buf.pos len;
    buf.pos <- pos'

  let add_bytes tmp ?(off = 0) ?(len = Bytes.length tmp) buf =
    if off < 0 || len < 0 || off + len > Bytes.length tmp
    then raise (Invalid_argument "BBuffer.add_bytes");

    let pos' = buf.pos + len in

    if pos' > buf.len
    then resize buf len;

    blit_bytes tmp off buf.buf buf.pos len;
    buf.pos <- pos'

  let add_bigstring tmp ?(off = 0) ?(len = B.Bigstring.length tmp) buf =
    if off < 0 || len < 0 || off + len > B.Bigstring.length tmp
    then raise (Invalid_argument "BBuffer.add_bigstring");

    let pos' = buf.pos + len in

    if pos' > buf.len
    then resize buf len;

    blit_bigstring tmp off buf.buf buf.pos len;
    buf.pos <- pos'

  let clear b = b.pos <- 0
end

external swap32 : int32 -> int32 = "%bswap_int32"
external swap64 : int64 -> int64 = "%bswap_int64"

let pp_hash fmt s =
  for i = 0 to String.length s - 1
  do Format.fprintf fmt "%02x" (Char.code @@ String.get s i) done

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

  type 'i t =
    { i_off         : int
    ; i_pos         : int
    ; i_len         : int
    ; read          : int
    ; _length        : int
    ; _reference     : reference
    ; _source_length : int
    ; _target_length : int
    ; _hunks         : 'i hunk list
    ; state         : 'i state }
  and 'i state =
    | Header    of ('i B.t -> 'i t -> 'i res)
    | List      of ('i B.t -> 'i t -> 'i res)
    | Is_insert of ('i B.t * int * int)
    | Is_copy   of ('i B.t -> 'i t -> 'i res)
    | End
    | Exception of error
  and 'i res =
    | Wait   of 'i t
    | Error  of 'i t * error
    | Cont   of 'i t
    | Ok     of 'i t * 'i hunks
  and 'i hunk =
    | Insert of 'i B.t
    | Copy   of int * int
  and reference =
    | Offset of int64
    | Hash   of string
  and 'i hunks =
    { reference     : reference
    ; hunks         : 'i hunk list
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
      pp fmt "(Insert %a)" B.pp buf
    | Copy (off, len) ->
      pp fmt "(Copy (%d, %d))" off len

  let pp_error fmt = function
    | Reserved_opcode byte -> pp fmt "(Reserved_opcode %02x)" byte
    | Wrong_insert_hunk (off, len, source) ->
      pp fmt "(Wrong_insert_hunk (off: %d, len: %d, source: %d))" off len source

  let pp_reference fmt = function
    | Hash hash -> pp fmt "(Reference %s)" hash
    | Offset off -> pp fmt "(Offset %Ld)" off

  let pp_hunks fmt ({ reference; hunks; length; source_length; target_length; } : 'i hunks) =
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
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
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
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
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
           then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
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
             | 0 -> Cont { t with state = Is_insert (B.from ~proof:src opcode, 0, opcode) }
             | _ -> Cont { t with state = Is_copy (copy opcode) })
           src t
    else ok { t with _hunks = List.rev t._hunks }

  let insert src t buffer (off, rest) =
    let n = min (t.i_len - t.i_pos) rest in
    B.blit src (t.i_off + t.i_pos) buffer off n;
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

module Window =
struct
  type 'a t =
    { rpos   : int
    ; wpos   : int
    ; size   : int
    ; buffer : 'a B.t }

  let make_by ~proof size =
    { rpos = 0
    ; wpos = 0
    ; size = size + 1
    ; buffer = B.from ~proof (size + 1) }

  let available_to_write { wpos; rpos; size; _ } =
    if wpos >= rpos then size - (wpos - rpos) - 1
    else rpos - wpos - 1

  let drop n ({ rpos; size; _ } as t) =
    { t with rpos = if rpos + n < size then rpos + n
                    else rpos + n - size }

  let move n ({ wpos; size; _ } as t) =
    { t with wpos = if wpos + n < size then wpos + n
                    else wpos + n - size }

  let write buf off len t =
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

  let blit t off dst dst_off len =
    let off = if t.wpos - off < 0
              then t.wpos - off + t.size
              else t.wpos - off in

    let pre = t.size - off in
    let extra = len - pre in

    if extra > 0 then begin
      B.blit t.buffer off dst dst_off pre;
      B.blit t.buffer 0 dst (dst_off + pre) extra
    end else
      B.blit t.buffer off dst dst_off len
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

  type ('i, 'o) t =
    { i_off   : int
    ; i_pos   : int
    ; i_len   : int
    ; local   : bool
    ; read    : int64
    ; o_z     : 'o B.t
    ; o_w     : 'o Decompress.Window.t
    ; o_h     : 'o B.t
    ; version : int32
    ; objects : int32
    ; counter : int32
    ; state   : ('i, 'o) state }
  and ('i, 'o) state =
    | Header    of ('i B.t -> ('i, 'o) t -> ('i, 'o) res)
    | Object    of ('i B.t -> ('i, 'o) t -> ('i, 'o) res)
    | VariableLength of ('i B.t -> ('i, 'o) t -> ('i, 'o) res)
    | Unzip     of { offset   : int64
                   ; consumed : int
                   ; length   : int
                   ; kind     : 'i kind
                   ; z        : ('i, 'o) Decompress.Inflate.t }
    | Hunks     of { offset   : int64
                   ; length   : int
                   ; consumed : int
                   ; z        : ('i, 'o) Decompress.Inflate.t
                   ; h        : 'i H.t }
    | Next      of { offset   : int64
                   ; consumed : int
                   ; length   : int
                   ; length'  : int
                   (* XXX(dinosaure): about [consumed], [length] and [length'],
                                      may be we need to consider theses as an
                                      int64 because the variable length (see
                                      [length]) can be up than [max_int].

                                      Indeed, Git can store a huge file but we
                                      have a limitation in OCaml. So, TODO!
                    *)
                   ; kind     : 'i kind }
    | Checksum  of ('i B.t -> ('i, 'o) t -> ('i, 'o) res)
    | End       of string
    | Exception of error
  and ('i, 'o) res =
    | Wait  of ('i, 'o) t
    | Flush of ('i, 'o) t
    | Error of ('i, 'o) t * error
    | Cont  of ('i, 'o) t
    | Ok    of ('i, 'o) t * string
  and 'i kind =
    | Commit
    | Tree
    | Blob
    | Tag
    | Hunk of 'i H.hunks

  let pp_kind fmt = function
    | Commit -> pp fmt "Commit"
    | Tree -> pp fmt "Tree"
    | Blob -> pp fmt "Blob"
    | Tag -> pp fmt "Tag"
    | Hunk hunks -> pp fmt "(Hunks %a)" H.pp_hunks hunks

  let pp fmt { i_off; i_pos; i_len; o_z; version; objects; counter; state; } =
    match state with
    | Unzip { z; _ } ->
      pp fmt "{@[<hov>i_off = %d;@ \
                      i_pos = %d;@ \
                      i_len = %d;@ \
                      version = %ld;@ \
                      objects = %ld;@ \
                      counter = %ld;@ \
                      z = %a@]}"
        i_off i_pos i_len version objects counter Decompress.Inflate.pp z
    | Hunks { z; h; _ } ->
      pp fmt "{@[<hov>i_off = %d;@ \
                      i_pos = %d;@ \
                      i_len = %d;@ \
                      version = %ld;@ \
                      objects = %ld;@ \
                      counter = %ld;@ \
                      z = %a;@ \
                      h = %a@]}"
        i_off i_pos i_len version objects counter Decompress.Inflate.pp z H.pp h
    | _ ->
      pp fmt "{@[<hov>i_off = %d;@ \
                      i_pos = %d;@ \
                      i_len = %d;@ \
                      version = %ld;@ \
                      objects = %ld;@ \
                      counter = %ld@]}"
        i_off i_pos i_len version objects counter

  let await t      = Wait t
  let flush t      = Flush t
  let error t exn  = Error ({ t with state = Exception exn }, exn)
  let continue t   = Cont t
  let ok t hash    = Ok ({ t with state = End hash }, hash)

  module KHeader =
  struct
    let rec check_byte chr k src t =
      if (t.i_len - t.i_pos) > 0 && B.get src (t.i_off + t.i_pos) = chr
      then k src { t with i_pos = t.i_pos + 1
                        ; read = Int64.add t.read 1L }
      else if (t.i_len - t.i_pos) = 0
      then await { t with state = Header (fun src t -> (check_byte[@tailcall]) chr k src t) }
      else error { t with i_pos = t.i_pos + 1
                        ; read = Int64.add t.read 1L }
                 (Invalid_byte (Char.code @@ B.get src (t.i_off + t.i_pos)))

    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
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
      then let num = Endian.get_u32 src (t.i_off + t.i_pos) in
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
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1
                             ; read = Int64.add t.read 1L }
      else await { t with state = VariableLength (fun src t -> (get_byte[@tailcall]) k src t) }
  end

  module KObject =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
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
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
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
                then H.eval t.o_z (H.refill 0 (Decompress.Inflate.used_out z) h)
                else H.eval t.o_z h
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
      match H.eval t.o_z (H.refill 0 (Decompress.Inflate.used_out z) h) with
      | `Await h ->
        Cont { t with state = Hunks { offset; length; consumed
                                    ; z = (Decompress.Inflate.flush 0 (B.length t.o_z) z)
                                    ; h } }
      | `Error (h, exn) -> error t (Hunk_error exn)
      | `Ok (h, objs) ->
        Cont { t with state = Hunks { offset; length; consumed
                                    ; z = (Decompress.Inflate.flush 0 (B.length t.o_z) z)
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
                                  ; z = Decompress.Inflate.flush 0 (B.length t.o_z)
                                        @@ Decompress.Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Decompress.Inflate.default (Decompress.Window.reset t.o_w) } }
    | 0b010 ->
      Cont { t with state = Unzip { offset   = off
                                  ; length   = len
                                  ; consumed = size_of_variable_length len
                                  ; kind = Tree
                                  ; z = Decompress.Inflate.flush 0 (B.length t.o_z)
                                        @@ Decompress.Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Decompress.Inflate.default (Decompress.Window.reset t.o_w) } }
    | 0b011 ->
      Cont { t with state = Unzip { offset   = off
                                  ; length   = len
                                  ; consumed = size_of_variable_length len
                                  ; kind = Blob
                                  ; z = Decompress.Inflate.flush 0 (B.length t.o_z)
                                        @@ Decompress.Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Decompress.Inflate.default (Decompress.Window.reset t.o_w) } }
    | 0b100 ->
      Cont { t with state = Unzip { offset   = off
                                  ; length   = len
                                  ; consumed = size_of_variable_length len
                                  ; kind = Tag
                                  ; z = Decompress.Inflate.flush 0 (B.length t.o_z)
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
                                           ; z = Decompress.Inflate.flush 0 (B.length t.o_z)
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
                                      ; z = Decompress.Inflate.flush 0 (B.length t.o_z)
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

  let default ~proof ?(chunk = 4096) ?window z_tmp z_win h_tmp =
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

  let from_absolute_offset (type a) ~(proof:a B.t) src_offset pack_offset z_tmp z_win h_tmp =
    { i_off   = src_offset
    ; i_pos   = 0
    ; i_len   = 0
    ; local   = true
    ; o_z     = z_tmp
    ; o_w     = z_win
    ; o_h     = h_tmp
    ; read    = pack_offset
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
      t.o_z, Decompress.Inflate.used_out z
    | Hunks { h; _ } ->
      t.o_h, 0
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
        unzip src t offset length consumed kind z
      | Hunks { offset; length; consumed; z; h; } ->
        hunks src t offset length consumed z h
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

module D =
struct
  type error = ..
  type error += Invalid_hash of string
  type error += Invalid_source of int64
  type error += Invalid_target of (int * int)
  type error += Unpack_error of P.error

  let pp = Format.fprintf

  let pp_error fmt = function
    | Invalid_hash hash ->
      Format.fprintf fmt "(Invalid_hash %a)" pp_hash hash
    | Invalid_source off ->
      Format.fprintf fmt "(Invalid_chunk %Ld)" off
    | Invalid_target (has, expected) ->
      Format.fprintf fmt "(Invalid_target (%d, %d))"
        has expected
    | Unpack_error exn ->
      Format.fprintf fmt "(Unpack_error %a)" P.pp_error exn

  type 'a t =
    { map_pck : 'a B.t
    ; fun_idx : (string -> Int64.t option) }

  let is_hunk t = match P.kind t with
    | P.Hunk _ -> true
    | _ -> false
  let hash_of_object kind length =
    let hdr = Format.sprintf "%s %d\000"
      (match kind with
       | P.Commit -> "commit"
       | P.Blob -> "blob"
       | P.Tree -> "tree"
       | P.Tag -> "tag"
       | P.Hunk _ -> raise (Invalid_argument "hash_of_object"))
      length
    in
    fun raw ->
      Nocrypto.Hash.SHA1.digestv
        [ Cstruct.of_string hdr
        ; B.to_cstruct raw ]
      |> Cstruct.to_string

  module Base =
  struct
    type 'i t =
      { kind     : 'i P.kind
      ; raw      : 'i B.t
      ; offset   : Int64.t
      ; length   : int
      ; consumed : int }

    let is_hunk { kind; _ } = match kind with
      | P.Hunk _ -> true
      | _ -> false
  end

  let apply { map_pck; fun_idx; } hunks base =
    let raw = B.from ~proof:map_pck hunks.H.target_length in

    let target_length = List.fold_left
       (fun acc -> function
        | H.Insert insert ->
          B.blit insert 0 raw acc (B.length insert); acc + B.length insert
        | H.Copy (off, len) ->
          B.blit base.Base.raw off raw acc len; acc + len)
       0 hunks.H.hunks
      in

      if (target_length = hunks.H.target_length)
      then Ok Base.{ kind     = base.kind
                   ; raw      = raw
                   ; offset   = base.offset
                   ; length   = target_length
                   ; consumed = base.consumed }
      else Error (Invalid_target (target_length, hunks.H.target_length))

  (* XXX(dinosaure): en français. La déserialization d'un fichier PACK se
                     découpe en plusieurs étapes:

                     1 PACK file (header + checksum)
                     2 Inflate (Decompress)
                     3 Git Object + Hunks
                     4 Git Object

                     De l'étape 1 à 3, le module {!P} suffit et on est bien face
                     à une interface *non-blocking* (dans le sens où nous avons
                     un état que vous pouvez arrêter et reprendre plus tard sans
                     perdre aucune information - il n'y a donc aucun prérequis
                     dans la taille de l'input). Malheureusement, pour passer à
                     l'étape 4, nous devons *peut-être* traiter une information
                     qui est apparu dans le flux (utilisé par {!P}) et qui est
                     désormais indisponible.

                     Cette fonction [delta] correspond exactement à cette
                     quatrième étape. Comme on peut le voir dans le code, on y
                     obtient un [offset] absolue dans un fichier PACK donné.
                     Ainsi, nous devons travailler sur un flux correspondant à
                     cette offset.

                     Imaginons un objet [Hunks] qui fait référence à un objet
                     Git qui ce trouve [N] bytes avant. Imaginons ensuite que
                     vous déserializer le fichier PACK à l'aide d'un input dont
                     la taille est de [M] bytes. Enfin, imaginons que [N > M],
                     dans ce cas, l'objet auquel fait référence le [Hunks]
                     n'est plus disponible dans votre input.

                     Ainsi, la fonction [delta] demande (pour l'instant), à ce
                     que votre fichier PACK soit mappé (et donc que l'entièreté
                     de son contenu soit disponible). Le premier problème est:
                     il n'est plus nécessaire de se forcer d'avoir une interface
                     *non-blocking* puisque tout le contenu du fichier PACK est
                     disponible. Mais le deuxième problème est plus
                     problématique et on peut le dénoter dans le code. En effet,
                     l'offset qu'on peut obtenir (qui correspond à un offset
                     relatif ou absolue - mais particulièrement absolue) tient
                     sur un entier 64 bits. Et OCaml ne peut pas traiter un
                     fichier aussi gros (puisqu'on travaille, pour une
                     architecture 64 bits, avec des entiers 63 bits - voir les
                     fonctions {!get} et {!blit}).

                     Alors comment résoudre la problématique ? Doit on oublier
                     l'existence (très rare, voir inexistante) d'un énorme
                     fichier PACK où un offset tenant sur un entier 64 bits est
                     nécessaire ? Pour l'instant, c'est le cas.

                     Maintenant, j'ai regardé comment Git fonctionne sur ce
                     sujet et j'ai remarqué l'existence de *window* permettant
                     de:
                       1. ne pas être obliger de mapper la totalité le fichier
                       mais seulement [1024 * 1024] bytes
                       2. travailler sur ces windows lorsqu'on a un offset
                       relatif ou absolute (en espérant que celui-ci soit
                       disponible - pour le coup, la documentation de Git est
                       très ... pas documenté)

                     Après, ce ne sont pas réellement des windows (comme on peut
                     le retrouver dans Decompress où il s'agit de garder une
                     partie de l'input) mais plutôt un système de cache - pour
                     preuve, la définition de ces windows est dans [cache.h].
                     Git va tout simplement garder plusieurs windows où chacunes
                     est associées à un fichier PACK (spécifiquement à son [file
                     descriptor]) et un offset dans ce fichier PACK. Il utilise
                     l'algorithme par comptage de référence pour faire office de
                     *garbage collector* auprès de ces windows. Enfin, dès qu'il
                     souhaite obtenir un objet Git par le biais d'une référence
                     absolue ou relative, il regarde son lot de windows pour
                     savoir si l'offset s'y retrouve.

                     Dans le cas où il trouve une window, il informe qu'il faut
                     garder la window (en incrémentant le nombre de référence)
                     et donne le contenu de la window. La déserialization
                     travaille désormais sur ce flux limité à [1024 * 1024]
                     bytes et on peut y déserializer l'objet.

                     Dans le cas où il ne trouve pas la window, il cherche le
                     fichier PACK associé et créer une nouvelle window de [1024
                     * 1024] bytes qui résultera d'un simple [map] à l'offset
                     absolue renseigné (paddé sur 1024) du [file descriptor] du
                     fichier PACK file.

                     Cela correspond plus donc à un système de cache qu'une
                     window que l'on pourrait utiliser tout au long de la
                     déserialization pour construire les objets [Hunks].

                     DONC, le constat est qu'on a toujours ce problème avec les
                     [Int64.t] et [nativeint] dans ce code. Le système de cache
                     pourrait résoudre le problème (puisqu'on travaillerrait
                     plus qu'avec des *chunks* de [1024 * 1024] bytes) mais il
                     me semble qu'il soit hors de propos dans la déserialization
                     d'un fichier PACK. De plus, l'attitude de Git par rapport à
                     ses objets est plutôt [lazy] dans le sens où la
                     construction de ces objets se fait à la demande (alors que
                     ici, elle peut se faire directement après qu'on est lu
                     l'objet - cela n'empêche pas d'adopter une manière [lazy]
                     avec cette interface, mais il faut créer le système de
                     cache autour de cette déserialization et c'est dans ce sens
                     que ce dernier est hors de propos).

                     Bref, je laisse ce commentaire pour m'en souvenir déjà et
                     pour les autres pour comprendre la problématique.
  *)
  let delta { map_pck; fun_idx; } hunks offset_of_hunks z_tmp z_win h_tmp =
      let absolute_offset = match hunks.H.reference with
        | H.Offset off -> Ok (Int64.sub offset_of_hunks off)
        | H.Hash hash -> match fun_idx hash with
          | Some off -> Ok off
          | None -> Error (Invalid_hash hash)
      in

      match absolute_offset with
      | Ok absolute_offset ->
        let chunk    = 0x8000 in
        let t        = P.from_absolute_offset ~proof:map_pck (Int64.to_int absolute_offset) absolute_offset z_tmp z_win h_tmp in
        let base_raw = B.from ~proof:map_pck hunks.H.source_length in

        let rec loop offset' result kind t =
          match P.eval map_pck t with
          | `Await t ->
            let chunk' = min (Int64.to_int (Int64.sub (Int64.of_int (B.length map_pck)) offset')) chunk in
            if chunk' > 0
            then loop (Int64.add offset' (Int64.of_int chunk')) result kind (P.refill (Int64.to_int offset') chunk' t)
            else Error (Invalid_source absolute_offset)
          | `Flush t ->
            let o, n = P.output t in
            B.blit o 0 base_raw result n;
            loop offset' (result + n) kind (P.flush 0 n t)
          | `Object t ->
            loop offset' result (Some (P.kind t, P.offset t, P.length t, P.consumed t)) (P.next_object t) (* XXX *)
          | `Error (t, exn) ->
            Error (Unpack_error exn)
          | `End (t, _) -> match kind with
            | Some (kind, offset, length, consumed) ->
              Ok (kind, offset, length, consumed)
            | None -> assert false
            (* XXX: This is not possible, the [`End] state comes only after the
                    [`Object] state and this state changes [kind] to [Some x].
             *)
        in

        (match loop absolute_offset 0 None t with
         | Ok (base_kind, base_offset, base_length, base_consumed) ->
           Ok Base.{ kind     = base_kind
                   ; raw      = base_raw
                   ; offset   = base_offset
                   ; length   = base_length
                   ; consumed = base_consumed }
         | Error exn -> Error exn)
      | Error exn -> Error exn

  let make map_pck fun_idx = { map_pck; fun_idx; }
end
