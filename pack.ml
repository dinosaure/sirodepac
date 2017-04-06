(* sort git objects by type, hash and size. *)

module Option =
struct
  let value ~default = function
    | Some a -> a
    | None -> default

  let ( >>| ) a f = match a with
    | Some a -> Some (f a)
    | None -> None
end

module Kind =
struct
  type t =
    | Commit
    | Tag
    | Tree
    | Blob

  let to_int = function
    | Commit -> 0
    | Tree -> 1
    | Blob -> 2
    | Tag -> 3

  let to_bin x =
    (to_int x) land 0b111
end

module Entry =
struct
  type t =
    { hash_name   : int
    ; hash_object : string
    ; kind        : Kind.t
    ; length      : int64 }

  (* XXX(dinosaure): hash from git to sort git objects.
                    in git, this hash is computed in an [int32].
  *)
  let hash name =
    let res = ref 0 in

    for i = 0 to String.length name - 1
    do if String.get name i <> ' '
      then res := (!res lsr 2) + (Char.code (String.get name i) lsl 24);
    done;

    !res

  type kind =
    | Commit of Minigit.Commit.t
    | Tree   of Minigit.Tree.t
    | Tag    of Minigit.Tag.t
    | Blob   of Minigit.Blob.t

  let make hash_object ?name obj =
    let hash_name = Option.(value ~default:0 (name >>| hash)) in

    match obj with
    | Tag tag ->
      { hash_name
      ; hash_object
      ; kind = Kind.Tag
      ; length = Int64.of_int @@ Minigit.Tag.raw_length tag }
      (* git considers than a git object can be huge. But we have the
          limitation of the integer in OCaml (depends on the architecture). So
          we need to fix [raw_length] and returns a [int64]. *)
    | Commit commit ->
      { hash_name
      ; hash_object
      ; kind = Kind.Commit
      ; length = Int64.of_int @@ Minigit.Commit.raw_length commit }
    | Tree tree ->
      { hash_name
      ; hash_object
      ; kind = Kind.Tree
      ; length = Int64.of_int @@ Minigit.Tree.raw_length tree }
    | Blob blob ->
      { hash_name
      ; hash_object
      ; kind = Kind.Blob
      ; length = Int64.of_int @@ Minigit.Blob.raw_length blob }

  let compare = Compare.lexicographic
    [ (fun a b -> Compare.int (Kind.to_int a.kind) (Kind.to_int b.kind))
    ; (fun a b -> Compare.int a.hash_name b.hash_name)
    ; (fun a b -> Compare.int64 a.length b.length) ]
    (* XXX(dinosaure): git compare the preferred base and the memory address of
                      the object to take the newest. obviously, it's not possible
                      to do this in OCaml.
    *)

  let from_commit hash commit =
    make hash (Commit commit)

  let from_tag hash tag =
    make hash (Tag tag)

  let from_tree hash ?path:name tree =
    make hash ?name (Tree tree)

  let from_blob hash ?path:name blob =
    make hash ?name (Blob blob)
end

let sp = Format.sprintf

module Int32 =
struct
  include Int32

  let ( && ) = Int32.logand
  let ( >> ) = Int32.shift_right
end

module Int64 =
struct
  include Int64

  let ( && ) = Int64.logand
  let ( >> ) = Int64.shift_right
  let ( << ) = Int64.shift_left (* >> (tuareg) *)
end

module P =
struct
  type error = ..
  type error += Deflate_error of Decompress.Deflate.error

  let pp_error fmt = function
    | Deflate_error exn -> Format.fprintf fmt "(Deflate_error %a)" Decompress.Deflate.pp_error exn

  type t =
    { o_off : int
    ; o_pos : int
    ; o_len : int
    ; write : int64
    ; radix : (int32 * int64) Radix.t
    ; objects : Entry.t array
    ; state : state }
  and k = Cstruct.t -> t -> res
  and state =
    | Header of k
    | Object of k
    | Raw    of int
    | Zip of { idx : int
             ; raw : Cstruct.t
             ; z   : (Decompress.B.bs, Decompress.B.bs) Decompress.Deflate.t }
    | Exception of error
  and res =
    | Wait of t
    | Flush of t
    | Error of t * error
    | Cont of t
    | Ok of t

  let await t = Wait t
  let flush t = Flush t
  let error t exn = Error ({ t with state = Exception exn }, exn)

  module KHeader =
  struct
    let rec put_byte byte k dst t =
      if (t.o_len - t.o_pos) > 0
      then begin
        Cstruct.set_uint8 dst (t.o_off + t.o_pos) byte;
        k dst { t with o_pos = t.o_pos + 1
                     ; write = Int64.add t.write 1L }
      end else Flush { t with state = Header (put_byte byte k) }

    let rec put_u32 integer k dst t =
      if (t.o_len - t.o_pos) > 3
      then begin
        Cstruct.BE.set_uint32 dst (t.o_off + t.o_pos) integer;
        k dst { t with o_pos = t.o_pos + 4
                     ; write = Int64.add t.write 4L }
      end else if (t.o_len - t.o_pos) > 0
      then begin
        let a1 = Int32.(to_int ((integer && 0xFF000000l) >> 24)) in
        let a2 = Int32.(to_int ((integer && 0x00FF0000l) >> 16)) in
        let a3 = Int32.(to_int ((integer && 0x0000FF00l) >> 8)) in
        let a4 = Int32.(to_int (integer && 0x000000FFl)) in

        (put_byte a1
         @@ put_byte a2
         @@ put_byte a3
         @@ put_byte a4
         @@ k)
        dst t
      end else Flush { t with state = Header (put_u32 integer k) }
  end

  module KObject =
  struct
    let rec put_byte byte k dst t =
      if (t.o_len - t.o_pos) > 0
      then begin
        Cstruct.set_uint8 dst (t.o_off + t.o_pos) byte;
        k dst { t with o_pos = t.o_pos + 1
                     ; write = Int64.add t.write 1L }
      end else Flush { t with state = Object (put_byte byte k) }

    let rec length n k dst t =
      let byte = Int64.(to_int (n && 0x7FL)) in
      let rest = Int64.(n >> 7) in

      if rest <> 0L
      then put_byte (byte lor 0x80) (length rest k) dst t
      else put_byte byte k dst t
  end

  let header_entry idx dst t =
    if idx = Array.length t.objects
    then Cont t
    else
      let crc    = 0l in (* TODO: calculate crc. *)
      let offset = t.write in

      let entry  = Array.get t.objects idx in
      let length = entry.Entry.length in (* TODO: to Big-Endian. *)
      let byte   = ((Kind.to_bin entry.Entry.kind) lsl 0x1F) in (* kind *)
      let byte   = byte lor Int64.(to_int (length && 0x0FL)) in (* part of the size *)

      let byte, continue =
        if Int64.(length >> 4) <> 0L
        then byte lor 0x80, true
        else byte, false
      in

      (* save the entry in the radix tree. *)
      let t = { t with radix = Radix.bind t.radix entry.Entry.hash_object (crc, offset) } in

      if continue
      then (KObject.put_byte byte
            @@ KObject.length Int64.(length >> 4)
            @@ fun dst t -> await { t with state = Raw idx })
          dst t
      else await { t with state = Raw idx }

  let raw_entry dst t idx raw z =
    let raw = Decompress.B.from_bigstring (Cstruct.to_bigarray raw) in
    let dst = Decompress.B.from_bigstring (Cstruct.to_bigarray dst) in

    match Decompress.Deflate.eval raw dst z with
    | `Await z -> await t
    | `Flush z -> flush t
    | `End z -> Cont { t with state = Object (header_entry (idx + 1)) }
    | `Error (z, exn) -> error t (Deflate_error exn)

  let number dst t =
    (* XXX(dinosaure): problem in 32-bits architecture. *)
    KHeader.put_u32 (Int32.of_int (Array.length t.objects))
      (fun dst t -> Cont t)
      dst t

  let version dst t =
    KHeader.put_u32 2l (fun dst t -> Cont t) dst t

  let header dst t =
    (KHeader.put_byte (Char.code 'P')
     @@ KHeader.put_byte (Char.code 'A')
     @@ KHeader.put_byte (Char.code 'C')
     @@ KHeader.put_byte (Char.code 'K')
     @@ fun dst t -> Cont { t with state = Header version })
    dst t

  let refill off len ?raw t = match t.state, raw with
    | Raw idx , Some raw ->
      { t with state = Zip { idx; raw; z = Decompress.Deflate.default ~proof:Decompress.B.proof_bigstring 4 } }
    | Raw idx, None ->
      raise (Invalid_argument "P.refill: expected a raw data")
    | Zip { idx; raw; z; }, Some new_raw ->
      { t with state = Zip { idx; raw = new_raw; z = Decompress.Deflate.no_flush off len z } }
    | Zip { idx; raw; z; }, None ->
      { t with state = Zip { idx; raw; z = Decompress.Deflate.no_flush off len z } }
    | _ -> raise (Invalid_argument "P.refill: bad state")

  let flush off len t =
    if (t.o_len - t.o_pos) = 0
    then match t.state with
      | Zip { idx; raw; z; } ->
        { t with o_off = off
              ; o_len = len
              ; o_pos = 0
              ; state = Zip { idx; raw; z = Decompress.Deflate.flush off len z } }
      | _ ->
        { t with o_off = off
             ; o_len = len
             ; o_pos = 0 }
    else raise (Invalid_argument (sp "P.flush: you lost something (pos: %d, len: %d)" t.o_pos t.o_len))

  let used_out t = t.o_pos

  let expect t = match t.state with
    | Raw idx ->
      (try
         let hash = (Array.get t.objects idx).Entry.hash_object in
         Some hash
       with exn -> raise (Invalid_argument "P.expect: invalid index"))
    | _ -> None

  let eval dst t =
    let eval0 t = match t.state with
      | Header k -> k dst t
      | Object k -> k dst t
      | Raw idx  -> await t
      | Zip { idx; raw; z; } -> raw_entry dst t idx raw z
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

  let default objects =
    { o_off = 0
    ; o_pos = 0
    ; o_len = 0
    ; write = 0L
    ; radix = Radix.empty
    ; objects
    ; state = (Header header) }
end
