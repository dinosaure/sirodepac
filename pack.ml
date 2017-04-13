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

  let to_bin = function
    | Commit -> 0b001
    | Tree -> 0b010
    | Blob -> 0b011
    | Tag -> 0b100

  let pp fmt = function
    | Commit -> Format.fprintf fmt "Commit"
    | Tree -> Format.fprintf fmt "Tree"
    | Blob -> Format.fprintf fmt "Blob"
    | Tag -> Format.fprintf fmt "Tag"
end

module Entry =
struct
  type t =
    { hash_name   : int
    ; hash_object : string
    ; kind        : Kind.t
    ; length      : int64 }

  let pp = Format.fprintf

  let pp fmt { hash_name; hash_object; kind; length; } =
    pp fmt "{ @[<hov>name = %d;@ \
                     hash = %a;@ \
                     kind = %a;@ \
                     length = %Ld;@] }"
      hash_name Hash.pp hash_object Kind.pp kind length

  (* XXX(dinosaure): hash from git to sort git objects.
                     in git, this hash is computed in an [int32],
                     so we have a problem for the 32-bits architecture.
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
      ; length = Minigit.Tag.raw_length tag }
    | Commit commit ->
      { hash_name
      ; hash_object
      ; kind = Kind.Commit
      ; length = Minigit.Commit.raw_length commit }
    | Tree tree ->
      { hash_name
      ; hash_object
      ; kind = Kind.Tree
      ; length = Minigit.Tree.raw_length tree }
    | Blob blob ->
      { hash_name
      ; hash_object
      ; kind = Kind.Blob
      ; length = Minigit.Blob.raw_length blob }

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
    { o_off   : int
    ; o_pos   : int
    ; o_len   : int
    ; write   : int64
    ; radix   : (Crc32.t * int64) Radix.t
    ; objects : Entry.t array
    ; hash    : Hash.ctx
    ; state   : state }
  and k = Cstruct.t -> t -> res
  and state =
    | Header of k
    | Object of k
    | Raw    of int * int64 * Crc32.t (* idx, off, crc *)
    | Zip of { idx : int
             ; raw : Cstruct.t
             ; off : int64
             ; crc : Crc32.t
             ; z   : (Decompress.B.bs, Decompress.B.bs) Decompress.Deflate.t }
    | Epilogue of { idx : int
                  ; off : int64
                  ; crc : Crc32.t }
    | Hash of k
    | End
    | Exception of error
  and res =
    | Wait of t
    | Flush of t
    | Error of t * error
    | Cont of t
    | Ok of t

  let pp = Format.fprintf

  let pp_state fmt = function
    | Header _ -> pp fmt "(Header #k)"
    | Object _ -> pp fmt "(Object #k)"
    | Raw (idx, off, crc)  -> pp fmt "(Raw idx:%d, off:%Ld, crc:%a)" idx off Crc32.pp crc
    | Zip { idx; off; crc; raw; z; } ->
      pp fmt "(Zip { @[<hov>idx = %d;@ \
                            raw = #raw;@ \
                            off = %Ld;@ \
                            crc = %a;@ \
                            z = %a;@] })"
        idx off Crc32.pp crc Decompress.Deflate.pp z
    | Epilogue { idx; off; crc; } ->
      pp fmt "(Epilogue { @[<hov>idx = %d;@ \
                                 off = %Ld;@ \
                                 crc = %a;@] })"
        idx off Crc32.pp crc
    | Hash _ -> pp fmt "(Hash #k)"
    | End -> pp fmt "End"
    | Exception exn -> pp fmt "(Exception %a)" pp_error exn

  let pp_list ?(sep = (fun fmt () -> ()) )pp_data fmt lst =
    let rec aux = function
      | [] -> ()
      | [ x ] -> pp_data fmt x
      | x :: r -> pp fmt "%a%a" pp_data x sep (); aux r
    in aux lst

  let pp fmt { o_off; o_pos; o_len; write; radix; objects; state; } =
    pp fmt "{ @[<hov>o_off = %d;@ \
                     o_pos = %d;@ \
                     o_len = %d;@ \
                     write = %Ld;@ \
                     radix = #tree;@ \
                     objects = #list;@ \
                     state = @[<hov>%a@];@] }"
      o_off o_pos o_len
      write
      pp_state state

  let await t = Wait t
  let flush dst t =
    let () = Hash.feed t.hash (Cstruct.to_string (Cstruct.sub dst t.o_off t.o_pos)) in
                              (* XXX(dinosaure): use [cstruct] instead [string] (avoid allocation). *)
    Flush t
  let error t exn = Error ({ t with state = Exception exn }, exn)
  let ok    t = Ok ({ t with state = End })

  module KHeader =
  struct
    let rec put_byte byte k dst t =
      if (t.o_len - t.o_pos) > 0
      then begin
        Cstruct.set_uint8 dst (t.o_off + t.o_pos) byte;
        k dst { t with o_pos = t.o_pos + 1
                     ; write = Int64.add t.write 1L }
      end else flush dst { t with state = Header (put_byte byte k) }

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
      end else flush dst { t with state = Header (put_u32 integer k) }
  end

  module KObject =
  struct
    let rec put_byte byte k dst t =
      if (t.o_len - t.o_pos) > 0
      then begin
        Cstruct.set_uint8 dst (t.o_off + t.o_pos) byte;
        k dst { t with o_pos = t.o_pos + 1
                     ; write = Int64.add t.write 1L }
      end else flush dst { t with state = Object (put_byte byte k) }

    let rec length n crc k dst t =
      let byte = Int64.(to_int (n && 0x7FL)) in
      let rest = Int64.(n >> 7) in

      if rest <> 0L
      then put_byte (byte lor 0x80) (length rest (Crc32.digestc crc (byte lor 0x80)) k) dst t
      else put_byte byte (k (Crc32.digestc crc byte)) dst t
  end

  module KHash =
  struct
    let rec put_byte byte k dst t =
      if (t.o_len - t.o_pos) > 0
      then begin
        Cstruct.set_uint8 dst (t.o_off + t.o_pos) byte;
        k dst { t with o_pos = t.o_pos + 1
                     ; write = Int64.add t.write 1L }
      end else flush dst { t with state = Hash (put_byte byte k) }

    let put_hash hash k dst t =
      if t.o_len - t.o_pos >= Hash.size
      then begin
        Cstruct.blit_from_string hash 0 dst (t.o_off + t.o_pos) Hash.size;
        k dst { t with o_pos = t.o_pos + Hash.size
                     ; write = Int64.add t.write (Int64.of_int Hash.size) }
      end else
        let rec loop rest dst t =
          if rest = 0
          then k dst t
          else
            let n = min rest (t.o_len - t.o_pos) in

            if n = 0
            then Flush { t with state = Hash (loop rest) }
            else begin
              Cstruct.blit_from_string hash (Hash.size - rest) dst (t.o_off + t.o_pos) n;
              Flush { t with state = Hash (loop (rest - n))
                           ; o_pos = t.o_pos + n
                           ; write = Int64.add t.write (Int64.of_int n) }
            end
        in

        loop Hash.size dst t
  end

  let hash dst t =
    KHash.put_hash (Hash.get t.hash) (fun _ -> ok) dst t

  let header_entry idx dst t =
    if idx = Array.length t.objects
    then Cont { t with state = Hash hash }
    else
      let offset = t.write in
      let entry  = Array.get t.objects idx in
      let length = entry.Entry.length in

      let byte   = ((Kind.to_bin entry.Entry.kind) lsl 4) in (* kind *)
      let byte   = byte lor Int64.(to_int (length && 0x0FL)) in (* part of the size *)

      let byte, continue =
        if Int64.(length >> 4) <> 0L
        then byte lor 0x80, true
        else byte, false
      in

      let crc    = Crc32.digestc Crc32.default byte in

      if continue
      then (KObject.put_byte byte
            @@ KObject.length Int64.(length >> 4) crc
            @@ fun crc dst t -> await { t with state = Raw (idx, offset, crc) })
          dst t
      else KObject.put_byte byte (fun dst t -> await { t with state = Raw (idx, offset, crc) }) dst t

  let epilogue_entry dst t idx offset crc =
    Cont { t with state = Object (header_entry (idx + 1))
                ; radix = Radix.bind t.radix (Array.get t.objects idx).Entry.hash_object (crc, offset) }

  let raw_entry dst t idx raw off crc z =
    let open Decompress in

    let raw' = B.from_bigstring (Cstruct.to_bigarray raw) in
    let dst' = B.from_bigstring (Cstruct.to_bigarray dst) in

    match Deflate.eval raw' dst' z with
    | `Await z ->
      await { t with state = Zip { idx; raw; off; crc; z; } }
    | `Flush z ->
      let crc = Crc32.digest ~off:(t.o_off + t.o_pos) ~len:(Deflate.used_out z) crc dst in

      flush dst { t with state = Zip { idx; raw; off; crc; z; }
                       ; o_pos = t.o_pos + (Deflate.used_out z)
                       ; write = Int64.add t.write (Int64.of_int (Deflate.used_out z)) }
    | `End z ->
      let crc = Crc32.digest ~off:(t.o_off + t.o_pos) ~len:(Deflate.used_out z) crc dst in

      Cont { t with state = Epilogue { idx; off; crc; }
                  ; o_pos = t.o_pos + (Deflate.used_out z)
                  ; write = Int64.add t.write (Int64.of_int (Deflate.used_out z)) }
    | `Error (z, exn) -> error t (Deflate_error exn)

  let number dst t =
    (* XXX(dinosaure): problem in 32-bits architecture. *)
    KHeader.put_u32 (Int32.of_int (Array.length t.objects))
      (fun dst t -> Cont { t with state = Object (header_entry 0) })
      dst t

  let version dst t =
    KHeader.put_u32 2l (fun dst t -> Cont { t with state = Header number }) dst t

  let header dst t =
    (KHeader.put_byte (Char.code 'P')
     @@ KHeader.put_byte (Char.code 'A')
     @@ KHeader.put_byte (Char.code 'C')
     @@ KHeader.put_byte (Char.code 'K')
     @@ fun dst t -> Cont { t with state = Header version })
      dst t

  let raw raw t =
    let open Decompress in

    match t.state with
    | Raw (idx, off, crc) ->
      let z =
        Deflate.flush (t.o_off + t.o_pos) (t.o_len - t.o_pos)
        @@ Deflate.default ~proof:B.proof_bigstring 4
      in
      { t with state = Zip { idx; raw; off; crc; z; } }
    | _ -> raise (Invalid_argument "P.raw: bad state")

  let current_raw t = match t.state with
    | Zip { raw; _ } -> raw
    | _ -> raise (Invalid_argument "P.current_raw: bad state")

  let finish_raw t = match t.state with
    | Zip { idx; raw; off; crc; z; } ->
      { t with state = Zip { idx; raw; off; crc; z = Decompress.Deflate.finish z }}
    | _ -> raise (Invalid_argument "P.finish_raw: bad state")

  let refill offset len ?raw t = match t.state, raw with
    | Zip { idx; raw; off; crc; z; }, Some new_raw ->
      { t with state = Zip { idx; raw = new_raw; off; crc; z = Decompress.Deflate.no_flush offset len z } }
    | Zip { idx; raw; off; crc; z; }, None ->
      { t with state = Zip { idx; raw; off; crc; z = Decompress.Deflate.no_flush offset len z } }
    | _ -> raise (Invalid_argument "P.refill: bad state")

  let flush offset len t =
    if (t.o_len - t.o_pos) = 0
    then
      match t.state with
      | Zip { idx; raw; off; crc; z; } ->
        { t with o_off = offset
              ; o_len = len
              ; o_pos = 0
               ; state = Zip { idx; raw; off; crc; z = Decompress.Deflate.flush offset len z } }
      | _ ->
        { t with o_off = offset
             ; o_len = len
             ; o_pos = 0 }
    else raise (Invalid_argument (sp "P.flush: you lost something (pos: %d, len: %d)" t.o_pos t.o_len))

  let used_out t = t.o_pos

  let expect t = match t.state with
    | Raw (idx, off, crc) ->
      (try
         let hash = (Array.get t.objects idx).Entry.hash_object in
         Some hash
       with exn -> raise (Invalid_argument "P.expect: invalid index"))
    | _ -> None

  let entry t = match t.state with
    | Raw (idx, off, crc) ->
      (try Array.get t.objects idx
       with exn -> raise (Invalid_argument "P.entry: invalid index"))
    | _ -> raise (Invalid_argument "P.entry: bad state")

  let idx t = t.radix

  let hash t = Hash.get t.hash

  let eval dst t =
    let eval0 t = match t.state with
      | Header k -> k dst t
      | Object k -> k dst t
      | Raw (idx, off, crc)  -> await t
      | Zip { idx; raw; off; crc; z; } -> raw_entry dst t idx raw off crc z
      | Epilogue { idx; off; crc; } -> epilogue_entry dst t idx off crc
      | Exception exn -> error t exn
      | Hash k -> k dst t
      | End -> Ok t
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
    ; hash = Hash.init ()
    ; state = (Header header) }
end
