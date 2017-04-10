open Unpack

module Mapper : MAPPER with type fd = Unix.file_descr =
struct
  type fd = Unix.file_descr

  let length fd = (Unix.LargeFile.fstat fd).Unix.LargeFile.st_size

  (* specialization of [min]. *)
  let min_int64 (a : int64) (b : int64) = if a < b then a else b

  let map fd ?pos ~share len =
    let max = length fd in (* avoid to grow the file. *)
    let max = match pos with
      | Some pos -> Int64.sub max pos
      | None -> max
    in
    let result = Bigarray.Array1.map_file fd ?pos Bigarray.Char Bigarray.c_layout share (Int64.to_int (min_int64 (Int64.of_int len) max)) in

    Cstruct.of_bigarray result
end

module D = D(Mapper)

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
      ; raw ]
    |> Cstruct.to_string

let pp_kind fmt = function
  | P.Commit -> Format.fprintf fmt "commit"
  | P.Tree   -> Format.fprintf fmt "tree  "
  | P.Blob   -> Format.fprintf fmt "blob  "
  | P.Tag    -> Format.fprintf fmt "tag   "
  | P.Hunk _ -> raise (Invalid_argument "pp_kind")

let header_to_string (kind, raw, length) =
  Format.sprintf "%s %d\000"
    (match kind with
     | P.Commit -> "commit"
     | P.Blob -> "blob"
     | P.Tree -> "tree"
     | P.Tag -> "tag"
     | _ -> raise (Invalid_argument "write_header"))
    length

let write_header encoder (kind, raw, length) =
  let hdr = header_to_string (kind, raw, length) in
  Faraday.write_string encoder hdr

let size_of_header (kind, raw, length) =
  String.length (header_to_string (kind, raw, length))

module Result =
struct
  let ( >>| ) x f =
    match x with Ok x -> Ok (f x) | Error exn -> Error exn

  let ( >>! ) x f =
    match x with Ok x -> Ok x | Error exn -> Error (f exn)

  let ( >>= ) x f =
    match x with Ok x -> f x | Error exn -> Error exn

  let unsafe_ok = function Ok x -> x | Error exn -> assert false
end

let deserialize (kind, raw, length) =
  let open Result in

  match kind with
  | P.Commit ->
    `Commit (unsafe_ok @@ Minigit.Commit.Decoder.to_result raw)
  | P.Tree ->
    `Tree (unsafe_ok @@ Minigit.Tree.Decoder.to_result raw)
  | P.Blob ->
    `Blob (unsafe_ok @@ Minigit.Blob.Decoder.to_result raw)
  | P.Tag ->
    `Tag (unsafe_ok @@ Minigit.Tag.Decoder.to_result raw)
  | P.Hunk _ -> assert false

let pp fmt = function
  | `Tag tag -> Minigit.Tag.pp fmt tag
  | `Commit commit -> Minigit.Commit.pp fmt commit
  | `Tree tree -> Minigit.Tree.pp fmt tree
  | `Blob blob -> Format.fprintf fmt "#blob"

let dual encoder (kind, raw, length) =
  write_header encoder (kind, raw, length);

  match kind with
  | P.Commit ->
    (match Minigit.Commit.Decoder.to_result raw with
     | Ok commit ->
       Minigit.Commit.Encoder.commit encoder commit
     | Error exn ->
       Format.eprintf "Invalid commit: %s\n%!" exn;
       Format.eprintf "%a\n%!" Cstruct.hexdump_pp raw;
       assert false)
  | P.Tree ->
    (match Minigit.Tree.Decoder.to_result raw with
     | Ok tree -> Minigit.Tree.Encoder.tree encoder tree
     | Error exn ->
       Format.eprintf "Invalid tree: %s\n%!" exn;
       Format.eprintf "%a\n%!" Cstruct.hexdump_pp raw;
       assert false)
  | P.Blob ->
    (match Minigit.Blob.Decoder.to_result raw with
     | Ok blob -> Minigit.Blob.Encoder.blob encoder blob
     | Error exn ->
       Format.eprintf "Invalid blob: %s\n%!" exn;
       Format.eprintf "%a\n%!" Cstruct.hexdump_pp raw;
       assert false)
  | P.Tag ->
    (match Minigit.Tag.Decoder.to_result raw with
     | Ok tag -> Minigit.Tag.Encoder.tag encoder tag
     | Error exn ->
       Format.eprintf "Invalid tag: %s\n%!" exn;
       Format.eprintf "%a\n%!" Cstruct.hexdump_pp raw;
       assert false)
  | P.Hunk _ -> assert false

let to_cstruct { Faraday.buffer; off; len; } = match buffer with
  | `Bigstring bs -> Cstruct.of_bigarray ~off ~len bs
  | `String bs -> Cstruct.of_string (String.sub bs off len)
  | `Bytes bs -> Cstruct.of_bytes (Bytes.sub bs off len)

let hash_of_encoder encoder =
  let digest = Nocrypto.Hash.SHA1.init () in

  match Faraday.serialize encoder
          (fun lst ->
           let lst = List.map to_cstruct lst in
           List.iter (Nocrypto.Hash.SHA1.feed digest) lst;
           `Ok (Cstruct.lenv lst)) with
  | `Yield ->
    if Faraday.has_pending_output encoder
    then begin
      Format.eprintf "The encoder is not totally drained\n%!";
      assert false
    end else
      Cstruct.to_string (Nocrypto.Hash.SHA1.get digest)
  | `Close ->
    Format.eprintf "Unexpected close of the encoder\n%!";
    assert false

(* check the offset provided by the deserialization of the PACK file with the
     offset from the IDX file (lazy implementation).
   check the decoding (from the PACK file) and the encoding (see Minigit and
     dual) of the Git object and produce an encoder object (which contains the
     Git object).
   check the hash produced by the result of the encoding (see hash_of_encoder)
     and compare with the hash provided by the parsing of the PACK file.
   check the CRC-32 provided by the IDX file and compare with the CRC-32 produced by
     the deserialization of the git-object inside the PACK file.
   print the result.
*)
let check hash kind raw length consumed offset crc ?base getter = match getter hash with
  | None ->
    Format.eprintf "Object %s not found\n%!"
      (Hash.to_pp hash);
    assert false
  | Some (crc', offset') ->
    if offset <> offset'
    then begin
      Format.eprintf "Offset of the object %s (%Ld) does not correspond with \
                      the IDX file (%Ld)\n%!"
        (Hash.to_pp hash)
        offset offset';
      assert false
    end;

    let real_length = match base with Some (_, _, x) -> x | None -> length in
    let encoder = Faraday.create (real_length + size_of_header (kind, raw, real_length)) in

    dual encoder (kind, raw, real_length);
    let hash' = hash_of_encoder encoder in

    Format.printf "has: %a, expect: %a\n%!" Crc32.pp crc' Crc32.pp crc;

    if hash <> hash' || crc <> crc'
    then begin
      Format.eprintf "Hash of the object %s does not correspond with the hash \
                      produced by the encoder %s\n%!"
        (Hash.to_pp hash)
        (Hash.to_pp hash');

      Format.eprintf "Or CRC-32 of the IDX (%a) file does not correspond to the \
                      CRC-32 of the PACK file (%a)\n%!"
        Crc32.pp crc'
        Crc32.pp crc;

      Format.eprintf "OCaml value: %a\n%!"
        pp (deserialize (kind, raw, real_length));

      dual encoder (kind, raw, real_length);

      let _ = Faraday.serialize encoder
        (fun lst ->
         let lst = List.map to_cstruct lst in
         let raw' = Cstruct.concat lst in

         Format.eprintf "BAD [%s]:\n%a\n%!"
           (Hash.to_pp (Hash.digest raw'))
           Cstruct.hexdump_pp raw';

         let raw =
           Cstruct.concat
             [ (Cstruct.of_string (header_to_string (kind, raw, length)))
             ; raw ]
         in

         Format.eprintf "OK (without header):\n%a\n%!"
           Cstruct.hexdump_pp raw;

         `Ok (Cstruct.lenv lst))
      in

      assert false
    end;

    match base with
    | Some (level, base_hash, base_length) ->
      Format.printf "%s %a %d %d %Ld %d %s\n%!"
        (Hash.to_pp hash)
        pp_kind kind
        length
        consumed
        offset
        level (Hash.to_pp base_hash)
    | None ->
      Format.printf "%s %a %d %d %Ld\n%!"
        (Hash.to_pp hash)
        pp_kind kind
        length
        consumed
        offset

type entry =
  | Commit of Minigit.Commit.t
  | Tree   of Minigit.Tree.t
  | Tag    of Minigit.Tag.t
  | Blob   of Minigit.Blob.t

let unpack ?(chunk = 0x8000) pack_filename get =
  let buf   = BBuffer.create chunk in
  let z_tmp = Decompress.B.from_bigstring (Decompress.B.Bigstring.create 0x8000) in
  let h_tmp = Decompress.B.from_bigstring (Decompress.B.Bigstring.create 0x8000) in
  let z_win = Decompress.Window.create ~proof:Decompress.B.proof_bigstring in

  let pack_fd = Unix.openfile pack_filename [ Unix.O_RDONLY ] 0o644 in
  let map     = Cstruct.of_bigarray @@ Bigarray.Array1.map_file pack_fd Bigarray.Char Bigarray.c_layout false (-1) in
  let rap     = D.make pack_fd get in

  let rec loop entries offset (t : P.t) =
    match P.eval map t with
    | `Await t ->
      let chunk = min (Cstruct.len map - offset) chunk in
      if chunk > 0
      then loop entries (offset + chunk) (P.refill offset chunk t)
      else Error (`File_error End_of_file)
    | `Flush t ->
      let o, n = P.output t in
      let () = BBuffer.add o ~len:n buf in
      loop entries offset (P.flush 0 n t)
    | `End (t, hash) ->
      Ok (hash, entries, rap)
    | `Error (t, exn) ->
      Error (`Unpack_error exn)
    | `Object t ->
      match P.kind t with
      | P.Hunk hunks ->
        let (rlength, rconsumed, roffset, rcrc) = P.length t, P.consumed t, P.offset t, P.crc t in

        let rec undelta ?(level = 1) hunks offset =
          match D.delta rap hunks offset z_tmp z_win h_tmp with
          | Error exn ->
            Error (`Delta_error exn)
          | Ok { D.Base.kind = P.Hunk hunks; offset; _ } ->
            (match undelta ~level:(level + 1) hunks offset with
             | Ok (base, _, level) ->

               Result.(D.apply hunks base
                       >>! (fun exn -> `Delta_error exn)
                       >>| (fun base ->
                            let base_hash = hash_of_object base.D.Base.kind base.D.Base.length base.D.Base.raw in
                            (base, base_hash, level)))
             | Error exn -> Error exn)
          | Ok base ->
            let base_hash = hash_of_object base.D.Base.kind base.D.Base.length base.D.Base.raw in
            Ok (base, base_hash, level)
        in

        (match undelta hunks (P.offset t) with
         | Ok (base, base_hash, level) ->
           (match D.apply hunks base with
            | Ok base ->
              let hash = hash_of_object base.D.Base.kind base.D.Base.length base.D.Base.raw in

              check hash base.D.Base.kind base.D.Base.raw rlength rconsumed roffset rcrc ~base:(level, base_hash, base.D.Base.length) get;

              (* [check] valids [raw]. *)
              let entry = match base.D.Base.kind with
                | P.Commit -> Commit Result.(unsafe_ok (Minigit.Commit.Decoder.to_result base.D.Base.raw))
                | P.Tree -> Tree Result.(unsafe_ok (Minigit.Tree.Decoder.to_result base.D.Base.raw))
                | P.Tag -> Tag Result.(unsafe_ok (Minigit.Tag.Decoder.to_result base.D.Base.raw))
                | P.Blob -> Blob Result.(unsafe_ok (Minigit.Blob.Decoder.to_result base.D.Base.raw))
                | P.Hunk _ -> assert false
              in

              loop ((hash, entry) :: entries) offset (P.next_object t)
            | Error exn -> Error (`Delta_error exn))
         | Error exn -> Error exn)
      | kind ->
        let raw = BBuffer.contents buf in
        let hash = hash_of_object kind (P.length t) raw in

        check hash kind raw (P.length t) (P.consumed t) (P.offset t) (P.crc t) get;

        (* [check] valids [raw]. *)
        let entry = match kind with
          | P.Commit -> Commit Result.(unsafe_ok (Minigit.Commit.Decoder.to_result raw))
          | P.Tree -> Tree Result.(unsafe_ok (Minigit.Tree.Decoder.to_result raw))
          | P.Tag -> Tag Result.(unsafe_ok (Minigit.Tag.Decoder.to_result raw))
          | P.Blob -> Blob Result.(unsafe_ok (Minigit.Blob.Decoder.to_result raw))
          | P.Hunk _ -> assert false
        in

        BBuffer.clear buf;
        loop ((hash, entry) :: entries) offset (P.next_object t)
  in

  loop [] 0 (P.default z_tmp z_win h_tmp)

(* Bigstring map. *)
let cstruct_map filename =
  let i = Unix.openfile filename [ Unix.O_RDONLY ] 0o644 in
  let m = Bigarray.Array1.map_file i Bigarray.Char Bigarray.c_layout false (-1) in
  Cstruct.of_bigarray m

type error = [ `Unoack_error of P.error
             | `Delta_error of D.error
             | `Lazy_index_error of Idx.Lazy.error
             | `Decoder_index_error of Idx.Decoder.error
             | `Encoder_index_error of Idx.Encoder.error
             | `Implementation_index_error
             | `File_error of exn ]

let encoder_index_err exn = `Encoder_index_error exn
let lazy_index_err exn = `Lazy_index_error exn
let dual_index_err ()  = `Dual_index_error

(* serialization of a IDX tree. *)
let serialize_idx_file tree =
  let state  = Idx.Encoder.default (Radix.to_sequence tree) in
  let output = Cstruct.create 0x800 in
  let result = BBuffer.create 0x800 in

  let rec loop t = match Idx.Encoder.eval output t with
    | `Flush t ->
      let len = Idx.Encoder.used_out t in
      BBuffer.add output ~off:0 ~len result;
      loop (Idx.Encoder.flush 0 0x800 t)
    | `End t ->
      if Idx.Encoder.used_out t <> 0
      then BBuffer.add output ~off:0 ~len:(Idx.Encoder.used_out t) result;

      Ok (Cstruct.to_string @@ BBuffer.contents result)
    | `Error (t, exn) -> Error exn
  in

  loop state

exception Break

(* check the parsing of the non-blocking deserialization of the IDX file.
   check the value of each hash from the lazy implementation with the
     non-blocking implementation.
*)
let check_idx_implementation refiller rai =
  Format.eprintf "check_idx_implementation\n%!";

  let getter hash = Idx.Lazy.find rai hash in

  match Idx.idx_to_tree refiller with
  | Error exn -> Error (`Decoder_index_error exn)
  | Ok tree ->
    try let () = Radix.fold (fun (key, value) () -> match getter key with
                             | None -> raise Break
                             | Some value' ->
                               if value <> value'
                               then raise Break
                               else ())
                 () tree
        in Ok (tree, rai)
    with Break -> Error `Implementation_index_error

(* check the serialization of the IDX file.
   check the deserialization of the serialization of the IDX file.
   check the value provided by the serialization of the IDX file with the lazy implementation.
 *)
let dual_idx (tree, rai) =
  let open Result in

  serialize_idx_file tree
  >>! encoder_index_err
  >>= fun map ->

  let refiller bytes =
    let pos = ref 0 in
    let len = String.length bytes in

    fun buffer ->
      let n = min (len - !pos) (Cstruct.len buffer) in
      Cstruct.blit_from_string bytes !pos buffer 0 n;
      pos := !pos + n;
      n
  in

  check_idx_implementation (refiller map) rai

let unzip l =
  let rec aux (l1, l2) = function
    | [] -> (l1, l2)
    | (a, b) :: r -> aux (a :: l1, b :: l2) r
  in aux ([], []) l

let zip l1 l2 =
  let rec aux acc = function
    | [], [] -> acc
    | (a :: ra), (b :: rb) -> aux ((a, b) :: acc) (ra, rb)
    | _ -> raise (Invalid_argument "zip")
  in

  aux [] (l1, l2)

let pack ?(chunk_size = 0x800) fmt (entries, rap) =
  let z_tmp = Decompress.B.from_bigstring (Decompress.B.Bigstring.create 0x8000) in
  let h_tmp = Decompress.B.from_bigstring (Decompress.B.Bigstring.create 0x8000) in
  let z_win = Decompress.Window.create ~proof:Decompress.B.proof_bigstring in

  let entries = Array.of_list
      (List.map (fun (hash, o) -> match o with
           | Tag o -> Pack.Entry.from_tag hash o
           | Commit o -> Pack.Entry.from_commit hash o
           | Tree o -> Pack.Entry.from_tree hash o
           | Blob o -> Pack.Entry.from_blob hash o)
          entries)
  in
  let dst = Cstruct.create chunk_size in

  Array.sort Pack.Entry.compare entries;

  let rec loop ?(raw = false) t =
    Format.eprintf "%a\n%!" Pack.P.pp t;

    match Pack.P.eval dst t with
    | `Await t ->
      (match Pack.P.expect t with
       | Some hash ->
         let base = Result.unsafe_ok (D.get rap hash z_tmp z_win h_tmp) in
                    (* XXX(dinosaure): previous tests check the [unsafe_ok]. *)
         loop (Pack.P.raw base.D.Base.raw t)
       | None ->
         if raw
         then loop (Pack.P.finish_raw t)
         else loop ~raw:true (Pack.P.refill 0 (Cstruct.len (Pack.P.current_raw t)) t))
    | `Flush t ->
      let n = Pack.P.used_out t in
      Format.fprintf fmt "%s%!" (Cstruct.to_string (Cstruct.sub dst 0 n));
      loop (Pack.P.flush 0 chunk_size t)
    | `Error (t, exn) -> Error (`Pack_error exn)
    | `End t ->
      if Pack.P.used_out t <> 0
      then Format.fprintf fmt "%s%!" (Cstruct.to_string (Cstruct.sub dst 0 (Pack.P.used_out t)));

      Ok ()
  in

  loop (Pack.P.default entries)

let () =
  let pack_filename = Sys.argv.(1) in (* alloc *)
  let ii = cstruct_map Sys.argv.(2) in (* alloc *)

  let out = open_out "pack.pack" in
  let fmt = Format.formatter_of_out_channel out in

  let refiller map =
    let pos = ref 0 in
    let len = Cstruct.len map in

    fun buffer ->
      let n = min (len - !pos) (Cstruct.len buffer) in
      Cstruct.blit map !pos buffer 0 n;
      pos := !pos + n;
      n
  in

  (* check the lazy implementation of the IDX file.
     check the non-blocking deserialization of the IDX file.
     check the non-blocking serialization of the IDX file.
     check the non-blocking deserialization of the PACK file.
   *)
  match Result.(Idx.Lazy.make ii
                >>! lazy_index_err
                >>= check_idx_implementation (refiller ii)
                >>= dual_idx
                >>| snd (* take the lazy implementation *)
                >>= (fun rai -> unpack pack_filename (fun hash -> Idx.Lazy.find rai hash))
                >>= (fun (hash, entries, rap) -> pack fmt (entries, rap))) with
  | Ok () -> Format.printf "End of parsing\n%!"
  | Error (`Unpack_error exn) ->
    Format.eprintf "unpack error: %a\n%!" P.pp_error exn
  | Error (`Pack_error exn) ->
    Format.eprintf "pack error: %a\n%!" Pack.P.pp_error exn
  | Error (`Delta_error exn) ->
    Format.eprintf "delta error: %a\n%!" D.pp_error exn
  | Error (`Lazy_index_error exn) ->
    Format.eprintf "lazy index error: %a\n%!" Idx.Lazy.pp_error exn
  | Error (`Decoder_index_error exn) ->
    Format.eprintf "index (decoder) error: %a\n%!" Idx.Decoder.pp_error exn
  | Error (`Encoder_index_error exn) ->
    Format.eprintf "index (encoder) error: %a\n%!" Idx.Encoder.pp_error exn
  | Error `Implementation_index_error ->
    Format.eprintf "implementation index error\n%!"
  | Error (`File_error exn) ->
    Format.eprintf "file error: %s\n%!" (Printexc.to_string exn)
