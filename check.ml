open Unpack

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
  Minienc.write_string encoder hdr

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
     | Ok commit -> Minigit.Commit.Encoder.write_commit encoder commit
     | Error exn ->
       Format.eprintf "Invalid commit: %a\n%!" Minidec.pp_error exn;
       Format.eprintf "%a\n%!"
         Hex.hexdump (Hex.of_string_fast (B.to_string raw));
       assert false)
  | P.Tree ->
    (match Minigit.Tree.Decoder.to_result raw with
     | Ok tree -> Minigit.Tree.Encoder.write_tree encoder tree
     | Error exn ->
       Format.eprintf "Invalid tree: %a\n%!" Minidec.pp_error exn;
       Format.eprintf "%a\n%!"
         Hex.hexdump (Hex.of_string_fast (B.to_string raw));
       assert false)
  | P.Blob ->
    (match Minigit.Blob.Decoder.to_result raw with
     | Ok blob -> Minigit.Blob.Encoder.write_blob encoder blob
     | Error exn ->
       Format.eprintf "Invalid blob: %a\n%!" Minidec.pp_error exn;
       Format.eprintf "%a\n%!"
         Hex.hexdump (Hex.of_string_fast (B.to_string raw));
       assert false)
  | P.Tag ->
    (match Minigit.Tag.Decoder.to_result raw with
     | Ok tag -> Minigit.Tag.Encoder.write_tag encoder tag
     | Error exn ->
       Format.eprintf "Invalid tag: %a\n%!" Minidec.pp_error exn;
       Format.eprintf "%a\n%!"
         Hex.hexdump (Hex.of_string_fast (B.to_string raw));
       assert false)
  | P.Hunk _ -> assert false

let to_cstruct = function
  | { Minienc.Vec.buf = `Bigstring s
    ; off
    ; len } ->
    Cstruct.of_bigarray ~off ~len s
  | { Minienc.Vec.buf = `String s
    ; off
    ; len } ->
    Cstruct.of_string (String.sub s off len)
  | { Minienc.Vec.buf = `Bytes s
    ; off
    ; len } ->
    Cstruct.of_bytes (Bytes.sub s off len)

let hash_of_encoder encoder =
  Minienc.flush_buffer encoder;

  let digest = Nocrypto.Hash.SHA1.init () in

  match Minienc.serialize encoder
          (fun lst ->
           let lst = List.map to_cstruct lst in
           List.iter (Nocrypto.Hash.SHA1.feed digest) lst;
           `Ok (Cstruct.lenv lst)) with
  | `Yield ->
    if Minienc.pending encoder
    then begin
      Format.eprintf "The encoder is not totally drained\n%!";
      assert false
    end else
      Cstruct.to_string (Nocrypto.Hash.SHA1.get digest)
  | `Close ->
    Format.eprintf "Unexpected close of the encoder\n%!";
    assert false

let encoder_tmp = B.from B.proof_bigstring 0x8000
let encoder = Minienc.from encoder_tmp

(* check the offset provided by the deserialization of the PACK file with the
     offset from the IDX file (lazy implementation).
   check the decoding (from the PACK file) and the encoding (see Minigit and
     dual) of the Git object and produce an encoder object (which contains the
     Git object).
   check the hash produced by the result of the encoding (see hash_of_encoder)
     and compare with the hash provided by the parsing of the PACK file.
   print the result.
*)
let check hash kind raw length consumed offset ?base getter = match getter hash with
  | None ->
    Format.eprintf "Object %s not found\n%!"
      (Minigit.Hash.to_pp hash);
    assert false
  | Some offset' ->
    if offset <> offset'
    then begin
      Format.eprintf "Offset of the object %s (%Ld) does not correspond with \
                      the IDX file (%Ld)\n%!"
        (Minigit.Hash.to_pp hash)
        offset offset';
      assert false
    end;

    let real_length = match base with Some (_, _, x) -> x | None -> length in

    dual encoder (kind, raw, real_length);
    let hash' = hash_of_encoder encoder in

    if hash <> hash'
    then begin
      Format.eprintf "Hash of the object %s does not correspond with the hash \
                      produced by the encoder %s\n%!"
        (Minigit.Hash.to_pp hash)
        (Minigit.Hash.to_pp hash');

      Format.eprintf "OCaml value: %a\n%!"
        pp (deserialize (kind, raw, real_length));

      dual encoder (kind, raw, real_length);

      let _ = Minienc.serialize encoder
        (fun lst ->
         let lst = List.map to_cstruct lst in
         let raw' = Cstruct.concat lst in

         Format.eprintf "BAD [%s]:\n%a\n%!"
           (Minigit.Hash.to_pp (Cstruct.to_string (Nocrypto.Hash.SHA1.digest raw')))
           Hex.hexdump (Hex.of_string_fast (Cstruct.to_string raw'));

         Format.eprintf "OK (without header):\n%a\n%!"
           Hex.hexdump (Hex.of_string_fast ((header_to_string (kind, raw, length))
                                            ^ B.to_string raw));

         `Ok (Cstruct.lenv lst))
      in

      assert false
    end;

    match base with
    | Some (level, base_hash, base_length) ->
      Format.printf "%s %a %d %d %Ld %d %s\n%!"
        (Minigit.Hash.to_pp hash)
        pp_kind kind
        length
        consumed
        offset
        level (Minigit.Hash.to_pp base_hash)
    | None ->
      Format.printf "%s %a %d %d %Ld\n%!"
        (Minigit.Hash.to_pp hash)
        pp_kind kind
        length
        consumed
        offset

let unpack ?(chunk = 0x8000) map idx get =
  let buf   = BBuffer.create ~proof:(B.from_bigstring B.Bigstring.empty) chunk in
  let z_tmp = B.from_bigstring (B.Bigstring.create 0x8000) in
  let h_tmp = B.from_bigstring (B.Bigstring.create 0x8000) in
  let z_win = Decompress.Window.create ~proof:B.proof_bigstring in

  let rap   = D.make map get in

  let rec loop offset (t : ('a, 'a) P.t) =
    match P.eval map t with
    | `Await t ->
      let chunk = min (B.length map - offset) chunk in
      if chunk > 0
      then loop (offset + chunk) (P.refill offset chunk t)
      else Error (`File_error End_of_file)
    | `Flush t ->
      let o, n = P.output t in
      let () = BBuffer.add o ~len:n buf in
      loop offset (P.flush 0 n t)
    | `End (t, hash) ->
      Ok hash
    | `Error (t, exn) ->
      Error (`Pack_error exn)
    | `Object t ->
      match P.kind t with
      | P.Hunk hunks ->
        let (rlength, rconsumed, roffset) = P.length t, P.consumed t, P.offset t in

        let rec undelta ?(level = 1) hunks offset =
          match D.delta rap hunks offset z_tmp z_win h_tmp with
          | Error exn ->
            Error (`Delta_error exn)
          | Ok { D.Base.kind = P.Hunk hunks; offset; _ } ->
            (match undelta ~level:(level + 1) hunks offset with
             | Ok (base, _, level) ->

               Result.(D.apply rap hunks base
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
           (match D.apply rap hunks base with
            | Ok base ->
              let hash = hash_of_object base.D.Base.kind base.D.Base.length base.D.Base.raw in

              check hash base.D.Base.kind base.D.Base.raw rlength rconsumed roffset ~base:(level, base_hash, base.D.Base.length) get;
              loop offset (P.next_object t)
            | Error exn -> Error (`Delta_error exn))
         | Error exn -> Error exn)
      | kind ->
        let raw = BBuffer.contents buf in
        let hash = hash_of_object kind (P.length t) raw in

        check hash kind raw (P.length t) (P.consumed t) (P.offset t) get;
        BBuffer.clear buf;
        loop offset (P.next_object t)
  in

  loop 0 (P.default ~proof:map ~chunk:chunk z_tmp z_win h_tmp)

(* Bigstring map. *)
let bs_map filename =
  let i = Unix.openfile filename [ Unix.O_RDONLY ] 0o644 in
  let m = Bigarray.Array1.map_file i Bigarray.Char Bigarray.c_layout false (-1) in
  B.from_bigstring m

type error = [ `Pack_error of P.error
             | `Delta_error of D.error
             | `Lazy_index_error of Idx.Lazy.error
             | `Index_error of Idx.Decoder.error
             | `Implementation_index_error
             | `File_error of exn ]

let lazy_index_err exn = `Lazy_index_error exn
let dual_index_err ()  = `Dual_index_error

exception Break

(* check the parsing of the non-blocking deserialization of the IDX file.
   check the value of each hash from the lazy implementation with the
     non-blocking implementation.
*)
let check_idx_implementation map rai =
  let pos = ref 0 in
  let len = B.length map in

  let refiller buffer =
    let n = min (len - !pos) (B.length buffer) in
    B.blit map !pos buffer 0 n;
    pos := !pos + n;
    n
  in

  let getter hash = Idx.Lazy.find rai hash in

  match Idx.idx_to_tree refiller with
  | Error exn -> Error (`Index_error exn)
  | Ok tree ->
    try let () = Radix.fold (fun (key, value) () ->
      match getter key with
      | None -> raise Break
      | Some value' ->
        if value <> value'
        then raise Break
        else ()) () tree
        in Ok rai
    with Break -> Error `Implementation_index_error

let () =
  let pp = bs_map Sys.argv.(1) in (* alloc *)
  let ii = bs_map Sys.argv.(2) in (* alloc *)

  (* check the lazy implementation of the IDX file.
     check the non-blocking deserialization of the IDX file.
     check the non-blocking deserialization of the PACK file.
   *)
  match Result.(Idx.Lazy.make ii
                >>! lazy_index_err
                >>= check_idx_implementation ii
                >>| (fun t -> unpack pp t (fun hash -> Idx.Lazy.find t hash))) with
  | Ok hash -> Format.printf "End of parsing\n%!"
  | Error (`Pack_error exn) ->
    Format.eprintf "pack error: %a\n%!" P.pp_error exn
  | Error (`Delta_error exn) ->
    Format.eprintf "delta error: %a\n%!" D.pp_error exn
  | Error (`Lazy_index_error exn) ->
    Format.eprintf "lazy index error: %a\n%!" Idx.Lazy.pp_error exn
  | Error (`Index_error exn) ->
    Format.eprintf "index error: %a\n%!" Idx.Decoder.pp_error exn
  | Error `Implementation_index_error ->
    Format.eprintf "implementation index error\n%!"
  | Error (`File_error exn) ->
    Format.eprintf "file error: %s\n%!" (Printexc.to_string exn)
