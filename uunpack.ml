open Unpack

let ( / ) = Filename.concat

(* See [bs.c]. *)
external bs_read : Unix.file_descr -> B.Bigstring.t -> int -> int -> int =
  "bigstring_read" [@@noalloc]
external bs_write : Unix.file_descr -> B.Bigstring.t -> int -> int -> int =
  "bigstring_write" [@@noalloc]

(* Abstract [Unix.read] with ['a B.t]. *)
let unix_read (type a) ch (tmp : a B.t) off len = match tmp with
  | B.Bytes v -> Unix.read ch v off len
  | B.Bigstring v -> bs_read ch v off len

let rec safe_mkdir dirname =
  if not (Sys.file_exists dirname) then
    begin safe_mkdir (Filename.dirname dirname);
          Unix.mkdir dirname 0o755 end

let safe_mkfile path =
  let () = List.rev path |> List.tl |> List.rev
           |> List.fold_left Filename.concat "./"
           |> safe_mkdir
  in
  Unix.openfile (List.fold_left Filename.concat "./" path)
    [ Unix.O_CREAT; Unix.O_RDWR; Unix.O_TRUNC ] 0o644

let proof   = Decompress.B.from_bigstring Decompress.B.Bigstring.empty
let dft_src = BBuffer.create ~proof 0x80000
let dft_dst = Decompress.B.Bigstring.create 0x8000

exception Lost_something
exception Deflate_error of Decompress.Deflate.error

let to_file ~path ec =
  Minienc.flush_buffer ec;

  let safe_write ?(off = 0) ~len write =
    let rec aux ~call off =
      let w = write off in

      if w + off = len then ()
      else if call < 4096 then aux ~call:(call + 1) (off + w)
      else raise Stack_overflow
    in
    aux ~call:0 off
  in

  let ic = safe_mkfile path in
  let dc = Decompress.Deflate.default ~proof 4 in

  let rec loop src dst dc =
    match Decompress.Deflate.eval src (B.from_bigstring dst) dc,
          Minienc.operation ec with
    | `Await dc, `Write ({ Minienc.Vec.buf = `Bigstring s; off; len } :: _) ->
      Minienc.shift ec len;
      loop (Decompress.B.from_bigstring s) dst (Decompress.Deflate.no_flush off len dc)
    | `Await dc, `Write ({ Minienc.Vec.buf = `String s; off; len } :: _) ->
      BBuffer.clear dft_src;
      BBuffer.add_string s ~off ~len dft_src;
      Minienc.shift ec len;
      loop (BBuffer.contents dft_src) dst (Decompress.Deflate.no_flush 0 (BBuffer.length dft_src) dc)
    | `Await dc, `Write ({ Minienc.Vec.buf = `Bytes s; off; len } :: _) ->
      BBuffer.clear dft_src;
      BBuffer.add_bytes s ~off ~len dft_src;
      Minienc.shift ec len;
      loop (BBuffer.contents dft_src) dst (Decompress.Deflate.no_flush 0 (BBuffer.length dft_src) dc)
    | `Await dc, (`Write [] | `Yield | `Close) ->
      if Minienc.pending ec
      then loop (BBuffer.contents dft_src) dst dc
      else loop (BBuffer.contents dft_src) dst (Decompress.Deflate.finish dc)
    | `Flush dc, _ ->
      let len = Decompress.Deflate.used_out dc in
      safe_write ~len (fun off -> bs_write ic dst off (len - off));
      loop (BBuffer.contents dft_src) dst (Decompress.Deflate.flush 0 0x8000 dc)
    | `End dc, (`Write [] | `Yield | `Close) ->
      let len = Decompress.Deflate.used_out dc in
      if len = 0 then Ok ()
      else let () = safe_write ~len (fun off -> bs_write ic dst off (len - off)) in Ok ()
    | `End _, `Write (_ :: _) ->
      Error (Lost_something)
    | `Error (dc, exn), _ ->
      Error (Deflate_error exn)
  in

  match loop (BBuffer.contents dft_src) dft_dst dc with
  | Error exn -> raise exn
  | Ok () -> Unix.close ic

let filename_of_sha1 sha1 =
  let pp = Minigit.Hash.to_pp sha1 in
  let prefix = String.sub pp 0 2 in
  let suffix = String.sub pp 2 38 in

  [ ".git"; "objects"; prefix; suffix ]

let write_header encoder kind length =
  let hdr = Format.sprintf "%s %d\000"
    (match kind with
     | P.Commit -> "commit"
     | P.Blob -> "blob"
     | P.Tree -> "tree"
     | P.Tag -> "tag"
     | P.Hunk _ -> raise (Invalid_argument "hash_of_object"))
    length
  in
  Minienc.write_string encoder hdr

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

let store ~sha1 encoder (kind, length, raw) =
  write_header encoder kind length;

  match kind with
  | P.Commit ->
    (match Minigit.Commit.Decoder.to_result raw with
     | Ok commit ->
       Minigit.Commit.Encoder.write_commit encoder commit;
       to_file ~path:(filename_of_sha1 sha1) encoder
     | Error exn -> Format.eprintf "Invalid commit: %a\n%!" Minidec.pp_error exn)
  | P.Tree ->
    (match Minigit.Tree.Decoder.to_result raw with
     | Ok tree ->
       Minigit.Tree.Encoder.write_tree encoder tree;
       to_file ~path:(filename_of_sha1 sha1) encoder
     | Error exn -> Format.eprintf "Invalid tree: %a\n%!" Minidec.pp_error exn)
  | P.Blob ->
    (match Minigit.Blob.Decoder.to_result raw with
     | Ok blob ->
       Minigit.Blob.Encoder.write_blob encoder blob;
       to_file ~path:(filename_of_sha1 sha1) encoder
     | Error exn -> Format.eprintf "Invalid blob: %a\n%!" Minidec.pp_error exn)
  | P.Tag ->
    (match Minigit.Tag.Decoder.to_result raw with
     | Ok tag ->
         Minigit.Tag.Encoder.write_tag encoder tag;
       to_file ~path:(filename_of_sha1 sha1) encoder
     | Error exn -> Format.eprintf "Invalid tag: %a\n%!" Minidec.pp_error exn)
  | P.Hunk _ -> assert false

let unpack ?(chunk = 0x8000) map idx get =
  let buf = BBuffer.create ~proof:(B.from_bigstring B.Bigstring.empty) chunk in
  let enc = Minienc.from (B.from_bigstring @@ B.Bigstring.create chunk) in
  let rap = D.make map get in

  let rec loop offset (t : ('a, 'a) P.t) =
    match P.eval map t with
    | `Await t ->
      let chunk = min (B.length map - offset) chunk in
      if chunk > 0
      then loop (offset + chunk) (P.refill offset chunk t)
      else raise End_of_file
    | `Flush t ->
      let o, n = P.output t in
      let () = BBuffer.add o ~len:n buf in
      loop offset (P.flush 0 n t)
    | `End (t, hash) -> ()
    | `Error (t, exn) ->
      Format.eprintf "Pack error: %a\n%!" P.pp_error exn;
      assert false
    | `Object t ->
      match P.kind t with
      | P.Hunk hunks ->
        let rec undelta hunks offset' =
          match D.delta rap hunks offset' with
          | Ok (P.Hunk hunks, _, offset', _) ->
            undelta hunks offset'
          | Ok (kind, raw, offset', length) ->
            store ~sha1:(hash_of_object kind length raw) enc (kind, length, raw);
            loop offset (P.next_object t)
          | Error exn ->
            Format.eprintf "Delta error: %a\n%!" D.pp_error exn;
            assert false
        in

        undelta hunks (P.offset t)
      | kind ->
        let raw = BBuffer.contents buf in
        store ~sha1:(hash_of_object kind (P.length t) raw) enc (kind, (P.length t), raw);

        BBuffer.clear buf;
        loop offset (P.next_object t)
  in

  loop 0 (P.default ~proof:map ~chunk:chunk ())

(* Bigstring map. *)
let bs_map filename =
  let i = Unix.openfile filename [ Unix.O_RDONLY ] 0o644 in
  let m = Bigarray.Array1.map_file i Bigarray.Char Bigarray.c_layout false (-1) in
  B.from_bigstring m

let () =
  let pp = bs_map Sys.argv.(1) in
  let ii = bs_map Sys.argv.(2) in

  let () =
    match RAI.make ii with
    | Ok t ->
      unpack pp t (fun hash -> RAI.find t hash)
    | Error exn ->
      Format.eprintf "%a\n%!" RAI.pp_error exn
  in

  Format.printf "End of parsing\n%!"
