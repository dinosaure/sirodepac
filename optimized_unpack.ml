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

let cstruct_map filename =
  let i = Unix.openfile filename [ Unix.O_RDONLY ] 0o644 in
  let m = Bigarray.Array1.map_file i Bigarray.Char Bigarray.c_layout false (-1) in
  Cstruct.of_bigarray m

module SHA1 =
struct
  include Rakia.SHA1.Bigstring

  type ctx = Rakia.SHA1.ctx

  let length = Rakia.SHA1.digest_size

  let to_cstruct x = Cstruct.of_bigarray x
  let of_cstruct x = (Cstruct.to_bigarray x :> t)

  let of_string s =
    let cs = Cstruct.create length in

    for i = 0 to length - 1
    do Cstruct.set_uint8 cs i (Char.code (String.get s i)) done;

    (Cstruct.to_bigarray cs :> t)

  let to_string x =
    Cstruct.to_string (to_cstruct x)

  let pp fmt hash =
    let cs = Cstruct.of_bigarray hash in

    for i = 0 to length - 1
    do Format.fprintf fmt "%02x" (Cstruct.get_uint8 cs i) done

  let to_pp_string hash =
    let buf = Buffer.create (length * 2) in
    let fmt = Format.formatter_of_buffer buf in

    Format.fprintf fmt "%a%!" pp hash;

    Buffer.contents buf

  let to_char x y =
    let code c = match c with
      | '0'..'9' -> Char.code c - 48 (* Char.code '0' *)
      | 'A'..'F' -> Char.code c - 55 (* Char.code 'A' + 10 *)
      | 'a'..'f' -> Char.code c - 87 (* Char.code 'a' + 10 *)
      | _ ->
        raise (Invalid_argument (Format.sprintf "Hash.to_char: %d is an invalid char" (Char.code c)))
    in
    Char.chr (code x lsl 4 + code y)

  let of_pp_string hash =
    let tmp = Rakia.Bi.create 20 in
    let buf = if String.length hash mod 2 = 1
              then hash ^ "0" else hash
    in

    let rec aux i j =
      if i >= 40 then ()
      else if j >= 40
      then raise (Invalid_argument "Hash.from_pp")
      else begin
        Rakia.Bi.set tmp (i / 2) (to_char buf.[i] buf.[j]);
        aux (j + 1) (j + 2)
      end
    in aux 0 1; tmp

  exception Break

  let equal a b =
    let open Bigarray.Array1 in

    if dim a <> dim b
    then false
    else
      try
        for i = 0 to dim a - 1
        do if get a i <> get b i then raise Break done;

        true
      with Break -> false
end

module Mapper : Unpack.MAPPER with type fd = Unix.file_descr =
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

module IDXDecoder  = Idx.Decoder(SHA1)
module IDXEncoder  = Idx.Encoder(SHA1)
module Decoder     = Unpack.MakeDecoder(SHA1)(Mapper)
module PACKDecoder = Decoder.P
module Radix       = Radix.Make(Rakia.Bi)
module PACKEncoder = Pack.MakePACKEncoder(SHA1)

module Commit = Minigit.Commit(SHA1)
module Tree   = Minigit.Tree(SHA1)
module Tag    = Minigit.Tag(SHA1)
module Blob   = Minigit.Blob

let idx_from_filename filename =
  let map = cstruct_map filename in
  let len = 0x800 in

  let rec loop tree off t =
    match IDXDecoder.eval map t with
    | `Await t ->
      let n = min (Cstruct.len map - off) len in
      let t = IDXDecoder.refill off n t in

      loop tree (off + n) t
    | `End (t, hash) -> (tree, hash)
    | `Hash (t, (hash, crc, offset)) ->
      loop (Radix.bind tree hash (crc, offset)) off t
    | `Error (t, exn) ->
      Format.eprintf "Invalid IDX: %a\n%!" IDXDecoder.pp_error exn;
      assert false
  in

  loop Radix.empty 0 (IDXDecoder.make ())

let z_tmp = Cstruct.create 0x800
let o_tmp = Cstruct.create 0x800
let h_tmp = Cstruct.create 0x800
let z_win = Decompress.Window.create ~proof:Decompress.B.proof_bigstring
let p_nam = Hashtbl.create 100

let save_names_of_tree hash raw =
  match Tree.Decoder.to_result raw with
  | Ok tree ->
    let path = try Hashtbl.find p_nam  hash with Not_found -> "" in
    List.iter (fun entry -> Hashtbl.add p_nam entry.Tree.node (Filename.concat path entry.Tree.name)) tree
  | Error exn -> Format.eprintf "Invalid Tree: %s\n%!" exn; assert false

let base_to_entry hash base =
  let open Decoder.Base in
  match base.kind with
  | `Commit ->
    PACKEncoder.Entry.from_commit hash
    @@ Result.unsafe_ok (Commit.Decoder.to_result base.raw)
  | `Tree ->
    PACKEncoder.Entry.from_tree hash ?path:(try Some (Hashtbl.find p_nam hash) with Not_found -> None)
    @@ Result.unsafe_ok (Tree.Decoder.to_result base.raw)
  | `Tag ->
    PACKEncoder.Entry.from_tag hash
    @@ Result.unsafe_ok (Tag.Decoder.to_result base.raw)
  | `Blob ->
    PACKEncoder.Entry.from_blob hash  ?path:(try Some (Hashtbl.find p_nam hash) with Not_found -> None)
    @@ Result.unsafe_ok (Blob.Decoder.to_result base.raw)

let pp_option pp_data fmt = function
  | Some x -> pp_data fmt x
  | None -> Format.fprintf fmt "<none>"

let make_pack pack_fmt idx_fmt pack entries =
  Array.fast_sort PACKEncoder.Entry.compare entries;

  let rec loop_pack ?(raw = false) t =
    match PACKEncoder.eval o_tmp t with
    | `Await t ->
      let hash = PACKEncoder.expect t in
      let base  = Result.unsafe_ok (Decoder.get pack hash z_tmp z_win) in
      let entry = PACKEncoder.entry t in

      Format.eprintf "Pack the git object (name = %a): %a\n%!" (pp_option Format.pp_print_string) (PACKEncoder.Entry.name entry) SHA1.pp hash;

      loop_pack (PACKEncoder.raw base.Decoder.Base.raw t)
    | `Flush t ->
      let n = PACKEncoder.used_out t in
      Format.fprintf pack_fmt "%s%!" (Cstruct.to_string (Cstruct.sub o_tmp 0 n));
      loop_pack (PACKEncoder.flush 0 (Cstruct.len o_tmp) t)
    | `Error (t, exn) ->
      Format.eprintf "PACK encoder: %a\n%!" PACKEncoder.pp_error exn;
      assert false
    | `End (t, hash) ->
      if PACKEncoder.used_out t <> 0
      then Format.fprintf pack_fmt "%s%!" (Cstruct.to_string (Cstruct.sub o_tmp 0 (PACKEncoder.used_out t)));

      PACKEncoder.idx t, hash
  in

  let rec loop_idx t =
    match IDXEncoder.eval o_tmp t with
    | `Flush t ->
      let n = IDXEncoder.used_out t in
      Format.fprintf idx_fmt "%s%!" (Cstruct.to_string (Cstruct.sub o_tmp 0 n));
      loop_idx (IDXEncoder.flush 0 (Cstruct.len o_tmp) t)
    | `End t ->
      if IDXEncoder.used_out t <> 0
      then Format.fprintf idx_fmt "%s%!" (Cstruct.to_string (Cstruct.sub o_tmp 0 (IDXEncoder.used_out t)));

      ()
    | `Error (t, exn) ->
      Format.eprintf "IDX encoder: %a\n%!" IDXEncoder.pp_error exn;
      assert false
  in

  let tree_idx, hash_pack = loop_pack (PACKEncoder.default h_tmp entries) in

  Format.printf "Hash produced: %a\n%!" SHA1.pp hash_pack;

  loop_idx (IDXEncoder.default (Radix.to_sequence tree_idx) hash_pack)

let idx_filename  = "pack.idx"
let pack_filename = "pack.pack"

exception Break

let bigarray_compare a b =
  let open Bigarray.Array1 in

  if dim a <> dim b
  then false
  else
    try
      for i = 0 to dim a - 1
      do if get a i <> get b i then raise Break done;

      true
    with Break -> false

let hash_of_git_object base =
  let typename = match base.Decoder.Base.kind with
    | `Commit -> "commit"
    | `Tree -> "tree"
    | `Blob -> "blob"
    | `Tag -> "tag"
  in

  let hdr = Format.sprintf "%s %Ld\000" typename base.Decoder.Base.length in

  SHA1.digestv [ (Cstruct.to_bigarray (Cstruct.of_string hdr))
               ; (Cstruct.to_bigarray base.Decoder.Base.raw) ]

let () =
  let (tree_idx, hash_idx) = idx_from_filename Sys.argv.(2) in
  let pack = Decoder.make (Unix.openfile Sys.argv.(1) [ Unix.O_RDONLY ] 0o644) (fun hash -> Radix.lookup tree_idx hash) in

  Format.printf "Calculate the max length needed for all git object.\n%!";

  let max_length = Radix.fold (fun (hash, _) acc -> match Decoder.needed pack hash z_tmp z_win with
    | Ok length -> max length acc
    | Error exn ->
      Format.eprintf "Invalid PACK: %a\n%!" Decoder.pp_error exn;
      assert false) 0 tree_idx
  in

  Format.printf "Max length: %d\n%!" max_length;

  let raw0, raw1 = Cstruct.create max_length, Cstruct.create max_length in

  let each_git_object (hash, (crc, offset)) acc =
    Format.printf "Save %a git object.\n%!" SHA1.pp hash;

    match Decoder.get' pack hash z_tmp z_win (raw0, raw1) with
    | Ok ({ Decoder.Base.kind = `Tree; _ } as base) ->
      save_names_of_tree hash base.Decoder.Base.raw;
      base_to_entry hash base :: acc
    | Ok base ->
      base_to_entry hash base :: acc
    | Error exn ->
      (Format.eprintf "Invalid PACK: %a\n%!" Decoder.pp_error exn;
       assert false)
  in

  let entries = Radix.fold each_git_object [] tree_idx in

  let pack_out = open_out pack_filename in
  let idx_out  = open_out idx_filename in
  let pack_fmt = Format.formatter_of_out_channel pack_out in
  let idx_fmt  = Format.formatter_of_out_channel idx_out in

  make_pack pack_fmt idx_fmt pack (Array.of_list entries);
  close_out pack_out;
  close_out idx_out;

  let (tree_idx, hash_idx) = idx_from_filename idx_filename in
  let pack = Decoder.make (Unix.openfile pack_filename [ Unix.O_RDONLY ] 0o644) (fun hash -> Radix.lookup tree_idx hash) in

  Format.printf "Calculate the max length needed for all git object.\n%!";

  let max_length = Radix.fold (fun (hash, _) acc -> match Decoder.needed pack hash z_tmp z_win with
    | Ok length -> max length acc
    | Error exn ->
      Format.eprintf "Invalid PACK: %a\n%!" Decoder.pp_error exn;
      assert false) 0 tree_idx
  in

  Format.printf "Max length: %d\n%!" max_length;

  let raw0, raw1 = Cstruct.create max_length, Cstruct.create max_length in

  let each_git_object (hash, (crc, offset)) =
    match Decoder.get' pack hash z_tmp z_win (raw0, raw1) with
    | Ok base ->
      Format.printf "Get the git object: %a\n%!" SHA1.pp hash;
    | Error exn ->
      Format.eprintf "Invalid PACK: %a\n%!" Decoder.pp_error exn;
      assert false
  in

  Radix.iter each_git_object tree_idx
