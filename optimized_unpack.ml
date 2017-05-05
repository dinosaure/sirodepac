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

  let hash = Hashtbl.hash

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

  let compare = compare
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

module ZC : Pack.Z =
struct
  type t =
    { state : Zlib.stream
    ; used_in : int
    ; used_out : int
    ; in_pos : int
    ; in_len : int
    ; out_pos : int
    ; out_len : int
    ; want_to_finish : bool }
  and error = Error

  let pp_error fmt Error = Format.fprintf fmt "(Deflate_error #zlib)" (* lazy *)

  let empty_in = Bytes.create 0
  let empty_out = Bytes.create 0

  let default level =
    { state = Zlib.deflate_init level true
    ; used_in = 0
    ; used_out = 0
    ; in_pos = 0
    ; in_len = 0
    ; out_pos = 0
    ; out_len = 0
    ; want_to_finish = false }

  let used_in { used_in; _ }   = used_in
  let used_out { used_out; _ } = used_out

  let finish t =
    { t with want_to_finish = true }

  let no_flush off len t =
    { t with in_pos = off; in_len = len; used_in = 0 }

  let flush off len t =
    { t with out_pos = off; out_len = len; used_out = 0 }

  let eval src' dst' t =
    if t.want_to_finish = false && t.in_len - t.used_in = 0
    then `Await t
    else if t.out_len - t.used_out = 0
    then `Flush t
    else begin
      let src = Bytes.create t.in_len in
      let dst = Bytes.create t.out_len in

      Cstruct.blit_to_bytes src' t.in_pos src 0 t.in_len;

      let (finished, used_in, used_out) =
        Zlib.deflate t.state src
          (if t.want_to_finish then 0 else t.used_in)
          (if t.want_to_finish then 0 else t.in_len - t.used_in)
          dst
          t.used_out
          (t.out_len - t.used_out)
          (if t.want_to_finish then Zlib.Z_FINISH else Zlib.Z_NO_FLUSH)
      in

      Cstruct.blit_from_bytes dst t.used_out dst' (t.out_pos + t.used_out) used_out;

      if finished
      then
        `End { t with used_in = t.used_in + used_in
                    ; used_out = t.used_out + used_out }
      else
        (if t.used_out + used_out = t.out_len
         then
           `Flush { t with used_in = t.used_in + used_in
                         ; used_out = t.used_out + used_out }
         else
           `Await { t with used_in = t.used_in + used_in
                         ; used_out = t.used_out + used_out })
    end
end

module ZO : Pack.Z =
struct
  open Decompress

  type t = (B.bs, B.bs) Deflate.t
  and error = Deflate.error

  let pp_error = Deflate.pp_error

  let default level = Deflate.default ~proof:B.proof_bigstring 4

  let finish   = Deflate.finish
  let used_in  = Deflate.used_in
  let used_out = Deflate.used_out
  let no_flush = Deflate.no_flush
  let flush    = Deflate.flush

  let eval src' dst' t =
    let src = B.from_bigstring @@ Cstruct.to_bigarray src' in
    let dst = B.from_bigstring @@ Cstruct.to_bigarray dst' in

    Deflate.eval src dst t
end

module IDXDecoder  = Idx.Decoder(SHA1)
module IDXEncoder  = Idx.Encoder(SHA1)
module Decoder     = Unpack.MakeDecoder(SHA1)(Mapper)
module PACKDecoder = Decoder.P
module Radix       = Radix.Make(Rakia.Bi)
module Delta       = Pack.MakeDelta(SHA1)
module PACKEncoder = Pack.MakePACKEncoder(SHA1)(ZC)

module Commit = Minigit.Commit(SHA1)
module Tree   = Minigit.Tree(SHA1)
module Tag    = Minigit.Tag(SHA1)
module Blob   = Minigit.Blob

let cstruct_map filename =
  let i = Unix.openfile filename [ Unix.O_RDONLY ] 0o644 in
  let m = Bigarray.Array1.map_file i Bigarray.Char Bigarray.c_layout false (-1) in
  Cstruct.of_bigarray m

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

(* global tempory buffer *)
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

let object_to_entry hash base revidx =
  let open Decoder.Object in

  match base.kind with
  | `Commit ->
    Delta.Entry.from_commit hash
    @@ Result.unsafe_ok (Commit.Decoder.to_result base.raw)
  | `Tree ->
    Delta.Entry.from_tree hash ?path:(try Some (Hashtbl.find p_nam hash) with Not_found -> None)
    @@ Result.unsafe_ok (Tree.Decoder.to_result base.raw)
  | `Tag ->
    Delta.Entry.from_tag hash
    @@ Result.unsafe_ok (Tag.Decoder.to_result base.raw)
  | `Blob ->
    Delta.Entry.from_blob hash ?path:(try Some (Hashtbl.find p_nam hash) with Not_found -> None)
    @@ Result.unsafe_ok (Blob.Decoder.to_result base.raw)

let pp_option pp_data fmt = function
  | Some x -> pp_data fmt x
  | None -> Format.fprintf fmt "<none>"

let make_pack pack_fmt idx_fmt pack access entries =
  let rec loop_pack ?(raw = false) t =
    match PACKEncoder.eval o_tmp t with
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

  let tree_idx, hash_pack = loop_pack (PACKEncoder.default h_tmp access entries) in

  Format.printf "Hash produced: %a\n%!" SHA1.pp hash_pack;

  loop_idx (IDXEncoder.default (Radix.to_sequence tree_idx) hash_pack)

let idx_filename  = "pack.idx"
let pack_filename = "pack.pack"

let hash_of_git_object base =
  let typename = match base.Decoder.Object.kind with
    | `Commit -> "commit"
    | `Tree -> "tree"
    | `Blob -> "blob"
    | `Tag -> "tag"
  in

  let hdr = Format.sprintf "%s %Ld\000" typename base.Decoder.Object.length in

  SHA1.digestv [ (Cstruct.to_bigarray (Cstruct.of_string hdr))
               ; (Cstruct.to_bigarray base.Decoder.Object.raw) ]

module Map = Map.Make(Int64)

let pp_pair pp_one pp_two fmt (a, b) =
  Format.fprintf fmt "@[<hov>(@[<hov>%a@],@ @[<hov>%a@])@]"
    pp_one a pp_two b

let pp_list ?(sep = (fun fmt () -> ()) )pp_data fmt lst =
  let rec aux = function
    | [] -> ()
    | [ x ] -> pp_data fmt x
    | x :: r -> Format.fprintf fmt "%a%a" pp_data x sep (); aux r
  in aux lst

let () =
  let (old_tree_idx, _) = idx_from_filename Sys.argv.(2) in
  let old_pack = Decoder.make (Unix.openfile Sys.argv.(1) [ Unix.O_RDONLY ] 0o644)
      (fun hash -> Radix.lookup old_tree_idx hash)
      (fun hash -> None)
  in

  let max_length = Radix.fold (fun (hash, _) acc -> match Decoder.needed old_pack hash z_tmp z_win with
    | Ok length -> max length acc
    | Error exn ->
      Format.eprintf "Invalid PACK: %a\n%!" Decoder.pp_error exn;
      assert false) 0 old_tree_idx
  in

  let old_raw0, old_raw1 = Cstruct.create max_length, Cstruct.create max_length in
  let revidx = Radix.fold (fun (hash, (_, offset)) acc -> Map.add offset hash acc) Map.empty old_tree_idx in

  let each_git_object (hash, (crc, offset)) acc =
    match Decoder.get' old_pack hash z_tmp z_win (old_raw0, old_raw1) with
    | Ok ({ Decoder.Object.kind = `Tree; _ } as base) ->
      save_names_of_tree hash base.Decoder.Object.raw;
      object_to_entry hash base (fun offset -> try Some (Map.find offset revidx) with Not_found -> None) :: acc
    | Ok base ->
      object_to_entry hash base (fun offset -> try Some (Map.find offset revidx) with Not_found -> None) :: acc
    | Error exn ->
      (Format.eprintf "Invalid PACK: %a\n%!" Decoder.pp_error exn;
       assert false)
  in

  let entries = Radix.fold each_git_object [] old_tree_idx in
  let entries =
    List.map (fun x -> try let name = Hashtbl.find p_nam x.Delta.Entry.hash_object in
                 { x with Delta.Entry.hash_name = Delta.Entry.hash name
                        ; name = Some name }
               with Not_found -> x)
      entries
  in

  match Delta.deltas entries
      (fun hash -> match Decoder.get old_pack hash z_tmp z_win with
         | Ok base -> Some base.Decoder.Object.raw
         | Error exn ->
           Format.eprintf "Silent error with %a: %a\n%!" SHA1.pp hash Decoder.pp_error exn;
           None)
      (fun hash -> false) (* we tag all object to [false]. *)
      10 50 with
  | Ok entries ->
    Format.eprintf "Start to write the new PACK file.\n%!";

    let pack_out = open_out pack_filename in
    let idx_out  = open_out idx_filename in
    let pack_fmt = Format.formatter_of_out_channel pack_out in
    let idx_fmt  = Format.formatter_of_out_channel idx_out in

    let access hash = match Decoder.get old_pack hash z_tmp z_win with
      | Ok base -> Some base.Decoder.Object.raw
      | Error exn ->
        Format.eprintf "Silent error with %a: %a\n%!" SHA1.pp hash Decoder.pp_error exn;
        None
    in

    make_pack pack_fmt idx_fmt old_pack access entries;
    close_out pack_out;
    close_out idx_out;

    let (new_tree_idx, _) = idx_from_filename idx_filename in
    let new_pack = Decoder.make (Unix.openfile pack_filename [ Unix.O_RDONLY ] 0o644) (fun hash -> Radix.lookup new_tree_idx hash) (fun hash -> None) in

    let max_length = Radix.fold (fun (hash, _) acc -> match Decoder.needed new_pack hash z_tmp z_win with
      | Ok length -> max length acc
      | Error exn ->
        Format.eprintf "Invalid PACK: %a\n%!" Decoder.pp_error exn;
        assert false) 0 new_tree_idx
    in

    let new_raw0, new_raw1 = Cstruct.create max_length, Cstruct.create max_length in

    let each_git_object (hash, (crc, offset)) =
      match Decoder.get' new_pack hash z_tmp z_win (new_raw0, new_raw1),
            Decoder.get' old_pack hash z_tmp z_win (old_raw0, old_raw1) with
      | Ok new_base, Ok old_base ->
        Format.eprintf "Get the git object: %a\n%!" SHA1.pp hash;
      | Error exn, _ | _, Error exn ->
        Format.eprintf "Invalid PACK: %a\n%!" Decoder.pp_error exn;
        assert false
    in

    Radix.iter each_git_object new_tree_idx

  | Error exn -> Format.eprintf "Delta error: %a\n%!" Delta.pp_error exn
