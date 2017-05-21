let () = Printexc.record_backtrace true

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

module Option =
struct
  let iter f = function Some x -> f x | None -> ()
  let map f = function Some x -> Some (f x) | None -> None
end

module SHA1 =
struct
  include Digestif.SHA1.Bigstring

  let length = Digestif.SHA1.digest_size

  let to_cstruct x = Cstruct.of_bigarray x
  let of_cstruct x = (Cstruct.to_bigarray x :> t)

  let of_string x = of_cstruct @@ Cstruct.of_string x
  let to_string x = Cstruct.to_string @@ to_cstruct x

  let of_hex_string x = of_hex (Cstruct.to_bigarray (Cstruct.of_string x))
  let to_hex_string x = Cstruct.to_string (Cstruct.of_bigarray (to_hex x))

  let equal = eq
  let hash  = Hashtbl.hash
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

module ZDC : Pack.Z =
struct
  type t =
    { state          : Zlib.stream
    ; used_in        : int
    ; used_out       : int
    ; in_pos         : int
    ; in_len         : int
    ; out_pos        : int
    ; out_len        : int
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

module ZDO : Pack.Z =
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

module ZIO : Unpack.Z with type window = Decompress.B.bs Decompress.Window.t =
struct
  open Decompress

  type t = (B.bs, B.bs) Inflate.t
  type error = Inflate.error
  type window = B.bs Window.t

  let pp_error = Inflate.pp_error
  let pp = Inflate.pp

  let default : window -> t = fun window -> Inflate.default window
  let window_reset window = Window.reset window

  let used_in = Inflate.used_in
  let used_out = Inflate.used_out
  let flush = Inflate.flush
  let refill = Inflate.refill
  let write = Inflate.write
  let eval src' dst' t =
    let src = B.from_bigstring @@ Cstruct.to_bigarray src' in
    let dst = B.from_bigstring @@ Cstruct.to_bigarray dst' in

    Inflate.eval src dst t
end

module ZIC : Unpack.Z with type window = unit =
struct
  type t =
    { state          : Zlib.stream
    ; used_in        : int
    ; used_out       : int
    ; in_pos         : int
    ; in_len         : int
    ; out_pos        : int
    ; out_len        : int
    ; write          : int
    ; finish         : bool }
  and error = Error
  and window = unit

  let pp_error fmt Error = Format.fprintf fmt "(Deflate_error #zlib)" (* lazy *)
  let pp fmt _ = Format.fprintf fmt "#inflateState"

  let empty_in = Bytes.create 0
  let empty_out = Bytes.create 0
  let window_reset () = ()
  let write { write; _ } = write

  let default level =
    { state = Zlib.inflate_init true
    ; used_in = 0
    ; used_out = 0
    ; in_pos = 0
    ; in_len = 0
    ; out_pos = 0
    ; out_len = 0
    ; write = 0
    ; finish = false }

  let used_in { used_in; _ }   = used_in
  let used_out { used_out; _ } = used_out

  let refill off len t =
    { t with in_pos = off; in_len = len; used_in = 0 }

  let flush off len t =
    { t with out_pos = off; out_len = len; used_out = 0 }

  let eval src' dst' t =
    if t.finish
    then `End t
    else if t.in_len - t.used_in = 0
    then `Await t
    else if t.out_len - t.used_out = 0
    then `Flush t
    else begin
      let src = Bytes.create t.in_len in
      let dst = Bytes.create t.out_len in

      Cstruct.blit_to_bytes src' t.in_pos src 0 t.in_len;

      let (finished, used_in, used_out) =
        Zlib.inflate t.state src
          (if t.finish then 0 else t.used_in)
          (if t.finish then 0 else t.in_len - t.used_in)
          dst
          t.used_out
          (t.out_len - t.used_out)
          Zlib.Z_SYNC_FLUSH
      in

      Cstruct.blit_from_bytes dst t.used_out dst' (t.out_pos + t.used_out) used_out;

      if finished
      then begin
        Zlib.inflate_end t.state;

        `End { t with used_in = t.used_in + used_in
                    ; used_out = t.used_out + used_out
                    ; write = t.write + used_out
                    ; finish = true }
      end else
        (if t.used_out + used_out = t.out_len
         then
           `Flush { t with used_in = t.used_in + used_in
                         ; used_out = t.used_out + used_out
                         ; write = t.write + used_out }
         else
           `Await { t with used_in = t.used_in + used_in
                         ; used_out = t.used_out + used_out
                         ; write = t.write + used_out })
    end

end

module IDXLazy     = Idx.Lazy(SHA1)
module IDXDecoder  = Idx.Decoder(SHA1)
module IDXEncoder  = Idx.Encoder(SHA1)
module Decoder     = Unpack.MakeDecoder(SHA1)(Mapper)(ZIO)
module PACKDecoder = Decoder.P
module Delta       = Pack.MakeDelta(SHA1)
module PACKEncoder = Pack.MakePACKEncoder(SHA1)(ZDO)
module Radix       = PACKEncoder.Radix

module Commit = Minigit.Commit(SHA1)
module Tree   = Minigit.Tree(SHA1)
module Tag    = Minigit.Tag(SHA1)
module Blob   = Minigit.Blob

let hash_of_object base =
  let typename = match base.Decoder.Object.kind with
    | `Commit -> "commit"
    | `Tree -> "tree"
    | `Blob -> "blob"
    | `Tag -> "tag"
  in

  let hdr = Format.sprintf "%s %Ld\000" typename base.Decoder.Object.length in

  SHA1.digestv [ Cstruct.to_bigarray @@ Cstruct.of_string hdr
               ; Cstruct.to_bigarray base.Decoder.Object.raw ]

let cstruct_map filename =
  let i = Unix.openfile filename [ Unix.O_RDONLY ] 0o644 in
  let m = Bigarray.Array1.map_file i Bigarray.Char Bigarray.c_layout false (-1) in
  Cstruct.of_bigarray m

let idx_from_filename filename =
  let map = cstruct_map filename in
  let len = 0x8000 in

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
let z_tmp = Cstruct.create 0x8000
let z_tmp0 = Cstruct.create 0x8000
let o_tmp = Cstruct.create 0x8000
let h_tmp = Cstruct.create 0x8000
let z_win = Decompress.Window.create ~proof:Decompress.B.proof_bigstring
let z_win0 = Decompress.Window.create ~proof:Decompress.B.proof_bigstring
let p_nam = Hashtbl.create 100

let save_names_of_tree hash raw =
  match Tree.Decoder.to_result raw with
  | Ok tree ->
    let path = try Hashtbl.find p_nam  hash with Not_found -> "" in
    List.iter (fun entry -> Hashtbl.add p_nam entry.Tree.node (Filename.concat path entry.Tree.name)) tree
  | Error exn -> Format.eprintf "Invalid Tree: %s\n%!" exn; assert false

let object_to_entry hash base =
  let open Decoder.Object in

  match base.kind with
  | `Commit ->
    Delta.Entry.make hash Pack.Kind.Commit base.length
  | `Tree ->
    Delta.Entry.make hash Pack.Kind.Tree ?name:(try Some (Hashtbl.find p_nam hash) with Not_found -> None) base.length
  | `Tag ->
    Delta.Entry.make hash Pack.Kind.Tag base.length
  | `Blob ->
    Delta.Entry.make hash Pack.Kind.Blob ?name:(try Some (Hashtbl.find p_nam hash) with Not_found -> None) base.length

let pp_option pp_data fmt = function
  | Some x -> pp_data fmt x
  | None -> Format.fprintf fmt "<none>"

external write : Unix.file_descr -> Cstruct.buffer -> int -> int -> int =
  "bigstring_write" [@@noalloc]

type deserializer =
  { window          : Unpack.Window.t
  ; consume         : int
  ; state           : Decoder.P.t
  ; hash            : SHA1.t
  ; final           : bool
  ; i_off           : int
  ; i_pos           : int
  ; i_len           : int
  ; apply_idx       : int option
  ; u_in            : int }

let pp_deserializer fmt t =
  let pp_option pp_data fmt = function
    | Some x -> pp_data fmt x
    | None -> Format.fprintf fmt "<none>"
  in

  Format.fprintf fmt "{ @[<hov>window = @[<hov>%a@];@ \
                               consume = %d;@ \
                               state = @[<hov>%a@];@ \
                               hash = @[<hov>%a@];@ \
                               final = %b;@ \
                               i_off = %d;@ \
                               i_pos = %d;@ \
                               i_len = %d;@ \
                               apply_idx = @[<hov>%a@];@ \
                               u_in = %d;@] }"
    Unpack.Window.pp t.window
    t.consume Decoder.P.pp t.state SHA1.pp t.hash t.final t.i_off t.i_pos t.i_len (pp_option Format.pp_print_int) t.apply_idx t.u_in

let s_tmp = Cstruct.create 0x8000

let apply src trg state = function
  | Decoder.P.HunkDecoder.Insert i ->
    (match state.apply_idx with
    | Some idx ->
      let n = min (state.i_len - state.i_pos) (Cstruct.len i - idx) in
      Cstruct.blit i idx trg state.i_pos n;
      { state with i_pos = state.i_pos + n
                  ; apply_idx = if idx + n = Cstruct.len i then None else Some (idx + n)
                  ; state = if idx + n = Cstruct.len i then Decoder.P.continue state.state else state.state }
    | None ->
      let n = min (state.i_len - state.i_pos) (Cstruct.len i) in
      Cstruct.blit i 0 trg state.i_pos n;
      { state with i_pos = state.i_pos + n
                  ; apply_idx = if n = Cstruct.len i then None else Some n
                  ; state = if n = Cstruct.len i then Decoder.P.continue state.state else state.state })
  | Decoder.P.HunkDecoder.Copy (off, len) ->
    (match state.apply_idx with
    | Some idx ->
      let n = min (state.i_len - state.i_pos) (len - idx) in
      Cstruct.blit src (off + idx) trg state.i_pos n;
      { state with i_pos = state.i_pos + n
                  ; apply_idx = if idx + n = len then None else Some (idx + n)
                  ; state = if idx + n = len then Decoder.P.continue state.state else state.state }
    | None ->
      let n = min (state.i_len - state.i_pos) len in
      Cstruct.blit src off trg state.i_pos n;
      { state with i_pos = state.i_pos + n
                  ; apply_idx = if n = len then None else Some n
                  ; state = if n = len then Decoder.P.continue state.state else state.state })

let fill trg deserializer =
  let o, n = Decoder.P.output deserializer.state in

  match deserializer.apply_idx with
  | Some idx ->
    let n' = min (n - idx) (deserializer.i_len - deserializer.i_pos) in
    Cstruct.blit o idx trg deserializer.i_pos n';
    { deserializer with i_pos = deserializer.i_pos + n'
                      ; apply_idx = if idx + n' = n then None else Some (idx + n')
                      ; state = if idx + n' = n then Decoder.P.flush 0 (Cstruct.len o) deserializer.state else deserializer.state }
  | None ->
    let n' = min n (deserializer.i_len - deserializer.i_pos) in
    Cstruct.blit o 0 trg deserializer.i_pos n';
    { deserializer with i_pos = deserializer.i_pos + n'
                      ; apply_idx = if n' = n then None else Some n'
                      ; state = if n' = n then Decoder.P.flush 0 (Cstruct.len o) deserializer.state else deserializer.state }

let rec filler ?(chunk = 0x8000) source pack s_tmp d_tmp t =
  if t.i_pos = t.i_len
  then Ok (t, source)
  else match Decoder.P.eval t.window.Unpack.Window.raw t.state with
    | `Await state ->
      let rest_in_window = min (t.window.Unpack.Window.len - t.consume) chunk in

      if rest_in_window > 0
      then
        filler
          source
          pack s_tmp d_tmp
          { t with consume = t.consume + rest_in_window
                 ; state = Decoder.P.refill t.consume rest_in_window state }
      else begin
        let window, relative_offset = Decoder.find pack Int64.(add t.window.Unpack.Window.off (of_int t.consume)) in
        filler
          source
          pack s_tmp d_tmp
          { t with window = window
                 ; consume = relative_offset
                 ; state = Decoder.P.refill 0 0 state }
      end
    | `Hunk (state, hunk) ->
      (match source with
       | Some raw ->
         let t' = apply raw s_tmp { t with state = state } hunk in
         filler source pack s_tmp d_tmp t'
       | None ->
        (* XXX(dinosaure): About this code. We continue to control the
                            memory fingerprint and prefer to re-use
                            [old_raw0] and [old_raw1] created in the
                            main function. We can change this code to
                            be more efficient (about memory) and limit
                            these buffer to [max_int32 + 0x10000].
                            Indeed, by the format (see Rabin's
                            fingerprint), we use only [max_int32 +
                            0x10000]. So for a huge git object, we can
                            limit the memory size to this - obviously,
                            if one of a git object is upper than
                            [max_int32 + 0x10000], it's certainly the
                            end of the world.

                            However, in this context, we ensure than
                            the serializer don't use [access] (which
                            one uses [old_raw0] and [old_raw1]). This
                            note appear only because I don't change yet
                            the code.
        *)
         let getter state =
           match Decoder.P.kind state with
           | Decoder.P.Hunk { Decoder.P.HunkDecoder.reference = Decoder.P.HunkDecoder.Offset ofs; _ } ->
             let absolute_offset = Int64.sub (Decoder.P.offset state) ofs in
             Decoder.get' pack absolute_offset z_tmp0 z_win0 d_tmp
           | Decoder.P.Hunk { Decoder.P.HunkDecoder.reference = Decoder.P.HunkDecoder.Hash hash; _ } ->
             Decoder.get pack hash z_tmp0 z_win0 d_tmp
           | _ -> assert false
         in

         (match getter state with
          | Ok base -> filler (Some base.Decoder.Object.raw) pack s_tmp d_tmp { t with state = state }
          | Error exn -> Error exn))
    | `Flush state ->
      let t' = fill s_tmp { t with state = state } in
      filler source pack s_tmp d_tmp t'
    | `Object state ->
      Ok ({ t with final = true
                 ; state = state }, source)
    | `End state -> assert false
    | `Error (state, exn) ->
      Format.eprintf "> %a\n%!" Decoder.P.pp_error exn;
      Error (Decoder.Unpack_error (state, t.window, exn))

let pp_option pp_data fmt = function
  | Some x -> pp_data fmt x
  | None -> Format.fprintf fmt "<none>"

let make_pack ?(chunk = 0x8000) pack_out idx_out pack (raw0, raw1) access entries =
  let rec loop_pack (deserializer, source) t =
    match PACKEncoder.eval s_tmp o_tmp t with
    | `Await t ->
      (match deserializer with
       | Some deserializer0 ->
         Format.eprintf "deserializer0: %a\n%!" pp_deserializer deserializer0;

         (match filler source pack s_tmp (raw0, raw1) deserializer0 with
          | Ok ({ final = false; _ } as deserializer1, source) ->
            Format.eprintf "`Await, `Flush: %a\n%!" pp_deserializer deserializer1;

            let used_in' = deserializer1.u_in + PACKEncoder.used_in t in

            let t, deserializer2 =
              if used_in' = deserializer1.i_pos
              then PACKEncoder.refill 0 0 t,
                   { deserializer1 with i_off = 0
                                      ; i_pos = 0
                                      ; i_len = Cstruct.len s_tmp
                                      ; u_in = 0 }
              else
                PACKEncoder.refill (deserializer1.i_off + used_in') (deserializer1.i_pos - used_in') t,
                { deserializer1 with u_in = used_in' }
            in

            loop_pack (Some deserializer2, source) t
          | Ok ({ final = true; _ } as deserializer1, source) ->
            Format.eprintf "`Await, `End: %a\n%!" pp_deserializer deserializer1;

            let used_in' = deserializer1.u_in + PACKEncoder.used_in t in

            let t, deserializer2 =
              if used_in' = deserializer1.i_pos
              then PACKEncoder.finish t, None
              else
                PACKEncoder.refill (deserializer1.i_off + used_in') (deserializer1.i_pos - used_in') t,
                Some { deserializer1 with u_in = used_in' }
            in

            loop_pack (deserializer2, source) t
          | Error exn -> Error exn)
       | None ->
         match pack.Decoder.idx (PACKEncoder.expect t) with
         | Some (crc, absolute_offset) ->
           let window, relative_offset = Decoder.find pack absolute_offset in
           let deserializer =
             { window
             ; consume = relative_offset
             ; state = Decoder.P.from_window window relative_offset z_tmp z_win
             ; hash  = (PACKEncoder.expect t)
             ; final = false
             ; i_off = 0
             ; i_pos = 0
             ; i_len = Cstruct.len s_tmp
             ; apply_idx = None
             ; u_in  = 0 }
           in
           loop_pack (Some deserializer, None) t
         | None -> Error (Decoder.Invalid_hash (PACKEncoder.expect t)))
    | `Flush t ->
      let n = PACKEncoder.used_out t in
      let deserializer =
        Option.map
          (fun deserializer -> { deserializer with u_in = deserializer.u_in + (PACKEncoder.used_in t) })
          deserializer
      in

      ignore @@ write pack_out (Cstruct.to_bigarray o_tmp) 0 n;
      loop_pack (deserializer, source) (PACKEncoder.flush 0 (Cstruct.len o_tmp) t)
    | `Error (t, exn) ->
      Format.eprintf "PACK encoder: %a\n%!" PACKEncoder.pp_error exn;
      assert false
    | `End (t, hash) ->
      if PACKEncoder.used_out t <> 0
      then ignore @@ write pack_out (Cstruct.to_bigarray o_tmp) 0 (PACKEncoder.used_out t);

      Ok (PACKEncoder.idx t, hash)
  in

  let rec loop_idx t =
    match IDXEncoder.eval o_tmp t with
    | `Flush t ->
      let n = IDXEncoder.used_out t in
      ignore @@ write idx_out (Cstruct.to_bigarray o_tmp) 0 n;
      loop_idx (IDXEncoder.flush 0 (Cstruct.len o_tmp) t)
    | `End t ->
      if IDXEncoder.used_out t <> 0
      then ignore @@ write idx_out (Cstruct.to_bigarray o_tmp) 0 (IDXEncoder.used_out t);

      ()
    | `Error (t, exn) ->
      Format.eprintf "IDX encoder: %a\n%!" IDXEncoder.pp_error exn;
      assert false
  in

  match loop_pack (None, None) (PACKEncoder.default h_tmp access entries) with
  | Ok (tree_idx, hash_pack) ->
    Format.printf "Hash produced: %a\n%!" SHA1.pp hash_pack;

    loop_idx (IDXEncoder.default (Radix.to_sequence tree_idx) hash_pack)
  | Error exn ->
    Format.eprintf "Error happens: %a\n%!" Decoder.pp_error exn

let idx_filename  = "pack.idx"
let pack_filename = "pack.pack"

let pp_pair pp_one pp_two fmt (a, b) =
  Format.fprintf fmt "@[<hov>(@[<hov>%a@],@ @[<hov>%a@])@]"
    pp_one a pp_two b

let pp_list ?(sep = (fun fmt () -> ()) )pp_data fmt lst =
  let rec aux = function
    | [] -> ()
    | [ x ] -> pp_data fmt x
    | x :: r -> Format.fprintf fmt "%a%a" pp_data x sep (); aux r
  in aux lst

let cstruct_copy x =
  let l = Cstruct.len x in
  let r = Cstruct.create l in
  Cstruct.blit x 0 r 0 l;
  r

let () =
  Format.eprintf "Start program.\n%!";

  let old_idx = Result.unsafe_ok @@ IDXLazy.make (cstruct_map Sys.argv.(2)) in

  Format.eprintf "IDX file loaded.\n%!";

  let old_pack = Decoder.make (Unix.openfile Sys.argv.(1) [ Unix.O_RDONLY ] 0o644)
      (fun hash -> None)
      (fun hash -> IDXLazy.find old_idx hash)
      (fun off  -> None)
      (fun hash -> None)
  in

  Format.eprintf "PACK file loaded.\n%!";

  let max_length = IDXLazy.fold old_idx (fun hash _ acc ->
      match Decoder.needed old_pack hash z_tmp z_win with
      | Ok length ->
        Format.eprintf "get length for %a: %d\n%!" SHA1.pp hash (max length acc);
        max length acc
      | Error exn ->
        Format.eprintf "Invalid PACK: %a\n%!" Decoder.pp_error exn;
        assert false) 0
  in

  Format.printf "Max length calculated: %d\n%!" max_length;

  let old_raw0, old_raw1 = Cstruct.create max_length, Cstruct.create max_length in
  let tmp_raw0, tmp_raw1 = Cstruct.create max_length, Cstruct.create max_length in

  let each_git_object hash (crc, offset) acc =
    match Decoder.get old_pack hash z_tmp z_win (old_raw0, old_raw1) with
    | Ok ({ Decoder.Object.kind = `Tree; _ } as base) ->
      if SHA1.neq (hash_of_object base) hash
      then assert false;
      if Crc32.neq crc (Decoder.Object.first_crc_exn base)
      then assert false;

      Format.eprintf "unpack object: %a\n%!" SHA1.pp hash;
      save_names_of_tree hash base.Decoder.Object.raw;
      object_to_entry hash base :: acc
    | Ok base ->
      if SHA1.neq (hash_of_object base) hash
      then assert false;
      if Crc32.neq crc  (Decoder.Object.first_crc_exn base)
      then assert false;

      Format.eprintf "unpack object: %a\n%!" SHA1.pp hash;
      object_to_entry hash base :: acc
    | Error exn ->
      (Format.eprintf "Invalid PACK: %a\n%!" Decoder.pp_error exn;
       assert false)
  in

  let entries = IDXLazy.fold old_idx each_git_object [] in
  let entries =
    List.map (fun x -> try let name = Hashtbl.find p_nam x.Delta.Entry.hash_object in
                 { x with Delta.Entry.hash_name = Delta.Entry.hash name
                        ; name = Some name }
               with Not_found -> x)
      entries
  in

  match Delta.deltas entries
      (fun hash -> match Decoder.get_with_allocation old_pack hash z_tmp z_win with
         | Ok base -> Some base.Decoder.Object.raw
         | Error exn ->
           Format.eprintf "Silent error with %a: %a\n%!" SHA1.pp hash Decoder.pp_error exn;
           None)
      (fun hash -> false) (* we tag all object to [false]. *)
      10 50 with
  | Ok entries ->
    Format.eprintf "Start to write the new PACK file.\n%!";

    let pack_out = Unix.openfile pack_filename [ Unix.O_WRONLY; Unix.O_TRUNC ] 0o644 in
    let idx_out  = Unix.openfile idx_filename [ Unix.O_WRONLY; Unix.O_TRUNC ] 0o644 in

    let access hash = match Decoder.get old_pack hash z_tmp z_win (old_raw0, old_raw1) with
      | Ok base -> Some base.Decoder.Object.raw
      | Error exn ->
        Format.eprintf "Silent error with %a: %a\n%!" SHA1.pp hash Decoder.pp_error exn;
        None
    in

    make_pack pack_out idx_out old_pack (tmp_raw0, tmp_raw1) access entries;
    Unix.close pack_out;
    Unix.close idx_out;

    let new_idx = Result.unsafe_ok @@ IDXLazy.make (cstruct_map idx_filename) in

    let new_pack = Decoder.make (Unix.openfile pack_filename [ Unix.O_RDONLY ] 0o644)
        (fun hash -> None)
        (fun hash -> IDXLazy.find new_idx hash)
        (fun off  -> None)
        (fun hash -> None)
    in

    let max_length = IDXLazy.fold new_idx (fun hash _ acc ->
        match Decoder.needed new_pack hash z_tmp z_win with
        | Ok length ->
          Format.eprintf "get length for %a: %d\n%!" SHA1.pp hash (max length acc);
          max length acc
        | Error exn ->
          Format.eprintf "Invalid PACK: %a\n%!" Decoder.pp_error exn;
        assert false) 0
    in

    Format.printf "Max length calculated: %d\n%!" max_length;

    let new_raw0, new_raw1 = Cstruct.create max_length, Cstruct.create max_length in

    let each_git_object hash (crc, offset) =
      match Decoder.get new_pack hash z_tmp z_win (new_raw0, new_raw1),
            Decoder.get old_pack hash z_tmp z_win (old_raw0, old_raw1) with
      | Ok new_base, Ok old_base ->
        if SHA1.neq (hash_of_object new_base) hash
        then assert false;
        if Crc32.neq crc (Decoder.Object.first_crc_exn new_base)
        then Format.eprintf "CRC-32 is %a but expected %a.\n%!" Crc32.pp (Decoder.Object.first_crc_exn new_base) Crc32.pp crc;

        Format.eprintf "unpack object: %a\n%!" SHA1.pp hash;
      | Error exn, _ | _, Error exn ->
        Format.eprintf "Invalid PACK: %a\n%!" Decoder.pp_error exn;
        assert false
    in

    IDXLazy.iter new_idx each_git_object

  | Error exn -> Format.eprintf "Delta error: %a\n%!" Delta.pp_error exn
