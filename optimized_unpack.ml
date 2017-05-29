let () = Printexc.record_backtrace true

let pp_cstruct fmt cs =
  Format.fprintf fmt "\"";
  for i = 0 to Cstruct.len cs - 1
  do if Cstruct.get_uint8 cs i > 32 && Cstruct.get_uint8 cs i < 127
    then Format.fprintf fmt "%c" (Cstruct.get_char cs i)
    else Format.fprintf fmt "."
  done;
  Format.fprintf fmt "\""

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

  let pp_error fmt Error = Format.fprintf fmt "(Inflate_error #zlib)" (* lazy *)
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

module Commit      = Minigit.Commit(SHA1)
module Tree        = Minigit.Tree(SHA1)
module Tag         = Minigit.Tag(SHA1)
module Blob        = Minigit.Blob

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
let s_tmp = Cstruct.create 0x8000

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
  ; o_pos           : int option
  ; used_in         : int }

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
                               o_pos = @[<hov>%a@];@ \
                               used_in = %d;@] }"
    Unpack.Window.pp t.window
    t.consume Decoder.P.pp t.state SHA1.pp t.hash t.final t.i_off t.i_pos t.i_len (pp_option Format.pp_print_int) t.o_pos t.used_in

let apply src trg t = function
  | Decoder.P.HunkDecoder.Insert i ->
    (match t.o_pos with
     | Some o_pos ->
       let n = min (t.i_len - t.i_pos) (Cstruct.len i - o_pos) in
       Cstruct.blit i o_pos trg t.i_pos n;
       { t with i_pos = t.i_pos + n
              ; o_pos = if o_pos + n = Cstruct.len i then None else Some (o_pos + n)
              ; state = if o_pos + n = Cstruct.len i then Decoder.P.continue t.state else t.state }
     | None ->
       let n = min (t.i_len - t.i_pos) (Cstruct.len i) in
       Cstruct.blit i 0 trg t.i_pos n;
       { t with i_pos = t.i_pos + n
              ; o_pos = if n = Cstruct.len i then None else Some n
              ; state = if n = Cstruct.len i then Decoder.P.continue t.state else t.state })
  | Decoder.P.HunkDecoder.Copy (off, len) ->
    (match t.o_pos with
     | Some o_pos ->
       let n = min (t.i_len - t.i_pos) (len - o_pos) in
       Cstruct.blit src (off + o_pos) trg t.i_pos n;
       { t with i_pos = t.i_pos + n
              ; o_pos = if o_pos + n = len then None else Some (o_pos + n)
              ; state = if o_pos + n = len then Decoder.P.continue t.state else t.state }
     | None ->
       let n = min (t.i_len - t.i_pos) len in
       Cstruct.blit src off trg t.i_pos n;
       { t with i_pos = t.i_pos + n
              ; o_pos = if n = len then None else Some n
              ; state = if n = len then Decoder.P.continue t.state else t.state })

let fill trg t =
  let o, n = Decoder.P.output t.state in

  match t.o_pos with
  | Some o_pos ->
    let n' = min (n - o_pos) (t.i_len - t.i_pos) in
    Cstruct.blit o o_pos trg t.i_pos n';
    { t with i_pos = t.i_pos + n'
           ; o_pos = if o_pos + n' = n then None else Some (o_pos + n')
           ; state = if o_pos + n' = n then Decoder.P.flush 0 (Cstruct.len o) t.state else t.state }
  | None ->
    let n' = min n (t.i_len - t.i_pos) in
    Cstruct.blit o 0 trg t.i_pos n';
    { t with i_pos = t.i_pos + n'
           ; o_pos = if n' = n then None else Some n'
           ; state = if n' = n then Decoder.P.flush 0 (Cstruct.len o) t.state else t.state }

(* XXX(dinosaure): This function fills [s_tmp] by what we expect:
                   - [source] is a [Cstruct.t option] - may be is more relevant to declare this inside [t].
                     It used to store the source needed to undelta-ified the git object expected
                   - [pack] is a *stop-the-world* state used to load the [source] git object
                   - [r_tmp] is two [Cstruct.t] used by [pack] to load [source]

                   As you understand, it's not mandatory to use [source], [pack]
                   and [r_tmp] but it can be happen when the git object
                   requested is delta-ified. So, semantically, [r_tmp] must not
                   be used by another computation (it's needed to be free).

                   Then, [filler] starts with [source = None] but for each call
                   of [filler], you need to keep [source] (and it's why [source]
                   is returned).

                   Finally, [s_tmp] contains a part of the git object requested.

                   When [t.final = false], that means the end of the git object.
 *)
let rec filler ?(chunk = 0x8000) source pack s_tmp r_tmp t =
  if t.i_pos = t.i_len
  then Ok (t, source)
  else match Decoder.P.eval t.window.Unpack.Window.raw t.state with
    | `Await state ->
      let rest_in_window = min (t.window.Unpack.Window.len - t.consume) chunk in

      if rest_in_window > 0
      then
        filler
          source
          pack s_tmp r_tmp
          { t with consume = t.consume + rest_in_window
                 ; state = Decoder.P.refill t.consume rest_in_window state }
      else begin
        let window, relative_offset = Decoder.find pack Int64.(add t.window.Unpack.Window.off (of_int t.consume)) in
        filler
          source
          pack s_tmp r_tmp
          { t with window = window
                 ; consume = relative_offset
                 ; state = Decoder.P.refill 0 0 state }
      end
    | `Hunk (state, hunk) ->
      (match source with
       | Some raw ->
         let t' = apply raw s_tmp { t with state = state } hunk in
         filler source pack s_tmp r_tmp t'
       | None ->
         (* XXX(dinosaure): it's only in this case when [pack], [source] and [r_tmp] is relevant.
                            note that [source] is one of buffers [r_tmp] physically.
          *)
         let getter state =
           match Decoder.P.kind state with
           | Decoder.P.Hunk { Decoder.P.HunkDecoder.reference = Decoder.P.HunkDecoder.Offset ofs; _ } ->
             let absolute_offset = Int64.sub (Decoder.P.offset state) ofs in
             Decoder.get' pack absolute_offset z_tmp0 z_win0 r_tmp
           | Decoder.P.Hunk { Decoder.P.HunkDecoder.reference = Decoder.P.HunkDecoder.Hash hash; _ } ->
             Decoder.get pack hash z_tmp0 z_win0 r_tmp
           | _ -> assert false
           (* XXX(dinosaure): [Decoder.P.eval] returns [`Hunk] only when the
                              [kind] of the requested object is a [Decoder.P.Hunk].
                              We can use GADT!!!!
            *)
         in

         (match getter state with
          | Ok base -> filler (Some base.Decoder.Object.raw) pack s_tmp r_tmp { t with state = state }
          | Error exn -> Error exn))
    | `Flush state ->
      let t' = fill s_tmp { t with state = state } in
      filler source pack s_tmp r_tmp t'
    | `Object state ->
      Ok ({ t with final = true
                 ; state = state }, source)
    | `End state -> assert false
    | `Error (state, exn) ->
      Error (Decoder.Unpack_error (state, t.window, exn))

let pp_option pp_data fmt = function
  | Some x -> pp_data fmt x
  | None -> Format.fprintf fmt "<none>"

let make_pack ?(chunk = 0x8000) pack_out idx_out pack (raw0, raw1) access entries =
  let rec loop_pack (deserializer, source, ctx) t =
    Format.eprintf "deserialization: %a\n%!" (pp_option pp_deserializer) deserializer;

    match PACKEncoder.eval s_tmp o_tmp t with
    | `Await t ->
      (* XXX(dinosaure): we need to explain this big part and why we want to
                         serialize the PACK file in this way. As you know, [git]
                         can handle a big file and it's possible to try to
                         serialize a huge git object (like a huge [blob]). In
                         this case, we have 2 ways to handle this:

                         - the naive way: it consists to load by [Decoder.get*]
                           the object we want to serialize. Then, we just need
                           to iter inside the git object and send it to
                           [zlib]/[decompress] to serialize. This way is very
                           easy because we don't need to deal so much with the
                           non-blocking state of [zlib]/[decompress] and just to
                           what we want in one run.

                           However, if we try to serialize a big git object
                           (like 1 Go needed in the memory), may be we will
                           catch the [Out_of_memory] easily too. So, in a scale
                           and industrial context, we need to change this way.a

                         - the second way (the current) is to deserialize the
                           git object requested in a fixed size buffer and
                           serialize it at the same time. So, clearly, it's not
                           easy because we need to deal with a first state to
                           deserialize what we want, then, deal with the
                           [PACKEncoder] state and internally the
                           [zlib]/[decompress] state. Finally, [PACKEncoder]
                           wants to serialize the git object in the [Raw] way,
                           or the [Hunk]/delta-ified way.

                         So, this part just send to the [PACKEncoder] state a
                         fixed size buffer of the git object requested/[expect]
                         continuously. Inside, we use directly
                         [zlib]/[decompress] to deflate the git object to the
                         PACK file or we destruct the input to some
                         [Hunk.Insert] and [Hunk.Copy] and serialize/deflate
                         them to the PACK file.

                         The big problem is to find a common semantic of the
                         computation between [decompress], [zlib] and the
                         [HunkEncoder] and hidden what is going on inside the
                         [PACKEncoder].

                         So, the [filler] function is a computation to fill
                         completely and continuously [s_tmp] and send it to the
                         [PACKEncoder] state. Obviously, we need a
                         [deserializer] state and keep the [source] used by the
                         [filler].

                         NOTE: to avoid clash of name and understand what happen
                         internally, it's better to rename, for each change, the
                         variable [t0] to [t1].
      *)
      (match deserializer with
       | Some deserializer0 ->
         (match filler source pack s_tmp (raw0, raw1) deserializer0 with
          | Ok ({ final = false; _ } as deserializer1, source) ->
            let used_in' = deserializer1.used_in + PACKEncoder.used_in t in

            let t, deserializer2 =
              if used_in' = deserializer1.i_pos
              then begin
                Format.eprintf "refill\n%!";

                PACKEncoder.refill 0 0 t,
                   { deserializer1 with i_off = 0
                                      ; i_pos = 0
                                      ; i_len = Cstruct.len s_tmp
                                      ; used_in = 0 }
              end else begin
                (* XXX(dinosaure): we ensure than what we send to the [PACKEncoder] is continuoulsy the git object expected.
                                   for each refill, we feed [ctx] and at the end of [filler], if I'm a killer OCaml developer,
                                   the SHA1 produced will be the same as expected.
                 *)
                SHA1.feed ctx (Cstruct.to_bigarray @@ Cstruct.sub s_tmp (deserializer1.i_off + used_in') (deserializer1.i_pos - used_in'));

                PACKEncoder.refill (deserializer1.i_off + used_in') (deserializer1.i_pos - used_in') t,
                { deserializer1 with used_in = used_in' }
              end
            in

            loop_pack (Some deserializer2, source, ctx) t
          | Ok ({ final = true; _ } as deserializer1, source) ->
            let used_in' = deserializer1.used_in + PACKEncoder.used_in t in

            let t, deserializer2, source0, ctx0 =
              if used_in' = deserializer1.i_pos
              then begin
                let hash_produced = SHA1.get ctx in

                if SHA1.neq hash_produced deserializer1.hash
                then begin Format.eprintf "expected:%a <> has:%a" SHA1.pp deserializer1.hash SHA1.pp hash_produced; assert false end;

                PACKEncoder.finish t, None, None, SHA1.init ()
              end else begin
                SHA1.feed ctx (Cstruct.to_bigarray @@ Cstruct.sub s_tmp (deserializer1.i_off + used_in') (deserializer1.i_pos - used_in'));

                PACKEncoder.refill (deserializer1.i_off + used_in') (deserializer1.i_pos - used_in') t,
                Some { deserializer1 with used_in = used_in' },
                source,
                ctx
              end
            in

            loop_pack (deserializer2, source0, ctx0) t
          | Error exn -> Error exn)
       | None ->
         let hdr = PACKEncoder.header_of_expected t in
         SHA1.feed ctx (Cstruct.to_bigarray @@ Cstruct.of_string hdr);

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
             ; i_len = 0
             ; o_pos = None
             ; used_in  = 0 }
           in
           loop_pack (Some deserializer, None, ctx) t
         | None -> Error (Decoder.Invalid_hash (PACKEncoder.expect t)))
    | `Flush t ->
      let n = PACKEncoder.used_out t in
      (* let deserializer =
        Option.map
          (fun deserializer -> { deserializer with used_in = deserializer.used_in + (PACKEncoder.used_in t) })
          deserializer
         in *)

      ignore @@ write pack_out (Cstruct.to_bigarray o_tmp) 0 n;
      loop_pack (deserializer, source, ctx) (PACKEncoder.flush 0 (Cstruct.len o_tmp) t)
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

  match loop_pack (None, None, SHA1.init ()) (PACKEncoder.default h_tmp access entries) with
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
        then begin Format.eprintf "SHA1 does not correspond for: %a\n%!" SHA1.pp hash; assert false end;
        if Crc32.neq crc (Decoder.Object.first_crc_exn new_base)
        then Format.eprintf "CRC-32 is %a but expected %a.\n%!" Crc32.pp (Decoder.Object.first_crc_exn new_base) Crc32.pp crc;

        Format.eprintf "unpack object: %a\n%!" SHA1.pp hash;
      | Error exn, _ | _, Error exn ->
        Format.eprintf "Invalid PACK: %a\n%!" Decoder.pp_error exn;
        assert false
    in

    IDXLazy.iter new_idx each_git_object

  | Error exn -> Format.eprintf "Delta error: %a\n%!" Delta.pp_error exn
