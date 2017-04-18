module BBuffer = Unpack.BBuffer

let pp_list ?(sep = (fun fmt () -> ()) )pp_data fmt lst =
  let rec aux = function
    | [] -> ()
    | [ x ] -> pp_data fmt x
    | x :: r -> Format.fprintf fmt "%a%a" pp_data x sep (); aux r
  in aux lst

let cstruct_pp fmt cs =
  Format.fprintf fmt "\"";
  for i = 0 to Cstruct.len cs - 1
  do if Cstruct.get_uint8 cs i > 32 && Cstruct.get_uint8 cs i < 127
    then Format.fprintf fmt "%c" (Cstruct.get_char cs i)
    else Format.fprintf fmt "."
  done;
  Format.fprintf fmt "\""

module Hunk =
struct
  type t =
    | Insert of Cstruct.t
    | Copy of int * int

  let pp fmt = function
    | Insert cs -> Format.fprintf fmt "(Insert [%a])" cstruct_pp cs
    | Copy (off, len) -> Format.fprintf fmt "(Copy (%d, %d))" off len

  let count_copy =
    List.fold_left (fun acc -> function Copy (_, len) -> acc + len | _ -> acc) 0

  (* XXX(dinosaure): only used to check. *)
  let produce base result hunks =
    let len = List.fold_left (fun acc -> function Insert cs -> Cstruct.len cs + acc | Copy (_, len) -> len + acc) 0 hunks in
    let res = Cstruct.create len in

    let w = List.fold_left
        (fun w -> function Insert cs -> Cstruct.blit cs 0 res w (Cstruct.len cs); w + (Cstruct.len cs)
                         | Copy (off, len) -> Cstruct.blit base off res w len; w + len)
        0 hunks
    in

    if Cstruct.compare res result <> 0
    then Format.eprintf "production is wrong: @[<hov>%a@]\n%!"
        (pp_list ~sep:(fun fmt () -> Format.fprintf fmt ";@ ") pp) hunks;

    assert (w = len);
    assert (Cstruct.compare res result = 0)

  let count a =
    List.fold_left (+) 0
    @@ List.map Cstruct.len
    @@ List.map snd
    @@ Array.to_list a

  (* XXX(dinosaure): need to be optimized (TODO). *)
  let split_to_127 acc cs =
    let rec aux acc current cs =
      if Cstruct.len (Array.get cs current) > 0x7F
      then
        let one = Cstruct.sub (Array.get cs current) 0 0x7F in

        Array.set cs current (Cstruct.sub (Array.get cs current) 0x7F (Cstruct.len (Array.get cs current) - 0x7F));

        aux (one :: acc) current cs
      (* terminate the recursion or continue with the rest. *)
      else if Cstruct.len (Array.get cs current) > 0
      then
        if current = Array.length cs - 1
        then (Array.get cs current :: acc)
        else aux (Array.get cs current :: acc) (current + 1) cs
      else
        (if current = Array.length cs - 1
         then acc
         else aux acc (current + 1) cs)
    in
    if Array.length cs = 0
    then acc
    else aux acc 0 cs

  let of_patience_diff hunk =
    List.fold_left (fun (off, acc) -> function
        | Pdiff.Range.New a ->
          let lst = List.map (fun x -> Insert x) (split_to_127 [] a) in
          (off, lst @ acc)
        | Pdiff.Range.Same a ->
          let len = count a in
          let res = Copy (off, len) in
          (off + len, res :: acc )
        | Pdiff.Range.Replace (o, n) ->
          let add = List.fold_left (+) 0 (List.map Cstruct.len (Array.to_list o)) in
          let res = List.map (fun x -> Insert x) (split_to_127 [] n) in
          (off + add, res @ acc)
        | Pdiff.Range.Old o ->
          let len = List.fold_left (+) 0 (List.map Cstruct.len (Array.to_list o)) in
          (off + len, acc))
      (0, [])
      hunk.Pdiff.Hunk.ranges
    |> fun (off, lst) -> List.rev lst
end

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
    ; name        : string option
    ; kind        : Kind.t
    ; length      : int64 }

  let pp = Format.fprintf

  let pp_option pp_data fmt = function
    | Some x -> pp_data fmt x
    | None -> pp fmt "<none>"

  let pp fmt { hash_name; hash_object; name; kind; length; } =
    pp fmt "{ @[<hov>name = %d and %a;@ \
                     hash = %a;@ \
                     kind = %a;@ \
                     length = %Ld;@] }"
      hash_name (pp_option Format.pp_print_string) name Hash.pp hash_object Kind.pp kind length

  (* XXX(dinosaure): hash from git to sort git objects.
                     in git, this hash is computed in an [int32],
                     so we have a problem for the 32-bits architecture.

                     TODO!
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

  let name entry = entry.name

  let make hash_object ?name obj =
    let hash_name = Option.(value ~default:0 (name >>| hash)) in

    match obj with
    | Tag tag ->
      { hash_name
      ; hash_object
      ; name
      ; kind = Kind.Tag
      ; length = Minigit.Tag.raw_length tag }
    | Commit commit ->
      { hash_name
      ; hash_object
      ; name
      ; kind = Kind.Commit
      ; length = Minigit.Commit.raw_length commit }
    | Tree tree ->
      { hash_name
      ; hash_object
      ; name
      ; kind = Kind.Tree
      ; length = Minigit.Tree.raw_length tree }
    | Blob blob ->
      { hash_name
      ; hash_object
      ; name
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
  let ( || ) = Int64.logor
  let ( - )  = Int64.sub
  let ( >> ) = Int64.shift_right
  let ( << ) = Int64.shift_left (* >> (tuareg) *)
end

module H =
struct
  type error = ..

  let pp_error fmt exn = () (* no error *)

  type t =
    { o_off         : int
    ; o_pos         : int
    ; o_len         : int
    ; write         : int (* XXX(dinosaure): difficult to write a Hunk bigger than [max_int].
                                             consider that it's safe to use [int] instead [int64]. *)
    ; reference     : reference
    ; source_length : int
    ; target_length : int
    ; hunks         : Hunk.t array
    ; state         : state }
  and reference =
    | Offset of int64
    | Hash of string
  and k = Cstruct.t -> t -> res
  and state =
    | Header of k
    | List of int
    | Hunk of k
    | Insert of k
    | Copy of k
    | End
    | Exception of error
  and res =
    | Error of t * error
    | Cont  of t
    | Flush of t
    | Ok    of t

  let pp = Format.fprintf

  let pp_state fmt = function
    | Header k -> pp fmt "(Header #k)"
    | List idx -> pp fmt "(List %d)" idx
    | Hunk k -> pp fmt "(Hunk #k)"
    | Insert k -> pp fmt "(Insert #k)"
    | Copy k -> pp fmt "(Copy #k)"
    | End -> pp fmt "End"
    | Exception exn -> pp fmt "(Error %a)" pp_error exn

  let pp_reference fmt = function
    | Offset off -> pp fmt "(Offset %Ld)" off
    | Hash hash -> pp fmt "(Hash %a)" Hash.pp hash

  let pp fmt t =
    pp fmt "{ @[<hov>o_off = %d;@ \
                     o_pos = %d;@ \
                     o_len = %d;@ \
                     write = %d;@ \
                     reference = %a;@ \
                     source_length = %d;@ \
                     target_length = %d;@ \
                     hunks = @[<hov>%a@];@ \
                     state = @[<hov>%a@]@] }"
      t.o_off t.o_pos t.o_len
      t.write pp_reference t.reference t.source_length t.target_length
      (pp_list ~sep:(fun fmt () -> pp fmt ";@ ") Hunk.pp) (Array.to_list t.hunks)
      pp_state t.state

  let ok t = Ok { t with state = End }
  let flush t = Flush t
  let error t exn = Error ({ t with state = Exception exn }, exn)

  module KHeader =
  struct
    let rec put_byte byte k dst t =
      if (t.o_len - t.o_pos) > 0
      then begin
        Cstruct.set_uint8 dst (t.o_off + t.o_pos) byte;

        k dst { t with o_pos = t.o_pos + 1
                     ; write = t.write + 1 }
      end else Flush { t with state = Header (put_byte byte k) }

    let rec length n k dst t =
      let byte = (n land 0x7F) in
      let rest = (n lsr 7) in

      if rest <> 0
      then put_byte (byte lor 0x80) (length rest k) dst t
      else put_byte byte k dst t
  end

  module KHunk =
  struct
    let rec put_byte byte k dst t =
      if (t.o_len - t.o_pos) > 0
      then begin
        Cstruct.set_uint8 dst (t.o_off + t.o_pos) byte;

        k dst { t with o_pos = t.o_pos + 1
                     ; write = t.write + 1 }
      end else Flush { t with state = Hunk (put_byte byte k) }
  end

  module KInsert =
  struct
    let rec put_raw raw k dst t =
      if (t.o_len - t.o_pos) > Cstruct.len raw
      then begin
        Cstruct.blit raw 0 dst (t.o_off + t.o_pos) (Cstruct.len raw);

        k dst { t with o_pos = t.o_pos + Cstruct.len raw
                     ; write = t.write + Cstruct.len raw }
      end else if (t.o_len - t.o_pos) > 0
      then
        let rec loop rest dst t =
          let n = min (t.o_len - t.o_pos) rest in
          Cstruct.blit raw (Cstruct.len raw - rest) dst (t.o_off + t.o_pos) n;

          if rest - n = 0
          then k dst { t with o_pos = t.o_pos + n
                            ; write = t.write + n }
          else Flush { t with o_pos = t.o_pos + n
                            ; write = t.write + n
                            ; state = Insert (loop (rest - n)) }
        in loop (Cstruct.len raw) dst t
      else Flush { t with state = Insert (put_raw raw k) }
  end

  module KCopy =
  struct
    let rec put_byte ?(force = false) byte k dst t =
      if byte <> 0 || force
      then
        if (t.o_len - t.o_pos) > 0
        then begin
          Cstruct.set_uint8 dst (t.o_off + t.o_pos) byte;
          k dst { t with o_pos = t.o_pos + 1
                       ; write = t.write + 1 }
        end else Flush { t with state = Copy (put_byte byte k) }
      else k dst t
  end

  let size_of_variable_length vl =
    let rec loop acc = function
      | 0 -> acc
      | n -> loop (acc + 1) (n lsr 7)
    in
    loop 1 (vl lsr 7)

  let how_many_bytes n =
    let rec aux acc n =
      if n = 0 then acc else aux (acc + 1) (n lsr 8)
    in if n = 0 then 1 else aux 0 n

  let insert idx raw dst t =
    KInsert.put_raw raw (fun dst t -> Cont { t with state = List (idx + 1) }) dst t

  let copy idx off len dst t =
    let o0 = off land 0xFF in
    let o1 = (off land 0xFF00) lsr 8 in
    let o2 = (off land 0xFF0000) lsr 16 in
    let o3 = (off land 0xFF000000) lsr 24 in

    let l0 = len land 0xFF in
    let l1 = (len land 0xFF00) lsr 8 in
    let l2 = (len land 0xFF0000) lsr 16 in

    (KCopy.put_byte ~force:true o0
     @@ KCopy.put_byte o1
     @@ KCopy.put_byte o2
     @@ KCopy.put_byte o3
     @@ KCopy.put_byte ~force:true l0
     @@ KCopy.put_byte l1
     @@ KCopy.put_byte l2
     @@ fun dst t -> Cont { t with state = List (idx + 1) })
    dst t

  let list idx dst t =
    if Array.length t.hunks <> idx
    then let hunk = Array.get t.hunks idx in
      match hunk with
      | Hunk.Insert raw ->
        assert (Cstruct.len raw > 0);

        let byte = Cstruct.len raw land 0x7F in (* TODO: be sure the length of the raw is lower than 0x7F. *)

        KHunk.put_byte byte (insert idx raw) dst t
      | Hunk.Copy (off, len) ->
        let n_offset = how_many_bytes off in
        let n_length = if len = 0x10000 then 1 else how_many_bytes len in

        let o = match n_offset with
          | 1 -> 0b0001
          | 2 -> 0b0011
          | 3 -> 0b0111
          | 4 -> 0b1111
          | _ ->
            assert false
        in

        let l = match n_length with
          | 1 -> 0b001
          | 2 -> 0b011
          | 3 -> 0b111
          | _ -> assert false
        in

        KHunk.put_byte (0x80 lor (l lsl 4) lor o) (copy idx off (if len = 0x10000 then 0 else len)) dst t
      else ok t

  let header dst t =
    (KHeader.length t.source_length
     @@ KHeader.length t.target_length
     @@ fun dst t -> Cont { t with state = List 0 })
    dst t

  let eval dst t =
    let eval0 t = match t.state with
      | Header k -> k dst t
      | List idx -> list idx dst t
      | Hunk k -> k dst t
      | Insert k -> k dst t
      | Copy k -> k dst t
      | End -> ok t
      | Exception exn -> error t exn
    in

    let rec loop t =
      match eval0 t with
      | Cont t -> loop t
      | Flush t -> `Flush t
      | Error (t, exn) -> `Error (t, exn)
      | Ok t -> `End t
    in

    loop t

  let default reference source_length target_length hunks =
    { o_off = 0
    ; o_pos = 0
    ; o_len = 0
    ; write = 0
    ; reference
    ; source_length
    ; target_length
    ; hunks = Array.of_list hunks
    ; state = Header header }

  let length t =
    size_of_variable_length t.source_length
    + size_of_variable_length t.target_length
    + List.fold_left
      (fun acc -> function
         | Hunk.Insert raw -> 1 + Cstruct.len raw + acc
         | Hunk.Copy (off, len) -> 1 + (how_many_bytes off) + (how_many_bytes len) + acc)
      0 (Array.to_list t.hunks)

  let flush off len t =
    { t with o_off = off
           ; o_len = len
           ; o_pos = 0 }

  let used_out t = t.o_pos
end

module P =
struct
  type error = ..
  type error += Deflate_error of Decompress.Deflate.error
  type error += Hunk_error of H.error

  let pp_error fmt = function
    | Deflate_error exn -> Format.fprintf fmt "(Deflate_error %a)" Decompress.Deflate.pp_error exn

  type base =
    { idx    : int
    ; offset : int64
    ; entry  : Entry.t
    ; raw    : Cstruct.t }

  let pp_base fmt { idx; offset; entry; raw; } =
    Format.fprintf fmt "{ @[<hov>idx = %d;@ \
                                 offset = %08Lx;@ \
                                 entry = @[<hov>%a@];@ \
                                 raw = #raw;@] }"
      idx offset Entry.pp entry

  type t =
    { o_off   : int
    ; o_pos   : int
    ; o_len   : int
    ; write   : int64
    ; radix   : (Crc32.t * int64) Radix.t
    ; objects : Entry.t array
    ; hash    : Hash.ctx
    ; h_tmp   : Cstruct.t
    ; buffer  : BBuffer.t
    ; window  : base Window.t (* offset, entry, raw *)
    ; state   : state }
  and k = Cstruct.t -> t -> res
  and state =
    | Header of k
    | Object of k
    | Raw    of { idx : int
                ; off : int64 }
    | Buffer of { idx : int
                ; raw : Cstruct.t
                ; off : int64 }
    | Finish of { idx : int
                ; off : int64 }
    | WriteHeader of k
    | WriteZip of { current : base
                  ; crc : Crc32.t
                  ; last : bool
                  ; z   : (Decompress.B.bs, Decompress.B.bs) Decompress.Deflate.t }
    | WriteHunk of { current : base
                   ; crc : Crc32.t
                   ; h : H.t
                   ; z : (Decompress.B.bs, Decompress.B.bs) Decompress.Deflate.t }
    | Save   of { current : base; crc : Crc32.t; }
    | Hash   of k
    | End
    | Exception of error
  and res =
    | Wait of t
    | Flush of t
    | Error of t * error
    | Cont of t
    | Ok of t
  and kind =
    | KOffset of base * base * Hunk.t list
    | KHash of string * Hunk.t list
    | KRaw of base (* as current *)

  let pp = Format.fprintf

  let pp_state fmt = function
    | Header _ -> pp fmt "(Header #k)"
    | Object _ -> pp fmt "(Object #k)"
    | Raw { idx; off; } ->
      pp fmt "(Raw { @[<hov>idx = %d;@ \
                            off = %Ld;@] })"
        idx off
    | Buffer { idx; off; raw; } ->
      pp fmt "(Buffer { @[<hov>idx = %d;@ \
                               raw = #raw;@ \
                               off = %Ld;@] })"
        idx off
    | Finish { idx; off; } ->
      pp fmt "(Finish { @[<hov>idx = %d;@ \
                               off = %Ld;@] })"
        idx off
    | WriteHeader kind -> pp fmt "(WriteHeader #k)"
    | WriteZip { current; crc; last; z; } ->
      pp fmt "(WriteZip { @[<hov>current = @[<hov>%a@];@ \
                                 last = %b;@ \
                                 z = %a;@] })"
        pp_base current last Decompress.Deflate.pp z
    | WriteHunk { current; crc; h; z; } ->
      pp fmt "(WriteHunk { @[<hov>current = @[<hov>%a@];@ \
                                  crc = %a;@ \
                                  h = %a;@ \
                                  z = %a;@] })"
        pp_base current Crc32.pp crc H.pp h Decompress.Deflate.pp z
    | Save { current; crc; } ->
      pp fmt "(Save { @[<hov>current = %a;@ \
                             crc = %a;@] })"
        pp_base current Crc32.pp crc
    | Hash _ -> pp fmt "(Hash #k)"
    | End -> pp fmt "End"
    | Exception exn -> pp fmt "(Exception %a)" pp_error exn

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
                              (* XXX(dinosaure): use [cstruct] instead [string] (avoid allocation). TODO! *)
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

  module KWriteHeader =
  struct
    let rec put_byte byte k dst t =
      if (t.o_len - t.o_pos) > 0
      then begin
        Cstruct.set_uint8 dst (t.o_off + t.o_pos) byte;
        k dst { t with o_pos = t.o_pos + 1
                     ; write = Int64.add t.write 1L }
      end else flush dst { t with state = WriteHeader (put_byte byte k) }

    let rec length n crc k dst t =
      let byte = Int64.(to_int (n && 0x7FL)) in
      let rest = Int64.(n >> 7) in

      if rest <> 0L
      then put_byte (byte lor 0x80) (length rest (Crc32.digestc crc (byte lor 0x80)) k) dst t
      else put_byte byte (k (Crc32.digestc crc byte)) dst t

    (* XXX(dinosaure): NOT THREAD SAFE! The [10] limitation is come from [git]. *)

    let tmp_offset = Bytes.create 10

    let rec offset n crc k dst t =
      let pos = ref 9 in
      let off = ref n in

      Bytes.set tmp_offset !pos (Char.chr Int64.(to_int (!off && 127L)));

      while Int64.(!off >> 7) <> 0L
      do
        off := Int64.(!off >> 7);
        pos := !pos - 1;
        Bytes.set tmp_offset !pos (Char.chr (128 lor Int64.(to_int ((!off - 1L) && 127L))));
      done;

      let rec loop idx crc dst t =
        if idx = 10
        then k crc dst t
        else
          let byte = Char.code (Bytes.get tmp_offset idx) in
          let crc = Crc32.digestc crc byte in

          put_byte (Char.code @@ Bytes.get tmp_offset idx) (loop (idx + 1) crc) dst t
      in

      loop !pos crc dst t
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

  let pp_list ?(sep = (fun fmt () -> ())) pp_data fmt lst =
    let rec aux = function
      | [] -> ()
      | [ x ] -> pp_data fmt x
      | x :: r -> pp_data fmt x; sep fmt (); aux r
    in
    aux lst

  let cstruct_to_array cs =
    let ar = Array.make (Cstruct.len cs) '\000' in

    for i = 0 to Cstruct.len cs - 1
    do Array.set ar i (Cstruct.get_char cs i) done;

    ar

  external identity : Cstruct.t -> Cstruct.t = "%identity"

  (* XXX(dinosaure): I'm not sure about this function. If we have a bug in the
                     serialization, may be is there. TODO! *)
  let split_at_newline cs =
    let lst = ref [] in
    let last = ref 0 in
    let i = ref 0 in

    while !i < Cstruct.len cs
    do
      if Cstruct.get_char cs !i = '\n'
      then begin
        lst := Cstruct.sub cs !last ((!i + 1) - !last) :: !lst;

        incr i;
        last := !i;
      end else incr i
    done;

    if !last <> Cstruct.len cs
    then lst := Cstruct.sub cs !last (Cstruct.len cs - !last) :: !lst;

    Array.of_list (List.rev !lst)

  let offset_entry idx dst t =
    if idx = Array.length t.objects
    then Cont { t with state = Hash hash }
    else
      let off = t.write in

      await { t with state = Raw { idx; off; } }

  let write_header kind dst t =
    match kind with
    | KOffset (source, target, hunk) ->
      let source_length = Cstruct.len source.raw in
      let target_length = Cstruct.len target.raw in
      let offset = Int64.sub target.offset source.offset in

      let h =
        H.flush 0 (Cstruct.len t.h_tmp)
        @@ H.default (H.Offset offset) source_length target_length hunk in

      let length = H.length h in

      let byte = 0b110 lsl 4 in
      let byte = byte lor (length land 0x0F) in

      let byte, continue =
        if length lsr 4 <> 0
        then byte lor 0x80, true
        else byte, false
      in

      let crc = Crc32.digestc Crc32.default byte in

      Hunk.produce source.raw target.raw hunk;

      let open Decompress in

      if continue
      then (KWriteHeader.put_byte byte
            @@ KWriteHeader.length (Int64.of_int (length lsr 4)) crc
            @@ fun crc -> KWriteHeader.offset offset crc
            @@ fun crc dst t ->
               let z = Deflate.flush (t.o_off + t.o_pos) (t.o_len - t.o_pos)
                 @@ Deflate.default ~proof:B.proof_bigstring 4 in

               Cont { t with state = WriteHunk { current = target
                                               ; crc
                                               ; h
                                               ; z } })
          dst t
      else (KWriteHeader.put_byte byte
            @@ KWriteHeader.offset offset crc
            @@ fun crc dst t ->
             let z = Deflate.flush (t.o_off + t.o_pos) (t.o_len - t.o_pos)
               @@ Deflate.default ~proof:B.proof_bigstring 4 in

             Cont { t with state = WriteHunk { current = target
                                             ; crc
                                             ; h
                                             ; z } })
          dst t

    | KHash (hash, hunk) -> Cont t
    | KRaw current ->
      let open Decompress in

      let length = current.entry.Entry.length in

      (* XXX(dinosaure): need to be clear, the variable length integer tells us
                         the expected size of the object AFTER the object is
                         inflated. *)

      let byte = ((Kind.to_bin current.entry.Entry.kind) lsl 4) in (* kind *)
      let byte = byte lor Int64.(to_int (length && 0x0FL)) in (* part of the size *)

      let byte, continue =
        if Int64.(length >> 4) <> 0L
        then byte lor 0x80, true
        else byte, false
      in

      let crc = Crc32.digestc Crc32.default byte in

      if continue
      then (KWriteHeader.put_byte byte
            @@ KWriteHeader.length Int64.(length >> 4) crc
            @@ fun crc dst t ->
               let z = Decompress.Deflate.flush (t.o_off + t.o_pos) (t.o_len - t.o_pos)
                 @@ Decompress.Deflate.default ~proof:Decompress.B.proof_bigstring 4 in

               Cont { t with state = WriteZip { current; crc; last = false; z; } })
          dst t
      else KWriteHeader.put_byte byte
          (fun dst t ->
             let z = Decompress.Deflate.flush (t.o_off + t.o_pos) (t.o_len - t.o_pos)
               @@ Decompress.Deflate.default ~proof:Decompress.B.proof_bigstring 4 in

             Cont { t with state = WriteZip { current; crc; last = false; z; } }) dst t

  let write_zip dst t current crc last z =
    let open Decompress in

    let src' = B.from_bigstring (Cstruct.to_bigarray current.raw) in
    let dst' = B.from_bigstring (Cstruct.to_bigarray dst) in

    let rec loop last z = match Deflate.eval src' dst' z with
      | `Await z ->
        if last
        then loop true (Deflate.finish z)
        else loop true (Deflate.no_flush 0 (Cstruct.len current.raw) z)
      | `Flush z ->
        let crc = Crc32.digest ~off:(t.o_off + t.o_pos) ~len:(Deflate.used_out z) crc dst in

        flush dst { t with state = WriteZip { current; crc; last; z; }
                         ; o_pos = t.o_pos + (Deflate.used_out z)
                         ; write = Int64.add t.write (Int64.of_int (Deflate.used_out z)) }
      | `End z ->
        let crc = Crc32.digest ~off:(t.o_off + t.o_pos) ~len:(Deflate.used_out z) crc dst in

        Cont { t with state = Save { current; crc; }
                    ; o_pos = t.o_pos + (Deflate.used_out z)
                    ; write = Int64.add t.write (Int64.of_int (Deflate.used_out z)) }
      | `Error (z, exn) -> error t (Deflate_error exn)
    in

    loop last z

  let write_hunk dst t current crc h z =
    let open Decompress in

    let src' = B.from_bigstring (Cstruct.to_bigarray t.h_tmp) in
    let dst' = B.from_bigstring (Cstruct.to_bigarray dst) in

    match H.eval t.h_tmp h with
    | `Flush h ->
      (match Deflate.eval src' dst' z with
       | `Flush z ->
         let crc = Crc32.digest ~off:(t.o_off + t.o_pos) ~len:(Deflate.used_out z) crc dst in

         flush dst { t with state = WriteHunk { current; crc; h; z; }
                          ; o_pos = t.o_pos + (Deflate.used_out z)
                          ; write = Int64.add t.write (Int64.of_int (Deflate.used_out z)) }
       | `End z -> assert false (* can't appear. *)
       | `Await z ->
         let z = Deflate.no_flush 0 (H.used_out h) z in
         let h = if H.used_out h = 0 then H.flush 0 (Cstruct.len t.h_tmp) h else H.flush 0 0 h in

         (* XXX(dinosaure): when the Hunk serializer returns `Flush for a first
                            time, we notice the Deflate state than we have some
                            data and stop the Hunk serializer to write something
                            (by [H.flush 0 0]). The continuation call a second
                            time the Hunk serializer and returns directly `Flush
                            but with [H.used_out = 0].

                            At this moment, the Deflate state computed all of
                            the input. So, we can safely continue to let the
                            Hunk serializer write something inside [h_tmp].

                            This is very close to the behaviour of [Decompress],
                            indeed, [Decompress] compute in one call of
                            [Deflate.eval] all input.
          *)

         Cont { t with state = WriteHunk { current; crc; h; z; } }
       | `Error (z, exn) -> error t (Deflate_error exn))
    | `End h ->
      (match Deflate.eval src' dst' z with
       | `Flush z ->
         let crc = Crc32.digest ~off:(t.o_off + t.o_pos) ~len:(Deflate.used_out z) crc dst in

         flush dst { t with state = WriteHunk { current; crc; h; z; }
                          ; o_pos = t.o_pos + (Deflate.used_out z)
                          ; write = Int64.add t.write (Int64.of_int (Deflate.used_out z)) }
       | `End z ->
         let crc = Crc32.digest ~off:(t.o_off + t.o_pos) ~len:(Deflate.used_out z) crc dst in

         Cont { t with state = Save { current; crc; }
                     ; o_pos = t.o_pos + (Deflate.used_out z)
                     ; write = Int64.add t.write (Int64.of_int (Deflate.used_out z)) }
       | `Await z ->
         if H.used_out h <> 0
         then
           let z = Deflate.no_flush 0 (H.used_out h) z in
           let h = H.flush 0 0 h in

           Cont { t with state = WriteHunk { current; crc; h; z; } }
         else
           begin
             let z = Deflate.finish z in

             assert (H.length h = h.H.write);

             (* XXX(dinosaure): same notice than previous (see `Flush), but at the end, we call [Deflate.finish]. *)

             Cont { t with state = WriteHunk { current; crc; h; z; } }
           end
       | `Error (z, exn) -> error t (Deflate_error exn))
    | `Error (h, exn) -> error t (Hunk_error exn)

  let save_entry dst t obj crc =
    Cont { t with state = Object (offset_entry (obj.idx + 1))
                ; radix = Radix.bind t.radix (Array.get t.objects obj.idx).Entry.hash_object (crc, obj.offset) }

  let delta current t =
    (* XXX(dinosaure): we choose one which has the most [Hunk.Copy]. *)

    let choose_best a b = match a, b with
      | None, None -> None
      | Some (idx_a, a), None -> if Hunk.count_copy a > 0 then Some (idx_a, a) else None
      | None, Some (idx_a, a) -> if Hunk.count_copy a > 0 then Some (idx_a, a) else None
      | Some (idx_a, a), Some (idx_b, b) ->
        let a' = Hunk.count_copy a in
        let b' = Hunk.count_copy b in

        if a' < b'
        then Some (idx_b, b)
        else Some (idx_a, a)
    in

    Window.foldi
      t.window
      (fun best idx base ->
         if current.entry.Entry.kind = base.entry.Entry.kind
            && current.entry.Entry.name = base.entry.Entry.name
         then begin
           let raw' = split_at_newline current.raw in
           let base_raw' = split_at_newline base.raw in
           let diff =
             Pdiff.get_hunks
               ~transform:identity
               ~compare:Cstruct.compare
               ~context:(-1)
               ~a:base_raw' ~b:raw'
           in

           let diff = List.hd diff in (* XXX(dinosaure): the context is -1, so we have only one hunk. *)
           let hunk = Hunk.of_patience_diff diff in

           Format.eprintf "%a diff %a: %d %% (%d / %d)\n%!"
             Hash.pp current.entry.Entry.hash_object
             Hash.pp base.entry.Entry.hash_object
             (Hunk.count_copy hunk * 100 / (Cstruct.len current.raw))
             (Hunk.count_copy hunk) (Cstruct.len current.raw);

           Hunk.produce base.raw current.raw hunk;

           choose_best best (Some (idx, hunk))
         end else best)
      None

  let finish_entry dst t idx offset =
    let raw = BBuffer.contents t.buffer in
    let raw = Cstruct.of_string (Cstruct.copy raw 0 (Cstruct.len raw)) in

    let current = { idx
                  ; offset
                  ; entry = Array.get t.objects idx
                  ; raw } in

    BBuffer.clear t.buffer;

    match delta current t with
    | Some (idx, hunk) ->
      let base = Window.nth t.window idx in

      Window.push t.window current;
      Cont { t with state = WriteHeader (write_header (KOffset (base, current, hunk))) }
    | None ->
      Window.push t.window current;
      Cont { t with state = WriteHeader (write_header (KRaw current)) }

  let number dst t =
    (* XXX(dinosaure): problem in 32-bits architecture. *)
    KHeader.put_u32 (Int32.of_int (Array.length t.objects))
      (fun dst t -> Cont { t with state = Object (offset_entry 0) })
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
    | Raw { idx; off; } ->
      { t with state = Buffer { idx; raw; off; } }
    | _ -> raise (Invalid_argument "P.raw: bad state")

  let current_raw t = match t.state with
    | Buffer { raw; _ } -> raw
    | _ -> raise (Invalid_argument "P.current_raw: bad state")

  let finish_raw t = match t.state with
    | Buffer { idx; raw; off; } ->
      { t with state = Finish { idx; off; } }
    | _ -> raise (Invalid_argument "P.finish_raw: bad state")

  let refill offset len ?raw t = match t.state, raw with
    | Buffer { idx; raw; off; }, Some new_raw ->
      BBuffer.add new_raw ~off:offset ~len t.buffer;
      { t with state = Buffer { idx; raw = new_raw; off; } }
    | Buffer { idx; raw; off; }, None ->
      BBuffer.add raw ~off:offset ~len t.buffer;
      { t with state = Buffer { idx; raw; off; } }
    | _ -> raise (Invalid_argument "P.refill: bad state")

  let flush offset len t =
    if (t.o_len - t.o_pos) = 0
    then
      match t.state with
      | WriteZip { current; crc; last; z; } ->
        { t with o_off = offset
               ; o_len = len
               ; o_pos = 0
               ; state = WriteZip { current; crc; last; z = Decompress.Deflate.flush offset len z } }
      | WriteHunk { current; crc; h; z; } ->
        { t with o_off = offset
               ; o_len = len
               ; o_pos = 0
               ; state = WriteHunk { current; crc; h; z = Decompress.Deflate.flush offset len z } }
      | _ ->
        { t with o_off = offset
               ; o_len = len
               ; o_pos = 0 }
    else raise (Invalid_argument (sp "P.flush: you lost something (pos: %d, len: %d)" t.o_pos t.o_len))

  let used_out t = t.o_pos

  let expect t = match t.state with
    | Raw { idx; off; } ->
      (try
         let hash = (Array.get t.objects idx).Entry.hash_object in
         Some hash
       with exn -> raise (Invalid_argument "P.expect: invalid index"))
    | _ -> None

  let entry t = match t.state with
    | Raw { idx; off; } ->
      (try Array.get t.objects idx
       with exn -> raise (Invalid_argument "P.entry: invalid index"))
    | _ -> raise (Invalid_argument "P.entry: bad state")

  let idx t = t.radix

  let hash t = Hash.get t.hash

  let eval dst t =
    let eval0 t = match t.state with
      | Header k -> k dst t
      | Object k -> k dst t
      | Raw { idx; off; } -> await t
      | Buffer { idx; raw; off; } -> await t
      | Finish { idx; off; } -> finish_entry dst t idx off
      | WriteHeader k -> k dst t
      | WriteZip { current; crc; last; z; } -> write_zip dst t current crc last z
      | WriteHunk { current; crc; h; z; } -> write_hunk dst t current crc h z
      | Save { current; crc; } -> save_entry dst t current crc
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

  let default ?(chunk = 1) h_tmp objects =
    { o_off = 0
    ; o_pos = 0
    ; o_len = 0
    ; write = 0L
    ; radix = Radix.empty
    ; buffer = BBuffer.create 0x800
    ; h_tmp
    ; window = Window.make chunk
    ; objects
    ; hash = Hash.init ()
    ; state = (Header header) }
end
