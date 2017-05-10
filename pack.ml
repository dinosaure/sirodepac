let pp_list ?(sep = (fun fmt () -> ()) )pp_data fmt lst =
  let rec aux = function
    | [] -> ()
    | [ x ] -> pp_data fmt x
    | x :: r -> Format.fprintf fmt "%a%a" pp_data x sep (); aux r
  in aux lst

let pp_cstruct fmt cs =
  Format.fprintf fmt "\"";
  for i = 0 to Cstruct.len cs - 1
  do if Cstruct.get_uint8 cs i > 32 && Cstruct.get_uint8 cs i < 127
    then Format.fprintf fmt "%c" (Cstruct.get_char cs i)
    else Format.fprintf fmt "."
  done;
  Format.fprintf fmt "\""

let pp_array pp_data fmt arr =
  Format.fprintf fmt "[| @[<hov>%a@] |]"
    (pp_list ~sep:(fun fmt () -> Format.fprintf fmt ";@ ") pp_data) (Array.to_list arr)

module type HASH =
sig
  type t = Rakia.Bi.t
  type ctx
  type buffer = Cstruct.buffer

  val length    : int
  val pp        : Format.formatter -> t -> unit

  val init      : unit -> ctx
  val feed      : ctx -> buffer -> unit
  val get       : ctx -> t

  val of_string : string -> t
  val to_string : t -> string

  val equal     : t -> t -> bool
  val compare   : t -> t -> int

  val of_hex_string : string -> t
  val to_hex_string : t -> string
end

module Hunk =
struct
  type t =
    | Insert of Cstruct.t
    | Copy of int * int

  let pp fmt = function
    | Insert cs -> Format.fprintf fmt "(Insert [%a])" pp_cstruct cs
    | Copy (off, len) -> Format.fprintf fmt "(Copy (%d, %d))" off len

  let from_rabin lst raw =
    List.map (function Rabin.C (off, len) -> Copy (off, len)
                     | Rabin.I (off, len) -> Insert (Cstruct.sub raw off len))
      lst
end

module Option =
struct
  let value ~default = function
    | Some a -> a
    | None -> default

  let ( >>| ) a f = match a with
    | Some a -> Some (f a)
    | None -> None

  let map f = function
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

module Entry (Hash : HASH) =
struct
  module Commit = Minigit.Commit(Hash)
  module Tree   = Minigit.Tree(Hash)
  module Tag    = Minigit.Tag(Hash)
  module Blob   = Minigit.Blob

  type t =
    { hash_name   : int
    ; hash_object : Hash.t
    ; name        : string option
    ; kind        : Kind.t
    ; preferred   : bool
    ; delta       : source
    ; length      : int64 }
  and source =
    | From of Hash.t
    | None
  (* XXX(dinosaure): I try to use GADT in this case and ... god I'm crazy. *)

  let pp_source fmt = function
    | From hash -> Format.fprintf fmt "Δ(%a)" Hash.pp hash
    | None -> Format.fprintf fmt "Τ"

  let pp_option pp_data fmt = function
    | Some x -> pp_data fmt x
    | None -> Format.fprintf fmt "<none>"

  let rec pp
    : type delta. Format.formatter -> t -> unit
    = fun fmt { hash_name; hash_object; name; kind; preferred; delta; length; } ->
    Format.fprintf fmt "{ @[<hov>name = %d and @[<hov>%a@];@ \
                                 hash = @[<hov>%a@];@ \
                                 kind = @[<hov>%a@];@ \
                                 preferred = %b;@ \
                                 delta = @[<hov>%a@];@ \
                                 length = %Ld;@] }"
      hash_name
      (pp_option Format.pp_print_string)
      name
      Hash.pp hash_object
      Kind.pp kind
      preferred
      pp_source delta
      length

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

  (* XXX(dinosaure): git hash only the basename. *)
  let hash name = hash (Filename.basename name)

  type kind =
    | Commit of Commit.t
    | Tree   of Tree.t
    | Tag    of Tag.t
    | Blob   of Blob.t

  let name entry = entry.name

  let make
    : type delta. Hash.t -> ?name:string -> ?preferred:bool -> ?delta:source -> kind -> t
    = fun hash_object ?name ?(preferred = false) ?(delta = None) obj ->
    let hash_name = Option.(value ~default:0 (name >>| hash)) in

    match obj with
    | Tag tag ->
      { hash_name
      ; hash_object
      ; name
      ; kind = Kind.Tag
      ; preferred
      ; delta
      ; length = Tag.raw_length tag }
    | Commit commit ->
      { hash_name
      ; hash_object
      ; name
      ; kind = Kind.Commit
      ; preferred
      ; delta
      ; length = Commit.raw_length commit }
    | Tree tree ->
      { hash_name
      ; hash_object
      ; name
      ; kind = Kind.Tree
      ; preferred
      ; delta
      ; length = Tree.raw_length tree }
    | Blob blob ->
      { hash_name
      ; hash_object
      ; name
      ; kind = Kind.Blob
      ; preferred
      ; delta
      ; length = Blob.raw_length blob }

  let compare a b =
    (* - first, sort by type. Different objects never delta with each other.
       - then sort by filename/dirname. hash of the basename occupies the top
         BITS_PER_BITS - DIR_BITS, and bottom DIR_BITS are for hash of leading
         path elements.
       - then if we are doing "thin" pack, the objects wa are not going to pack
         but we know about are sorted earlier than other object.
       - and finally sort by size, larger to smaller
     *)

    let int_of_bool v = if v then 1 else 0 in

    if Kind.to_int a.kind > Kind.to_int b.kind
    then (-1)
    else if Kind.to_int a.kind < Kind.to_int b.kind
    then (1)
    else if a.hash_name > b.hash_name
    then (-1)
    else if a.hash_name < b.hash_name
    then (1)
    else if int_of_bool a.preferred > int_of_bool b.preferred
    then (-1)
    else if int_of_bool a.preferred < int_of_bool b.preferred
    then (1)
    else if a.length > b.length
    then (-1)
    else if a.length < b.length
    then (1)
    else Pervasives.compare a b

    (* XXX(dinosaure): git compare the memory position then but it's irrelevant in OCaml. *)

  let from_commit hash ?preferred ?delta commit =
    make ?preferred ?delta hash (Commit commit)

  let from_tag hash ?preferred ?delta tag =
    make ?preferred ?delta hash (Tag tag)

  let from_tree hash ?preferred ?delta ?path:name tree =
    make ?preferred ?delta hash ?name (Tree tree)

  let from_blob hash ?preferred ?delta ?path:name blob =
    make ?preferred ?delta hash ?name (Blob blob)

  let topological_sort lst =
    let lst =
      (* XXX(dinosaure): sanitize and remove self-dependency. *)
      List.map (function
          | ({ delta = From hash; hash_object; _ } as x) ->
            if Hash.equal hash hash_object then { x with delta = None } else x
          | x -> x)
        lst
    in

    let edges, rest = List.partition (fun entry -> entry.delta = None) lst in

    let deps hash =
      (* XXX(dinosaure): in according to git, result of [List.filter] contains only one element. *)
      try List.filter (fun e -> Hash.equal e.hash_object hash) lst
      with Not_found -> []
    in

    let rec loop acc later rest progress = match rest, later with
      | [], [] -> List.rev acc
      | [], later ->
        if progress
        then loop acc [] later false
        else raise (Invalid_argument "Entry.topological_sort: un-orderable list")
      | ({ delta = From hash; _  } as x) :: r, later ->
        let deps = deps hash in

        (* We ensure than the [deps] is available previously. *)
        let ensure = List.for_all
            (fun dep -> List.exists (fun x -> Hash.equal x.hash_object dep.hash_object) acc)
            deps
        in

        if ensure
        then loop (x :: acc) later r true
        else loop acc (x :: later) r progress
      | x :: r, later -> loop (x :: acc) later r true
    in

    loop edges [] rest false
end

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
  let ( / ) = Int64.div
end

module MakeHunkEncoder (Hash : HASH) =
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
    | Hash of Hash.t
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

  let rec put_byte ~ctor byte k dst t =
    if (t.o_len - t.o_pos) > 0
    then begin
      Cstruct.set_uint8 dst (t.o_off + t.o_pos) byte;

      k dst { t with o_pos = t.o_pos + 1
                    ; write = t.write + 1 }
    end else Flush { t with state = ctor (fun dst t -> (put_byte[@tailcall]) ~ctor byte k dst t) }

  module KHeader =
  struct
    let put_byte = put_byte ~ctor:(fun k -> Header k)

    let rec length n k dst t =
      let byte = (n land 0x7F) in
      let rest = (n lsr 7) in

      if rest <> 0
      then put_byte (byte lor 0x80) (length rest k) dst t
      else put_byte byte k dst t
  end

  module KHunk =
  struct
    let put_byte = put_byte ~ctor:(fun k -> Hunk k)
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
        end else Flush { t with state = Copy (put_byte ~force byte k) }
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

  let copy idx (off, (flag_o1, flag_o2, flag_o3)) (len, (flag_len1, flag_len2)) dst t =
    let o0 = off land 0xFF in
    let o1 = (off land 0xFF00) lsr 8 in
    let o2 = (off land 0xFF0000) lsr 16 in
    let o3 = (off land 0xFF000000) lsr 24 in

    let l0 = len land 0xFF in
    let l1 = (len land 0xFF00) lsr 8 in
    let l2 = (len land 0xFF0000) lsr 16 in

    (KCopy.put_byte ~force:true o0
     @@ KCopy.put_byte ~force:(o2 <> 0 || o3 <> 0)o1
     @@ KCopy.put_byte ~force:(o3 <> 0) o2
     @@ KCopy.put_byte o3
     @@ KCopy.put_byte ~force:true l0
     @@ KCopy.put_byte ~force:(l2 <> 0) l1
     @@ KCopy.put_byte l2
     @@ fun dst t ->
        Cont { t with state = List (idx + 1) })
    dst t

  let list idx dst t =
    if Array.length t.hunks <> idx
    then let hunk = Array.get t.hunks idx in
      match hunk with
      | Hunk.Insert raw ->
        assert (Cstruct.len raw > 0 && Cstruct.len raw <= 0x7F);

        let byte = Cstruct.len raw land 0x7F in
          (* TODO: be sure the length of the raw is lower than 0x7F.

             XXX(dinosaure): this is the case because we called
                             [of_patience_diff] (which calls [split_to_127]).
           *)

        KHunk.put_byte byte (insert idx raw) dst t
      | Hunk.Copy (off, len) ->
        let n_offset = how_many_bytes off in
        let n_length = if len = 0x10000 then 1 else how_many_bytes len in

        let o, fo = match n_offset with
          | 1 -> 0b0001, (false, false, false)
          | 2 -> 0b0011, (true, false, false)
          | 3 -> 0b0111, (true, true, false)
          | 4 -> 0b1111, (true, true, true)
          | _ ->
            assert false
        in

        let l, fl = match n_length with
          | 1 -> 0b001, (false, false)
          | 2 -> 0b011, (true, false)
          | 3 -> 0b111, (true, true)
          | _ -> assert false
        in

        KHunk.put_byte (0x80 lor (l lsl 4) lor o) (copy idx (off, fo)  (if len = 0x10000 then (0, (false, false)) else len, fl)) dst t
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

  let length src_len trg_len hunks =
    size_of_variable_length src_len
    + size_of_variable_length trg_len
    + List.fold_left
      (fun acc -> function
         | Hunk.Insert raw -> 1 + Cstruct.len raw + acc
         | Hunk.Copy (off, len) ->
           1 + (how_many_bytes off) + (if len = 0x10000 then 1 else how_many_bytes len) + acc)
      0 hunks

  let flush off len t =
    { t with o_off = off
           ; o_len = len
           ; o_pos = 0 }

  let used_out t = t.o_pos

  let consume off t =
    { t with o_off = off
           ; o_pos = t.o_pos - off}
end

module MakeDelta (Hash : HASH) =
struct
  module Entry  = Entry(Hash)

  type t =
    { mutable delta : delta }
  and delta =
    | Z
    | S of { length     : int
           ; depth      : int
           ; hunks      : Rabin.e list
           ; src        : t
           ; src_length : int64 (* XXX(dinosaure): this is the length of the inflated raw of [src]. *)
           ; src_hash   : Hash.t }
    (* XXX(dinosaure): I try to use GADT (peano number) and ... I really crazy. *)

  module MV =
  struct
    type nonrec t = t * Cstruct.t * Rabin.Index.t

    let weight (_, raw, rabin) =
      1 + Cstruct.len raw + 1 + (Rabin.Index.memory_size rabin)
  end

  module AV = struct type nonrec t = t * Cstruct.t * Rabin.Index.t let weight _ = 1 end

  module type WINDOW = Lru.F.S with type k = Entry.t and type v = t * Cstruct.t * Rabin.Index.t
  type window = (module WINDOW)

  let rec pp_delta fmt = function
    | Z -> Format.fprintf fmt "Τ"
    | S { length; depth; hunks; src; } ->
      Format.fprintf fmt "(Δ { @[<hov>length = %d;@ \
                                      depth = %d;@ \
                                      hunks = @[<hov>%a@];@ \
                                      src = @[<hov>%a@];@] }"
        length depth (pp_list ~sep:(fun fmt () -> Format.fprintf fmt ";@ ") Rabin.pp) hunks pp src
  and pp fmt { delta; } =
    Format.fprintf fmt "{ @[<hov>delta = @[<hov>%a@];@] }"
      pp_delta delta

  type error = Invalid_hash of Hash.t

  let pp_error fmt = function
    | Invalid_hash hash -> Format.fprintf fmt "(Invalid_hash %a)" Hash.pp hash

  let rec depth = function
    | { delta = S { depth; _ } } -> depth
    | { delta = Z } -> 0

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

  let length src_len trg_len hunks =
    size_of_variable_length src_len
    + size_of_variable_length trg_len
    + List.fold_left
      (fun acc -> function
         | Rabin.I (_, len) -> 1 + len + acc
         | Rabin.C (off, len) ->
           1 + (how_many_bytes off) + (if len = 0x10000 then 1 else how_many_bytes len) + acc)
      0 hunks

  let only_insert = List.for_all (function Rabin.I _ -> true | Rabin.C _ -> false)

  let delta
    : type window. window -> (module WINDOW with type t = window) -> int -> Entry.t -> Cstruct.t -> t -> (Entry.t * Rabin.e list * int) option
    = fun window window_pack max trg_entry trg_raw trg ->
    let limit src = match trg.delta with
      | S { length; src; _ } ->
        length * (max - (depth src)) / (max - (depth trg + 1))
      | Z ->
        (Int64.to_int trg_entry.Entry.length / 2 - 20) * (max - (depth src)) / (max - 1)
    in

    let choose a b = match a, b with
      | None, None -> None
      | Some a, None -> Some a (* XXX(dinosaure): [a] is considered as the best. *)
      | None, Some (a, hunks_a, len_a) ->
        if not (only_insert hunks_a) then Some (a, hunks_a, len_a) else None
        (* Rabin's fingerprint can produce only [Insert] hunks and we avoid that. *)
      | Some (a, hunks_a, len_a), Some (b, hunks_b, len_b) ->
        if len_a < len_b
        then Some (a, hunks_a, len_a)
        else Some (b, hunks_b, len_b)
    in

    let apply src_entry (src, src_raw, rabin) best =
      let diff =
        if src_entry.Entry.length < trg_entry.Entry.length
        then Int64.to_int (Int64.sub trg_entry.Entry.length src_entry.Entry.length)
        else 0
      in

      if src_entry.Entry.kind <> trg_entry.Entry.kind
       || (depth src) = max
       || (limit src) = 0
       || (limit src) <= diff
       || trg_entry.Entry.length < Int64.(src_entry.Entry.length / 32L)
       || Hash.equal src_entry.Entry.hash_object trg_entry.Entry.hash_object
      then best
      else
        let hunks  = Rabin.delta rabin trg_raw in
        let length = length (Cstruct.len src_raw) (Cstruct.len trg_raw) hunks in

        choose best (Some (src_entry, hunks, length))
    in

    let module Window = (val window_pack) in

    if not trg_entry.Entry.preferred
       && (depth trg) < max
    then Window.fold apply None window
    else None

  let int_of_bool v = if v then 1 else 0

  let pp_option pp_data fmt = function
    | Some x -> pp_data fmt x
    | None -> Format.fprintf fmt "<none>"

  let ok v = Ok v

  type write =
    { mutable fill : bool
    ; tagged       : bool
    ; entry        : Entry.t * t }

  (* XXX(dinosaure): git prioritize some entries in imperative weird way. we
                     can't reproduce the same with a small cost so we decide to
                     let the user to prioritize some entries by the [tag]
                     function and keep the lexicographic order:

                     - tagged
                     - commit
                     - tag
                     - tree
                     - rest

                     We can't don't care about that in other side ...
   *)
  let sort tag lst =
    let compare a b =
      if int_of_bool a.tagged > int_of_bool b.tagged
      then (1)
      else if int_of_bool a.tagged < int_of_bool b.tagged
      then (-1)
      else if Kind.to_int (fst a.entry).Entry.kind > Kind.to_int (fst b.entry).Entry.kind
      then (-1)
      else if Kind.to_int (fst a.entry).Entry.kind < Kind.to_int (fst b.entry).Entry.kind
      then (1)
      else Pervasives.compare a b
    in

    List.map (fun ((e, d) as entry) -> { fill = false; tagged = tag e; entry; }) lst
    |> List.stable_sort compare
    |> List.map (fun { entry; _ } -> entry)

  exception Uncaught_hash of Hash.t

  module MemoryCache = Lru.F.Make(Entry)(MV)
  module AbstractCache = Lru.F.Make(Entry)(AV)

  let deltas ?(memory = false) entries get tag window max =
    let to_delta e = match e.Entry.delta, e.Entry.preferred with
      | Entry.None, false -> e.Entry.length >= 50L
      | _ -> false
    in

    let tries =
      List.filter to_delta entries
      |> List.stable_sort Entry.compare
    in

    let untries =
      List.filter (fun x -> not (to_delta x)) entries
      |> Entry.topological_sort
    in

    try
      let window_pack =
        if memory
        then (module MemoryCache : WINDOW)
        else (module AbstractCache : WINDOW)
      in

      let module Window = (val window_pack) in
      let window_pack = (module Window : WINDOW with type t = Window.t) in

      let window = Window.empty window in
      let normal = Hashtbl.create (List.length tries) in

      (* XXX(dinosaure): [normalize] apply the diff to all [untries] entries.
                         however, we need to apply to [untries] a topological sort to
                         ensure than when we try to apply a diff in one
                         /untries/ entry, we already computed the source
                         (available in [tries] or [untries]). It's why we keep
                         an hash-table and update this hash-table for each diff.
       *)
      let normalize =
        List.map (fun trg_entry -> match trg_entry.Entry.delta with
            | Entry.None -> (trg_entry, { delta = Z })
            | Entry.From hash ->
              let src = try Hashtbl.find normal hash with Not_found -> { delta = Z } in

              (* XXX(dinosaure): if we can't find [src] in the hash-table, that
                                 means the source object is outside the PACK
                                 file because we ensure than if [trg_entry] has
                                 a dependence, by the topological sort, we
                                 already computed all /in-PACK/ sources
                                 necessary for the next and update the
                                 hash-table with these sources.
               *)

              match get hash, get trg_entry.Entry.hash_object with
              | Some src_raw, Some trg_raw ->
                let rabin  = Rabin.Index.make ~copy:false src_raw in (* we don't keep [rabin]. *)
                let hunks  = Rabin.delta rabin trg_raw in
                let length = length (Cstruct.len src_raw) (Cstruct.len trg_raw) hunks in
                let depth  = depth src + 1 in
                let base   = { delta = S { length
                                         ; depth
                                         ; hunks
                                         ; src
                                         ; src_length = Int64.of_int (Cstruct.len src_raw)
                                         ; src_hash = hash } }
                in

                Hashtbl.add normal trg_entry.Entry.hash_object base;

                (trg_entry, base)
              | None, Some _ -> raise (Uncaught_hash hash)
              | Some _, None -> raise (Uncaught_hash trg_entry.Entry.hash_object)
              | None, None -> assert false)
      in

      List.fold_left
        (fun (window, acc) entry -> match get entry.Entry.hash_object with
          | None -> raise (Uncaught_hash entry.Entry.hash_object)
          | Some raw ->
            let base   = { delta = Z } in
            let rabin  = Rabin.Index.make ~copy:false raw in (* we keep [rabin] with [raw] in the [window]. *)
            let window = Window.add entry (base, raw, rabin) window in

            match delta window window_pack max entry raw base with
            | None -> window, (entry, base) :: acc
            | Some (src_entry, hunks, length) ->
              match Window.find ~promote:true src_entry window with
              | Some ((src, src_raw, rabin), window) ->
                let depth  = depth src + 1 in

                base.delta <- S { length
                                ; depth
                                ; hunks
                                ; src
                                ; src_length = src_entry.Entry.length
                                ; src_hash = src_entry.Entry.hash_object };
                Hashtbl.add normal entry.Entry.hash_object base;

                window, ({ entry with Entry.delta = Entry.From src_entry.Entry.hash_object }, base) :: acc
              | None -> window, (entry, base) :: acc)
        (window, []) tries
      |> fun (window, res) -> List.rev res
                              |> List.append (normalize untries)
                              |> sort tag |> ok
    with Uncaught_hash hash -> Error (Invalid_hash hash)
end

module type Z =
sig
  type t
  type error

  val pp_error : Format.formatter -> error -> unit

  val default  : int -> t
  val flush    : int -> int -> t -> t
  val no_flush : int -> int -> t -> t
  val finish   : t -> t
  val used_in  : t -> int
  val used_out : t -> int
  val eval     : Cstruct.t -> Cstruct.t -> t -> [ `Flush of t | `Await of t | `Error of (t * error) | `End of t ]
end

module MakePACKEncoder (Hash : HASH) (_ : Z) =
struct
  module Entry = Entry(Hash)
  module Delta = MakeDelta(Hash)
  module Radix = Radix.Make(Rakia.Bi)
  module Write = Set.Make(Hash)
  module HunkEncoder = MakeHunkEncoder(Hash)

  module Deflate = Decompress.Deflate

  type error = ..
  type error += Deflate_error of Deflate.error
  type error += Hunk_error of HunkEncoder.error
  type error += Invalid_entry of Entry.t * Delta.t
  type error += Invalid_hash of Hash.t

  let pp_error fmt = function
    | Deflate_error exn -> Format.fprintf fmt "(Deflate_error %a)" Deflate.pp_error exn
    | Hunk_error exn -> Format.fprintf fmt "(Hunk_error %a)" HunkEncoder.pp_error exn
    | Invalid_entry (entry, delta) -> Format.fprintf fmt "(Invalid_entry @[<hov>(%a,@ %a)@])"
                                        Entry.pp entry Delta.pp delta
    | Invalid_hash hash -> Format.fprintf fmt "(Invalid_hash %a)" Hash.pp hash

  let pp_option pp_data fmt = function
    | Some x -> pp_data fmt x
    | None -> Format.fprintf fmt "<none>"

  type t =
    { o_off   : int
    ; o_pos   : int
    ; o_len   : int
    ; write   : int64
    ; radix   : (Crc32.t * int64) Radix.t
    ; access  : (Hash.t -> Cstruct.t option)
    ; hash    : Hash.ctx
    ; h_tmp   : Cstruct.t
    ; state   : state }
  and k = Cstruct.t -> t -> res
  and state =
    | Header of k
    | Object of k
    | WriteK of k
    | WriteZ of { x   : Entry.t * Cstruct.t
                ; r   : (Entry.t * Delta.t) list
                ; crc : Crc32.t
                ; off : int64
                ; ui  : int
                ; z   : (Decompress.B.bs, Decompress.B.bs) Deflate.t }
    | WriteH of { x   : Entry.t * Delta.t * Cstruct.t
                ; r   : (Entry.t * Delta.t) list
                ; crc : Crc32.t
                ; off : int64
                ; ui  : int
                ; h   : HunkEncoder.t
                ; z   : (Decompress.B.bs, Decompress.B.bs) Deflate.t }
    | Save   of { x   : Entry.t
                ; r   : (Entry.t * Delta.t) list
                ; crc : Crc32.t
                ; off : int64 }
    | Hash   of k
    | End    of Hash.t
    | Exception of error
  and res =
    | Flush of t
    | Error of t * error
    | Cont of t
    | Ok of t * Hash.t
  and kind =
    | KindOffset
    | KindHash
    | KindRaw

  let pp = Format.fprintf

  let flush dst t =
    Hash.feed t.hash (Cstruct.to_bigarray (Cstruct.sub dst t.o_off t.o_pos));
    Flush t

  let error t exn = Error ({ t with state = Exception exn }, exn)
  let ok t hash   = Ok ({ t with state = End hash }, hash)

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

  let rec put_byte ~ctor byte k dst t =
    if (t.o_len - t.o_pos) > 0
    then begin
      Cstruct.set_uint8 dst (t.o_off + t.o_pos) byte;
      k dst { t with o_pos = t.o_pos + 1
                    ; write = Int64.add t.write 1L }
    end else flush dst { t with state = ctor (fun dst t -> put_byte ~ctor byte k dst t) }

  module KWriteK =
  struct
    let put_byte = put_byte ~ctor:(fun k -> WriteK k)

    let tmp_header = Bytes.create 10

    let header kind len crc k dst t =
      let byt = ref ((kind lsl 4) lor Int64.(to_int (len && 15L))) in
      let len = ref Int64.(len >> 4) in
      let pos = ref 0 in

      while !len <> 0L
      do
        Bytes.set tmp_header !pos (Char.unsafe_chr (!byt lor 0x80));
        byt := Int64.(to_int (!len && 0x7FL));
        len := Int64.(!len >> 7);
        pos := !pos + 1;
      done;

      Bytes.set tmp_header !pos (Char.unsafe_chr !byt);
      pos := !pos + 1;

      let rec loop idx crc dst t =
        if idx < !pos
        then
          let byte = Char.code (Bytes.get tmp_header idx) in
          let crc = Crc32.digestc crc byte in

          put_byte byte (loop (idx + 1) crc) dst t
        else k crc dst t
      in

      loop 0 crc dst t

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
        off := Int64.sub !off 1L;
      done;

      let rec loop idx crc dst t =
        if idx = 10
        then k crc dst t
        else
          let byte = Char.code (Bytes.get tmp_offset idx) in
          let crc = Crc32.digestc crc byte in

          put_byte byte (loop (idx + 1) crc) dst t
      in

      loop !pos crc dst t

    let hash hash crc k dst t =
      if t.o_len - t.o_pos >= Hash.length
      then begin
        let crc = Crc32.digest crc hash in

        Cstruct.blit hash 0 dst (t.o_off + t.o_pos) Hash.length;

        k crc dst { t with o_pos = t.o_pos + Hash.length
                         ; write = Int64.add t.write (Int64.of_int Hash.length) }
      end else
        let rec loop rest crc dst t =
          if rest = 0
          then k crc dst t
          else
            let n = min rest (t.o_len - t.o_pos) in

            if n = 0
            then flush dst { t with state = Hash (loop rest crc) }
            else begin
              let crc = Crc32.digest crc ~off:(Hash.length - rest) ~len:n hash in

              Cstruct.blit hash (Hash.length - rest) dst (t.o_off + t.o_pos) n;

              loop (rest - n) crc dst { t with o_pos = t.o_pos + n
                                             ; write = Int64.add t.write (Int64.of_int n) }
            end
        in

        loop Hash.length crc dst t
  end

  module KHash =
  struct
    let rec put_byte byte k dst t =
      if (t.o_len - t.o_pos) > 0
      then begin
        Cstruct.set_uint8 dst (t.o_off + t.o_pos) byte;
        k dst { t with o_pos = t.o_pos + 1
                     ; write = Int64.add t.write 1L }
      end else Flush { t with state = Hash (put_byte byte k) }

    let put_hash hash k dst t =
      if t.o_len - t.o_pos >= Hash.length
      then begin
        Cstruct.blit hash 0 dst (t.o_off + t.o_pos) Hash.length;
        k dst { t with o_pos = t.o_pos + Hash.length
                     ; write = Int64.add t.write (Int64.of_int Hash.length) }
      end else
        let rec loop rest dst t =
          if rest = 0
          then k dst t
          else
            let n = min rest (t.o_len - t.o_pos) in

            if n = 0
            then Flush { t with state = Hash (loop rest) }
            else begin
              Cstruct.blit hash (Hash.length - rest) dst (t.o_off + t.o_pos) n;
              Flush { t with state = Hash (loop (rest - n))
                           ; o_pos = t.o_pos + n
                           ; write = Int64.add t.write (Int64.of_int n) }
            end
        in

        loop Hash.length dst t
  end

  let hash dst t =
    Hash.feed t.hash (Cstruct.to_bigarray (Cstruct.sub dst t.o_off t.o_pos));
    let hash = Hash.get t.hash in

    KHash.put_hash (Cstruct.of_bigarray hash) (fun dst t -> ok t hash) dst t

  let writek kind entry entry_raw entry_delta rest dst t =
    match kind, entry_delta with
    | KindRaw, { Delta.delta = Delta.Z } ->
      let abs_off = t.write in

      Format.eprintf "%a: Raw\n%!" Hash.pp entry.Entry.hash_object;

      (KWriteK.header (Kind.to_bin entry.Entry.kind) entry.Entry.length Crc32.default
       @@ fun crc dst t ->
       let z = Deflate.default ~proof:Decompress.B.proof_bigstring 4 in
       let z = Deflate.flush (t.o_off + t.o_pos) (t.o_len - t.o_pos) z in

       Cont { t with state = WriteZ { x = (entry, entry_raw)
                                    ; r = rest
                                    ; crc
                                    ; off = abs_off
                                    ; ui = 0
                                    ; z } })
      dst t

    | KindHash, { Delta.delta = Delta.S { length; depth; hunks; src; src_length; src_hash; } } ->
      let hunks      = Hunk.from_rabin hunks entry_raw in
      let trg_length = Cstruct.len entry_raw in
      let abs_off    = t.write in

      (* XXX(dinosaure): we can obtain the source hash by [entry.delta]. TODO! *)

      Format.eprintf "%a: Hash from %a (source length: %Ld, target length: %d)\n%!"
        Hash.pp entry.Entry.hash_object Hash.pp src_hash src_length trg_length;

      let h =
        HunkEncoder.flush 0 (Cstruct.len t.h_tmp)
        @@ HunkEncoder.default (HunkEncoder.Hash src_hash) (Int64.to_int src_length) trg_length hunks
      in

      (KWriteK.header 0b111 (Int64.of_int length) Crc32.default
       @@ fun crc -> KWriteK.hash (Cstruct.of_bigarray src_hash) crc
       @@ fun crc dst t ->
       let z = Deflate.default ~proof:Decompress.B.proof_bigstring 4 in
       let z = Deflate.flush (t.o_off + t.o_pos) (t.o_len - t.o_pos) z in

       Cont { t with state = WriteH { x = (entry, entry_delta, entry_raw)
                                    ; r = rest
                                    ; crc
                                    ; off = abs_off
                                    ; ui = 0
                                    ; h
                                    ; z } })
        dst t

    | KindOffset, { Delta.delta = Delta.S { length; depth; hunks; src; src_length; src_hash; } } ->
      (match Radix.lookup t.radix src_hash with
       | Some (src_crc, src_off) ->

         let hunks      = Hunk.from_rabin hunks entry_raw in
         let trg_length = Cstruct.len entry_raw in
         let abs_off    = t.write in
         let rel_off    = Int64.sub abs_off src_off in

         Format.eprintf "%a: Offset from %Ld:%Ld (source length: %Ld, target length: %d)\n%!"
           Hash.pp entry.Entry.hash_object rel_off abs_off src_length trg_length;

         let h =
           HunkEncoder.flush 0 (Cstruct.len t.h_tmp)
           @@ HunkEncoder.default (HunkEncoder.Offset rel_off) (Int64.to_int src_length) trg_length hunks
         in

         (KWriteK.header 0b110 (Int64.of_int length) Crc32.default
          @@ fun crc -> KWriteK.offset rel_off crc
          @@ fun crc dst t ->
          let z = Deflate.default ~proof:Decompress.B.proof_bigstring 4 in
          let z = Deflate.flush (t.o_off + t.o_pos) (t.o_len - t.o_pos) z in

          Cont { t with state = WriteH { x = (entry, entry_delta, entry_raw)
                                       ; r = rest
                                       ; crc
                                       ; off = abs_off
                                       ; ui = 0
                                       ; h
                                       ; z } })
           dst t
       | None -> assert false)

    | _, _ -> assert false

  (* XXX(dinosaure): the last cases can't appear. in [iter] we check than when
                     we have [KindOffset], the source entry is saved in the
                     radix tree and [iter] catch all other cases and move the
                     state to an error.
   *)

  let writez dst t ((entry, entry_raw) as x) r crc off used_in z =
    let src' = Decompress.B.from_bigstring (Cstruct.to_bigarray entry_raw) in
    let dst' = Decompress.B.from_bigstring (Cstruct.to_bigarray dst) in

    (* assert than [entry_raw] is physically the same and immutable at each call of [writez].
       that means the deflate state compute all the time the same [src'] buffer.
       so, [used_in] is equivalent semantically for each call.
     *)

    let rec loop used_in z = match Deflate.eval src' dst' z with
      | `Await z ->
        if used_in = Cstruct.len entry_raw
        then loop used_in (Deflate.finish z)
        else loop (Deflate.used_in z) (Deflate.no_flush (Deflate.used_in z) (Cstruct.len entry_raw - Deflate.used_in z) z)
      | `Flush z ->
        let crc = Crc32.digest ~off:(t.o_off + t.o_pos) ~len:(Deflate.used_out z) crc dst in

        flush dst { t with state = WriteZ { x; r; crc; off; ui = (Deflate.used_in z); z; }
                         ; o_pos = t.o_pos + (Deflate.used_out z)
                         ; write = Int64.add t.write (Int64.of_int (Deflate.used_out z)) }
      | `End z ->
        let crc = Crc32.digest ~off:(t.o_off + t.o_pos) ~len:(Deflate.used_out z) crc dst in

        Cont { t with state = Save { x = entry; r; crc; off; }
                    ; o_pos = t.o_pos + (Deflate.used_out z)
                    ; write = Int64.add t.write (Int64.of_int (Deflate.used_out z)) }
      | `Error (z, exn) -> error t (Deflate_error exn)
    in

    loop used_in z

  let writeh dst t ((entry, entry_delta, entry_raw) as x) r crc off used_in h z =
    let src' = Decompress.B.from_bigstring (Cstruct.to_bigarray t.h_tmp) in
    let dst' = Decompress.B.from_bigstring (Cstruct.to_bigarray dst) in

    match Deflate.eval src' dst' z with
    | `Await z ->
      (match HunkEncoder.eval t.h_tmp h with
       | `Flush h ->
         let used_in' = used_in + Deflate.used_in z in
           (* ce qu'a consommé [zlib] ou [decompress] entre [used_in] et [HunkEncoder.used_out h]:
              - dans le cas de [decompress], [used_in] retournera toujours le nombre de bytes qu'à écrit [HunkEncoder]
              - dans le cas de [zlib], [used_in] peut retourner un nombre inférieur à ce qu'à donné [HunkDecoder]

              tant que [used_in] ne sera pas équivalent à [HunkEncoder.used_out], [t.h_tmp] est considéré comme physiquement le même et immutable.
           *)

         let z, h, ui =
           if used_in' = HunkEncoder.used_out h
           then Deflate.no_flush 0 0 z, HunkEncoder.flush 0 (Cstruct.len t.h_tmp) h, 0
           else Deflate.no_flush used_in' (HunkEncoder.used_out h - used_in') z, h, used_in'
         in

         (* on assure que, dans ce contexte, [HunkEncoder.used_out] est constant.
            en effet, tant qu'on aura pas tout consommé, [h] reste exactement le même.
          *)

         Cont { t with state = WriteH { x; r; crc; off; ui; h; z; } }
       | `End h ->
         let used_in' = used_in + Deflate.used_in z in

         let z, h, ui =
           if used_in' = HunkEncoder.used_out h
           then Deflate.finish z, h, used_in'
           else Deflate.no_flush used_in' (HunkEncoder.used_out h - used_in') z, h, used_in'
         in

         Cont { t with state = WriteH { x; r; crc; off; ui; h; z; } }
       | `Error (h, exn) -> error t (Hunk_error exn))
    | `Flush z ->
      let crc = Crc32.digest ~off:(t.o_off + t.o_pos) ~len:(Deflate.used_out z) crc dst in
      let used_in' = used_in + Deflate.used_in z in

      flush dst { t with state = WriteH { x; r; crc; off; ui = used_in'; h; z; }
                       ; o_pos = t.o_pos + (Deflate.used_out z)
                       ; write = Int64.add t.write (Int64.of_int (Deflate.used_out z)) }
    | `End z ->
      let crc = Crc32.digest ~off:(t.o_off + t.o_pos) ~len:(Deflate.used_out z) crc dst in

      Cont { t with state = Save { x = entry; r; crc; off; }
                  ; o_pos = t.o_pos + (Deflate.used_out z)
                  ; write = Int64.add t.write (Int64.of_int (Deflate.used_out z)) }
    | `Error (z, exn) -> error t (Deflate_error exn)

  let iter lst dst t = match lst with
    | [] -> Cont { t with state = Hash hash }
    | (entry, delta) :: r ->
      match entry.Entry.delta, delta, t.access entry.Entry.hash_object with
      | Entry.From src_hash, { Delta.delta = Delta.S _ }, Some raw ->
        if Radix.exists t.radix src_hash
        then Cont { t with state = WriteK (writek KindOffset entry raw delta r) }
        else Cont { t with state = WriteK (writek KindHash entry raw delta r) }
      | Entry.None, { Delta.delta = Delta.Z }, Some raw ->
        Cont { t with state = WriteK (writek KindRaw entry raw delta r) }
      | _, _, Some _ -> error t (Invalid_entry (entry, delta))
      | _, _, None -> error t (Invalid_hash entry.Entry.hash_object)

  let save dst t x r crc off =
    Cont { t with state = Object (iter r)
                ; radix = Radix.bind t.radix x.Entry.hash_object (crc, off) }

  let number lst dst t =
    (* XXX(dinosaure): problem in 32-bits architecture. TODO! *)
    KHeader.put_u32 (Int32.of_int (List.length lst))
      (fun dst t -> Cont { t with state = Object (iter lst) })
      dst t

  let version lst dst t =
    KHeader.put_u32 2l (fun dst t -> Cont { t with state = Header (number lst) }) dst t

  let header lst dst t =
    (KHeader.put_byte (Char.code 'P')
     @@ KHeader.put_byte (Char.code 'A')
     @@ KHeader.put_byte (Char.code 'C')
     @@ KHeader.put_byte (Char.code 'K')
     @@ fun dst t -> Cont { t with state = Header (version lst) })
      dst t

  let used_out t = t.o_pos

  let idx t = t.radix

  let eval dst t =
    let eval0 t = match t.state with
      | Header k -> k dst t
      | Object k -> k dst t
      | WriteK k -> k dst t
      | WriteZ { x; r; crc; off; ui; z; } -> writez dst t x r crc off ui z
      | WriteH { x; r; crc; off; ui; h; z; } -> writeh dst t x r crc off ui h z
      | Save { x; r; crc; off; } -> save dst t x r crc off
      | Exception exn -> error t exn
      | Hash k -> k dst t
      | End hash -> Ok (t, hash)
    in

    let rec loop t =
      match eval0 t with
      | Cont t -> loop t
      | Flush t -> `Flush t
      | Ok (t, hash) -> `End (t, hash)
      | Error (t, exn) -> `Error (t, exn)
    in

    loop t

  let flush offset len t =
    if (t.o_len - t.o_pos) = 0
    then
      match t.state with
      | WriteZ { x; r; crc; off; ui; z; } ->
        { t with o_off = offset
               ; o_len = len
               ; o_pos = 0
               ; state = WriteZ { x; r; crc; off; ui; z = Deflate.flush offset len z } }
      | WriteH { x; r; crc; off; ui; h; z; } ->
        { t with o_off = offset
               ; o_len = len
               ; o_pos = 0
               ; state = WriteH { x; r; crc; off; ui; h; z = Deflate.flush offset len z } }
      | _ ->
        { t with o_off = offset
               ; o_len = len
               ; o_pos = 0 }
    else raise (Invalid_argument (Format.sprintf "PACKEncoder.flush: you lost something (pos: %d, len: %d)" t.o_pos t.o_len))

  let default h_tmp access objects =
    { o_off = 0
    ; o_pos = 0
    ; o_len = 0
    ; write = 0L
    ; radix = Radix.empty
    ; access
    ; h_tmp
    ; hash = Hash.init ()
    ; state = (Header (header objects)) }
end
