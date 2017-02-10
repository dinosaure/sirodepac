let () = Printexc.record_backtrace true

external swap32 : int32 -> int32 = "%bswap_int32"
external swap64 : int64 -> int64 = "%bswap_int64"

let pp_hash fmt s =
  for i = 0 to String.length s - 1
  do Format.fprintf fmt "%02x" (Char.code @@ String.get s i) done

let sp = Format.sprintf

let bin_of_int d =
  if d < 0 then invalid_arg "bin_of_int" else
  if d = 0 then "0" else
  let rec aux acc d =
    if d = 0 then acc else
    aux (string_of_int (d land 1) :: acc) (d lsr 1)
  in
  String.concat "" (aux [] d)

module T =
struct
  type hash_compare =
    | Eq
    | Prefix
    | Contain
    | Inf of int
    | Sup of int

  let rec hash_cmp s1 p1 l1 s2 p2 l2 =
    if p1 = l1
    then (if p2 = l2 then Eq else Prefix)
    else (if p2 = l2 then Contain else
          let c1 = s1.[p1] in
          let c2 = s2.[p2] in
          if c1 = c2
          then hash_cmp s1 (p1 + 1) l1 s2 (p2 + 1) l2
          else if c1 < c2 then Inf p1
          else (* c1 > c2 *) Sup p1)

  let rec critbit p c1 c2 =
    if (c1 land 128) <> (c2 land 128)
    then p
    else critbit (p - 1) (c1 lsl 1) (c2 lsl 1)

  let critbit c1 c2 =
    if c1 = c2
    then raise Not_found
    else critbit 7 (int_of_char c1) (int_of_char c2)

  type t' =
    | L of string * Int64.t
    | T of t' * string * Int64.t
    | B of t' * t' * int * int

  type t = t' option

  let empty = None

  let rec first_key t =
    match t with
    | L (k, _) -> k
    | T (_, k, _) -> k
    | B (l, r, _, _) ->
      first_key l

  let rec bind t s sl v =
    match t with
    | L (k, d) ->
      let kl = String.length k in
      (match hash_cmp s 0 sl k 0 kl with
       | Eq -> L (s, v)
       | Prefix -> T (t, s, v)
       | Contain -> T (L (s, v), k, d)
       | Inf p ->
         let b = critbit s.[p] k.[p] in
         B (L (s, v), t, p, b)
       | Sup p ->
         let b = critbit s.[p] k.[p] in
         B (t, L (s, v), p, b))
    | T (m, k, d) ->
      let kl = String.length k in
      if kl = sl
      then T (m, k, v)
      else bind m s sl v
    | B (l, r, i, b) ->
      if sl > i
      then (if (int_of_char s.[i] land (1 lsl b)) = 0
            then B (bind l s sl v, r, i, b)
            else B (l, bind r s sl v, i, b))
      else let k = first_key l in
           match hash_cmp s 0 sl k 0 sl with
           | Eq | Prefix -> T (t, s, v)
           | Contain -> assert false
           | Inf p ->
             let bn = critbit s.[p] k.[p] in
             B (L (s, v), t, p, bn)
           | Sup p ->
             let bn = critbit s.[p] k.[p] in
             B (t, L (s, v), p, bn)

  let bind t s v =
    match t with
    | None -> Some (L (s, v))
    | Some t ->
      let sl = String.length s in
      Some (bind t s sl v)

  let rec iter f t = match t with
    | L (k, v) -> f k v
    | T (m, k, d) ->
      f k d; iter f m
    | B (l, r, _, _) ->
      iter f l; iter f r

  let iter f = function
    | Some t -> iter f t
    | None -> ()

  let rec lookup t s sl =
    match t with
    | L (k, v) -> if s = k then Some v else None
    | T (m, k, v) ->
      let kl = String.length k in
      if kl < sl
      then lookup m s sl
      else if kl > sl
      then None
      else (* kl = sl *) (if s = k then Some v else None)
    | B (l, r, i, b) ->
      if sl > i
      then let dir = if ((int_of_char s.[i]) land (1 lsl b)) = 0 then l else r in
           lookup dir s sl
      else None

  let lookup t s =
    match t with
    | None -> None
    | Some t ->
      let sl = String.length s in
      lookup t s sl
end

module B = Zlib.B

(** Implementation of deserialization of a list of hunks (from a PACK file) *)
module H =
struct
  type error = ..
  type error += Reserved_opcode of int
  type error += Wrong_insert_hunk of int * int * int

  type 'i t =
    { i_off         : int
    ; i_pos         : int
    ; i_len         : int
    ; read          : int
    ; _length        : int
    ; _reference     : reference
    ; _source_length : int
    ; _target_length : int
    ; _hunks         : 'i hunk list
    ; state         : 'i state }
  and 'i state =
    | Header    of ('i B.t -> 'i t -> 'i res)
    | List      of ('i B.t -> 'i t -> 'i res)
    | Is_insert of ('i B.t * int * int)
    | Is_copy   of ('i B.t -> 'i t -> 'i res)
    | End
    | Exception of error
  and 'i res =
    | Wait   of 'i t
    | Error  of 'i t * error
    | Cont   of 'i t
    | Ok     of 'i t * 'i hunks
  and 'i hunk =
    | Insert of 'i B.t
    | Copy   of int * int
  and reference =
    | Offset of int
    | Hash   of string
  and 'i hunks =
    { reference     : reference
    ; hunks         : 'i hunk list
    ; length        : int
    ; source_length : int
    ; target_length : int }

  let pp = Format.fprintf

  let pp_lst ~sep pp_data fmt lst =
    let rec aux = function
      | [] -> ()
      | [ x ] -> pp_data fmt x
      | x :: r -> pp fmt "%a%a" pp_data x sep (); aux r
    in aux lst

  let pp_obj fmt = function
    | Insert buf ->
      pp fmt "(Insert %a)" B.pp buf
    | Copy (off, len) ->
      pp fmt "(Copy (%d, %d))" off len

  let pp_error fmt = function
    | Reserved_opcode byte -> pp fmt "(Reserved_opcode %02x)" byte
    | Wrong_insert_hunk (off, len, source) ->
      pp fmt "(Wrong_insert_hunk (off: %d, len: %d, source: %d))" off len source

  let pp_reference fmt = function
    | Hash hash -> pp fmt "(Reference %s)" hash
    | Offset off -> pp fmt "(Offset %d)" off

  let pp_hunks fmt ({ reference; hunks; length; source_length; target_length; } : 'i hunks) =
    pp fmt "{@[<hov>reference = %a;@ \
                    hunks = [@[<hov>%a@]];@ \
                    length = %d;@ \
                    source_length = %d;@ \
                    target_length = %d]}"
      pp_reference reference
      (pp_lst ~sep:(fun fmt () -> pp fmt ";@ ") pp_obj) hunks
      length source_length target_length

  let pp fmt { i_off; i_pos; i_len; read; _length; _reference;
               _source_length; _target_length; _hunks; state; } =
    pp fmt "{@[<hov>i_off = %d;@ \
                    i_pos = %d;@ \
                    i_len = %d;@ \
                    read = %d;@ \
                    length = %d;@ \
                    reference = %a;@ \
                    source_length = %d;@ \
                    target_length = %d;@ \
                    hunks = [@[<hov>%a@]]@]}"
      i_off i_pos i_len read _length pp_reference _reference
      _source_length _target_length
      (pp_lst ~sep:(fun fmt () -> pp fmt ";@ ") pp_obj) _hunks

  let await t     = Wait t
  let error t exn = Error ({ t with state = Exception exn }, exn)
  let ok t        = Ok ({ t with state = End },
                          { reference     = t._reference
                          ; hunks         = t._hunks
                          ; length        = t._length
                          ; source_length = t._source_length
                          ; target_length = t._target_length })

  module KHeader =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src
             { t with i_pos = t.i_pos + 1
                    ; read = t.read + 1 }
      else await { t with state = Header (fun src t -> (get_byte[@tailcall]) k src t) }

    let rec length msb (len, bit) k src t = match msb with
      | true ->
        get_byte
          (fun byte src t ->
             let msb = byte land 0x80 <> 0 in
             (length[@tailcall]) msb (((byte land 0x7F) lsl bit) lor len, bit + 7)
             k src t)
          src t
      | false -> k len src t

    let length k src t =
      get_byte
        (fun byte src t ->
           let msb = byte land 0x80 <> 0 in
           length msb ((byte land 0x7F), 7) k src t)
        src t
  end

  module KList =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src 
             { t with i_pos = t.i_pos + 1
                    ; read = t.read + 1 }
      else await { t with state = List (fun src t -> (get_byte[@tailcall]) k src t) }
  end

  let rec copy opcode =
    let rec get_byte flag k src t =
      if not flag
      then k 0 src t
      else if (t.i_len - t.i_pos) > 0
           then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
                k byte src 
                  { t with i_pos = t.i_pos + 1
                         ; read = t.read + 1 }
           else await { t with state = Is_copy (fun src t -> (get_byte[@tailcall]) flag k src t) }
    in

    get_byte ((opcode lsr 0) land 1 <> 0)
    @@ fun o0 -> get_byte ((opcode lsr 1) land 1 <> 0)
    @@ fun o1 -> get_byte ((opcode lsr 2) land 1 <> 0)
    @@ fun o2 -> get_byte ((opcode lsr 3) land 1 <> 0)
    @@ fun o3 -> get_byte ((opcode lsr 4) land 1 <> 0)
    @@ fun l0 -> get_byte ((opcode lsr 5) land 1 <> 0)
    @@ fun l1 -> get_byte ((opcode lsr 6) land 1 <> 0)
    @@ fun l2 src t ->
      let dst = o0 lor (o1 lsl 8) lor (o2 lsl 16) lor (o3 lsl 24) in
      let len =
        let len0 = l0 lor (l1 lsl 8) lor (l2 lsl 16) in
        if len0 = 0 then 0x10000 else len0
        (* XXX: see git@07c92928f2b782330df6e78dd9d019e984d820a7
           patch-delta.c:l. 50 *)
      in

      if dst + len > t._source_length
      then error t (Wrong_insert_hunk (dst, len, t._source_length))
      else Cont { t with _hunks = (Copy (dst, len)) :: t._hunks
                       ; state = List list }

  and list src t =
    if t.read < t._length
    then KList.get_byte
           (fun opcode src t ->
             if opcode = 0 then error t (Reserved_opcode opcode)
             else match opcode land 0x80 with
             | 0 -> Cont { t with state = Is_insert (B.from ~proof:src opcode, 0, opcode) }
             | _ -> Cont { t with state = Is_copy (copy opcode) })
           src t
    else ok { t with _hunks = List.rev t._hunks }

  let insert src t buffer (off, rest) =
    let n = min (t.i_len - t.i_pos) rest in
    B.blit src (t.i_off + t.i_pos) buffer off n;
    if rest - n = 0
    then Cont ({ t with _hunks = (Insert buffer) :: t._hunks
                      ; i_pos = t.i_pos + n
                      ; read = t.read + n
                      ; state = List list })
    else await { t with i_pos = t.i_pos + n
                      ; read = t.read + n
                      ; state = Is_insert (buffer, off + n, rest - n) }

  let header src t =
    (KHeader.length
     @@ fun _source_length -> KHeader.length
     @@ fun _target_length src t ->
        Cont ({ t with state = List list
                     ; _source_length
                     ; _target_length }))
    src t

  let eval src t =
    let eval0 t = match t.state with
      | Header k -> k src t
      | List k -> k src t
      | Is_insert (buffer, off, rest) -> insert src t buffer (off, rest)
      | Is_copy k -> k src t
      | End -> ok t
      | Exception exn -> error t exn
    in

    let rec loop t =
      match eval0 t with
      | Cont t -> loop t
      | Wait t -> `Await t
      | Error (t, exn) -> `Error (t, exn)
      | Ok (t, objs) -> `Ok (t, objs)
    in

    loop t

  let default _length _reference =
    { i_off = 0
    ; i_pos = 0
    ; i_len = 0
    ; read  = 0
    ; _length
    ; _reference
    ; _source_length = 0
    ; _target_length = 0
    ; _hunks = []
    ; state = Header header }

  let sp = Format.sprintf

  let refill off len t =
    if t.i_pos = t.i_len 
    then { t with i_off = off
                ; i_len = len
                ; i_pos = 0 }
    else raise (Invalid_argument (sp "H.refill: you lost something"))

  let available_in t = t.i_len
  let used_in t = t.i_pos
  let read t = t.read
end

(** This module  provides a  Random Access  Memory from  an IDX  file.  From any
    hashes,  the function [find] get the offset  of the git object.  We keep the
    IDX file with a ['a B.t] as long as you use [find].
*)
module RAI =
struct
  type error = ..
  type error += Invalid_header of string
  type error += Invalid_version of Int32.t
  type error += Invalid_index

  let pp = Format.fprintf
  let pp_error fmt = function
    | Invalid_header header -> pp fmt "(Invalid_header %s)" header
    | Invalid_version version -> pp fmt "(Invalid_version %ld)" version
    | Invalid_index -> pp fmt "Invalid_index"

  type 'a t =
    { map : 'a B.t
    ; fanout_offset : int
    ; hashes_offset : int
    ; crcs_offset   : int
    ; values_offset : int
    ; v64_offset    : int option
    ; cache         : Int64.t LRU.t }

  let has map off len =
    if (off < 0 || len < 0 || off + len > B.length map)
    then raise (Invalid_argument (Printf.sprintf "%d:%d:%d" off len (B.length
    map)))
    else true

  let check_header map =
    if has map 0 4
    then (if B.get map 0 = '\255'
          && B.get map 1 = '\116'
          && B.get map 2 = '\079'
          && B.get map 3 = '\099'
          then Ok ()
          else Error (Invalid_header (B.to_string @@ B.sub map 0 4)))
    else Error Invalid_index

  let check_version map =
    if has map 4 4
    then (if swap32 @@ B.get_u32 map 4 = 2l
          then Ok ()
          else Error (Invalid_version (swap32 @@ B.get_u32 map 4)))
    else Error Invalid_index

  let number_of_hashes map =
    if has map 8 (256 * 4)
    then let n = swap32 @@ B.get_u32 map (8 + (255 * 4)) in
         Ok (8, n)
    else Error Invalid_index

  let bind v f = match v with Ok v -> f v | Error _ as e -> e
  let ( >>= ) = bind
  let ( *> ) u v = match u with Ok _ -> v | Error _ as e -> e

  let make map =
    check_header map
    *> check_version map
    *> number_of_hashes map
    >>= fun (fanout_offset, number_of_hashes) ->
      let number_of_hashes = Int32.to_int number_of_hashes in
      let hashes_offset = 8 + (256 * 4) in
      let crcs_offset   = 8 + (256 * 4) + (number_of_hashes * 20) in
      let values_offset = 8 + (256 * 4) + (number_of_hashes * 20) + (number_of_hashes * 4) in

      Ok { map
         ; fanout_offset
         ; hashes_offset
         ; crcs_offset
         ; values_offset
         ; v64_offset = None
         ; cache = LRU.make 1024 }

  exception Break

  let compare buf off hash =
    try for i = 0 to 19
        do if B.get buf (off + i) <> String.get hash i
           then raise Break
        done; true
    with Break -> false

  exception ReturnT
  exception ReturnF

  let lt buf off hash =
    try for i = 0 to 19
        do let a = Char.code @@ B.get buf (off + i) in
           let b = Char.code @@ String.get hash i in

           if a > b
           then raise ReturnT
           else if a <> b then raise ReturnF;
        done; false
    with ReturnT -> true
       | ReturnF -> false

  let binary_search buf hash =
    let rec aux off len buf =
      if len = 20
      then begin
        (off / 20)
      end else
        let len' = ((len / 40) * 20) in
        let off' = off + len' in

        if compare buf off' hash
        then (off' / 20)
        else if lt buf off' hash then aux off len' buf
        else aux off' (len - len') buf
    in

    aux 0 (B.length buf) buf

  let fanout_idx t hash =
    let idx = Char.code @@ String.get hash 0 in
    if has t.map (t.fanout_offset + (4 * idx)) 4
    && has t.map (t.fanout_offset + (4 * (idx + 1))) 4
    then let off1 = Int32.to_int @@ swap32 @@ B.get_u32 t.map (t.fanout_offset + (4 * idx)) in
         let off0 =
           if idx = 0 then 0
           else Int32.to_int
                @@ swap32
                @@ B.get_u32 t.map (t.fanout_offset + (4 * (idx - 1))) in

         if has t.map (t.hashes_offset + (off0 * 20)) ((off1 - off0) * 20)
         then Ok (binary_search (B.sub t.map (t.hashes_offset + (off0 * 20)) ((off1 - off0) * 20)) hash + off0)
         else Error Invalid_index
    else Error Invalid_index

  let find t hash =
    match LRU.find t.cache hash with
    | Some offset -> Some offset
    | None ->
      match fanout_idx t hash with
      | Ok idx ->
        let off = swap32 @@ B.get_u32 t.map (t.values_offset + (idx * 4)) in
        Some (Int64.of_int32 off)
      | Error _ -> None
end

(** Implementation of deserialization of an IDX file

    Provides a binding between hash and offset. You can use theses bindings as a
    Patricia-tree (see {module T}) and close (and free) the IDX file. Otherwise,
    you need to keep the IDX file.
*)
module I =
struct
  type error = ..
  type error += Invalid_byte of int
  type error += Invalid_version of Int32.t
  type error += Invalid_index_of_bigoffset of int
  type error += Expected_bigoffset_table

  let pp = Format.fprintf
  let pp_error fmt = function
    | Invalid_byte byte -> pp fmt "(Invalid_byte %02x)" byte
    | Invalid_version version -> pp fmt "(Invalid_version %ld)" version
    | Invalid_index_of_bigoffset idx -> pp fmt "(Invalid_index_of_bigoffset %d)" idx
    | Expected_bigoffset_table -> pp fmt "Expected_bigoffset_table"

  type 'i t =
    { i_off   : int
    ; i_pos   : int
    ; i_len   : int
    ; fanout  : Int32.t array
    ; hashes  : string Queue.t
    ; crcs    : Int32.t Queue.t
    ; offsets : (Int32.t * bool) Queue.t
    ; state   : 'i state }
  and 'i state =
    | Header    of ('i B.t -> 'i t -> 'i res)
    | Fanout    of ('i B.t -> 'i t -> 'i res)
    | Hash      of ('i B.t -> 'i t -> 'i res)
    | Crc       of ('i B.t -> 'i t -> 'i res)
    | Offset    of ('i B.t -> 'i t -> 'i res)
    | Ret
    | End
    | Exception of error
  and 'i res =
    | Wait   of 'i t
    | Error  of 'i t * error
    | Cont   of 'i t
    | Result of 'i t * (string * Int32.t * Int64.t)
    | Ok     of 'i t

  let await t     = Wait t
  let error t exn = Error ({ t with state = Exception exn }, exn)
  let ok t        = Ok { t with state = End }

  let to_int32 b0 b1 b2 b3 =
    let (<<) = Int32.shift_left in
    let (||) = Int32.logor in
    (Int32.of_int b0 << 24)
    || (Int32.of_int b1 << 16)
    || (Int32.of_int b2 << 8)
    || (Int32.of_int b3)

  let to_int64 b0 b1 b2 b3 b4 b5 b6 b7 =
    let (<<) = Int64.shift_left in
    let (||) = Int64.logor in
    (Int64.of_int b0 << 56)
    || (Int64.of_int b1 << 48)
    || (Int64.of_int b2 << 40)
    || (Int64.of_int b3 << 32)
    || (Int64.of_int b4 << 24)
    || (Int64.of_int b5 << 16)
    || (Int64.of_int b6 << 8)
    || (Int64.of_int b7)

  module KHeader =
  struct
    let rec check_byte chr k src t =
      if (t.i_len - t.i_pos) > 0 && B.get src (t.i_off + t.i_pos) = chr
      then k src { t with i_pos = t.i_pos + 1 }
      else if (t.i_len - t.i_pos) = 0
      then await { t with state = Header (fun src t -> (check_byte[@tailcall]) chr k src t) }
      else error { t with i_pos = t.i_pos + 1 }
                 (Invalid_byte (Char.code @@ B.get src (t.i_off + t.i_pos)))

    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1 }
      else await { t with state = Header (fun src t -> (get_byte[@tailcall]) k src t) }

    let rec get_u32 k src t =
      if (t.i_len - t.i_pos) > 3
      then let num = B.get_u32 src (t.i_off + t.i_pos) in
           k (swap32 num) src
             { t with i_pos = t.i_pos + 4 }
      else if (t.i_len - t.i_pos) > 0
      then (get_byte
            @@ fun byte0 -> get_byte
            @@ fun byte1 -> get_byte
            @@ fun byte2 -> get_byte
            @@ fun byte3 src t ->
               k (to_int32 byte0 byte1 byte2 byte3) src t)
            src t
      else await { t with state = Header (fun src t -> (get_u32[@tailcall]) k src t) }
  end

  module KFanoutTable =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1 }
      else await { t with state = Fanout (fun src t -> (get_byte[@tailcall]) k src t) }

    let rec get_u32 k src t =
      if (t.i_len - t.i_pos) > 3
      then let num = B.get_u32 src (t.i_off + t.i_pos) in
           k (swap32 num) src
             { t with i_pos = t.i_pos + 4 }
      else if (t.i_len - t.i_pos) > 0
      then (get_byte
            @@ fun byte0 -> get_byte
            @@ fun byte1 -> get_byte
            @@ fun byte2 -> get_byte
            @@ fun byte3 src t ->
               k (to_int32 byte0 byte1 byte2 byte3) src t)
            src t
      else await { t with state = Fanout (fun src t -> (get_u32[@tailcall]) k src t) }
  end

  module KHash =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1 }
      else await { t with state = Hash (fun src t -> (get_byte[@tailcall]) k src t) }

    let get_hash k src t =
      let buf = Buffer.create 20 in

      let rec loop i src t =
        if i = 20
        then k (Buffer.contents buf) src t
        else
          get_byte (fun byte src t ->
                     Buffer.add_char buf (Char.chr byte);
                     (loop[@tailcall]) (i + 1) src t)
            src t
      in

      loop 0 src t
  end

  module KCrc =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1 }
      else await { t with state = Crc (fun src t -> (get_byte[@tailcall]) k src t) }

    let rec get_u32 k src t =
      if (t.i_len - t.i_pos) > 3
      then let num = B.get_u32 src (t.i_off + t.i_pos) in
           k (swap32 num) src
             { t with i_pos = t.i_pos + 4 }
      else if (t.i_len - t.i_pos) > 0
      then (get_byte
            @@ fun byte0 -> get_byte
            @@ fun byte1 -> get_byte
            @@ fun byte2 -> get_byte
            @@ fun byte3 src t ->
               k (to_int32 byte0 byte1 byte2 byte3) src t)
            src t
      else await { t with state = Crc (fun src t -> (get_u32[@tailcall]) k src t) }
  end

  module KOffset =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1 }
      else await { t with state = Crc (fun src t -> (get_byte[@tailcall]) k src t) }

    let rec get_u32 k src t =
      if (t.i_len - t.i_pos) > 3
      then let num = B.get_u32 src (t.i_off + t.i_pos) in
           k (swap32 num) src
             { t with i_pos = t.i_pos + 4 }
      else if (t.i_len - t.i_pos) > 0
      then (get_byte
            @@ fun byte0 -> get_byte
            @@ fun byte1 -> get_byte
            @@ fun byte2 -> get_byte
            @@ fun byte3 src t ->
               k (to_int32 byte0 byte1 byte2 byte3) src t)
            src t
      else await { t with state = Offset (fun src t -> (get_u32[@tailcall]) k src t) }

    let get_u32 k src t =
      get_u32
        (fun u32 src t ->
          k (u32, Int32.equal 0l (Int32.logand u32 0x80000000l)) src t)
        src t

    let rec get_u64 k src t =
      if (t.i_len - t.i_pos) > 7
      then let num = B.get_u64 src (t.i_off + t.i_pos) in
           k (swap64 num) src
             { t with i_pos = t.i_pos + 8 }
      else if (t.i_len - t.i_pos) > 0
      then (get_byte
            @@ fun byte0 -> get_byte
            @@ fun byte1 -> get_byte
            @@ fun byte2 -> get_byte
            @@ fun byte3 -> get_byte
            @@ fun byte4 -> get_byte
            @@ fun byte5 -> get_byte
            @@ fun byte6 -> get_byte
            @@ fun byte7 src t ->
               k (to_int64 byte0 byte1 byte2 byte3 byte4 byte5 byte6 byte7) src t)
            src t
      else await { t with state = Offset (fun src t -> (get_u64[@tailcall]) k src t) }
  end

  let rest ?boffsets src t =
    match Queue.pop t.hashes, Queue.pop t.crcs, Queue.pop t.offsets with
    | hash, crc, (offset, true) -> Result ({ t with state = Ret }, (hash, crc, Int64.of_int32 offset))
    | exception Queue.Empty -> ok t
    | hash, crc, (offset, false) -> match boffsets with
      | None -> error t (Expected_bigoffset_table)
      | Some arr ->
        let idx = Int32.to_int (Int32.logand offset 0x7FFFFFFFl) in
        if idx >= 0 && idx < Array.length arr
        then Result ({ t with state = Ret }, (hash, crc, Array.get arr idx))
        else error t (Invalid_index_of_bigoffset idx)

  let rec boffsets arr idx max src t =
    if idx >= max
    then rest ~boffsets:arr src t
    else KOffset.get_u64
           (fun offset src t ->
              Array.set arr idx offset;
              (boffsets[@tailcall]) arr (succ idx) max src t)
           src t

  let rec offsets idx boffs max src t =
    if Int32.compare idx max >= 0
    then (if boffs > 0 then boffsets (Array.make boffs 0L) 0 boffs src t else rest src t)
    else KOffset.get_u32
           (fun (offset, msb) src t ->
             Queue.add (offset, msb) t.offsets;
             (offsets[@tailcall])
               (Int32.succ idx)
               (if not msb then succ boffs else boffs)
               max
               src t)
           src t

  let rec crcs idx max src t =
    if Int32.compare idx max >= 0
    then offsets 0l 0 max src t
    else KCrc.get_u32
           (fun crc src t ->
             Queue.add crc t.crcs;
             (crcs[@tailcall]) (Int32.succ idx) max src t)
           src t

  let rec hashes idx max src t =
    if Int32.compare idx max >= 0
    then Cont { t with state = Crc (crcs 0l max) }
    else KHash.get_hash
           (fun hash src t ->
              Queue.add hash t.hashes;
              (hashes[@tailcall]) (Int32.succ idx) max src t)
           src t

  let rec fanout idx src t =
    match idx with
    | 256 ->
      Cont { t with state = Hash (hashes 0l (Array.fold_left max 0l t.fanout)) }
    | n ->
      KFanoutTable.get_u32
        (fun entry src t ->
         Array.set t.fanout idx entry; (fanout[@tailcall]) (idx + 1) src t)
        src t

  let header src t =
    (KHeader.check_byte '\255'
     @@ KHeader.check_byte '\116'
     @@ KHeader.check_byte '\079'
     @@ KHeader.check_byte '\099'
     @@ KHeader.get_u32
     @@ fun version src t ->
        if version = 2l
        then Cont { t with state = Fanout (fanout 0) }
        else error t (Invalid_version version))
    src t

  let make () =
    { i_off   = 0
    ; i_pos   = 0
    ; i_len   = 0
    ; fanout  = Array.make 256 0l
    ; hashes  = Queue.create ()
    ; crcs    = Queue.create ()
    ; offsets = Queue.create ()
    ; state   = Header header }

  let sp = Format.sprintf

  let refill off len t =
    if (t.i_len - t.i_pos) = 0
    then { t with i_off = off
                ; i_len = len
                ; i_pos = 0 }
    else raise (Invalid_argument (sp "I.refill: you lost something (pos: %d, len: %d)" t.i_pos t.i_len))

  let rec eval src t =
    let eval0 t = match t.state with
      | Header k -> k src t
      | Fanout k -> k src t
      | Hash k -> k src t
      | Crc k -> k src t
      | Offset k -> k src t
      | Ret -> rest src t
      | End -> ok t
      | Exception exn -> error t exn
    in

    let rec loop t =
      match eval0 t with
      | Cont t -> loop t
      | Wait t -> `Await t
      | Ok t -> `End t
      | Result (t, hash) -> `Hash (t, hash)
      | Error (t, exn) -> `Error (t, exn)
    in

    loop t
end

(** Implementatioon of deserialization of a PACK file *)
module P =
struct
  type error = ..
  type error += Invalid_byte of int
  type error += Reserved_kind of int
  type error += Invalid_kind of int
  type error += Inflate_error of Zlib.Inflate.error
  type error += Hunk_error of H.error
  type error += Hunk_input of int * int
  type error += Invalid_length of int * int
  type error += Invalid_state

  let pp = Format.fprintf
  let pp_error fmt = function
    | Invalid_byte byte              -> pp fmt "(Invalid_byte %02x)" byte
    | Reserved_kind byte             -> pp fmt "(Reserved_byte %02x)" byte
    | Invalid_kind byte              -> pp fmt "(Invalid_kind %02x)" byte
    | Inflate_error err              -> pp fmt "(Inflate_error %a)" Zlib.Inflate.pp_error err
    | Invalid_length (expected, has) -> pp fmt "(Invalid_length (%d <> %d))" expected has
    | Hunk_error err                 -> pp fmt "(Hunk_error %a)" H.pp_error err
    | Hunk_input (expected, has)     -> pp fmt "(Hunk_input (%d <> %d))" expected has
    | Invalid_state                  -> pp fmt "Invalid_state"

  type ('i, 'o) t =
    { i_off   : int
    ; i_pos   : int
    ; i_len   : int
    ; local   : bool
    ; read    : int
    ; o_z     : 'o B.t
    ; o_h     : 'o B.t
    ; version : Int32.t
    ; objects : Int32.t
    ; counter : Int32.t
    ; state   : ('i, 'o) state }
  and ('i, 'o) state =
    | Header    of ('i B.t -> ('i, 'o) t -> ('i, 'o) res)
    | Object    of ('i B.t -> ('i, 'o) t -> ('i, 'o) res)
    | VariableLength of ('i B.t -> ('i, 'o) t -> ('i, 'o) res)
    | Unzip     of { offset : int; length : int; kind : 'i kind; z : ('i, 'o) Zlib.Inflate.t; }
    | Hunks     of { offset : int; length : int; z : ('i, 'o) Zlib.Inflate.t; h : 'i H.t; }
    | Next      of { offset : int; length : int; count : int; kind : 'i kind; }
    | Checksum  of ('i B.t -> ('i, 'o) t -> ('i, 'o) res)
    | End       of string
    | Exception of error
  and ('i, 'o) res =
    | Wait  of ('i, 'o) t
    | Flush of ('i, 'o) t
    | Error of ('i, 'o) t * error
    | Cont  of ('i, 'o) t
    | Ok    of ('i, 'o) t * string
  and 'i kind =
    | Commit
    | Tree
    | Blob
    | Tag
    | Hunk of 'i H.hunks

  let pp_kind fmt = function
    | Commit -> pp fmt "Commit"
    | Tree -> pp fmt "Tree"
    | Blob -> pp fmt "Blob"
    | Tag -> pp fmt "Tag"
    | Hunk hunks -> pp fmt "(Hunks %a)" H.pp_hunks hunks

  let pp fmt { i_off; i_pos; i_len; o_z; version; objects; counter; state; } =
    match state with
    | Unzip { z; _ } ->
      pp fmt "{@[<hov>i_off = %d;@ \
                      i_pos = %d;@ \
                      i_len = %d;@ \
                      version = %ld;@ \
                      objects = %ld;@ \
                      counter = %ld;@ \
                      z = %a@]}"
        i_off i_pos i_len version objects counter Zlib.Inflate.pp z
    | Hunks { z; h; _ } ->
      pp fmt "{@[<hov>i_off = %d;@ \
                      i_pos = %d;@ \
                      i_len = %d;@ \
                      version = %ld;@ \
                      objects = %ld;@ \
                      counter = %ld;@ \
                      z = %a;@ \
                      h = %a@]}"
        i_off i_pos i_len version objects counter Zlib.Inflate.pp z H.pp h
    | _ ->
      pp fmt "{@[<hov>i_off = %d;@ \
                      i_pos = %d;@ \
                      i_len = %d;@ \
                      version = %ld;@ \
                      objects = %ld;@ \
                      counter = %ld@]}"
        i_off i_pos i_len version objects counter

  let await t      = Wait t
  let flush t      = Flush t
  let error t exn  = Error ({ t with state = Exception exn }, exn)
  let continue t   = Cont t
  let ok t hash    = Ok ({ t with state = End hash }, hash)

  module KHeader =
  struct
    let rec check_byte chr k src t =
      if (t.i_len - t.i_pos) > 0 && B.get src (t.i_off + t.i_pos) = chr
      then k src { t with i_pos = t.i_pos + 1
                        ; read = t.read + 1 }
      else if (t.i_len - t.i_pos) = 0
      then await { t with state = Header (fun src t -> (check_byte[@tailcall]) chr k src t) }
      else error { t with i_pos = t.i_pos + 1
                        ; read = t.read + 1 }
                 (Invalid_byte (Char.code @@ B.get src (t.i_off + t.i_pos)))

    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1
                             ; read = t.read + 1 }
      else await { t with state = Header (fun src t -> (get_byte[@tailcall]) k src t) }

    let swap n =
      let (>>) = Int32.shift_right in
      let (<<) = Int32.shift_left in
      let (||) = Int32.logor in
      let ( & ) = Int32.logand in

      ((n >> 24) & 0xffl)
      || ((n << 8) & 0xff0000l)
      || ((n >> 8) & 0xff00l)
      || ((n << 24) & 0xff000000l)

    let to_int32 b0 b1 b2 b3 =
      let (<<) = Int32.shift_left in
      let (||) = Int32.logor in
      (Int32.of_int b0 << 24)
      || (Int32.of_int b1 << 16)
      || (Int32.of_int b2 << 8)
      || (Int32.of_int b3)

    let rec get_u32 k src t =
      if (t.i_len - t.i_pos) > 3
      then let num = B.get_u32 src (t.i_off + t.i_pos) in
           k (swap num) src
             { t with i_pos = t.i_pos + 4
                    ; read = t.read + 4 }
      else if (t.i_len - t.i_pos) > 0
      then (get_byte
            @@ fun byte0 -> get_byte
            @@ fun byte1 -> get_byte
            @@ fun byte2 -> get_byte
            @@ fun byte3 src t ->
               k (to_int32 byte0 byte1 byte2 byte3) src t)
            src t
      else await { t with state = Header (fun src t -> (get_u32[@tailcall]) k src t) }
  end

  module KVariableLength =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1
                             ; read = t.read + 1 }
      else await { t with state = VariableLength (fun src t -> (get_byte[@tailcall]) k src t) }
  end

  module KObject =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1
                             ; read = t.read + 1 }
      else await { t with state = Object (fun src t -> (get_byte[@tailcall]) k src t) }

    let get_hash k src t =
      let buf = Buffer.create 20 in

      let rec loop i src t =
        if i = 20
        then k (Buffer.contents buf) src t
        else
          get_byte (fun byte src t ->
                     Buffer.add_char buf (Char.chr byte);
                     (loop[@tailcall]) (i + 1) src t)
            src t
      in

      loop 0 src t
  end

  module KChecksum =
  struct
    let rec get_byte k src t =
      if (t.i_len - t.i_pos) > 0
      then let byte = Char.code @@ B.get src (t.i_off + t.i_pos) in
           k byte src { t with i_pos = t.i_pos + 1
                             ; read = t.read + 1 }
      else await { t with state = Checksum (fun src t -> (get_byte[@tailcall]) k src t) }

    let get_hash k src t =
      let buf = Buffer.create 20 in

      let rec loop i src t =
        if i = 20
        then k (Buffer.contents buf) src t
        else
          get_byte (fun byte src t ->
                     Buffer.add_char buf (Char.chr byte);
                     (loop[@tailcall]) (i + 1) src t)
            src t
      in

      loop 0 src t
  end

  let rec length msb (len, bit) k src t = match msb with
    | true ->
      KVariableLength.get_byte
        (fun byte src t ->
           let msb = byte land 0x80 <> 0 in
           (length[@tailcall]) msb (len lor ((byte land 0x7F) lsl bit), bit + 7)
           k src t)
        src t
    | false -> k len src t

  let rec offset msb off k src t = match msb with
    | true ->
      KVariableLength.get_byte
        (fun byte src t ->
           let msb = byte land 0x80 <> 0 in
           (offset[@tailcall]) msb (((off + 1) lsl 7) lor (byte land 0x7F))
           k src t)
        src t
    | false -> k off src t

  let hunks src t offset length z h =
    match Zlib.Inflate.eval src t.o_z z with
    | `Await z ->
      await { t with state = Hunks { offset; length; z; h; }
                   ; i_pos = t.i_pos + Zlib.Inflate.used_in z
                   ; read = t.read + Zlib.Inflate.used_in z }
    | `Error (z, exn) -> error t (Inflate_error exn)
    | `End z ->
      let ret = if Zlib.Inflate.used_out z <> 0
                then H.eval t.o_z (H.refill 0 (Zlib.Inflate.used_out z) h)
                else H.eval t.o_z h
      in
      (match ret with
       | `Ok (h, hunks) ->
         Cont { t with state = Next { length
                                    ; count = Zlib.Inflate.write z
                                    ; offset
                                    ; kind = Hunk hunks }
                     ; i_pos = t.i_pos + Zlib.Inflate.used_in z
                     ; read = t.read + Zlib.Inflate.used_in z }
       | `Await h ->
         error t (Hunk_input (length, H.read h))
       | `Error (h, exn) -> error t (Hunk_error exn))
    | `Flush z ->
      match H.eval t.o_z (H.refill 0 (Zlib.Inflate.used_out z) h) with
      | `Await h ->
        Cont { t with state = Hunks { offset; length
                                    ; z = (Zlib.Inflate.flush 0 (B.length t.o_z) z)
                                    ; h } }
      | `Error (h, exn) -> error t (Hunk_error exn)
      | `Ok (h, objs) ->
        Cont { t with state = Hunks { offset; length
                                    ; z = (Zlib.Inflate.flush 0 (B.length t.o_z) z)
                                    ; h } }

  let switch typ off len src t =
    match typ with
    | 0b000 | 0b101 -> error t (Reserved_kind typ)
    | 0b001 ->
      Cont { t with state = Unzip { offset = off; length = len
                                  ; kind = Commit
                                  ; z = Zlib.Inflate.flush 0 (B.length t.o_z)
                                        @@ Zlib.Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Zlib.Inflate.default; } }
    | 0b010 ->
      Cont { t with state = Unzip { offset = off; length = len
                                  ; kind = Tree
                                  ; z = Zlib.Inflate.flush 0 (B.length t.o_z)
                                        @@ Zlib.Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Zlib.Inflate.default; } }
    | 0b011 ->
      Cont { t with state = Unzip { offset = off; length = len
                                  ; kind = Blob
                                  ; z = Zlib.Inflate.flush 0 (B.length t.o_z)
                                        @@ Zlib.Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Zlib.Inflate.default; } }
    | 0b100 ->
      Cont { t with state = Unzip { offset = off; length = len
                                  ; kind = Tag
                                  ; z = Zlib.Inflate.flush 0 (B.length t.o_z)
                                        @@ Zlib.Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                        @@ Zlib.Inflate.default; } }
    | 0b110 ->
      KObject.get_byte
        (fun byte src t ->
           let msb = byte land 0x80 <> 0 in
           offset msb (byte land 0x7F)
             (fun offset src t ->
               Cont { t with state = Hunks { offset = off; length = len
                                           ; z = Zlib.Inflate.flush 0 (B.length t.o_z)
                                                 @@ Zlib.Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                                 @@ Zlib.Inflate.default
                                           ; h = H.default len (H.Offset offset) } })
             src t)
        src t
    | 0b111 ->
      KObject.get_hash
        (fun hash src t ->
          Cont { t with state = Hunks { offset = off; length = len
                                      ; z = Zlib.Inflate.flush 0 (B.length t.o_z)
                                            @@ Zlib.Inflate.refill (t.i_off + t.i_pos) (t.i_len - t.i_pos)
                                            @@ Zlib.Inflate.default
                                      ; h = H.default len (H.Hash hash) } })
        src t
    | _  -> error t (Invalid_kind typ)

  let checksum src t =
    KChecksum.get_hash
      (fun hash src t -> ok t hash)
      src t

  let kind src t =
    let offset = t.read in

    KObject.get_byte
      (fun byte src t ->
         let msb = byte land 0x80 <> 0 in
         let typ = (byte land 0x70) lsr 4 in
         length msb (byte land 0x0F, 4) (switch typ offset) src t)
      src t

  let unzip src t offset length kind z =
    match Zlib.Inflate.eval src t.o_z z with
    | `Await z ->
      await { t with state = Unzip { offset; length
                                   ; kind
                                   ; z }
                   ; i_pos = t.i_pos + Zlib.Inflate.used_in z
                   ; read = t.read + Zlib.Inflate.used_in z }
    | `Flush z ->
      flush { t with state = Unzip { offset; length
                                   ; kind
                                   ; z } }
    | `End z ->
      if Zlib.Inflate.used_out z <> 0
      then flush { t with state = Unzip { offset; length
                                        ; kind
                                        ; z } }
      else Cont { t with state = Next { length
                                      ; count = Zlib.Inflate.write z
                                      ; offset
                                      ; kind; }
                       ; i_pos = t.i_pos + Zlib.Inflate.used_in z
                       ; read = t.read + Zlib.Inflate.used_in z }
    | `Error (z, exn) -> error t (Inflate_error exn)

  let next src t length count kind =
    if length = count
    then Cont t
    else error t (Invalid_length (length, count))

  let number src t =
    KHeader.get_u32
      (fun objects src t ->
         Cont { t with objects = objects
                     ; counter = objects
                     ; state = Object kind })
      src t

  let version src t =
    KHeader.get_u32
      (fun version src t ->
         number src { t with version = version })
      src t

  let header src t =
    (KHeader.check_byte 'P'
     @@ KHeader.check_byte 'A'
     @@ KHeader.check_byte 'C'
     @@ KHeader.check_byte 'K'
     @@ version)
    src t

  let default ~proof ?(chunk = 4096) () =
    { i_off   = 0
    ; i_pos   = 0
    ; i_len   = 0
    ; local   = false
    ; o_z     = B.from ~proof chunk
    ; o_h     = B.from ~proof chunk
    ; read    = 0
    ; version = 0l
    ; objects = 0l
    ; counter = 0l
    ; state   = Header header }

  let size_of_vl vl =
    let rec loop acc = function
      | 0 -> acc
      | n -> loop (acc + 1) (n lsr 7)
    in
    loop 1 (vl lsr 4) (* avoid type and msb *)

  let from_hunks (type a) ~(proof:a B.t) hunks offset =
    { i_off   = offset
    ; i_pos   = 0
    ; i_len   = 0
    ; local   = true
    ; o_z     = B.from ~proof hunks.H.target_length
    ; o_h     = B.from ~proof hunks.H.target_length
    ; read    = offset (* from_hunks used with a map of pack-file *)
    ; version = 0l
    ; objects = 1l
    ; counter = 1l
    ; state   = Object kind }

  let sp = Format.sprintf

  let refill off len t =
    if (t.i_len - t.i_pos) = 0
    then match t.state with
         | Unzip { offset; length; kind; z; } ->
           { t with i_off = off
                  ; i_len = len
                  ; i_pos = 0
                  ; state = Unzip { offset; length; kind; z = Zlib.Inflate.refill off len z; } }
         | Hunks { offset; length; z; h; } ->
           { t with i_off = off
                  ; i_len = len
                  ; i_pos = 0
                  ; state = Hunks { offset; length; z = Zlib.Inflate.refill off len z; h; } }
         | _ -> { t with i_off = off
                       ; i_len = len
                       ; i_pos = 0 }
    else raise (Invalid_argument (sp "P.refill: you lost something (pos: %d, len: %d)" t.i_pos t.i_len))

  let flush off len t = match t.state with
    | Unzip { offset; length; kind; z; } ->
      { t with state = Unzip { offset; length; kind; z = Zlib.Inflate.flush off len z; } }
    | _ -> raise (Invalid_argument "P.flush: bad state")

  let output t = match t.state with
    | Unzip { z; _ } ->
      t.o_z, Zlib.Inflate.used_out z
    | Hunks { h; _ } ->
      t.o_h, 0
    | _ -> raise (Invalid_argument "P.output: bad state")

  let next_object t = match t.state with
    | Next _ when not t.local ->
      if Int32.pred t.counter = 0l
      then { t with state = Checksum checksum
                  ; counter = Int32.pred t.counter }
      else { t with state = Object kind
                  ; counter = Int32.pred t.counter }
    | Next _ -> { t with state = End "" }
    | _ -> raise (Invalid_argument "P.next_object: bad state")

  let kind t = match t.state with
    | Next { kind; _ } -> kind
    | _ -> raise (Invalid_argument "P.kind: bad state")

  let offset t = match t.state with
    | Next { offset; _ } -> offset
    | _ -> raise (Invalid_argument "P.offset: bad state")

  let rec eval src t =
    let eval0 t = match t.state with
      | Header k -> k src t
      | Object k -> k src t
      | VariableLength k -> k src t
      | Unzip { offset; length; kind; z; } ->
        unzip src t offset length kind  z
      | Hunks { offset; length; z; h; } ->
        hunks src t offset length z h
      | Next { length; count; kind; } -> next src t length count kind
      | Checksum k -> k src t
      | End hash -> ok t hash
      | Exception exn -> error t exn
    in

    let rec loop t =
      match eval0 t with
      | Cont ({ state = Next _ } as t) -> `Object t
      | Cont t -> loop t
      | Wait t -> `Await t
      | Flush t -> `Flush t
      | Ok (t, hash) -> `End (t, hash)
      | Error (t, exn) -> `Error (t, exn)
    in

    loop t
end

module D =
struct
  type error = ..
  type error += Invalid_hash of string
  type error += Invalid_source of int
  type error += Invalid_target of (int * int)
  type error += Unpack_error of P.error

  let pp = Format.fprintf

  let pp_error fmt = function
    | Invalid_hash hash -> pp fmt "(Invalid_hash %a)" pp_hash hash
    | Invalid_source off -> pp fmt "(Invalid_chunk %d)" off
    | Invalid_target (has, expected) -> pp fmt "(Invalid_target (%d, %d))"
      has expected
    | Unpack_error exn -> pp fmt "(Unpack_error %a)" P.pp_error exn

  type 'a t =
    { map_pck : 'a B.t
    ; fun_idx : (string -> Int64.t option) }

  let delta { map_pck; fun_idx; } hunks offset =
    match hunks.H.reference with
    | H.Offset off ->
      let offset   = offset - off in
      let length   = 4096 in
      let t        = P.from_hunks ~proof:map_pck hunks offset in
      let unpacked = B.from ~proof:map_pck hunks.H.source_length in
      let res      = B.from ~proof:map_pck hunks.H.target_length in

      let rec loop offset' result kind t =
        match P.eval map_pck t with
        | `Await t ->
          let length = min (B.length map_pck - offset') length in
          if length > 0
          then loop (offset' + length) result kind (P.refill offset' length t)
          else Error (Invalid_source offset)
        | `Flush t ->
          let o, n = P.output t in
          B.blit o 0 unpacked result n;
          loop offset' (result + n) kind (P.flush 0 n t)
        | `Object t ->
          loop offset' result (Some (P.kind t, P.offset t)) (P.next_object t) (* XXX *)
        | `Error (t, exn) ->
          Error (Unpack_error exn)
        | `End (t, _) -> match kind with
          | Some (kind, offset) -> Ok (kind, offset)
          | None -> assert false
          (** XXX: This is not possible, the [`End] state comes only after the
           *  [`Object] state and this state changes [kind] to [Some x].
           *)
      in

      (match loop offset 0 None t with
       | Ok (kind, offset) ->
         let target_length = List.fold_left
          (fun acc -> function
           | H.Insert raw ->
             B.blit raw 0 res acc (B.length raw); acc + B.length raw
           | H.Copy (off, len) ->
             B.blit unpacked off res acc len; acc + len)
          0 hunks.H.hunks
         in

         if (target_length = hunks.H.target_length)
         then Ok (kind, res, offset)
         else Error (Invalid_target (target_length, hunks.H.target_length))
       | Error exn -> Error exn)

    | H.Hash hash -> match fun_idx hash with
      | Some off -> assert false (* TODO *)
      | None -> Error (Invalid_hash hash)

  let make map_pck fun_idx = { map_pck; fun_idx; }
end

(** See [bs.c]. *)
external bs_read : Unix.file_descr -> B.Bigstring.t -> int -> int -> int =
  "bigstring_read" [@@noalloc]
external bs_write : Unix.file_descr -> B.Bigstring.t -> int -> int -> int =
  "bigstring_write" [@@noalloc]

(** Abstract [Unix.read] with ['a B.t]. *)
let unix_read (type a) ch (tmp : a B.t) off len = match tmp with
  | B.Bytes v -> Unix.read ch v off len
  | B.Bigstring v -> bs_read ch v off len

(** Abstract [Buffer.add_substring] with ['a B.t]. *)
let buffer_add_substring (type a) buf (tmp : a B.t) off len = match tmp with
  | B.Bytes v -> Buffer.add_substring buf (Bytes.unsafe_to_string v) off len
  | B.Bigstring v ->
    let v = B.Bigstring.sub v off len in
    Buffer.add_substring buf (B.Bigstring.to_string v) 0 len

let unpack ?(chunk = 4096) map idx =
  let buf = Buffer.create chunk in
  let rap = D.make map idx in

  let rec loop offset (t : ('a, 'a) P.t) =
    match P.eval map t with
    | `Await t ->
      let chunk = min (B.length map - offset) chunk in
      if chunk > 0
      then loop (offset + chunk) (P.refill offset chunk t)
      else raise End_of_file
    | `Flush t ->
      let o, n = P.output t in
      let () = buffer_add_substring buf o 0 n in
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
          | Ok (P.Hunk hunks, _, offset') ->
            undelta hunks offset'
          | Ok (kind, raw, offset') ->
            Format.printf "%a\n%!" P.pp_kind kind;
            Format.printf "%a\n%!" Hex.hexdump (Hex.of_string_fast (B.to_string raw));

            loop offset (P.next_object t)
          | Error exn ->
            Format.printf "Delta error: %a\n%!" D.pp_error exn;
            assert false
        in

        undelta hunks (P.offset t)
      | kind ->
        Format.printf "%a\n%!" P.pp_kind kind;
        Format.printf "%a\n%!" Hex.hexdump (Hex.of_string_fast @@ Buffer.contents buf);
        Buffer.clear buf; (* clean buffer *)

        loop offset (P.next_object t)
  in

  loop 0 (P.default ~proof:map ~chunk:chunk ())

(** Bytes map. *)
let st_map filename =
  let i = open_in filename in
  let n = in_channel_length i in
  let m = Bytes.create n in
  really_input i m 0 n;
  B.from_bytes m

(** Bigstring map. *)
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
      unpack pp (fun hash -> RAI.find t hash)
    | Error exn ->
      Format.eprintf "%a\n%!" RAI.pp_error exn
  in

  Format.printf "End of parsing\n%!"
