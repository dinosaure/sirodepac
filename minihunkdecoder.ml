type t =
  { i_off : int
  ; i_pos : int
  ; i_len : int
  ; state : state }
and k = Cstruct.t -> t -> res
and state =
  | Header of k
  | List of k
  | IsInsert of k
  | IsCopy of k
  | Return of hunk
  | Final
and res =
  | Cont of t
  | Wait of t
  | End of t
and hunk =
  | Insert of Cstruct.t
  | Copy of int * int

let pp_list ?(sep = (fun fmt () -> ())) pp_data fmt lst =
  let rec aux = function
    | [] -> ()
    | [ x ] -> pp_data fmt x
    | x :: r -> pp_data fmt x; sep fmt (); aux r
  in
  aux lst

let pp_cstruct fmt cs =
  Format.fprintf fmt "\"";
  for i = 0 to Cstruct.len cs - 1
  do if Cstruct.get_uint8 cs i > 32 && Cstruct.get_uint8 cs i < 127
    then Format.fprintf fmt "%c" (Cstruct.get_char cs i)
    else Format.fprintf fmt "."
  done;
  Format.fprintf fmt "\""

let pp_hunk fmt = function
  | Insert raw -> Format.fprintf fmt "(Insert %a)" pp_cstruct raw
  | Copy (off, len) -> Format.fprintf fmt "(Copy (%d, %d))" off len

let rec get_byte ~ctor k src t =
    if (t.i_len - t.i_pos) > 0
    then let byte = Cstruct.get_uint8 src (t.i_off + t.i_pos) in
        k byte src
          { t with i_pos = t.i_pos + 1 }
    else Wait { t with state = ctor (fun src t -> (get_byte[@tailcall]) ~ctor k src t) }

let rec length msb (len, bit) k src t =
  let get_byte = get_byte ~ctor:(fun k -> Header k) in

  match msb with
  | true ->
    get_byte
      (fun byte src t ->
        let msb = byte land 0x80 <> 0 in
        (length[@tailcall]) msb (((byte land 0x7F) lsl bit) lor len, bit + 7)
        k src t)
      src t
  | false -> k len src t

let length k src t =
  let get_byte = get_byte ~ctor:(fun k -> Header k) in

  get_byte
    (fun byte src t ->
      let msb = byte land 0x80 <> 0 in
      length msb ((byte land 0x7F), 7) k src t)
    src t

let rec insert res len raw src t =
  if res = 0
  then Cont { t with state = Return (Insert raw) }
  else
    (if t.i_len - t.i_pos > 0
     then begin
       let n = min res (t.i_len - t.i_pos) in
       Cstruct.blit src (t.i_off + t.i_pos) raw (len - res) n;
       Cont { t with state = IsInsert (insert (res - n) len raw)
                   ; i_pos = t.i_pos + n }
     end else Wait { t with state = IsInsert (insert res len raw) })

let copy opcode src t =
  let get_byte flag k src t =
    if flag
    then get_byte ~ctor:(fun k -> IsCopy k) k src t
    else k 0 src t
  in

  (get_byte (opcode land 0x01 <> 0)
   @@ fun o0 -> get_byte (opcode land 0x02 <> 0)
   @@ fun o1 -> get_byte (opcode land 0x04 <> 0)
   @@ fun o2 -> get_byte (opcode land 0x08 <> 0)
   @@ fun o3 -> get_byte (opcode land 0x10 <> 0)
   @@ fun l0 -> get_byte (opcode land 0x20 <> 0)
   @@ fun l1 -> get_byte (opcode land 0x40 <> 0)
   @@ fun l2 src t ->
   let dst = ref 0 in
   let len = ref 0 in

   if opcode land 0x01 <> 0 then dst := o0;
   if opcode land 0x02 <> 0 then dst := !dst lor (o1 lsl 8);
   if opcode land 0x04 <> 0 then dst := !dst lor (o2 lsl 16);
   if opcode land 0x08 <> 0 then dst := !dst lor (o3 lsl 24);

   if opcode land 0x10 <> 0 then len := l0;
   if opcode land 0x20 <> 0 then len := !len lor (l1 lsl 8);
   if opcode land 0x40 <> 0 then len := !len lor (l2 lsl 16);

   let dst = !dst in
   let len = if !len = 0 then 0x10000 else !len in

   Cont { t with state = Return (Copy (dst, len)) })
  src t


let list src t =
  let get_byte = get_byte ~ctor:(fun k -> List k) in

  (get_byte @@ fun opcode src t -> match opcode land 0x80 with
   | 0 -> Cont { t with state = IsInsert (insert opcode opcode (Cstruct.create opcode)) }
   | _ -> Cont { t with state = IsCopy (copy opcode) })
  src t

let header src t =
  (length
   @@ fun source_length -> length
   @@ fun target_length src t -> Cont { t with state = List list })
  src t

let eval0 src t =
  match t.state with
  | Header k -> k src t
  | List k -> k src t
  | IsCopy k -> k src t
  | IsInsert k -> k src t
  | Return _ -> Wait t
  | Final -> End t

let eval src t =
  let rec loop t = match eval0 src t with
    | Cont t -> loop t
    | Wait ({ state = Return hunk } as t) -> `Hunk (hunk, t)
    | Wait t -> `Await t
    | End t -> `End t
  in loop t

let refill off len t =
  if t.i_len - t.i_pos = 0
  then { t with i_off = off
              ; i_pos = 0
              ; i_len = len }
  else raise (Invalid_argument "You lost something")

let finish t =
  match t.state with
  | List _ -> { t with state = Final }
  | _ -> raise (Invalid_argument "Bad state (finish)")

let continue t =
  match t.state with
  | Return _ -> { t with state = List list }
  | _ -> raise (Invalid_argument "Bad state (continue)")

external read : Unix.file_descr -> Cstruct.buffer -> int -> int -> int =
  "bigstring_read" [@@noalloc]

let default =
  { i_off = 0
  ; i_pos = 0
  ; i_len = 0
  ; state = Header header }

let () =
  let raw = Cstruct.create 0x8000 in

  let rec compute t = match eval raw t with
    | `Hunk (hunk, t) ->
      Format.printf "%a\n%!" pp_hunk hunk;
      compute (continue t)
    | `Await t ->
      let n = read Unix.stdin (Cstruct.to_bigarray raw) 0 0x8000 in

      if n = 0
      then compute (finish t)
      else compute (refill 0 n t)
    | `End t -> ()
  in

  compute default
