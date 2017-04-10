type t = private int32

external crc32_bytes : t -> Bytes.t -> int -> int -> t = "caml_st_crc32"
external crc32_bigarray : t -> Cstruct.buffer -> int -> int -> t = "caml_ba_crc32"

let digest v ?(off = 0) ?len c =
  let len = match len with
    | Some len -> len
    | None -> Cstruct.len c
  in
  let b = Cstruct.to_bigarray c in
  crc32_bigarray v b off len

let digestv v cs = List.fold_left (fun v c -> digest v c) v cs

let digestc v byte = crc32_bytes v (Bytes.make 1 (Char.unsafe_chr byte)) 0 1

external of_int32 : int32 -> t = "%identity"
external to_int32 : t -> int32 = "%identity"

let pp fmt x =
  Format.fprintf fmt "%08lx" (to_int32 x)

let default = of_int32 0l

(*

let cstruct_map filename =
  let i = Unix.openfile filename [ Unix.O_RDONLY ] 0o644 in
  let m = Bigarray.Array1.map_file i Bigarray.Char Bigarray.c_layout false (-1) in
  Cstruct.of_bigarray m

let () =
  let filename = Sys.argv.(1) in
  let chunk = 32 in

  let map = cstruct_map filename in

  let rec loop off rest crc =
    let n = min chunk rest in

    if n = 0
    then Format.printf "%a\n%!" pp crc
    else loop (off + n) (rest - n) (digest crc ~off ~len:n map)
  in

  let crc = digest default ~off:0 ~len:2 map in

  Format.printf "start with: %a\n%!" pp crc;

  loop 2 (Cstruct.len map - 2) crc;

*)
