let to_char x y =
  let code c = match c with
    | '0'..'9' -> Char.code c - 48 (* Char.code '0' *)
    | 'A'..'F' -> Char.code c - 55 (* Char.code 'A' + 10 *)
    | 'a'..'f' -> Char.code c - 87 (* Char.code 'a' + 10 *)
    | _ -> raise (Invalid_argument (Format.sprintf "Hex.to_char: %d is an invalid char" (Char.code c)))
  in
  Char.chr (code x lsl 4 + code y)

let hexa = "0123456789abcdef"
and hexa1 =
  "0000000000000000111111111111111122222222222222223333333333333333\
   4444444444444444555555555555555566666666666666667777777777777777\
   88888888888888889999999999999999aaaaaaaaaaaaaaaabbbbbbbbbbbbbbbb\
   ccccccccccccccccddddddddddddddddeeeeeeeeeeeeeeeeffffffffffffffff"
and hexa2 =
  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\
   0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\
   0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\
   0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

let of_string_fast s =
  let len = String.length s in
  let buf = Bytes.create (len * 2) in
  for i = 0 to len - 1 do
    Bytes.unsafe_set buf (i * 2)
      (String.unsafe_get hexa1 (Char.code (String.unsafe_get s i)));
    Bytes.unsafe_set buf (succ (i * 2))
      (String.unsafe_get hexa2 (Char.code (String.unsafe_get s i)));
  done;
  `Hex (Bytes.unsafe_to_string buf)

let hexdump_s ?(print_row_numbers=true) ?(print_chars=true) (`Hex s) =
  let char_len = 16 in
  let hex_len = char_len * 2 in
  let buf = Buffer.create ((String.length s) * 4) in
  let ( <= ) buf s = Buffer.add_string buf s in
  let n = String.length s in
  let rows = (n / hex_len) + (if n mod hex_len = 0 then 0 else 1) in
  for row = 0 to rows-1 do
    let last_row = row = rows-1 in
    if print_row_numbers then
      buf <= Printf.sprintf "%.8d: " row;
    let row_len = if last_row then
        (let rem = n mod hex_len in
         if rem = 0 then hex_len else rem)
      else hex_len in
    for i = 0 to row_len-1 do
      if i mod 4 = 0 && i <> 0 then buf <= Printf.sprintf " ";
      let i = i + (row * hex_len) in
      buf <= Printf.sprintf "%c" (String.get s i)
    done;
    if last_row then
      let missed_chars = hex_len - row_len in
      let pad = missed_chars in
      (* Every four chars add spacing *)
      let pad = pad + (missed_chars / 4) in
      buf <= Printf.sprintf "%s" (String.make pad ' ')
    else ();
    if print_chars then begin
      buf <= "  ";
      let rec aux i j =
        if i > row_len - 2 then ()
        else begin
          let pos = i + (row * hex_len) in
          let pos' = pos + 1 in
          let c = to_char (String.get s pos) (String.get s pos') in
          let () = match c with
            | '\033' .. '\126'  -> buf <= Printf.sprintf "%c" c
            | _ -> buf <= "."
          in ();
          aux (j+1) (j+2)
        end
      in
      aux 0 1;
    end;
    buf <= "\n";
  done;
  Buffer.contents buf

let hexdump fmt hex =
  Format.fprintf fmt "%s" (hexdump_s hex)
