type ('a, 'b) t = string constraint 'a = [> ]
  constraint 'b = [ `SHA1 ]

type ctx = Nocrypto.Hash.SHA1.t

let size        = Nocrypto.Hash.SHA1.digest_size
let digestv l   = Cstruct.to_string (Nocrypto.Hash.SHA1.digestv l)
let digest c    = Cstruct.to_string (Nocrypto.Hash.SHA1.digest c)
let hmac ~key c = Cstruct.to_string (Nocrypto.Hash.SHA1.hmac ~key c)
let init        = Nocrypto.Hash.SHA1.init
let get ctx     = Cstruct.to_string (Nocrypto.Hash.SHA1.get ctx)
let feed ctx s  = Nocrypto.Hash.SHA1.feed ctx (Cstruct.of_string s)

external tree   : string -> ([ `Tree ],   [ `SHA1 ]) t = "%identity"
external commit : string -> ([ `Commit ], [ `SHA1 ]) t = "%identity"
external blob   : string -> ([ `Blob ],   [ `SHA1 ]) t = "%identity"
external tag    : string -> ([ `Tag ],    [ `SHA1 ]) t = "%identity"

let to_char x y =
  let code c = match c with
    | '0'..'9' -> Char.code c - 48 (* Char.code '0' *)
    | 'A'..'F' -> Char.code c - 55 (* Char.code 'A' + 10 *)
    | 'a'..'f' -> Char.code c - 87 (* Char.code 'a' + 10 *)
    | _ ->
      raise (Invalid_argument (Format.sprintf "Hash.to_char: %d is an invalid char" (Char.code c)))
  in
  Char.chr (code x lsl 4 + code y)

let from_pp
  : sentinel:'sentinel -> string -> ('sentinel, [ `SHA1 ]) t
  = fun ~sentinel x ->
    let tmp = Bytes.create 20 in
    let buf = if String.length x mod 2 = 1
              then x ^ "0" else x
    in

    let rec aux i j =
      if i >= 40 then ()
      else if j >= 40
      then raise (Invalid_argument "Hash.from_pp")
            (* XXX(dinosaure): this case is impossible because
                              if j is odd and we only add (+) 2,
                              j stay odd as long as we compute.

                              So j <> 40, in any case.
            *)
      else begin
        Bytes.set tmp (i / 2) (to_char buf.[i] buf.[j]);
        aux (j + 1) (j + 2)
      end
    in aux 0 1; Bytes.unsafe_to_string tmp

let pp fmt hash =
  for i = 0 to String.length hash - 1
  do Format.fprintf fmt "%02x" (Char.code @@ String.get hash i) done

let to_pp hash =
  let buf = Buffer.create 40 in
  let fmt = Format.formatter_of_buffer buf in
  Format.fprintf fmt "%a%!" pp hash;
  Buffer.contents buf
