module Option =
struct
  let ( >>= ) x f = match x with Some x -> Some (f x) | None -> None
end

module Result =
struct
  let ( >>| ) a f = match a with Ok a -> Ok (f a) | Error exn -> Error exn
end

let pp_list ?(sep = "") pp_data fmt lst =
  let rec aux = function
    | [] -> ()
    | [ x ] -> pp_data fmt x
    | x :: r -> Format.fprintf fmt "%a%s" pp_data x sep; aux r
  in aux lst

let pp_option pp_data fmt = function
  | Some v -> pp_data fmt v
  | None -> Format.pp_print_string fmt "<none>"

module User =
struct
  type tz_offset =
    { sign    : [ `Plus | `Minus ]
    ; hours   : int
    ; minutes : int }
  type t =
    { name  : string
    ; email : string
    ; date  : int64 * tz_offset option }

  let pp_tz_offset fmt { sign
                       ; hours
                       ; minutes } =
    Format.fprintf fmt "{ @[<hov>sign = %s;@ \
                                 hours = %d;@ \
                                 minutes = %d;@] }"
      (match sign with `Plus -> "+" | `Minus -> "-")
      hours
      minutes

  let pp fmt { name
             ; email
             ; date = (second, tz_offset) } =
    Format.fprintf fmt "{ @[<hov>name = %s;@ \
                                 email = %s;@ \
                                 date = (%Ld, %a);@] }"
      name email second (pp_option pp_tz_offset) tz_offset

  module Decoder =
  struct
    open Angstrom

    let lt = char '<'
    let gt = char '>'
    let sp = char ' '
    let pl = char '+'
    let mn = char '-'

    let is_not_lt chr = chr <> '<'
    let is_not_gt chr = chr <> '>'

    let int64 = take_while (function '0' .. '9' -> true | _ -> false) >>| Int64.of_string

    let user =
      take_while is_not_lt <* take 1 <* commit
      >>= fun name    -> let name = String.sub name 0 (String.length name - 1) in
                         take_while is_not_gt <* commit
      >>= fun email   -> take 2 *> int64 <* commit
      >>= fun second  -> sp *> ((pl *> return `Plus)
                                 <|> (mn *> return `Minus))
                         <* commit
      >>= fun sign    -> take 2 >>| int_of_string <* commit
      >>= fun hours   -> take 2 >>| int_of_string <* commit
      >>= fun minutes ->
        let tz_offset =
          if sign = `Plus
          && hours = 0
          && minutes = 0
          then None else Some { sign
                              ; hours
                              ; minutes }
        in
        return { name
               ; email
               ; date = (second, tz_offset) }
      <* commit

    let to_result cs =
      Angstrom.Unbuffered.parse ~input:(`Bigstring (Cstruct.to_bigarray cs)) user
      |> Angstrom.Unbuffered.state_to_result
  end

  module Encoder =
  struct
    open Farfadet

    let lt = '<'
    let gt = '>'
    let sp = ' '

    let int64 e x = string e (Int64.to_string x)

    let digit e x =
      if x < 10
      then eval e [ char $ '0'; !!string ] (string_of_int x)
      else if x < 100
      then string e (string_of_int x)
      else raise (Invalid_argument "User.Encoder.digit")

    let sign' e = function
      | `Plus -> char e '+'
      | `Minus -> char e '-'

    let date e = function
      | None -> string e "+0000"
      | Some { sign; hours; minutes } ->
        eval e [ !!sign'; !!digit; !!digit ] sign hours minutes

    let user e t =
      eval e [ !!string; char $ sp; char $ lt; !!string; char $ gt; char $ sp; !!int64; char $ sp; !!date ]
        t.name t.email (fst t.date) (snd t.date)
  end
end

module Commit =
struct
  type t =
    { tree      : ([ `Tree ],   [ `SHA1 ]) Hash.t
    ; parents   : ([ `Commit ], [ `SHA1 ]) Hash.t list
    ; author    : User.t
    ; committer : User.t
    ; message   : string  }

  let pp fmt { tree
             ; parents
             ; author
             ; committer
             ; message } =
    Format.fprintf fmt "{ @[<hov>tree = %a;@ \
                                 parents = [ @[<hov>%a@] ];@ \
                                 author = %a;@ \
                                 committer = %a;@ \
                                 message = @[<hov>%a@];@] }"
      Hash.pp tree
      (pp_list ~sep:"; " Hash.pp) parents
      User.pp author User.pp committer
      Format.pp_print_text message;

  module Decoder =
  struct
    open Angstrom

    let sp = char ' '
    let lf = char '\x0a'
    let is_not_lf chr = chr <> '\x0a'

    let binding
      : type a. key:string -> value:a Angstrom.t -> a Angstrom.t
      = fun ~key ~value ->
      string key *> sp *> value <* lf <* commit

    let commit =
      binding ~key:"tree" ~value:(take_while is_not_lf) <* commit
      >>= fun tree      -> many (binding ~key:"parent"
                                         ~value:(take_while is_not_lf))
                           <* commit
      >>= fun parents   -> binding ~key:"author" ~value:User.Decoder.user
                           <* commit
      >>= fun author    -> binding ~key:"committer" ~value:User.Decoder.user
                           <* commit
      >>= fun committer -> commit
      *>  return { tree = Hash.from_pp ~sentinel:`Tree tree
                 ; parents = List.map (Hash.from_pp ~sentinel:`Commit) parents
                 ; author
                 ; committer
                 ; message = "" }
      <* commit

    let to_result cs =
      let open Unbuffered in

      let rec aux = function
        | Fail (_, path, err) -> Error (String.concat " > " path ^ ": " ^ err)
        | Partial _ -> Error "incomplete input"
        | Done (committed, v) ->
          if Cstruct.len cs - committed > 0
          then let msg = Cstruct.to_string (Cstruct.sub cs committed (Cstruct.len cs - committed)) in
                   Ok { v with message = msg }
          else Ok v
      in

      Angstrom.Unbuffered.parse ~input:(`Bigstring (Cstruct.to_bigarray cs)) commit
      |> aux
  end

  module Encoder =
  struct
    open Farfadet

    let sp = ' '
    let lf = '\x0a'

    let parents e x = eval e [ string $ "parent"; char $ sp; !!string ] (Hash.to_pp x)

    let predicate f w e x =
      if f x
      then eval e w x
      else ()

    let is_empty l = List.length l <> 0

    let commit e t =
      let sep = (fun e () -> char e lf), () in

      eval e [ string $ "tree"; char $ sp; !!string; char $ lf
             ; !!(option (seq (list ~sep parents) (fun e () -> char e lf)))
             ; string $ "author"; char $ sp; !!User.Encoder.user; char $ lf
             ; string $ "committer"; char $ sp; !!User.Encoder.user; char $ lf
             ; !!string ]
        (Hash.to_pp t.tree)
        (match t.parents with [] -> None | lst -> Some (lst, ()))
        t.author
        t.committer
        t.message
  end
end

module Tree =
struct
  type entry =
    { perm : perm
    ; name : string
    ; node : 'a. ('a, [ `SHA1 ]) Hash.t }
  and perm =
    [ `Normal | `Everybody | `Exec | `Link | `Dir | `Commit ]
  and t = entry list

  let pp_entry fmt { perm
                   ; name
                   ; node } =
    Format.fprintf fmt "{ @[<hov>perm = %s;@ \
                                 name = %s;@ \
                                 node = %a;@] }"
      (match perm with
       | `Normal -> "normal"
       | `Everybody -> "everybody"
       | `Exec -> "exec"
       | `Link -> "link"
       | `Dir -> "dir"
       | `Commit -> "commit")
      name Hash.pp node

  let pp fmt tree =
    Format.fprintf fmt "[ @[<hov>%a@] ]"
      (pp_list ~sep:"; " pp_entry) tree

  module Decoder =
  struct
    open Angstrom

    let sp = char ' '
    let nl = char '\x00'
    let is_not_sp chr = chr <> ' '
    let is_not_nl chr = chr <> '\x00'

    let escape = '\042'

    let perm_of_string = function
      | "44"
      | "100644" -> `Normal
      | "100664" -> `Everybody
      | "100755" -> `Exec
      | "120000" -> `Link
      | "40000"  -> `Dir
      | "160000" -> `Commit
      | s -> raise (Invalid_argument "perm_of_string")

    let decode path =
      if not (String.contains path escape)
      then path
      else
        let n = String.length path in
        let b = Buffer.create n in
        let last = ref 0 in
        for i = 0 to n - 1
        do if path.[i] = escape
           then begin
             if i - !last > 0
             then Buffer.add_substring b path !last (i - !last);
             if i + 1 < n
             then let c = Char.chr (Char.code path.[i + 1] - 1) in
                  Buffer.add_char b c;
             last := i + 2;
           end
        done;

        if n - !last > 0
        then Buffer.add_substring b path !last (n - !last);
        Buffer.contents b

    let hash = Angstrom.take 20

    let sp = Format.sprintf

    let entry =
      take_while is_not_sp >>= fun perm ->
        (try return (perm_of_string perm)
         with _ -> fail (sp "Invalid permission %s" perm))
        <* commit
      >>= fun perm -> take 1 *> take_while is_not_nl <* commit
      >>= fun name -> take 1 *> hash <* commit
      >>= fun hash ->
        return { perm
               ; name
               ; node = (hash : ([ `Unknow ], [ `SHA1 ]) Hash.t) }
      <* commit

    let tree = many entry

    let to_result cs =
      let open Angstrom.Unbuffered in

      let bs = Cstruct.to_bigarray cs in

      let rec aux = function
        | Done (committed, v) -> Ok v
        | Partial { committed; continue; } -> aux @@ continue (`String "") Complete
        | Fail (_, path, err) -> Error (String.concat " > " path ^ ": " ^ err)
      in

      Angstrom.Unbuffered.parse ~input:(`Bigstring bs) tree
      |> aux
  end

  module Encoder =
  struct
    open Farfadet

    let sp = ' '
    let nl = '\x00'

    let string_of_perm = function
      | `Normal    -> "100644"
      | `Everybody -> "100664"
      | `Exec      -> "100755"
      | `Link      -> "120000"
      | `Dir       -> "40000"
      | `Commit    -> "160000"

    let entry e t =
      eval e [ !!string; char $ sp; !!string; char $ nl; !!string ]
        (string_of_perm t.perm)
        t.name (* XXX(dinosaure): in ocaml-git, we need to escape the byte
                                  '\x42' but I don't find any process like that
                                  in git and if I escape like ocaml-git, the
                                  SHA1 is different.
                *)
        t.node

    let tree e t =
      (list entry) e t
  end
end

module Blob =
struct
  type 'i t = { content : Cstruct.t }

  module Decoder =
  struct
    let to_result cs = Ok { content = cs }
  end

  module Encoder =
  struct
    let blob e { content } =
      Faraday.schedule_bigstring e (Cstruct.to_bigarray content)
  end
end

module Tag =
struct
  type t =
    { obj     : 'a. ('a, [ `SHA1 ]) Hash.t
    ; kind    : kind
    ; tag     : string
    ; tagger  : User.t option
    ; message : string }
  and kind = Blob | Commit | Tag | Tree

  let pp_kind fmt = function
    | Blob   -> Format.fprintf fmt "Blob"
    | Commit -> Format.fprintf fmt "Commit"
    | Tag    -> Format.fprintf fmt "Tag"
    | Tree   -> Format.fprintf fmt "Tree"

  let pp fmt { obj
             ; kind
             ; tag
             ; tagger
             ; message } =
    Format.fprintf fmt "{ @[<hov>obj = %a;@ \
                                 kind = %a;@ \
                                 tag = %s;@ \
                                 tagger = %a;@ \
                                 message = @[<hov>%a@]@] }"
      Hash.pp obj
      pp_kind kind
      tag
      (pp_option User.pp) tagger
      Format.pp_print_text message

  module Decoder =
  struct
    open Angstrom

    let sp = char ' '
    let lf = char '\x0a'
    let is_not_lf chr = chr <> '\x0a'

    let obj = take 40

    let kind =
      (string "blob"       *> return Blob)
      <|> (string "commit" *> return Commit)
      <|> (string "tag"    *> return Tag)
      <|> (string "tree"   *> return Tree)

    let binding
      : type a. key:string -> value:a Angstrom.t -> a Angstrom.t
      = fun ~key ~value ->
      string key *> sp *> value <* lf

    let sentinel_from_kind = function
      | Blob   -> `Blob
      | Commit -> `Commit
      | Tree   -> `Tree
      | Tag    -> `Tag

    let tag =
      binding ~key:"object" ~value:obj <* commit
      >>= fun obj    -> binding ~key:"type" ~value:kind
                        <* commit
      >>= fun kind   -> binding ~key:"tag" ~value:(take_while is_not_lf)
                        <* commit
      >>= fun tag    -> (option None
                                (binding ~key:"tagger" ~value:User.Decoder.user
                                 >>= fun user -> return (Some user)))
                        <* commit
      >>= fun tagger -> take 1 *> commit *>
        return { obj = Hash.from_pp ~sentinel:(sentinel_from_kind kind) obj
               ; kind
               ; tag
               ; tagger
               ; message = "" }
      <* commit

    let to_result cs =
      let open Unbuffered in

      let rec aux = function
        | Done (committed, v) ->
          if Cstruct.len cs - committed > 0
          then let msg = Cstruct.to_string (Cstruct.sub cs committed (Cstruct.len cs - committed)) in
                   Ok { v with message = msg }
          else Ok v
        | Fail (_, path, err) -> Error (String.concat " > " path ^ ": " ^ err)
        | Partial _ -> Error "incomplete input"
      in

      Angstrom.Unbuffered.parse ~input:(`Bigstring (Cstruct.to_bigarray cs)) tag
      |> aux
  end

  module Encoder =
  struct
    open Farfadet

    let sp = ' '
    let lf = '\x0a'

    let string_of_kind = function
      | Blob   -> "blob"
      | Commit -> "commit"
      | Tree   -> "tree"
      | Tag    -> "tag"

    let tag e t =
      let tagger e x = eval e [ string $ "tagger"; char $ sp; !!User.Encoder.user; char $ lf ] x in

      eval e [ string $ "object"; char $ sp; !!string; char $ lf
             ; string $ "type"; char $ sp; !!string; char $ lf
             ; string $ "tag"; char $ sp; !!string; char $ lf
             ; !!(option tagger); char $ lf
             ; !!string ]
        (Hash.to_pp t.obj)
        (string_of_kind t.kind)
        t.tag
        t.tagger
        t.message
  end
end
