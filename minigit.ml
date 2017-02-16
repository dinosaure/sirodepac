type 'a hash = string constraint 'a = [>]

external hash_tree   : string -> [ `Tree ] hash   = "%identity"
external hash_commit : string -> [ `Commit ] hash = "%identity"
external hash_blob   : string -> [ `Blob ] hash   = "%identity"
external hash_tag    : string -> [ `Tag ] hash    = "%identity"

let pp_hash fmt hash =
  for i = 0 to String.length hash - 1
  do Format.fprintf fmt "%02x" (Char.code @@ String.get hash i) done

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
    let lt = '<'
    let gt = '>'
    let is_not_lt chr = chr <> lt
    let is_not_gt chr = chr <> gt

    let user =
      let open Minidec in

      count_while is_not_lt
      >>= fun n       -> substring (n - 1) <* skip 2
      >>= fun name    -> take_while is_not_gt
      >>= fun email   -> skip 2 *> int64
      >>= fun second  -> char ' ' *> ((char '+' *> return `Plus)
                                      <|> (char '-' *> return `Minus))
      >>= fun sign    -> take 2 >>| int_of_string
      >>= fun hours   -> take 2 >>| int_of_string
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

    let to_result i =
      let open Result in
      Minidec.to_result i user >>| fst
  end
end

module Commit =
struct
  type t =
    { tree      : [ `Tree ] hash
    ; parents   : [ `Commit ] hash list
    ; author    : User.t
    ; committer : User.t
    ; message   : string  }

  let pp fmt { tree
             ; parents
             ; author
             ; committer
             ; message } =
    Format.fprintf fmt "{ @[<hov>tree = %s;@ \
                                 parents = [ @[<hov>%a@] ];@ \
                                 author = %a;@ \
                                 committer = %a;@ \
                                 message = @[<hov>%a@];@] }"
      tree
      (pp_list ~sep:"; " Format.pp_print_string) parents
      User.pp author User.pp committer
      Format.pp_print_text message;

  module Decoder =
  struct
    let sp = '\x20'
    let lf = '\x0a'
    let is_not_lf chr = chr <> lf

    let binding
      : type a. key:string -> value:a Minidec.t -> a Minidec.t
      = fun ~key ~value ->
      let open Minidec in

      string key *> char sp *> value <* char lf

    let commit =
      let open Minidec in

      binding ~key:"tree" ~value:(take_while is_not_lf)
      >>= fun tree      -> many (binding ~key:"parent" ~value:(take_while is_not_lf))
      >>= fun parents   -> binding ~key:"author" ~value:User.Decoder.user
      >>= fun author    -> binding ~key:"committer" ~value:User.Decoder.user
      >>= fun committer -> char lf
      *>  return { tree = hash_tree tree
                 ; parents = List.map hash_commit parents
                 ; author
                 ; committer
                 ; message = "" }

    let to_result i =
      match Minidec.to_result i commit with
      | Ok (v, p) ->
        let msg = B.to_string @@ B.sub i p (B.length i - p - 1) in
        Ok { v with message = msg }
      | Error exn -> Error exn
  end
end

module Tree =
struct
  type entry =
    { perm : perm
    ; name : string
    ; node : 'a. 'a hash }
  and perm =
    [ `Normal | `Exec | `Link | `Dir | `Commit ]
  and t = entry list

  let pp_entry fmt { perm
                   ; name
                   ; node } =
    Format.fprintf fmt "{ @[<hov>perm = %s;@ \
                                 name = %s;@ \
                                 node = %a;@] }"
      (match perm with
       | `Normal -> "normal"
       | `Exec -> "exec"
       | `Link -> "link"
       | `Dir -> "dir"
       | `Commit -> "commit")
      name pp_hash node

  let pp fmt tree =
    Format.fprintf fmt "[ @[<hov>%a@] ]"
      (pp_list ~sep:"; " pp_entry) tree

  module Decoder =
  struct
    let sp = '\x20'
    let nl = '\x00'
    let is_not_sp chr = chr <> sp
    let is_not_nl chr = chr <> nl

    let escape = '\042'

    let perm_of_string = function
      | "44" | "100644" -> `Normal
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

    let hash = Minidec.take 20

    type Minidec.error += Invalid_perm of string

    let entry =
      let open Minidec in

      take_while is_not_sp >>= fun perm ->
        (try return (perm_of_string perm)
         with _ -> fail (Invalid_perm perm))
      >>= fun perm -> skip 1 *> take_while is_not_nl >>| decode
      >>= fun name -> skip 1 *> hash
      >>= fun hash ->
        return { perm
               ; name
               ; node = (hash : [ `Unknow ] hash) }

    let tree = Minidec.many entry

    let to_result i =
      let open Result in
      Minidec.to_result i tree >>| fst
  end
end

module Blob =
struct
  type t = { content : string }

  module Decoder =
  struct
    let to_result i = { content = i }
  end
end

module Tag =
struct
  type t =
    { obj     : 'a. 'a hash
    ; kind    : kind
    ; tag     : string
    ; tagger  : User.t
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
      pp_hash obj
      pp_kind kind
      tag
      User.pp tagger
      Format.pp_print_text message

  module Decoder =
  struct
    let sp = '\x20'
    let lf = '\x0a'
    let is_not_lf chr = chr <> lf

    let obj = Minidec.take 40

    let kind =
      let open Minidec in

      (string "blob" *> return Blob)
      <|> (string "commit" *> return Commit)
      <|> (string "tag" *> return Tag)
      <|> (string "tree" *> return Tree)

    let binding
      : type a. key:string -> value:a Minidec.t -> a Minidec.t
      = fun ~key ~value ->
      let open Minidec in

      string key *> char sp *> value <* char lf

    let hash_from_kind = function
      | Blob -> hash_blob
      | Commit -> hash_commit
      | Tree -> hash_tree
      | Tag -> hash_tag

    let tag =
      let open Minidec in

      binding ~key:"object" ~value:obj
      >>= fun obj    -> binding ~key:"type"   ~value:kind
      >>= fun kind   -> binding ~key:"tag"    ~value:(take_while is_not_lf)
      >>= fun tag    -> binding ~key:"tagger" ~value:User.Decoder.user
      >>= fun tagger -> skip 1 *>
        return { obj = hash_from_kind kind obj
               ; kind
               ; tag
               ; tagger
               ; message = "" }

    let to_result i =
      match Minidec.to_result i tag with
      | Ok (v, p) ->
        let msg = B.to_string @@ B.sub i p (B.length i - p - 1) in
        Ok { v with message = msg }
      | Error exn -> Error exn
  end
end
