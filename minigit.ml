module B =
struct
  include Decompress.B

  let blit_string src src_off dst dst_off len =
    for i = 0 to len - 1
    do set dst (dst_off + i) (String.get src (src_off + i)) done

  let blit_bytes src src_off dst dst_off len =
    for i = 0 to len - 1
    do set dst (dst_off + i) (Bytes.get src (src_off + i)) done

  let blit_bigstring src src_off dst dst_off len =
    for i = 0 to len - 1
    do set dst (dst_off + i) (Bigstring.get src (src_off + i)) done
end

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
    let lt = Minidec.char '<'
    let gt = Minidec.char '>'
    let sp = Minidec.char ' '
    let pl = Minidec.char '+'
    let mn = Minidec.char '-'

    let is_not_lt chr = chr <> '<'
    let is_not_gt chr = chr <> '>'

    let user =
      let open Minidec in

      count_while is_not_lt <* commit
      >>= fun n       -> substring (n - 1) <* skip 2 <* commit
      >>= fun name    -> take_while is_not_gt <* commit
      >>= fun email   -> skip 2 *> int64 <* commit
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

    let to_result i =
      let open Result in
      Minidec.to_result i user >>| fst
  end

  module Encoder =
  struct
    let lt = Minifor.Const.char '<'
    let gt = Minifor.Const.char '>'
    let sp = Minifor.Const.char ' '

    let bint64 =
      let open Minifor.Blitter in
      let blit src = B.blit_string (Int64.to_string src) in
      let length x = String.length @@ Int64.to_string x in
      { blit; length }

    let write_date encoder = function
      | None -> Minienc.write_string encoder "+0000"
      | Some { sign; hours; minutes } ->
        let open Minifor in
        let open Minifor.Infix in
        let date = char **! string **! nil in
        let fmt  = finalize (make date) in

        eval encoder fmt
          (match sign with | `Plus -> '+' | `Minus -> '-')
          (Format.sprintf "%02d%02d" hours minutes)

    let write_user encoder t =
      let open Minifor in
      let open Minifor.Infix in

      let user = string **! sp ** lt ** string **! gt ** sp ** int64 **? sp ** write_date **= nil in

      let fmt  = finalize (make user) bint64 in

      eval encoder fmt
        t.name
        t.email
        None (fst t.date)
        (snd t.date)
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
    let sp = Minidec.char ' '
    let lf = Minidec.char '\x0a'
    let is_not_lf chr = chr <> '\x0a'

    let binding
      : type a. key:string -> value:a Minidec.t -> a Minidec.t
      = fun ~key ~value ->
      let open Minidec in

      string key *> sp *> value <* lf <* commit

    let commit =
      let open Minidec in

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

    let to_result i =
      match Minidec.to_result i commit with
      | Ok (v, p) ->
        if B.length i - p > 0
        then let msg = B.to_string @@ B.sub i p (B.length i - p) in
             Ok { v with message = msg }
        else Ok v
      | Error (exn, _) -> Error exn
  end

  module Encoder =
  struct
    let sp = Minifor.Const.char ' '
    let lf = Minifor.Const.char '\x0a'

    let bparents =
      let open Minifor.Blitter in
      let blit src _ dst dst_off len =
        let len' = List.fold_left
          (fun off x ->
            B.blit_string "parent " 0 dst off 7;
            B.blit_string (Hash.to_pp x) 0 dst (off + 7) 40;
            B.set dst (off + 7 + 40) '\x0a';
            off + 7 + 40 + 1)
          dst_off src
        in
        assert (len' = dst_off + len)
      in
      let length l = List.fold_left (fun a x -> a + 7 + 40 + 1) 0 l in
      { blit; length; }

    let write_commit encoder t =
      let open Minifor in
      let open Minifor.Infix in

      let commit =
        (Const.string "tree") ** sp ** string **! lf **
        (list string) **?
        (Const.string "author") ** sp ** User.Encoder.write_user **= lf **
        (Const.string "committer") ** sp ** User.Encoder.write_user **= lf **
        string **! nil
      in

      let fmt = finalize (make commit) bparents in

      eval encoder fmt
        (Hash.to_pp t.tree)
        None t.parents
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
    let sp = Minidec.char ' '
    let nl = Minidec.char '\x00'
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

    let hash = Minidec.take 20

    type Minidec.error += Invalid_perm of string

    let entry =
      let open Minidec in

      take_while is_not_sp >>= fun perm ->
        (try return (perm_of_string perm)
         with _ -> fail (Invalid_perm perm))
        <* commit
      >>= fun perm -> skip 1 *> take_while is_not_nl <* commit
      >>= fun name -> skip 1 *> hash <* commit
      >>= fun hash ->
        return { perm
               ; name
               ; node = (hash : ([ `Unknow ], [ `SHA1 ]) Hash.t) }
      <* commit

    let tree = Minidec.many entry

    let to_result i =
      match Minidec.to_result i tree with
      | Ok (tree, _) -> Ok tree
      | Error (exn, _) -> Error exn
  end

  module Encoder =
  struct
    let sp = Minifor.Const.char ' '
    let nl = Minifor.Const.char '\x00'

    let string_of_perm = function
      | `Normal    -> "100644"
      | `Everybody -> "100664"
      | `Exec      -> "100755"
      | `Link      -> "120000"
      | `Dir       -> "40000"
      | `Commit    -> "160000"

    let escaped_characters =
      [ '\x42'; '\x00'; '\x2f' ]

    let has s l =
      List.fold_left
        (||)
        false
        (List.map (String.contains s) l)

    let encode path =
      if not (has path escaped_characters)
      then path
      else
        let n = String.length path in
        let b = Buffer.create n in
        let last = ref 0 in
        for i = 0 to n - 1
        do if List.mem path.[i] escaped_characters
           then begin
             let c = Char.chr (Char.code path.[i] + 1) in
             if i - !last > 0
             then Buffer.add_substring b path !last (i - !last);
             Buffer.add_char b '\x42';
             Buffer.add_char b c;
             last := i + 1;
           end
        done;
        if n - !last > 0
        then Buffer.add_substring b path !last (n - !last);
        Buffer.contents b

    let write_entry encoder t =
      let open Minifor in
      let open Minifor.Infix in

      let entry = string **! sp ** string **! nl ** string **! nil in
      let fmt   = finalize (make entry) in

      eval encoder fmt
        (string_of_perm t.perm)
        t.name (* XXX(dinosaure): in ocaml-git, we need to escape the byte
                                  '\x42' but I don't find any process like that
                                  in git and if I escape like ocaml-git, the
                                  SHA1 is different.
                *)
        t.node

    let write_tree encoder t =
      List.iter (write_entry encoder) t
  end
end

module Blob =
struct
  type 'i t = { content : 'i B.t }

  module Decoder =
  struct
    let to_result i = Ok { content = i }
  end

  module Encoder =
  struct
    let write_blob encoder { content } =
      Minienc.schedule encoder content
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
    let sp = Minidec.char ' '
    let lf = Minidec.char '\x0a'
    let is_not_lf chr = chr <> '\x0a'

    let obj = Minidec.take 40

    let kind =
      let open Minidec in

      (string "blob"       *> return Blob)
      <|> (string "commit" *> return Commit)
      <|> (string "tag"    *> return Tag)
      <|> (string "tree"   *> return Tree)

    let binding
      : type a. key:string -> value:a Minidec.t -> a Minidec.t
      = fun ~key ~value ->
      let open Minidec in

      string key *> sp *> value <* lf

    let sentinel_from_kind = function
      | Blob   -> `Blob
      | Commit -> `Commit
      | Tree   -> `Tree
      | Tag    -> `Tag

    let tag =
      let open Minidec in

      binding ~key:"object" ~value:obj <* commit
      >>= fun obj    -> binding ~key:"type" ~value:kind
                        <* commit
      >>= fun kind   -> binding ~key:"tag" ~value:(take_while is_not_lf)
                        <* commit
      >>= fun tag    -> (option (binding ~key:"tagger" ~value:User.Decoder.user
                                 >>| fun user -> Some user)
                                (return None))
                        <* commit
      >>= fun tagger -> skip 1 *> commit *>
        return { obj = Hash.from_pp ~sentinel:(sentinel_from_kind kind) obj
               ; kind
               ; tag
               ; tagger
               ; message = "" }
      <* commit

    let to_result i =
      match Minidec.to_result i tag with
      | Ok (v, p) ->
        let msg = B.to_string @@ B.sub i p (B.length i - p) in
        Ok { v with message = msg }
      | Error (exn, _) -> Error exn
  end

  module Encoder =
  struct
    let sp = Minifor.Const.char ' '
    let lf = Minifor.Const.char '\x0a'

    let string_of_kind = function
      | Blob   -> "blob"
      | Commit -> "commit"
      | Tree   -> "tree"
      | Tag    -> "tag"

    let write_tag encoder t =
      let open Minifor in
      let open Minifor.Infix in

      let write_user_option encoder = function
        | Some user ->
          Minienc.write_string encoder "tagger ";
          User.Encoder.write_user encoder user;
          Minienc.write_char encoder '\x0a'
        | None -> ()
      in

      let tag =
        (Const.string "object") ** sp ** string **! lf **
        (Const.string "type") ** sp ** string **! lf **
        (Const.string "tag") ** sp ** string **! lf **
        write_user_option **= lf **
        string **! nil
      in

      let fmt = finalize (make tag) in

      eval encoder fmt
        (Hash.to_pp t.obj)
        (string_of_kind t.kind)
        t.tag
        t.tagger
        t.message
  end
end
