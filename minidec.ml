(* Copyright (c) 2016 Inhabited Type LLC.

   All rights reserved.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions
   are met:

   1. Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.

   3. Neither the name of the author nor the names of his contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE CONTRIBUTORS ``AS IS'' AND ANY EXPRESS
   OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
   WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
   DISCLAIMED.  IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE FOR
   ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
   DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
   OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
   HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
   STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
   ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
   POSSIBILITY OF SUCH DAMAGE.

   XXX(dinosaure): This module is most inspired by the angstrom project but I
   update the core to have a pure functionnal approach of the parser
   combinator (specifically about the `commit` function) and use a more
   simplified API about the input (provided by decompress).

   However, the design and the most of this API was thinked by Inhabited Type.
*)

module B =
struct
  include Decompress.B

  let count_while v ?(init = 0) predicate =
    let i = ref init in
    let l = length v in

    while !i < l && predicate (get v !i) do incr i done;

    !i - init
end

module I =
struct
  type t = { init : int
           ; commit : int }

  let sub : type a. a B.t -> t -> int -> int -> a B.t =
    fun s t off len ->
      let off = off - t.init in
      B.sub s off len

  let get : type a. a B.t -> t -> int -> char =
    fun s t i ->
      let i = i - t.init in
      B.get s i

  let count_while : type a. a B.t -> t -> ?init:int -> (char -> bool) -> int =
    fun s t ?(init = 0) predicate ->
      B.count_while s ~init:(init - t.init) predicate

  let length : type a. a B.t -> t -> int =
    fun s t -> B.length s + t.init
end

type statut =
  | Complete
  | Incomplete

type error = ..
type error += End
type error += Expected_character of (char list * char)
type error += Expected_string of (string * string)

let pp_list ?(sep = "") pp_data fmt lst =
  let rec aux = function
    | [] -> ()
    | [ x ] -> pp_data fmt x
    | x :: r -> Format.fprintf fmt "%a%s" pp_data x sep; aux r
  in aux lst

let pp_error fmt = function
  | End -> Format.fprintf fmt "End"
  | Expected_character (chrs, chr) ->
    Format.fprintf fmt "(Expected_character (%a, %c))"
      (pp_list ~sep:"; " Format.pp_print_char) chrs
      chr
  | Expected_string (has, expect) ->
    Format.fprintf fmt "(Expected_string (%s, %s))"
      has expect
  | exn ->
    Format.fprintf fmt "<Minidec.exn>"

type ('a, 'i) state =
  | Read of { committed : int; k : 'i B.t -> statut -> ('a, 'i) state }
  | Done of ('a * int)
  | Fail of error

type ('a, 'i) k = 'i B.t -> int -> I.t -> statut -> 'a
type ('a, 'i) fail = (error -> ('a, 'i) state, 'i) k
type ('a, 'r, 'i) success = ('a -> ('r, 'i) state, 'i) k

type 'a t =
  { f : 'r 'i. (('r, 'i) fail -> ('a, 'r, 'i) success -> ('r, 'i) state, 'i) k }

let return v = { f = fun i p c s _ succ -> succ i p c s v }
let fail exn = { f = fun i p c s fail _ -> fail i p c s exn }

let ( >>= ) a f = { f = fun i p c s fail succ ->
  let succ' i' p' c' s' v = (f v).f i' p' c' s' fail succ in
  a.f i p c s fail succ' }

let ( >>| ) a f = { f = fun i p c s fail succ ->
  let succ' i' p' c' s' v = succ i' p' c' s' (f v) in
  a.f i p c s fail succ' }

let ( <$> ) f m = m >>| f

let ( <|> ) u v =
  { f = fun i p c s fail succ ->
    let fail' i' p' c' s' exn =
      if p < c'.I.commit
      then fail i' p' c' s' exn
      else v.f i' p c' s' fail succ
    in

    u.f i p c s fail' succ }

let ( *> ) a b =
  { f = fun i p c s fail succ ->
    let succ' i' p' c' s' _ = b.f i' p' c' s' fail succ in
    a.f i p c s fail succ' }

let ( <* ) a b =
  { f = fun i p c s fail succ ->
    let succ' i' p' c' s' x =
      let succ'' i'' p'' c'' s'' _ = succ i'' p'' c'' s'' x in
      b.f i' p' c' s' fail succ''
    in
    a.f i p c s fail succ' }

let lift f m = f <$> m

let lift2 f m1 m2 =
  { f = fun i p c s fail succ ->
    let succ' i' p' c' s' m' =
      let succ'' i'' p'' c'' s'' m'' =
        succ i'' p'' c'' s'' (f m' m'')
      in
      m2.f i' p' c' s' fail succ''
    in
    m1.f i p c s fail succ' }

let parse, only =
  let fail_k    i p c s exn = Fail exn in
  let success_k i p c s v = Done (v, p - c.I.init) in

  (fun i p -> p.f i 0 I.{ init = 0; commit = 0; } Incomplete fail_k success_k),
  (fun i p -> p.f i 0 I.{ init = 0; commit = 0; } Complete fail_k success_k)

let to_result ?refiller i p =
  let rec aux committed refiller state = match state, refiller with
    | Done (v, p), _ -> Ok (v, p)
    | Read { committed
           ; k }, Some refiller ->
      let (i, s) = refiller committed i in
      k i s |> aux committed (Some refiller)
    | Read _, None -> Error (End, committed)
    | Fail exn, _ -> Error (exn, committed)
  in match refiller with
     | Some refiller -> aux 0 (Some refiller) (parse i p)
     | None -> aux 0 None (only i p)

let rec prompt i p c fail succ =
  let uncommitted = I.length i c - c.I.commit in
  let commit      = c.I.commit in

  let continue i s =
    let len = B.length i in
    if len < uncommitted
    then raise (Failure "prompt: input shrunk!")
    else let c' = I.{ init = commit
                    ; commit } in
         if len = uncommitted
         then if s = Complete
              then fail i p c' Complete
              else prompt i p c' fail succ
         else succ i p c' s
  in

  Read { committed = c.I.commit - c.I.init
       ; k = continue }

let commit =
  { f = fun i p c s fail succ ->
    succ i p { c with I.commit = p } s () }

let await =
  { f = fun i p c s fail succ ->
    match s with
    | Complete -> fail i p c s End
    | Incomplete ->
      let succ' i' p' c' s' = succ i' p' c' s' ()
      and fail' i' p' c' s' = fail i' p' c' s' End in
      prompt i p c fail' succ' }

let ensure n i p c s fail succ =
  let rec go =
    { f = fun i' p' c' s' fail' succ' ->
      if p' + n <= I.length i' c'
      then succ' i' p' c' s' ()
      else (await *> go).f i' p' c' s' fail' succ' }
  in
  (await *> go).f i p c s fail succ

let char' ~expected predicate =
  { f = fun i p c s fail succ ->
    if p < I.length i c
    then match predicate (I.get i c p) with
         | None -> fail i p c s (Expected_character (expected, I.get i c p))
         | Some v -> succ i (p + 1) c s v
    else let succ' i' p' c' s' () =
           match predicate (I.get i' c' p') with
           | None -> fail i' p' c' s' (Expected_character (expected, I.get i' c' p'))
           | Some v -> succ i' (p' + 1) c' s' v
         in

         ensure 1 i p c s fail succ' }

let satisfy ~expected predicate =
  char' ~expected (fun c -> if predicate c then Some c else None)

let skip n =
  { f = fun i p c s fail succ ->
    if p + n < I.length i c
    then succ i (p + n) c s ()
    else let succ' i' p' c' s' () =
           succ i (p' + n) c' s' () in
         ensure n i p c s fail succ' }

let count n p =
  let rec aux = function
    | 0 -> return []
    | n -> lift2 (fun x r -> x :: r) p (aux (n - 1))
  in aux n

let char chr =
  satisfy ~expected:[ chr ] (fun chr' -> chr' = chr)

let substring n =
  { f = fun i p c s fail succ ->
    succ i (p + n) c s (B.to_string @@ I.sub i c p n) }

let ensure n =
  { f = fun i p c s fail succ ->
    if p + n <= I.length i c
    then succ i p c s ()
    else ensure n i p c s fail succ }
  *> substring n

let peek_char =
  { f = fun i p c s fail succ ->
    if p < I.length i c
    then succ i p c s (Some (I.get i c p))
    else if s = Complete
    then succ i p c s None
    else let succ' i' p' c' s' = succ i' p' c' s' (Some (I.get i' c' p'))
         and fail' i' p' c' s' = succ i' p' c' s' None in
         prompt i p c fail' succ' }

let count_while ?(init = 0) predicate =
  let rec go acc =
    { f = fun i p c s fail succ ->
      let n = I.count_while i c ~init:(p + acc) predicate in
      let acc' = n + acc in

      if p + acc' < I.length i c || s = Complete
      then succ i p c s acc'
      else
        let succ' i' p' c' s' = (go acc).f i' p' c' s' fail succ
        and fail' i' p' c' s' = succ i' p' c' s' acc' in
        prompt i p c fail' succ' }
  in go init

let string s =
  let len = String.length s in
  ensure len >>= fun s' ->
    if s = s' then return s' else fail (Expected_string (s, s'))

let take_while predicate =
  count_while predicate >>= substring

let take n =
  ensure (max n 0)

let fix f =
  let rec o = lazy (f r)
  and r = { f = fun i p c s fail succ ->
    Lazy.(force o).f i p c s fail succ }
  in r

let option x p = p <|> x

let many p =
  fix (fun m -> (lift2 (fun x r -> x :: r) p m) <|> return [])

let int64 =
  let is_digit = function '0' .. '9' -> true | _ -> false in
  take_while is_digit >>| Int64.of_string
