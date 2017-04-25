let () = Printexc.record_backtrace true

module A = Bigarray.Array1

(* XXX(dinosaure): semantically, [j] is an index. *)
let ( <|> ) ar (i, j) =
  if j <= i
  then [| |]
  else Array.sub ar i (j - i)

module List =
struct
  include List

  let pp ?(sep = (fun fmt -> ())) pp_data fmt lst =
    let rec aux = function
      | [] -> ()
      | [ x ] -> pp_data fmt x
      | x :: r -> pp_data fmt x; sep fmt; aux r
    in
    aux lst

  let concat_map f l = List.map f l |> List.concat
end

module Option =
struct
  let map ~func a    = match a with Some a -> Some (func a) | None -> None
  let bind a f       = match a with Some a -> (f a) | None -> None
  let value_exn      = function Some a -> a | None -> raise (Invalid_argument "Option.value_exn")
  let value ~default = function Some a -> a | None -> default
  let some x         = Some x

  let pp pp_data fmt = function
    | Some x -> pp_data fmt x
    | None -> Format.fprintf fmt "<none>"
end

module OrderedSequence :
sig
  type elt = int * int
  type t = private elt array

  val compare_elt : elt -> elt -> int

  val create : (int * int) list -> t
  val is_empty : t -> bool

  val pp_elt : Format.formatter -> elt -> unit
  val pp : Format.formatter -> t -> unit
end = struct
  type elt = int * int

  let compare_elt = Compare.lexicographic
      [ (fun (_, y) (_, y') -> Compare.int y y')
      ; (fun (x, _) (x', _) -> Compare.int x x') ]

  type t = elt array

  let create l =
    let t = Array.of_list l in
    Array.sort compare_elt t; (* ordering *)
    t

  let is_empty x = Array.length x = 0

  let pp_elt fmt (x, y) = Format.fprintf fmt "(%d, %d)" x y

  let pp fmt ar =
    Format.fprintf fmt "[| @[<hov>%a@] |]"
      (List.pp
         ~sep:(fun fmt -> Format.fprintf fmt ";@ ")
         pp_elt)
      (Array.to_list ar)
end

(* XXX(dinosaure): implementation of the patience sorting algorithm as explained at
                   http://en.wikipedia.org/wiki/Patience_sorting
 *)
module Patience =
struct
  module Pile =
  struct
    type 'a t = 'a Stack.t

    let create first =
      let stk = Stack.create () in
      Stack.push first stk;
      stk

    let top stk = Stack.top stk

    let push stk value = Stack.push stk value

    let pp pp_data fmt stk =
      let pp_list ?(sep = (fun fmt -> ())) pp_data fmt lst =
        let rec aux = function
          | [] -> ()
          | [ x ] -> pp_data fmt x
          | x :: r -> pp_data fmt x; sep fmt; aux r
        in
        aux lst
      in
      Format.fprintf fmt "{< @[<hov>%a@] >}"
        (pp_list ~sep:(fun fmt -> Format.fprintf fmt ";@ ") pp_data)
        (Stack.fold (fun acc x -> x :: acc) [] stk)
  end

  module Piles =
  struct
    type 'a t = 'a Pile.t Dequeue.t

    let empty () = Dequeue.create ()

    let nth t i direction =
      let get index offset =
        Option.bind (index t) (fun index -> Dequeue.nth t (index + offset))
      in match direction with
      | `Left -> get Dequeue.front_index i
      | `Right -> get Dequeue.back_index (- i)

    let push t p = Dequeue.push_back t p

    let pp pp_data fmt piles =
      Dequeue.pp (Pile.pp pp_data) fmt piles
  end

  module BackPointers =
  struct
    (* in the terminology of the Wikipedia article, this corrresponds to a card
       together with its back-pointers.
     *)
    type 'a tag = 'a t
    and 'a t =
      { value : 'a
      ; tag   : 'a tag option }

    let to_list t =
      let rec to_list acc t =
        match t.tag with
        | None -> t.value :: acc
        | Some t' -> to_list (t.value :: acc) t'
      in
      to_list [] t

    let rec pp pp_data fmt tag =
      let pp_option pp_data fmt = function
        | Some a -> pp_data fmt a
        | None -> Format.fprintf fmt "<none>"
      in
      Format.fprintf fmt "{ @[<hov>value = %a;@ \
                                   tag = %a;@] }"
        pp_data tag.value
        (pp_option (pp pp_data)) tag.tag
  end

  module PlayPatience :
  sig
    val play : OrderedSequence.t
      -> tag:(pile:int option
              -> piles:OrderedSequence.elt BackPointers.t Piles.t
              -> OrderedSequence.elt BackPointers.tag option)
      -> OrderedSequence.elt BackPointers.tag Piles.t
  end = struct
    let findi_from_left piles x =
      let last_pile  = Piles.nth piles 0 `Right in (* first, see if any work. *)

      let dummy_pile
        : OrderedSequence.elt BackPointers.t Stack.t
        = Pile.create { BackPointers.value = (x, 0)
                                   ; tag = None } in
      (* dummy pile used for comparisons. *)

      let compare pile1 pile2 =
        let top pile = fst (Pile.top pile).BackPointers.value in
        Compare.int (top pile1) (top pile2)
      in

      match last_pile with
      | Some last_pile ->
        if compare last_pile dummy_pile < 0
        then None
        else
          BinarySearch.search piles
            ~compare
            ~get:Dequeue.nth_exn
            ~length:Dequeue.length
            `FSG dummy_pile
      | None -> None

    (* [play ar ~tag] plays patience with the greedy algorithm as described in
       the Wikipedia artcicle, taking [ar] to be the deck of cards, It returns
       the resulting [Piles.t]. Before putting an element of [ar] in a pile, it
       tags it using [tag]. [tag] takes as its arguments the full [Piles.t] in
       its current state, and also the specific [Pile.t] that the element of
       [ar] is being added to.
     *)
    let play ar ~tag =
      let ar = (ar : OrderedSequence.t :> OrderedSequence.elt array) in

      if Array.length ar = 0
      then raise (Invalid_argument "PlayPatience.play")
      else
        let piles = Piles.empty () in
        Array.iter
          (fun (x : OrderedSequence.elt) ->
            let pile     = findi_from_left piles (fst x) in
            let tagged   = { BackPointers.value = x
                           ; tag = tag ~pile ~piles } in

            match pile with
            | None -> Piles.push piles (Pile.create tagged)
            | Some idx -> Pile.push tagged (Dequeue.nth_exn piles idx))
          ar;
        piles
  end

  let longest_increasing_subsequence ar =
    if OrderedSequence.is_empty ar
    then []
    else
      let module P = PlayPatience in
      let tag ~pile ~piles =
        match pile with
        | None -> Piles.nth piles 0 `Right
                  |> Option.map ~func:Pile.top
        | Some i ->
          if i = 0 then None
          else Piles.nth piles (i - 1) `Left
               |> Option.value_exn
               |> Pile.top
               |> Option.some
      in

      let piles = P.play ar ~tag in
      Piles.nth piles 0 `Right
      |> Option.value_exn
      |> Pile.top
      |> BackPointers.to_list
end

(* specialization with [int]. *)
let max_int : int -> int -> int = Pervasives.max

let longest_increasing_subsequence ar =
  let ar  = (ar : OrderedSequence.t :> (int * int) array) in
  let len = Array.length ar in

  if len <= 1
  then Array.to_list ar
  else begin
    let maxlen = ref 0 in
    let m      = Array.make (len + 1) (-1) in
    let pred   = Array.make (len + 1) (-1) in

    for i = 0 to len - 1
    do
      let p = BinarySearch.search
                ~len:(max_int (!maxlen - 1) 0) ~pos:1
                ~get:Array.get
                ~length:Array.length
                ~compare:OrderedSequence.compare_elt ar `FGE ar.(i)
              |> Option.value ~default:0
      in

      pred.(i) <- m.(p);

      if (p = !maxlen) || (Pervasives.(<) ar.(i) ar.(p + 1))
      then begin
        m.(p + 1) <- i;

        if (p + 1) > !maxlen
        then maxlen := p + 1;
      end;
   done;

   let rec loop acc p =
     if p = (-1)
     then acc
     else loop (ar.(p) :: acc) pred.(p)
   in

   loop [] m.(!maxlen)
  end

type 'key hashtbl = (module Hashtbl.S with type key = 'key)

type _ s =
  | Bigarray : ('elt, _, Bigarray.c_layout) Bigarray.Array1.t s
  | Array    : 'elt array s

module type A =
sig
  type t
  type elt

  val length : t -> int
  val get    : t -> int -> elt
  val sub    : t -> int -> int -> t
  val empty  : t
  val to_array : t -> elt array
  val to_list  : t -> elt list

  val sentinel : t s
end

type ('elt, 'arr) scalar = (module A with type t = 'arr and type elt = 'elt)

(* patience diff algorithm by Bram Cohen as seen in Bazaar.1.14.1 *)
let unique_lcs
  : type elt arr. array:(elt, arr) scalar -> hashtbl:elt hashtbl -> (arr * int * int) -> (arr * int * int) -> OrderedSequence.elt list
  = fun ~array ~hashtbl (alpha, alo, ahi) (bravo, blo, bhi) ->
  let module H = (val hashtbl) in
  let module A = (val array) in
  let unique = H.create (min (ahi - alo) (bhi - blo)) in

  for i = alo to ahi - 1
  do
    let x = A.get alpha i in

    try let _ = H.find unique x in
      H.replace unique x `Neg
    with Not_found -> H.replace unique x (`UIA i)
  done;

  for i = blo to bhi - 1
  do
    let x = A.get bravo i in
    try match H.find unique x with
      | `Neg -> ()
      | `UIA i_a -> H.replace unique x (`UIAB (i_a, i))
      | `UIAB (i_a, i_b) -> H.replace unique x `Neg
    with Not_found -> ()
  done;

  let diff =
    let unique = H.fold
        (fun key value acc -> match value with
           | `UIAB i_a_b -> i_a_b :: acc
           | `Neg | `UIA _ -> acc)
        unique []
    in

    OrderedSequence.create unique
  in

  Patience.longest_increasing_subsequence diff

(* [matches a b] returns a list of pairs (i, j) such that a.(i) = b.(j) and such
   that the list is strictly increasing in both its first and second
   coordinates.contents

   This is done by first applying [unique_lcs] to find matches from a to b among
   those elements which are unique in both a and b, and then recursively
   applying [matches] to each subinterval determined by those matches. The
   uniqueness requirement is waived for blocks of matching lines at the
   beginning or end.contents

   I couldn't figure out how to do this efficiently in a functionnal way, so
   this is pretty much a straight translation of the original Python code.
*)
let matches
  : type elt arr. array:(elt, arr) scalar -> hashtbl:elt hashtbl -> compare:(elt -> elt -> int) -> arr -> arr -> (int * int) list
  = fun ~array ~hashtbl ~compare alpha bravo ->
  let matches_ref_length = ref 0 in
  let matches_ref        = ref [] in

  let add_match m =
    incr matches_ref_length;
    matches_ref := m :: !matches_ref
  in

  let module A = (val array) in

  let rec recurse_matches alo blo ahi bhi =
    let old_left = !matches_ref_length in

    if not (alo >= ahi || blo >= bhi)
    then begin
      let last_a_pos = ref (alo - 1) in
      let last_b_pos = ref (blo - 1) in

      unique_lcs ~array ~hashtbl (alpha, alo, ahi) (bravo, blo, bhi)
      |> List.iter
        (fun (apos, bpos) ->
           if !last_a_pos + 1 <> apos || !last_b_pos + 1 <> bpos
           then recurse_matches (!last_a_pos + 1) (!last_b_pos + 1) apos bpos;

           last_a_pos := apos;
           last_b_pos := bpos;
           add_match (apos, bpos));

      if !matches_ref_length > old_left
      then recurse_matches (!last_a_pos + 1) (!last_b_pos + 1) ahi bhi
      else if (compare (A.get alpha alo) (A.get bravo blo) = 0)
      then begin
        let alo = ref alo in
        let blo = ref blo in

        while (!alo < ahi && !blo < bhi && (compare (A.get alpha !alo) (A.get bravo !blo) = 0))
        do
          add_match (!alo, !blo);
          incr alo;
          incr blo;
        done;

        recurse_matches !alo !blo ahi bhi
      end else if (compare (A.get alpha (ahi - 1)) (A.get bravo (bhi - 1)) = 0)
      then begin
        let nahi = ref (ahi - 1) in
        let nbhi = ref (bhi - 1) in

        while (!nahi > alo && !nbhi > blo && compare (A.get alpha (!nahi - 1)) (A.get bravo (!nbhi - 1)) = 0)
        do
          decr nahi;
          decr nbhi;
        done;

        recurse_matches (!last_a_pos + 1) (!last_b_pos + 1) !nahi !nbhi;

        for i = 0 to (ahi - !nahi - 1)
        do add_match (!nahi + i, !nbhi + i) done
      end else match A.sentinel with
        | Array ->
          PlainDiff.iter_matches
            (A.sub alpha alo (ahi - alo))
            (A.sub bravo blo (bhi - blo))
            (fun (i1, i2) -> add_match (alo + i1, blo + i2))
        | Bigarray ->
          PlainDiff.iter_matches_bigarray
            (A.sub alpha alo (ahi - alo))
            (A.sub bravo blo (bhi - blo))
            (fun (i1, i2) -> add_match (alo + i1, blo + i2))
      (* XXX(dinosaure): this hack ... *)
    end
  in
  recurse_matches 0 0 (A.length alpha) (A.length bravo);
  List.rev !matches_ref

module MatchingBlock =
struct
  type t =
    { a_start : int
    ; b_start : int
    ; length  : int }

  let pp fmt { a_start; b_start; length; } =
    Format.fprintf fmt "{ @[<hov>a_start = %d; \
                                 b_start = %d; \
                                 length = %d;@] }"
      a_start b_start length
end

let collapse_sequences matches =
  let collapsed = ref [] in
  let start_a = ref None in
  let start_b = ref None in
  let length  = ref 0 in

  List.iter (fun (i_a, i_b) ->
      match !start_a, !start_b with
      | Some start_a_val, Some start_b_val
        when (i_a = start_a_val + !length && i_b = start_b_val + !length) ->
        incr length
      | _ ->
        let () = match !start_a, !start_b with
          | Some start_a_val, Some start_b_val ->
            let matching_block =
              { MatchingBlock.a_start = start_a_val
              ; b_start = start_b_val
              ; length = !length }
            in
            collapsed := matching_block :: !collapsed
          | _ -> ()
        in

        start_a := Some i_a;
        start_b := Some i_b;
        length := 1)
    matches;

  let () = match !start_a, !start_b with
    | Some start_a_val, Some start_b_val when !length <> 0 ->
      let matching_block =
        { MatchingBlock.a_start = start_a_val
        ; b_start = start_b_val
        ; length = !length }
      in
      collapsed := matching_block :: !collapsed
    | _ -> ()
  in

  List.rev !collapsed

let get_matching_blocks
  : type elt arr. array:(elt, arr) scalar ->
                  hashtbl:elt hashtbl ->
                  compare:(elt -> elt -> int) ->
                  a:arr -> b:arr -> MatchingBlock.t list
  = fun ~array ~hashtbl ~compare ~a ~b ->
  let module A = (val array) in

  let matches = matches ~array ~hashtbl ~compare a b |> collapse_sequences in
  let last =
    { MatchingBlock.a_start = A.length a
    ; b_start = A.length b
    ; length = 0 }
  in

  List.append matches [ last ]

module Range =
struct
  type 'a t =
    | Same of ('a * 'a) array
    | Old  of 'a array
    | New  of 'a array
    | Replace of 'a array * 'a array

  let pp ?(sep = (fun fmt -> ())) pp_data fmt = function
    | Same ar ->
      Format.fprintf fmt "= %a" pp_data (Array.map fst ar);
      sep fmt
    | Old ar ->
      Format.fprintf fmt "< %a" pp_data ar;
      sep fmt
    | New ar ->
      Format.fprintf fmt "> %a" pp_data ar;
      sep fmt
    | Replace (a, b) ->
      Format.fprintf fmt "<r%a" pp_data a;
      sep fmt;
      Format.fprintf fmt "r>%a" pp_data b;
      sep fmt

  let all_same ranges =
    List.for_all
      (function Same _ -> true | _ -> false)
      ranges

  let old_only ranges =
    List.concat_map
      (function Replace (a_range, _) -> [ Old a_range ]
              | New _ -> []
              | range -> [ range ])
      ranges

  let new_only ranges =
    List.concat_map
      (function Replace (_, b_range) -> [ New b_range ]
              | Old _ -> []
              | range -> [ range ])
      ranges
end

module Hunk =
struct
  type 'a t =
    { a_start : int
    ; a_size  : int
    ; b_start : int
    ; b_size  : int
    ; ranges  : 'a Range.t list }

  let make a_start a_stop b_start b_stop ranges =
    { a_start = a_start + 1
    ; a_size  = a_stop - a_start
    ; b_start = b_start + 1
    ; b_size  = b_stop - b_start
    ; ranges  = List.rev ranges }

  let all_same hunk = Range.all_same hunk.ranges

  let pp_list ?(sep = (fun fmt -> ())) pp_data fmt lst =
    let rec aux = function
      | [] -> ()
      | [ x ] -> pp_data fmt x
      | x :: r -> pp_data fmt x; sep fmt; aux r
    in
    aux lst

  let pp pp_data fmt hunk =
    Format.fprintf fmt "{ @[<hov>a_start = %d;@ \
                                 a_size = %d;@ \
                                 b_start = %d;@ \
                                 b_size = %d;@ \
                                 ranges = [@[<hov>%a@]]@] }"
      hunk.a_start hunk.a_size
      hunk.b_start hunk.b_size
      (pp_list (Range.pp ~sep:(fun fmt -> Format.fprintf fmt "@\n") pp_data)) hunk.ranges
end

let get_range_rev
  : type elt arr. array:(elt, arr) scalar -> hashtbl:elt hashtbl -> compare:(elt -> elt -> int) -> a:arr -> b:arr -> elt Range.t list
  = fun ~array ~hashtbl ~compare ~a ~b ->
  let module A = (val array) in

  let ( <|> ) ar (i, j) =
    if j <= i
    then A.empty
    else A.sub ar i (j - i)
  in

  let rec aux matching_blocks i j l =
    match matching_blocks with
    | x :: r ->
      let a_index, b_index, size =
        x.MatchingBlock.a_start,
        x.MatchingBlock.b_start,
        x.MatchingBlock.length
      in

      if a_index < i || b_index < j
      then aux r i j l
      else
        let range_opt =
          if i < a_index && j < b_index
          then
            let a_range = a <|> (i, a_index) in
            let b_range = b <|> (j, b_index) in
            Some (Range.Replace (A.to_array a_range, A.to_array b_range))
          else if i < a_index
          then
            let a_range = a <|> (i, a_index) in
            Some (Range.Old (A.to_array a_range))
          else if j < b_index
          then
            let b_range = b <|> (j, b_index) in
            Some (Range.New (A.to_array b_range))
          else None
        in

        let l = match range_opt with Some range -> range :: l | None -> l in
        let a_stop, b_stop = a_index + size, b_index + size in

        let l =
          if size = 0
          then l
          else
            let a_range = a <|> (a_index, a_stop) in
            let b_range = b <|> (b_index, b_stop) in
            let range   = Array.map2
                (fun x y -> (x, y))
                (A.to_array a_range) (A.to_array b_range)
            in
            Range.Same range :: l
        in

        aux r a_stop b_stop l
    | [] -> List.rev l
  in

  let matching_blocks = get_matching_blocks ~array ~hashtbl ~compare ~a ~b in
  aux matching_blocks 0 0 []

let all_same hunks =
  match hunks with
  | [] -> true
  | _ -> match List.filter Hunk.all_same hunks with
    | [] -> false
    | _ -> true

let get_hunks
  : type elt arr. array:(elt, arr) scalar -> hashtbl:elt hashtbl -> compare:(elt -> elt -> int) -> context:int -> a:arr -> b:arr -> elt Hunk.t list
  = fun ~array ~hashtbl ~compare ~context ~a ~b ->
  let module A = (val array) in
  let ranges = get_range_rev ~array ~hashtbl ~compare ~a ~b in

  if context < 0
  then
    let singleton = Hunk.make 0 (A.length a) 0 (A.length b) (List.rev ranges)
    in [ singleton ]
  else
    let rec aux rest ranges alo ahi blo bhi acc = match rest with
      | [] ->
        let hunk = Hunk.make alo ahi blo bhi ranges in (* finish the last hunk. *)
        let acc  = hunk :: acc in (* add it to the accumulator. *)

        List.rev acc (* finish! *)
      | Range.Same range :: [] ->
        let stop   = min (Array.length range) context in (* if the last range is a [Same], we might need to crop to context. *)
        let range  = Range.Same (range <|> (0, stop)) in
        let ranges = range :: ranges in
        (* finish the current hunk. *)
        let ahi    = ahi + stop in
        let bhi    = bhi + stop in
        let hunk   = Hunk.make alo ahi blo bhi ranges in
        let acc    = hunk :: acc in (* add it to the accumulator. *)

        List.rev acc (* finish! *)
      | Range.Same range :: rest ->
        let size = Array.length range in

        if size > context * 2
        then
          let range' = Range.Same (range <|> (0, context)) in (* if this [Same] range is sufficiently large, split off a new hunk. *)
          let ranges = range' :: ranges in
          (* advance both hi's by context. *)
          let ahi = ahi + context in
          let bhi = bhi + context in
          let hunk = Hunk.make alo ahi blo bhi ranges in (* finish the current hunk. *)
          let acc = hunk :: acc in
          (* calculate ranges for the next hunk. *)
          let alo = ahi + size - 2 * context in
          let ahi = alo in
          let blo = bhi + size - 2 * context in
          let bhi = blo in

          (* push the remainder of the [Equal] range back onto [rest]. *)
          let rest = Range.Same (range <|> (size - context, size)) :: rest in
          aux rest [] alo ahi blo bhi acc
        else
          (* toherwise, this range is small enough that it qualifies as context for
             the both the previous and forthcoming range, so simply add it to
             [ranges] untouched.
           *)
          let ranges = Range.Same range :: ranges in
          let ahi = ahi + size in
          let bhi = bhi + size in
          aux rest ranges alo ahi blo bhi acc
      | range :: rest ->
        (* any range that isn't an [Equal] is important and not just context, so keep
           it in [ranges].
         *)
        let ranges = range :: ranges in
        (* rest could be anything, so extract informations. *)
        let ahi, bhi =
          match range with
          | Range.Same _ -> assert false (* we eliminate the possibility of a [Same] above. *)
          | Range.New range ->
            let stop = bhi + (Array.length range) in
            (ahi, stop)
          | Range.Old range ->
            let stop = ahi + (Array.length range) in
            (stop, bhi)
          | Range.Replace (a_range, b_range) ->
            let a_stop = ahi + Array.length a_range in
            let b_stop = bhi + Array.length b_range in
            (a_stop, b_stop)
        in
        aux rest ranges alo ahi blo bhi acc
    in
    let ranges, alo, ahi, blo, bhi =
      match ranges with
      (* if the first range is an [Equal], shave off the front of the range, according to
         context. Keep it on the ranges list so hunk construction can see where the range
         begins.
       *)
      | Range.Same range :: rest ->
        let stop = Array.length range in
        let start = max 0 (stop - context) in
        let range = Range.Same (range <|> (start, stop)) in
        (range :: rest, start, start, start, start)
      | rest -> (rest, 0, 0, 0, 0)
    in
    aux ranges [] alo ahi blo bhi []

let concat_map_ranges func hunks =
  List.map
    (fun hunk -> { hunk with Hunk.ranges = List.concat_map func hunk.Hunk.ranges })
    hunks

let unified hunks =
  concat_map_ranges
    (function Range.Replace (a_range, b_range) -> [ Range.Old a_range; Range.New b_range ]
            | range -> [ range ])
    hunks

let old_only hunks =
  concat_map_ranges
    (function Range.Replace (a_range, _) -> [ Range.Old a_range ]
            | Range.New _ -> []
            | range -> [ range ])
    hunks

let new_only hunks =
  concat_map_ranges
    (function Range.Replace (_, b_range) -> [ Range.New b_range ]
            | Range.Old _ -> []
            | range -> [ range ])
    hunks

let ranges hunks =
  List.concat_map
    (fun hunk -> hunk.Hunk.ranges)
    hunks
