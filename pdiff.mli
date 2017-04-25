module MatchingBlock :
sig
  type t =
    { a_start : int
    ; b_start : int
    ; length  : int }

  val pp : Format.formatter -> t -> unit
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

val get_matching_blocks :
  array:('elt, 'arr) scalar
  -> hashtbl:'elt hashtbl
  -> compare:('elt -> 'elt -> int)
  -> a:'arr
  -> b:'arr
  -> MatchingBlock.t list

val matches :
  array:('elt, 'arr) scalar
  -> hashtbl:'elt hashtbl
  -> compare:('elt -> 'elt -> int)
  -> 'arr
  -> 'arr
  -> (int * int) list

module Range :
sig
  type 'a t =
    | Same of ('a * 'a) array
    | Old  of 'a array
    | New  of 'a array
    | Replace of 'a array * 'a array

  val all_same : 'a t list -> bool
  val old_only : 'a t list -> 'a t list
  val new_only : 'a t list -> 'a t list
  val pp       : ?sep:(Format.formatter -> unit) -> (Format.formatter -> 'a array -> unit) -> Format.formatter -> 'a t -> unit
end

module Hunk :
sig
  type 'a t =
    { a_start : int
    ; a_size  : int
    ; b_start : int
    ; b_size  : int
    ; ranges  : 'a Range.t list }

  val all_same : 'a t -> bool
  val pp_list  : ?sep:(Format.formatter -> unit) -> (Format.formatter -> 'a -> unit) -> Format.formatter -> 'a list -> unit
  val pp       : (Format.formatter -> 'a array -> unit) -> Format.formatter -> 'a t -> unit
end

val get_hunks :
  array:('elt, 'arr) scalar
  -> hashtbl:'elt hashtbl
  -> compare:('elt -> 'elt -> int)
  -> context:int
  -> a:'arr
  -> b:'arr
  -> 'elt Hunk.t list

val all_same : 'a Hunk.t list -> bool
val unified  : 'a Hunk.t list -> 'a Hunk.t list
val old_only : 'a Hunk.t list -> 'a Hunk.t list
val new_only : 'a Hunk.t list -> 'a Hunk.t list
val ranges   : 'a Hunk.t list -> 'a Range.t list
