module MatchingBlock :
sig
  type t =
    { a_start : int
    ; b_start : int
    ; length  : int }

  val pp : Format.formatter -> t -> unit
end

type 'key hashtbl = (module Hashtbl.S with type key = 'key)

val get_matching_blocks :
  hashtbl:'b hashtbl ->
  transform:('a -> 'b)
  -> compare:('b -> 'b -> int)
  -> a:'a array
  -> b:'a array
  -> MatchingBlock.t list

val matches :
  hashtbl:'a hashtbl ->
  compare:('a -> 'a -> int)
  -> 'a array
  -> 'a array
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
  hashtbl:'b hashtbl ->
  transform:('a -> 'b)
  -> compare:('b -> 'b -> int)
  -> context:int
  -> a:'a array
  -> b:'a array
  -> 'a Hunk.t list

val all_same : 'a Hunk.t list -> bool
val unified  : 'a Hunk.t list -> 'a Hunk.t list
val old_only : 'a Hunk.t list -> 'a Hunk.t list
val new_only : 'a Hunk.t list -> 'a Hunk.t list
val ranges   : 'a Hunk.t list -> 'a Range.t list
