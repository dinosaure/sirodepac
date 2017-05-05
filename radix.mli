module type KEY =
sig
  type t

  val get : t -> int -> char
  val length : t -> int
end

module Make (Key : KEY) :
sig
  type 'a t
  type 'a sequence = ('a -> unit) -> unit

  val empty       : 'a t
  val is_empty    : 'a t -> bool
  val bind        : 'a t -> Key.t -> 'a -> 'a t
  val lookup      : 'a t -> Key.t -> 'a option
  val exists      : 'a t -> Key.t -> bool
  val fold        : (Key.t * 'a -> 'b -> 'b) -> 'b -> 'a t -> 'b
  val iter        : (Key.t * 'a -> unit) -> 'a t -> unit
  val to_sequence : 'a t -> (Key.t * 'a) sequence
  val to_list     : 'a t -> (Key.t * 'a) list
  val pp          : (Format.formatter -> Key.t -> 'a -> unit) -> Format.formatter -> 'a t -> unit
end
