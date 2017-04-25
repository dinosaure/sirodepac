module type KEY =
sig
  type t

  val compare : t -> t -> int
  val get : t -> int -> char
end

module Make (Key : KEY) :
sig
  type 'a t

  val make   : unit -> 'a t
  val bind   : Key.t -> 'a -> 'a t -> unit
  val length : int -> 'a t -> int
  val get    : int -> 'a t -> (Key.t * 'a) list
end
