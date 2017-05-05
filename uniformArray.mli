type 'a t

val empty : _ t
val create : int -> 'a -> 'a t
val get : 'a t -> int -> 'a
val set : 'a t -> int -> 'a -> unit
val length : 'a t -> int
val iter : 'a t -> ('a -> unit) -> unit
val copy : 'a t -> 'a t
val blit : 'a t -> int -> 'a t -> int -> int -> unit
val pp : (Format.formatter -> 'a -> unit) -> Format.formatter -> 'a t -> unit
