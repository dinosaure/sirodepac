type 'a t

val empty : _ t
val length : 'a t -> int
val create : int -> 'a t
val is_none : 'a t -> int -> bool
val is_some : 'a t -> int -> bool
val get : 'a t -> int -> 'a option
val get_some_exn : 'a t -> int -> 'a
val set : 'a t -> int -> 'a option -> unit
val set_some : 'a t -> int -> 'a -> unit
val set_none : 'a t -> int -> unit
val clear : 'a t -> unit
val blit : 'a t -> int -> 'a t -> int -> int -> unit
