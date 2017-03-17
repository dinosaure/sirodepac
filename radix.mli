type 'a t

val empty    : 'a t
val is_empty : 'a t -> bool
val bind     : 'a t -> string -> 'a -> 'a t
val lookup   : 'a t -> string -> 'a option
