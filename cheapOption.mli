type 'a t

val none : _ t
val some : 'a -> 'a t
val is_none : _ t -> bool
val is_some : _ t -> bool
val value_exn : 'a t -> 'a
val value_unsafe : 'a t -> 'a
val of_option : 'a option -> 'a t
val to_option : 'a t -> 'a option
