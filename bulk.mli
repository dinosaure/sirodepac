type 'a t

val make : int -> 'a t
val add  : 'a t -> 'a -> unit
val iter : 'a t -> ('a -> unit) -> unit
val find : 'a t -> ('a -> bool) -> 'a option
