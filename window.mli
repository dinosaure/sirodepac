type 'a t

val make : int -> 'a t
val capacity : 'a t -> int
val max : 'a t -> int
val length : 'a t -> int
val blit_from : 'a t -> 'a array -> int -> int -> unit
val clear : 'a t -> unit
val reset : 'a t -> unit
val take_front_exn : 'a t -> 'a
val junk_front : 'a t -> unit
val iter : 'a t -> ('a -> unit) -> unit
val push_back : 'a t -> 'a -> unit
