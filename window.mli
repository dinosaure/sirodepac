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
val iteri : 'a t -> (int -> 'a -> unit) -> unit
val fold : 'a t -> ('b -> 'a -> 'b) -> 'b -> 'b
val foldi : 'a t -> ('b -> int -> 'a -> 'b) -> 'b -> 'b
val nth_exn : 'a t -> int -> 'a
val push_back : 'a t -> 'a -> unit
