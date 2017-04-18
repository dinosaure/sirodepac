type 'a t

val make : int -> 'a t
val iter : 'a t -> ('a -> unit) -> unit
val iteri : 'a t -> (int -> 'a -> unit) -> unit
val fold : 'a t -> ('b -> 'a -> 'b) -> 'b -> 'b
val foldi : 'a t -> ('b -> int -> 'a -> 'b) -> 'b -> 'b
val push : 'a t -> 'a -> unit
val nth : 'a t -> int -> 'a
