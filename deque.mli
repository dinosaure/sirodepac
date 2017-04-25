type 'a t

val create : ?init:int -> ?never_shrink:bool -> unit -> 'a t
val length : 'a t -> int
val is_empty : 'a t -> bool

val front : 'a t -> int option
val back : 'a t -> int option

val foldi' : 'a t -> [ `front_to_back | `back_to_front ] -> 'b -> (int -> 'b -> 'a -> 'b) -> 'b
val fold' : 'a t -> [ `front_to_back | `back_to_front ] -> 'b -> ('b -> 'a -> 'b) -> 'b
val iteri' : 'a t -> [ `front_to_back | `back_to_front ] -> (int -> 'a -> unit) -> unit
val iter' : 'a t -> [ `front_to_back | `back_to_front ] -> ('a -> unit) -> unit
val fold : 'a t -> 'b -> ('b -> 'a -> 'b) -> 'b
val foldi : 'a t -> 'b -> (int -> 'b -> 'a -> 'b) -> 'b
val iteri : 'a t -> (int -> 'a -> unit) -> unit
val iter : 'a t -> ('a -> unit) -> unit

val enqueue_back : 'a t -> 'a -> unit
val enqueue_front : 'a t -> 'a -> unit
val enqueue : 'a t -> [ `front | `back ] -> 'a -> unit

val peek_front : 'a t -> 'a option
val peek_back : 'a t -> 'a option
val peek : 'a t -> [ `front | `back ] -> 'a option

val dequeue_back : 'a t -> 'a option
val dequeue_front : 'a t -> 'a option
val dequeue : 'a t -> [ `front | `back ] -> 'a option

val get : 'a t -> int -> 'a
val get_opt : 'a t -> int -> 'a option

val pp : (Format.formatter -> 'a -> unit) -> Format.formatter -> 'a t -> unit
