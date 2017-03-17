type 'a t

val make   : unit -> 'a t
val bind   : string -> 'a -> 'a t -> unit
val length : int -> 'a t -> int
val get    : int -> 'a t -> (string * 'a) list
