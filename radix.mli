type 'a t
type 'a sequence = ('a -> unit) -> unit

val empty       : 'a t
val is_empty    : 'a t -> bool
val bind        : 'a t -> string -> 'a -> 'a t
val lookup      : 'a t -> string -> 'a option
val fold        : (string * 'a -> 'b -> 'b) -> 'b -> 'a t -> 'b
val iter        : (string * 'a -> unit) -> 'a t -> unit
val to_sequence : 'a t -> (string * 'a) sequence
val to_list     : 'a t -> (string * 'a) list
val pp          : (Format.formatter -> string -> 'a -> unit) -> Format.formatter -> 'a t -> unit
