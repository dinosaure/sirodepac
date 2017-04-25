type t

val get : t -> int -> Obj.t
val set : t -> int -> Obj.t -> unit
val length : t -> int
val create : int -> t
val empty : t
val blit : t -> int -> t -> int -> int -> unit
val copy : t -> t
