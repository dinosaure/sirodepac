module B : module type of Decompress.B
  with type st   = Decompress.B.st
   and type bs   = Decompress.B.bs
   and type 'a t = 'a Decompress.B.t

type statut =
  | Complete
  | Incomplete

type error = ..
type error += Expected_character of (char list * char)
type error += Expected_string of (string * string)
type error += End

val pp_error : Format.formatter -> error -> unit

type ('a, 'i) state =
  | Read of { committed : int
            ; k : 'i B.t -> statut -> ('a, 'i) state }
  | Done of ('a * int)
  | Fail of error

type 'a t

val return : 'a -> 'a t
val fail   : error -> _ t

val ( >>= ) : 'a t -> ('a -> 'b t) -> 'b t
val ( >>| ) : 'a t -> ('a -> 'b) -> 'b t
val ( <$> ) : ('a -> 'b) -> 'a t -> 'b t
val ( <|> ) : 'a t -> 'a t -> 'a t
val ( *> )  : 'a t -> 'b t -> 'b t
val ( <* )  : 'a t -> 'b t -> 'a t

val lift    : ('a -> 'b) -> 'a t -> 'b t
val lift2   : ('a -> 'b -> 'c) -> 'a t -> 'b t -> 'c t

val parse   : 'a B.t -> 'b t -> ('b, 'a) state
val only    : 'a B.t -> 'b t -> ('b, 'a) state

val to_result : ?refiller:(int -> 'a B.t -> 'a B.t * statut) -> 'a B.t -> 'b t -> ('b * int, error * int) result

val commit      : unit t
val await       : unit t
val skip        : int -> unit t
val count       : int -> 'a t -> 'a list t
val char        : char -> char t
val ensure      : int -> string t
val peek_char   : char option t
val count_while : ?init: int -> (char -> bool) -> int t
val string      : string -> string t
val take_while  : (char -> bool) -> string t
val take        : int -> string t
val fix         : ('a t -> 'a t) -> 'a t
val option      : 'a t -> 'a t -> 'a t
val many        : 'a t -> 'a list t
val int64       : int64 t
val substring   : int -> string t
