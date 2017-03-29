module B = Decompress.B

module type MAPPER =
sig
  type fd

  val map : fd -> ?pos:int64 -> share:bool -> int -> B.Bigstring.t
end

module Window =
struct
  type t =
    { raw : B.Bigstring.t
    ; off : int64
    ; len : int } (* [len] must be positive. *)

  let inside offset t =
    offset >= t.off && offset < Int64.add t.off (Int64.of_int len)
end

module Make (Mapper : MAPPER) =
struct
  module Lru = Lru.F.Make(Int64)(Window)

  type t =
    { file  : Mapper.fd
    ; win   : Window.t list
    ; idx   : 'a. ('a, [ `SHA1 ]) Hash.t -> int64 option
    ; hash  : ([ `PACK ], [ `SHA1 ]) Hash.t }

  let get hash t =
    match t.idx hash with
    | None -> None
    | Some off ->
      
end
