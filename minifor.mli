module B : module type of Minienc.B
  with type st = Minienc.B.st
   and type bs = Minienc.B.bs
   and type 'a t = 'a Minienc.B.t

module Blitter :
sig
  type ('i, 'a) t =
    { blit   : 'a -> int -> 'i B.t -> int -> int -> unit
    ; length : 'a -> int }
end

type vec =
  { off : int
  ; len : int option }

type _ atom =
  | BEu16  : int    atom
  | BEu32  : int32  atom
  | BEu64  : int64  atom
  | LEu16  : int    atom
  | LEu32  : int32  atom
  | LEu64  : int64  atom
  | Int16  : int    atom
  | Int32  : int32  atom
  | Int64  : int64  atom
  | String : string atom
  | Bool   : bool   atom
  | Char   : char   atom
  | Seq    : 'a atom * 'b atom -> ('a * 'b) atom
  | Opt    : 'a atom -> 'a option atom
  | List   : 'a atom -> 'a list atom

val beint16 : int    atom
val beint32 : int32  atom
val beint64 : int64  atom
val leint16 : int    atom
val leint32 : int32  atom
val leint64 : int64  atom
val int     : int    atom
val int32   : int32  atom
val int64   : int64  atom
val string  : string atom
val seq     : 'a atom -> 'b atom -> ('a * 'b) atom
val bool    : bool   atom
val char    : char   atom
val option  : 'a atom -> 'a option atom
val list    : 'a atom -> 'a list atom

type 'a const =
  | ConstChar      : char -> char const

  | ConstString    : vec * String.t      -> String.t      const
  | ConstBytes     : vec * Bytes.t       -> Bytes.t       const
  | ConstBigstring : vec * B.Bigstring.t -> B.Bigstring.t const

module Const :
sig
  val char      : char -> char const

  val string    : ?off:int -> ?len:int -> string        -> String.t      const
  val bytes     : ?off:int -> ?len:int -> Bytes.t       -> Bytes.t       const
  val bigstring : ?off:int -> ?len:int -> B.Bigstring.t -> B.Bigstring.t const
end

type ('i, 'ty, 'v, 'cty, 'cv) list =
  | Nil       : ('i, 'v, 'v, 'cv, 'cv) list
  | Close     : ('i, unit, unit, 'cv, 'cv) list
  | Const     :
       'a const
    *  ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i, 'ty, 'v, 'cty, 'cv) list
  | Direct    :
       'a atom
    *  ('i, 'ty,       'v, 'cty, 'cv) list
    -> ('i, 'a -> 'ty, 'v, 'cty, 'cv) list
  | Sched     :
       'a const
    *  ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i, 'ty, 'v, 'cty, 'cv) list
  | Blitter   :
       'a atom
    *  ('i, 'ty,              'v,                 'cty, 'cv) list
    -> ('i, vec option -> 'a -> 'ty, 'v, ('i, 'a) Blitter.t -> 'cty, 'cv) list
  | Yield     :
       ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i, 'ty, 'v, 'cty, 'cv) list
  | Shift     :
       int
    *  ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i, 'ty, 'v, 'cty, 'cv) list
  | Flush     :
       (unit -> unit)
    *  ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i, 'ty, 'v, 'cty, 'cv) list
  | App       :
       ('i Minienc.t -> 'a -> unit)
    *  ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i, 'a -> 'ty, 'v, 'cty, 'cv) list

type ('i, 'ty, 'v, 'final, 'cty, 'cv) ty

type ('i, 'ty, 'v, 'cty, 'cv) t =
  ('i, 'ty, 'v, 'cty, 'cv, 'cv) ty
  constraint 'cv = _ ty

val concat :
     ('i, 'ty, 'v, 'cty, 'cv) list
  -> ('i,  'v, 'r,  'cv, 'cr) list
  -> ('i, 'ty, 'r, 'cty, 'cr) list

val nil   : ('i, 'v, 'v, 'cv, 'cv) list
val close : ('i, unit, unit, 'cv, 'cv) list

module Infix :
sig
  val nil   : ('i, 'v, 'v, 'cv, 'cv) list
  val close : ('i, unit, unit, 'cv, 'cv) list

  val ( ** )  :
       'a const
    -> ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i, 'ty, 'v, 'cty, 'cv) list
  val ( **! ) :
       'a atom
    -> ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i, 'a -> 'ty, 'v, 'cty, 'cv) list
  val ( **~ ) :
       'a const
    -> ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i, 'ty, 'v, 'cty, 'cv) list
  val ( **? ) :
       'a atom
    -> ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i, vec option -> 'a -> 'ty, 'v, ('i, 'a) Blitter.t -> 'cty, 'cv) list
  val ( **> ) :
       int
    -> ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i, 'ty, 'v, 'cty, 'cv) list
  val ( **| ) :
       (unit -> unit)
    -> ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i, 'ty, 'v, 'cty, 'cv) list
  val ( **= ) :
       ('i Minienc.t -> 'a -> unit)
    -> ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i, 'a -> 'ty, 'v, 'cty, 'cv) list
  val ( ! )   :
       ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i, 'ty, 'v, 'cty, 'cv) list
  val ( @@ )  :
       ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i,  'v, 'r,  'cv, 'cr) list
    -> ('i, 'ty, 'r, 'cty, 'cr) list
end

val yield :
     ('i, 'ty, 'v, 'cty, 'cv) list
  -> ('i, 'ty, 'v, 'cty, 'cv) list

val shift :
     int
  -> ('i, 'ty, 'v, 'cty, 'cv) list
  -> ('i, 'ty, 'v, 'cty, 'cv) list

val add_const :
     'a const
  -> ('i, 'ty, 'v, 'cty, 'cv) list
  -> ('i, 'ty, 'v, 'cty, 'cv) list

val add_direct :
     'a atom
  -> ('i, 'ty, 'v, 'cty, 'cv) list
  -> ('i, 'a -> 'ty, 'v, 'cty, 'cv) list

val add_sched :
     'a const
  -> ('i, 'ty, 'v, 'cty, 'cv) list
  -> ('i, 'ty, 'v, 'cty, 'cv) list

val add_blitter :
     'a atom
  -> ('i, 'ty, 'v, 'cty, 'cv) list
  -> ('i, vec option -> 'a -> 'ty, 'v, ('i, 'a) Blitter.t -> 'cty, 'cv) list

val flush :
     (unit -> unit)
  -> ('i, 'ty, 'v, 'cty, 'cv) list
  -> ('i, 'ty, 'v, 'cty, 'cv) list

val add_app     :
     ('i Minienc.t -> 'a -> unit)
  -> ('i, 'ty, 'v, 'cty, 'cv) list
  -> ('i, 'a -> 'ty, 'v, 'cty, 'cv) list

val finalize :
  ('i, 'ty, 'v, [ `NotF ], 'cty,
   ('i, 'ty, 'v, [ `F ], _) t) ty -> 'cty

val make     :
     ('i, 'ty, 'v, 'cty, 'cv) list
  -> ('i, 'ty, 'v, [ `NotF ], 'cty, 'cv) ty

val keval :
     'i Minienc.t
  -> ('i, 'ty, 'v, _, _) t
  -> ('i Minienc.t -> 'v)
  -> 'ty

val eval  :
     'i Minienc.t
  -> ('i, 'ty, unit, _, _) t
  -> 'ty
