module B : module type of Minienc.B
  with type st = Minienc.B.st
   and type bs = Minienc.B.bs
   and type 'a t = 'a Minienc.B.t
         = Minienc.B

module Blitter =
struct
  type ('i, 'a) t =
    { blit    : 'a -> int -> 'i B.t -> int -> int -> unit
    ; length  : 'a -> int }
end

type vec = { off : int
           ; len : int option }

type (_, _) prelist =
  | Empty : ('a, 'a) prelist
  | Cons  :
        ('i, 'a) Blitter.t * ('h, 'r) prelist
    -> (('i, 'a) Blitter.t -> 'h, 'r) prelist

type _ atom =
  | BEu16   : int    atom
  | BEu32   : int32  atom
  | BEu64   : int64  atom
  | LEu16   : int    atom
  | LEu32   : int32  atom
  | LEu64   : int64  atom
  | Int16   : int    atom
  | Int32   : int32  atom
  | Int64   : int64  atom
  | String  : string atom
  | Bool    : bool   atom
  | Char    : char   atom
  | Seq     : 'a atom * 'b atom -> ('a * 'b) atom
  | Opt     : 'a atom -> 'a option atom
  | List    : 'a atom -> 'a list atom

let beint16  = BEu16
let beint32  = BEu32
let beint64  = BEu64
let leint16  = LEu16
let leint32  = LEu32
let leint64  = LEu64
let int      = Int16
let int32    = Int32
let int64    = Int64
let string   = String
let bool     = Bool
let char     = Char
let seq x y  = Seq (x, y)
let option x = Opt x
let list x   = List x

type 'a const =
  | ConstChar      : char -> char const
  | ConstString    : vec * String.t      -> String.t      const
  | ConstBytes     : vec * Bytes.t       -> Bytes.t       const
  | ConstBigstring : vec * B.Bigstring.t -> B.Bigstring.t const

module Const =
struct
  let char s =
    ConstChar s

  let string    ?(off = 0) ?len s =
    ConstString    ({ off; len }, s)
  let bytes     ?(off = 0) ?len s =
    ConstBytes     ({ off; len }, s)
  let bigstring ?(off = 0) ?len s =
    ConstBigstring ({ off; len }, s)
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

type ('i, 'ty, 'v, 'final, 'cty, 'cv) ty =
  | Convertible :
       ('i, 'ty, 'v, 'cty, 'cv) list
    -> ('i, 'ty, 'v, [ `NotF ], 'cty, 'cv) ty
  | Finalized :
       ('i, 'ty, 'v, 'cty, ('i, 'ty, 'v, [ `F ], 'cv, 'cv) ty) list
    *               ('cty, ('i, 'ty, 'v, [ `F ], 'cv, 'cv) ty) prelist
    -> ('i, 'ty, 'v, [ `F ], 'cv, 'cv) ty

type ('i, 'ty, 'v, 'cty, 'cv) t =
  ('i, 'ty, 'v, 'cty, 'cv, 'cv) ty
  constraint 'cv = _ ty

let rec concat
  : type i ty v r cty cv cr.
     (i, ty, v, cty, cv) list
  -> (i,  v, r,  cv, cr) list
  -> (i, ty, r, cty, cr) list
  = fun l1 l2 -> match l1 with
    | Nil -> l2
    | Close -> l2
    | Const   (x, r) -> Const   (x, concat r l2)
    | Direct  (x, r) -> Direct  (x, concat r l2)
    | Sched   (x, r) -> Sched   (x, concat r l2)
    | Blitter (x, r) -> Blitter (x, concat r l2)
    | Yield       r  -> Yield   (concat r l2)
    | Shift   (x, r) -> Shift   (x, concat r l2)
    | Flush   (f, r) -> Flush   (f, concat r l2)
    | App     (f, r) -> App     (f, concat r l2)

let nil             = Nil
let close           = Close
let add_const   x r = Const   (x, r)
let add_direct  x r = Direct  (x, r)
let add_sched   x r = Sched   (x, r)
let add_blitter x r = Blitter (x, r)
let add_app     f r = App     (f, r)
let yield x         = Yield x
let shift x l       = Shift (x, l)
let flush f l       = Flush (f, l)

module Infix =
struct
  let nil             = Nil
  let close           = Close

  let ( ** )  x r = add_const x r
  let ( **! ) x r = add_direct x r
  let ( **~ ) x r = add_sched x r
  let ( **? ) x r = add_blitter x r
  let ( **> ) x l = shift x l
  let ( **| ) f l = flush f l
  let ( **= ) f r = add_app f r
  let ( ! ) x     = yield x
  let ( @@ )      = concat
end

(* XXX(dinosaure): clash between {!ty} and [type ty']. *)

let rec finalize
  : type i ty' v cty. (i, ty', v, _, cty, (i, ty', v, _, _) t) ty -> cty
  = fun (Convertible l) ->
    finalize_list l @@ fun ll -> Finalized (l, ll)
and finalize_list
  : type i ty v cty cv.
    (i, ty, v, cty, cv) list -> ((cty, cv) prelist -> cv) -> cty
  = fun p k -> match p with
    | Nil   -> k Empty
    | Close -> k Empty
    | Const      (_, r) -> finalize_list r k
    | Sched      (_, r) -> finalize_list r k
    | Direct     (_, r) -> finalize_list r k
    | Yield          r  -> finalize_list r k
    | Shift      (_, r) -> finalize_list r k
    | Flush      (_, r) -> finalize_list r k
    | App        (_, r) -> finalize_list r k
    | Blitter    (_, r) ->
      fun c -> finalize_list r (fun cl -> k (Cons (c, cl)))

let rec write_atom
  : type i a. i Minienc.t -> a atom -> a -> unit
  = fun encoder -> function
    | BEu16 -> Minienc.BE.write_u16 encoder
    | BEu32 -> Minienc.BE.write_u32 encoder
    | BEu64 -> Minienc.BE.write_u64 encoder
    | LEu16 -> Minienc.LE.write_u16 encoder
    | LEu32 -> Minienc.LE.write_u32 encoder
    | LEu64 -> Minienc.LE.write_u64 encoder
    | Int16 -> Minienc.NE.write_u16 encoder
    | Int32 -> Minienc.NE.write_u32 encoder
    | Int64 -> Minienc.NE.write_u64 encoder
    | Char  -> Minienc.write_char   encoder
    | String     ->
      fun x      -> Minienc.write_string encoder x
    | Seq (a, b) ->
      fun (x, y) ->
        write_atom encoder a x;
        write_atom encoder b y
    | Bool -> (function
               | true  -> Minienc.NE.write_u16 encoder 1
               | false -> Minienc.NE.write_u16 encoder 0)
    | List x     -> List.iter (write_atom encoder x)
    | Opt x      ->
      function Some a -> write_atom encoder x a
             | None   -> ()

let write_const
  : type i a. i Minienc.t -> a const -> unit
  = fun encoder -> function
    | ConstString (v, s)    ->
      Minienc.write_string    encoder ~off:v.off ?len:v.len s
    | ConstBytes  (v, s)    ->
      Minienc.write_bytes     encoder ~off:v.off ?len:v.len s
    | ConstBigstring (v, s) ->
      Minienc.write_bigstring encoder ~off:v.off ?len:v.len s
    | ConstChar chr ->
      Minienc.write_char      encoder chr

let sched_const
  : type i a. i Minienc.t -> a const -> unit
  = fun encoder -> function
    | ConstString    (v, s) ->
      Minienc.schedule_string    encoder ~off:v.off ?len:v.len s
    | ConstBytes     (v, s) ->
      Minienc.schedule_bytes     encoder ~off:v.off ?len:v.len s
    | ConstBigstring (v, s) ->
      Minienc.schedule_bigstring encoder ~off:v.off ?len:v.len s
    | ConstChar chr ->
      Minienc.write_char         encoder chr

let rec eval_list
  : type i ty v cty cv.
       i Minienc.t
    -> (i, ty, v, cty, cv) list
    -> (cty, 'res) prelist
    -> ((cv, 'res) prelist -> i Minienc.t -> v)
    -> ty
  = fun encoder l cl k -> match l with
    | Nil   ->
      k cl encoder
    | Close ->
      Minienc.close encoder
    | Const (x, r) ->
      write_const encoder x;
      eval_list encoder r cl k
    | Sched (x, r) ->
      sched_const encoder x;
      eval_list encoder r cl k
    | Direct (a, r) ->
      fun x ->
        write_atom encoder a x;
        eval_list encoder r cl k
    | Yield r ->
      Minienc.yield encoder;
      eval_list encoder r cl k
    | Blitter (a, r) ->
      let Cons (c, cl) = cl in
      fun vec x ->
        let off = match vec with Some { off; _ } -> Some off | None -> None in
        let len = match vec with Some { len; _ } -> len | None -> None in

        Minienc.write_gen
          encoder
          ~blit:c.Blitter.blit
          ~length:c.Blitter.length
          ?off ?len x;
        eval_list encoder r cl k
    | Shift (a, r) ->
      Minienc.shift encoder a;
      eval_list encoder r cl k
    | Flush (f, r) ->
      Minienc.flush encoder f;
      eval_list encoder r cl k
    | App   (f, r) ->
      fun x ->
        f encoder x;
        eval_list encoder r cl k

and keval_ty
  :    'i Minienc.t
    -> ('i, 'ty, 'v, 'cty, _ ty as 'res) list
    -> ('c, 'res) prelist
    -> ('i Minienc.t -> 'v) -> 'ty
  = fun encoder l cl k ->
    eval_list encoder l cl (fun Empty encoder -> k encoder)

let keval
  : type final. 'i Minienc.t -> ('i, _, _, final, _, _) ty -> _
  = fun encoder fmt k ->
    match fmt with
    | Finalized (fmt, cl) -> keval_ty encoder fmt cl k
    | Convertible fmt -> keval_ty encoder fmt Empty k

let eval encoder fmt = keval encoder fmt (fun x -> ())

let make l : _ ty =
  Convertible l
