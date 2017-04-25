(** Unpack a PACK file. *)

module type HASH =
sig
  type t

  val pp : Format.formatter -> t -> unit
  val length : int
  val of_string : string -> t
end

(** Decoder of a Hunk object.

    Limitation: the Hunk object must be lower than [max_native_int]. The decoder
    compute a flow of [Cstruct.t]. You can reuse the same [Cstruct.t] for each
    call of [eval].

    The decoder create a list of [hunk]. A [hunk] is an [Copy] when you need to
    copy a pattern from the base git object. A [hunk] is an [Insert] when you
    need to copy/[blit] the [Cstruct.t]. All [Cstruct.t] of [Insert] constructor
    was allocate (the length of the [Cstruct.t] can't be upper than [0x7F]).

    TODO: we can optimize the decoder and avoid the allocaton of the [Insert]
    hunk. I can explain that only in my mind.
  *)
module type HunkDecoder =
sig
  module Hash : HASH

  (** Errors of the decoder. *)
  type error =
    | Reserved_opcode of int
      (** Appears when we have a wrong byte to declare a new block. *)
    | Wrong_copy_hunk of int * int * int
      (** Appears when the [Copy] hunk refers to a pattern outside the source object. *)

  val pp_error : Format.formatter -> error -> unit

  type t

  type hunks =
    { reference     : reference
      (** See {! H.reference}. *)
    ; hunks         : hunk list
      (** See {! H.hunk}. *)
    ; length        : int
      (** [length] of the Hunk inflated object in the PACK file. *)
    ; source_length : int
      (** [length] of the base object. *)
    ; target_length : int
      (** real [length] of the git object. *)
    }
  and reference =
    | Offset of int64 (** Absolute offset of the base object. *)
    | Hash of Hash.t (** Hash of the base object. *)
  and hunk =
    | Insert of Cstruct.t
      (** When we apply the base object, we need to write the [Cstruct.t] raw. *)
    | Copy of int * int
      (** With [Off (off, len)], when we apply the base object, we write the
          pattern (which has [len] byte(s)) from the serialized base object
          located at [off] (absolute offset).
        *)

  val pp : Format.formatter -> t -> unit
  val pp_hunks : Format.formatter -> hunks -> unit
  val pp_reference : Format.formatter -> reference -> unit

  (** [eval src t] computes the current flow [src]. This function returns:

      - [`Await t]: iff the state expects more byte(s).
      - [`Error (t, exn)]: when the state catches an error [exn] (see {!H.error}).
      - [`Ok (t, obj)]: when state finishes to decode the Hunk object and returns the OCaml value [obj] (see {!H.hunks}).

      When we return [`Await], you should use {!H.refill}.
    *)
  val eval : Cstruct.t -> t ->
    [ `Await of t
    | `Error of (t * error)
    | `Ok of (t * hunks) ]

  (** [default length reference] makes a new state to decoding a Hunk flow.

      The [length] argument corresponds to the length of the [Cstruct.t] flow.
      It is provided by the PACK file.

      The [reference] argument corresponds to the base git object. It can be an
      [Offset] or a [Hash]. It is provided by the PACK file.
   *)
  val default : int -> reference -> t

  (** [refill off len t] informs to the state than the next [Cstruct.t] input
      start to [off] and it has [len] byte(s).
    *)
  val refill : int -> int -> t -> t

  (** [available_in t] returns how many bytes was not computed in the current [Cstruct.t]. *)
  val available_in : t -> int
  (** [used_in t] returns how many bytes was computed in the current [Cstruct.t]. *)
  val used_in : t -> int

  (** [read t] returns how many bytes was computed finally. According to the
      PACK standard, at the end, [read t] returns the same value than the [length]
      argument in the [default] function.
    *)
  val read : t -> int
end

(** A Window of the PACK file.

    A [Window] corresponds as an area of the PACK file. In [git], we don't open
    and [mmap] the entire PACK file but only a part. We use the same abstraction
    in this implementation to avoid the limitation of the [native_int] offset in
    OCaml.

    So, when you want a git object from a PACK file, we need only the file
    descriptor. Then, by the IDX file, we can get the absolute offset of this
    git object. So, we create a new window, which contains the full (or a part
    of) the git object requested.

    We [mmap] [1024 * 1024] bytes padded to a multiple of [4096] of the PACK
    file and start to decode the git object. If the area is not enough to
    compute all of the git object, because the PACK decoder is a non-blocking
    decoder, we just need to re-allocate a new [Window] which contains the
    absolute offset plus the current offset (provided by the decoder state of
    the PACK file) and continue to decode the git object with the new window.

    So, we have no limitation about an offset upper than [max_native_int] and we
    avoid this limitation specific to OCaml. We limit the allocation to what we
    need and we don't [mmap] the entire PACK file (because it can be huge).
  *)
module Window :
sig
  (** The Window. *)
  type t

  (** [inside offset window] returns [true] if [window] contains the [offseŧ]. *)
  val inside : int64 -> t -> bool
end

(** Decoder of the PACK file.

    Limitation: an object can be upper than [max_native_int]. This case should
    be not appear from a {!Commit}, {!Tree}, {!Tag} and {!Hunk} (otherwise your
    git repository is shitty), but appear for a {!Blob}. Indeed, [git] can store
    a huge file (when the length is upper than [max_native_int]).

    This decoder is a non-blocking decoder and save the state in any blocking
    situation. So it can compute a huge PACK file (a PACK more than
    [max_native_int]) if it used with a cache of {! Window.t}.

    TODO: because the {! Hunk.t} decoder can be optimized to not allocate any
    [Cstruct.t], this decoder need to follow this optimization.
  *)
module type PACKDecoder =
sig
  module Hash        : HASH
  module HunkDecoder : HunkDecoder

  type error =
    | Invalid_byte of int
      (** Appears when the header of the PACK file is wrong. *)
    | Reserved_kind of int
      (** Appears when the kind of the current PACK object is [0]. *)
    | Invalid_kind of int
      (** Appears when the kind of the current PACK object is undefined.
          TODO: this case does not appear, lol ...
        *)
    | Inflate_error of Decompress.Inflate.error
      (** Appears when [Decompress] returns an error when it tries to inflate a
          PACK object.
        *)
    | Hunk_error of H.error
      (** Appears when {! H.eval} returns an error. *)
    | Hunk_input of int * int
      (** The Hunk object is length-defined. So, when we try to decode this
          specific PACK object, if the {!H.t} expects some new bytes and we
          already get all input, this error appears (with respectively how many
          bytes we expected and how many bytes the {!H.t} computed).
        *)
    | Invalid_length of int * int
      (** Appears when the length of the inflated object does not correspond by
          what was it noticed by the PACK file.
        *)

  val pp_error : Format.formatter -> error -> unit

  (** The PACK decoder state. *)
  type t

  (** The kind of the git object

      A git object can be:
      - A [Commit]
      - A [Tree]
      - A [Blob]
      - A [Tag]
      - A [Hunk]: this is not a git object but a specific PACK object. The {!
        H.hunks} refers to another PACK object (so, can be a {! H.hunks} too)
        and it's just a result of a [diff] between the reference object and the
        current object.
    *)
  type kind =
    | Commit
    | Tree
    | Blob
    | Tag
    | Hunk of H.hunks

  val pp_kind : Format.formatter -> kind -> unit
  val pp : Format.formatter -> t -> unit

  (** [default tmp window] makes a new decoder of a PACK file.

      [tmp0] is a [Cstruct.t] used as an output of [Decompress.Inflate]. We let
      the user to allocate the optimized length [Cstruct.t] as an output of
      [Decompress.Inflate] and we take the ownership in this [Cstruct.t] (so you
      don't use it for another compute).

      Then, [window] is a [Decompress.Window]. Indeed, this structure allocate a
      large buffer so we let the user to allocate this structure when he wants.
      Then, we take the ownership and use (and re-use) this window for all git
      objects. Don't use it for another process too. We enforce the type of the
      [Window] to compute only a [bigstring].

      This state does not allocate any large OCaml object.
    *)
  val default : Cstruct.t -> Decompress.B.bs Decompress.Window.t -> t

  (** [from_window pack_window off_in_window tmp window] makes a new decoderof a
      PACK file specialized from a {! Window.t}.

      This state compute only one object and set some intern values to
      correspond to the area targeted by the [pack_window]. [off] is the
      relative offset inside the window. Then, [tmp] and [window] have the same
      purpose than {! P.default}.
    *)
  val from_window : Window.t -> int -> Cstruct.t -> Decompress.B.bs Decompress.Window.t -> t

  (** [refill off len t] informs the state than the next [Cstruct.t] input start
      to [off] and it has [len] byte(s).
    *)
  val refill : int -> int -> t -> t

  (** [flush off len t] informs to the state than you saved the output produced
      by the state and (a new?) [Cstruct.t] can be used at [off] and it has
      [len] bytes.

      This function can be called only when the state computes a git object (so,
      [kind <> Hunk]) and inflate the input. So, the output can be considered as
      a serialization of a git object.

      Otherwise, we raise an [Invalid_argument].
    *)
  val flush : int -> int -> t -> t

  (** [output t] returns the output procuded by the state when it inflates a git
      object. In reality, we returns the [Cstruct.t] [tmp] given at {! P.default} or
      {! P.from_window}. We return how many bytes we have written too.

      This function can be called only when the state computes a git object (so,
      [kind <> Hunk]) and inflate the input. Otherwise, we raise an
      [Invalid_argument].
    *)
  val output : t -> Cstruct.t * int

  (** When the state finishes a PACK object, it sticks to the current object
      (with all information) and let the user to do what he wants. Then, when
      the user computes what he wants, he can use the [next_object t] to informs
      the state to pass to the next object. Then, the new state produced
      (consider than the state is pure) losts all information about the
      current/previous object and compute the next one.

      This function can be called only when [eval t] returns an [`Object].
      Otherwise, we raise an [Invalid_argument] (see {! P.eval}).
    *)
  val next_object : t -> t

  (** When the state returns an [`Object], this function returns the {!kind} of
      the current object. If you call this function in the other state than
      [`Object], we raise an [Invalid_argument].
    *)
  val kind : t -> kind

  (** When the state returns an [`Object], this function returns the [length] of
      the current inflate object inside the PACK file. If the object is not a
      [Hunk], it's the length of the git object. Otherwise, see {!H.hunk} to
      know the length of the git object. If you call this function in the other
      state than [`Object], we raise an [Invalid_argument].
    *)
  val length : t -> int


  (** When the state returns an [`Object], you can know how many byte the state
      used to compute the current object (including the variable length for a
      git object and the offset for a hunk). If you call this function in the
      other state than [`Object], we raise an [Invalid_argument].
    *)
  val consumed : t -> int

  (** When the state returns an [`Object], you can know where the object is it
      in the PACK file. It is an absolute offset independently if state the
      state is come from {! P.default} or {! P.from_window}. If you call this
      function in the other state than [`Object], we raise an
      [Invalid_argument].
    *)
  val offset : t -> int64

  (** When the state returns an [`Object], you can know the CRC-32 computed by
      the state when it digest all byte necessary to get the current object.
      This value should be the same than the value given by the IDX file. If you
      call this function in the other state than [`Object], we raise an
      [Invalid_argument].
    *)
  val crc : t -> Crc32.t

  (** [eval src t] computes the current flow [src]. This function returns:

      - [`Await t]: iff the state expects more byte(s).
      - [`Error (t, exn)]: when the state catches an error [exn] (see {!P.error}).
      - [`Object t]: when the state finished one PACK object.
      - [`Flush t]: appears only when the state compute a git object (so, [kind
        <> Hunk]). In this case, the [output] function returns a [Cstruct.t]
        which contains a part of the serialized git object. Why a part? Because
        may be the [tmp] [Cstruct.t] than you given to {!P.default} or
        {!P.from_window} was may be smaller than the git object. In this case,
        you need to store all output for each case when {!P.eval} returns
        [`Flush] inside a buffer.
      - [`End (t, hash)]: when the states finishes to decode all of the PACK
        input and returns the hash noticed inside the PACK input.

      When we return [`Await], you should use {!P.refill}. When we return
      [`Flush], you should use {!P.flush} and {!P.output}. Then, when we return
      [`Object] you can use {!P.next_object} to got to the next object,
      {!P.kind} to know the kind of the current PACK object, {!P.length},
      {!P.consumed}, {!P.offset} and {!P.crc} to know some informations about
      the current PACK object.
    *)
  val eval : Cstruct.t -> t ->
    [ `Object of t
    | `Await of t
    | `Flush of t
    | `End of (t * string)
    | `Error of (t * error) ]
end

(** Mapper module.

    It's an abstraction of the file-descriptor and the [map] system-call.
  *)
module type MAPPER =
sig
  type fd

  val length : fd -> int64
  val map : fd -> ?pos:int64 -> share:bool -> int -> Cstruct.t
end

(** User-friendly module to get a Git object from a PACK file.

    This module implement an user-friendly function to get a serialized Git
    object. Indeed, the PACK file can contain a {!H.hunks} and it's may be
    complex and cost to compute this PACK object and retrieve the original Git
    object.

    So, this module provide an easy-to-use function {!D.get} to retrieve the Git
    object. We optimized the process in the same way than [git]. If you want to
    understrand what is going on, you can read the code (some comments is
    available inside).
  *)
module Decoder :
sig
  type error =
    | Invalid_hash of string
      (** Appears when the user requested a wrong hash. *)
    | Invalid_offset of int64
      (** Appears when the given offset is not available inside the PACK file. *)
    | Invalid_target of (int * int)
      (** Appears when the result of the application of a {H.hunks} returns a bad raw. *)
    | Unpack_error of PACKDecoder.error
      (** Appears when we have an {!P.error}. *)

  val pp_error : Format.formatter -> error -> unit

  (** A impure state of a PACK file. *)
  type t

  (** An homogeneic representation of a PACK object. *)
  module Base :
  sig
    (** A PACK object. *)
    type t =
      { kind     : P.kind
      (** the kind {!P.kind} of the PACK object. *)
      ; raw      : Cstruct.t
      (** the content of the PACK object (if [kind = P.Hunk], this raw contains nothing). *)
      ; offset   : int64
      (** the absolute offset inside the PACK file. *)
      ; length   : int
      (** the not real length of the PACK object ([length = Cstruct.len raw]). *)
      ; consumed : int
      (** how many bytes we consumed to compute this PACK object. *)
      }
  end

  (** [apply hunks base raw] applies a [hunks] from a [base] to [raw] and make a
      new PACK object.

      If the length of the [raw] is less than [hunks.H.target_length], we raise
      an [Invalid_argument].
    *)
  val apply : H.hunks -> Base.t -> Cstruct.t -> (Base.t, error) result

  (** [delta t hunks offset tmp window raw] returns the base object referenced
      by the [hunks]. You need to notice (by [offset]) the absolute offset of
      the [hunks]. Then, you notice the [tmp], the inflate [window] and the
      [raw] which contains the data of the base object requested.

      If the length of the [raw] is less than [hunks.H.source_length], we raise
      an [Invalid_argument].
    *)
  val delta : ?chunk:int -> t -> H.hunks -> int64 -> Cstruct.t -> Decompress.B.bs Decompress.Window.t -> Cstruct.t -> (Base.t, error) result

  (** [make ?bucket file_descriptor idx] makes a new impure state which can
      contain [bucket] {!Window.t}. We need to notice the file descriptor of the
      PACK file and a function [idx], result of the deserializaion of the IDX
      file.
    *)
  val make : ?bucket:int -> Mapper.fd -> (string -> (Crc32.t * int64) option) -> t

  (** [get t hash tmp window] returns the Git object referenced by the [hash]
      and need a [tmp] buffer for inflate and an inflate window. This function
      allocates 2 [Cstruct.t] to make the Git object.
    *)
  val get : ?chunk:int -> t -> string -> Cstruct.t -> Decompress.B.bs Decompress.Window.t -> (Base.t, error) result
end
