module type HASH =
sig
  type t = Cstruct.buffer
  type ctx
  type buffer = Cstruct.buffer

  val length    : int
  val pp        : Format.formatter -> t -> unit

  val init      : unit -> ctx
  val feed      : ctx -> buffer -> unit
  val get       : ctx -> t

  val of_string : string -> t
  val to_string : t -> string

  val equal     : t -> t -> bool
  val compare   : t -> t -> int

  val of_hex_string : string -> t
  val to_hex_string : t -> string
end

module type Z =
sig
  type t
  type error

  val pp_error : Format.formatter -> error -> unit

  val default  : int -> t
  val flush    : int -> int -> t -> t
  val no_flush : int -> int -> t -> t
  val finish   : t -> t
  val used_in  : t -> int
  val used_out : t -> int
  val eval     : Cstruct.t -> Cstruct.t -> t -> [ `Flush of t | `Await of t | `Error of (t * error) | `End of t ]
end

module Kind :
sig
  type t =
    | Commit
    | Tag
    | Tree
    | Blob
    (** The type of kind. *)

  val to_int : t -> int
  (** [to_int t] returns an unique [int] value of the kind [t]. This value can
     be used to sort a list of kinds. *)
  val to_bin : t -> int
  (** [to_bin t] returns a binary code to serialize the kind [t]. *)

  val pp : Format.formatter -> t -> unit
  (** A pretty-printer of {!t}. *)
end

module type ENCODER =
sig
  module Hash : HASH
  module Deflate : Z

  (** The entry module. It used to able to manipulate the meta-data only needed
     by the delta-ification of the git object (and avoid to de-serialize all of
     the git object to compute the delta-ification). *)
  module Entry :
  sig
    module Commit : (module type of Minigit.Commit(Hash))
    module Tree   : (module type of Minigit.Tree(Hash))
    module Tag    : (module type of Minigit.Tag(Hash))
    module Blob   : (module type of Minigit.Blob)

    type t
    (** The type of an entry. *)

    (** The type of a source. *)
    type source =
      | From of Hash.t
      (** To notice than an user-defined source exists for the delta-ification. *)
      | None
      (** To notice than no user-defined source exists. *)

    val pp : Format.formatter -> t -> unit
    (** A pretty-printer for {!t}. *)

    val pp_source : Format.formatter -> source -> unit
    (** A pretty-printer for {!source}. *)

    val hash : string -> int
    (** [hash path] produces a integer to correspond with [path]. *)

    val make : Hash.t -> ?name:string -> ?preferred:bool -> ?delta:source -> Kind.t -> int64 -> t
    (** [make hash ?name ?preferred ?delta kind length] returns a new entry when:

        {ul

        {- [hash] corresponds to the unique ID of the git object.}

        {- [name] corresponds to the optional name of the git object. For a [Tree]
      or a [Blob], the client should notice the given name (to optimize the
      delta-ification).}

        {- [preferred] notices to the delta-ification algorithm to prefer to use
      this entry as a source.}

        {- [delta] notices an existing user specified source for the entry (and
      the delta-ification algorithm will use it).}

        {- [kind] corresponds to the kind of the object.}

        {- [length] corresponds to the real inflated length of the object.}} *)

    val id : t -> Hash.t
    (** [id t] returns the unique identifier of the entry [t]. *)

    val name : t -> string -> t
    (** [name t name] provides a new [t] with the specified [name]. *)

    val compare : t -> t -> int
    (** The comparison function for the entry {!t}. It returns [0] when [a] equal
      [b]. It returns a negative integer if [a] is {i less} than [b] and a
      positive integer when [a] is {i greater} than [b]. This function is used to
      sort a list of entries with the git heuristic and produce an optimal
      delta-ification.

        According to git, it's a lexicographic sort by the {!Kind.t}, the optional
      name of the entry (hashed by {!hash}) and sorted by size (larger to
        smaller). *)

    (** Convenience functions. *)

    val from_commit  : Hash.t -> ?preferred:bool -> ?delta:source -> Commit.t -> t
    val from_tag     : Hash.t -> ?preferred:bool -> ?delta:source -> Tag.t -> t
    val from_tree    : Hash.t -> ?preferred:bool -> ?delta:source -> ?path:string -> Tree.t -> t
    val from_blob    : Hash.t -> ?preferred:bool -> ?delta:source -> ?path:string -> Blob.t -> t

    val topological_sort : t list -> t list
    (** [topological_sort lst] orders [lst] so that no entry precedes an entry it
      depends upon. So you will find any entries considered as a source before
      any entries when you apply the delta-ification. *)
  end

  module Delta :
  sig
    (** The type of the delta-ification. *)
    type t =
      { mutable delta : delta }
    and delta =
      | Z (** Means no delta-ification. *)
      | S of { length     : int
             (** Length of the PACK object. *)
             ; depth      : int
             (** Depth of the PACK object. *)
             ; hunks      : Rabin.e list
             (** Compression list. *)
             ; src        : t
             (** Source. *)
             ; src_length : int64
             (** Length of the source object. *)
             ; src_hash   : Hash.t
             (** Hash of the source object. *)
             ; }
             (** Delta-ification with a description of the source. *)

    (** The type of error. *)
    type error = Invalid_hash of Hash.t
    (** Appears when we have an invalid hash. *)

    val pp_error : Format.formatter -> error -> unit
    (** A pretty-printer for {!error}. *)

    val deltas : ?memory:bool -> Entry.t list -> (Hash.t -> Cstruct.t option) -> (Entry.t -> bool) -> int -> int -> ((Entry.t * t) list, error) result
    (** [deltas ?memory lst getter tagger window depth].

        This is the main algorithm about the serialization of the git object in
       a PACK file. The purpose is to order and delta-ify entries.

        [getter] is a function to access to the real inflated raw of the git
       object requested by the hash. This function must allocate the raw. The
       algorithm takes the ownership anyway.

        [tagger] is a function to annotate an entry as preferred to serialize
       firstly.

        [window] corresponds to the number of how many object(s) we keep for the
       delta-ification or, if [memory] is [true], ho many byte(s) we keep for
       the delta-ification. The client can control the memory consumption of
       this algorithm precisely if he wants.

        [depth] is the maximum of the delta-ification allowed.

        If you want to understand the algorithm, look the source code. *)
  end

  module Radix : module type of Radix.Make(struct include Hash let get = Bigarray.Array1.get let length _ = Hash.length end)

  module H :
  sig
    type error
    (** The type of error. *)

    val pp_error : Format.formatter -> error -> unit
    (** The pretty-printer of {!error}. *)

    type t
    (** The type of the encoder. *)

    (** The type of the reference. It's a negative offset or the hash of the
        source object. *)
    type reference =
      | Offset of int64
      | Hash of Hash.t

    val pp : Format.formatter -> t -> unit
    (** The pretty-printer of {!t}. *)

    val default : reference -> int -> int -> Rabin.e list -> t
    (** Make a new encoder state {!t} from a {!reference}. We need to notice the
        length of the inflated source and the length of the inflated target then,
        the compression list. *)

    val refill : int -> int -> t -> t
    (** [refill off len t] provides a new [t] with [len] bytes to read, starting
        at [off]. This byte range is read by calls to {!eval} with [t] until
        [`Await] is returned. The encoder expects the target raw (not the
        source) to write [Insert] opcodes. The client must send the target raw
        continuously because internally, we assert than a continuous stream of
        the target raw and pick only the needed byte range. *)

    val flush : int -> int -> t -> t
    (** [flush off len t] provides [t] with [len] bytes to write, starting at
        [off]. This byte range is written by calls to {!eval} with [t] until
        [`Flush] is returned. *)

    val finish : t -> t
    (** [finish t] provides a new [t] which does not expect any input. *)

    val eval : Cstruct.t -> Cstruct.t -> t -> [ `Await of t | `Flush of t | `End of t | `Error of (t * error) ]
    (** [eval src t] is:

        {ul

        {- [`Await t] iff [t] needs more input storage. The client must use
        {!refill} to provide a new buffer and then call {!eval} with [`Await]
        until other value returned.}

        {- [`Flush t] iff [t] needs more output storage. The client must use
        {!flush} to provide a new buffer and then call {!eval} with [`Flush]
        until [`End] is returned.}

        {- [`End t] when [t] is done. Then, [t] sticks on this situation, the
        client can remove it.}

        {- [`Error (t, exn)] iff the encoder [t] meet an {!error} [exn]. The
        encoder can't continue and sticks in this situation.}} *)

    val used_in : t -> int
    (** [used_in ŧ] returns how many byte [t] consumed in the current buffer
        noticed to the previous call of {!eval}. *)

    val used_out : t -> int
    (** [used_out ŧ] returns how many byte [t] wrote in the current buffer
        noticed to the previous call of {!eval}. *)
  end

  (** The type error. *)
  type error =
    | Deflate_error of Deflate.error
      (** Appears when the deflate algorithm raises an error. *)
    | Hunk_error of H.error
      (** Appears when the Hunk encoder raises an error. *)
    | Invalid_entry of Entry.t * Delta.t
      (** When the encoder catches an invalid entry (this case does not appear). *)
    | Invalid_hash of Hash.t
      (** Appears when the hash requested does not exist. *)

  val pp_error : Format.formatter -> error -> unit
  (** A pretty-printer for {!error}. *)

  type t
  (** The type of the encoder. *)

  val used_out : t -> int
  (** [used_out ŧ] returns how many byte [t] wrote in the current buffer
      noticed to the previous call of {!eval}. *)

  val used_in : t -> int
  (** [used_in ŧ] returns how many byte [t] consumed in the current buffer
      noticed to the previous call of {!eval}. *)

  val flush : int -> int -> t -> t
  (** [flush off len t] provides [t] with [len] bytes to write, starting at
      [off]. This byte range is written by calls to {!eval} with [t] until
      [`Flush] is returned. *)

  val refill : int -> int -> t -> t
  (** [refill off len t] provides a new [t] with [len] bytes to read, starting
      at [off]. This byte range is read by calls to {!eval} with [t] until
      [`Await] is returned. The client should fill by the {!expect} git
      object. *)

  val finish : t -> t
  (** [finish t] provides a new [t] which terminate to serialize the current git
      object. *)

  val expect : t -> Hash.t
  (** [expect t] returns the object expected to the serialization. At this time,
      the serializer requests the inflated raw of this git object. The client is
      able to use it only when {!eval} return [`Await]. *)

  val idx : t -> (Crc32.t * int64) Radix.t
  (** [idx t] returns a {!Radix} tree which contains the CRC-32 checksum and the
      absolute offset for each git object serialized. *)

  val default : Cstruct.t -> (Entry.t * Delta.t) list -> t
  (** Make a new encoder {!t} of the PACK stream.

      [tmp] is a [Cstruct.t] used as an internal output of the Hunk Encoder
      {!H}. We let the user to allocate the optimized length for this temporary
      buffer and we take the ownership (so, you don't use it to another
      computer).

      Then, the client need to notice the ordered list of what he wants to
      serialize. *)

  val eval : Cstruct.t -> Cstruct.t -> t -> [ `Flush of t | `Await of t | `End of (t * Hash.t) | `Error of (t * error) ]
end

module MakePACKEncoder (Hash : HASH) (Deflate : Z) : ENCODER
  with module Hash = Hash
   and module Deflate = Deflate
