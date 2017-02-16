type read  = [ `Read ]
type write = [ `Write ]

type ('a, 'i) t constraint 'a = [< `Read | `Write ]

val read_and_write : 'i B.t -> ([ read | write ], 'i) t
val read_only      : 'i B.t -> (read, 'i) t
val write_only     : 'i B.t -> (write, 'i) t

val length    : ('a, 'i) t -> int
val get       : ([> read ], 'i) t -> int -> char
val set       : ([> write ], 'i) t -> int -> char -> unit
val get_u16   : ([> read ], 'i) t -> int -> int
val get_u32   : ([> read ], 'i) t -> int -> int32
val get_u64   : ([> read ], 'i) t -> int -> int64
val sub       : ([> read ] as 'a, 'i) t -> int -> int -> ('a, 'i) t
val fill      : ([> write ], 'i) t -> int -> int -> char -> unit
val blit      : ([> read ], 'i) t -> int ->
                ([> write ], 'i) t -> int -> int -> unit
val pp        : Format.formatter ->  ([> read ], 'i) t -> unit
val tpp       : Format.formatter ->  ([> read ], 'i) t -> unit
val to_string : ([> read ], 'i) t -> string
val adler32   : ([> read ], 'i) t -> int32 -> int -> int -> int32

val proof     : ('a, 'i) t -> 'i B.t
