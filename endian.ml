module B = Decompress.B

external swap32 : int32 -> int32 = "%bswap_int32"
external swap64 : int64 -> int64 = "%bswap_int64"
external swap16 : int -> int     = "%bswap16"
external id16   : int -> int     = "%identity"
external id32   : int32 -> int32 = "%identity"
external id64   : int64 -> int64 = "%identity"

let get_u16 s off =
  if not Sys.big_endian
  then swap16 (B.get_u16 s off)
  else B.get_u16 s off

let get_u32 s off =
  if not Sys.big_endian
  then swap32 (B.get_u32 s off)
  else B.get_u32 s off

let get_u64 s off =
  if not Sys.big_endian
  then swap64 (B.get_u64 s off)
  else B.get_u64 s off

let set_u16 s off v =
  if not Sys.big_endian
  then (B.set_u16 s off (swap16 v))
  else B.set_u16 s off v

let set_u32 s off v =
  if not Sys.big_endian
  then (B.set_u32 s off (swap32 v))
  else B.set_u32 s off v

let set_u64 s off v =
  if not Sys.big_endian
  then (B.set_u64 s off (swap64 v))
  else B.set_u64 s off v

let u16_to_be = if not Sys.big_endian then swap16 else id16
let u32_to_be = if not Sys.big_endian then swap32 else id32
let u64_to_be = if not Sys.big_endian then swap64 else id64
