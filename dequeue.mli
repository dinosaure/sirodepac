(* Copyright (c) 2013, Simon Cruanes
   All rights reserved.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are met:

   Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.  Redistributions in binary
   form must reproduce the above copyright notice, this list of conditions and
   the following disclaimer in the documentation and/or other materials
   provided with the distribution.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
   ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
   WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
   DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
   FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
   DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
   SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
   CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. *)

type 'a t

exception Empty

val create       : unit -> 'a t
val clear        : 'a t -> unit
val is_empty     : 'a t -> bool

val push_front   : 'a t -> 'a -> unit
val push_back    : 'a t -> 'a -> unit
val peek_front   : 'a t -> 'a
val peek_back    : 'a t -> 'a
val take_front   : 'a t -> 'a
val take_back    : 'a t -> 'a

val append_front : into: 'a t -> 'a t -> unit
val append_back  : into: 'a t -> 'a t -> unit

val iter         : ('a -> unit) -> 'a t -> unit
val fold         : ('a -> 'b -> 'a) -> 'a -> 'b t -> 'a

val to_rev_list  : 'a t -> 'a list
val to_list      : 'a t -> 'a list
