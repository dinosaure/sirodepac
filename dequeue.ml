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

type 'a cell =
  | Zero
  | One of 'a
  | Two of 'a * 'a
  | Three of 'a * 'a * 'a

type 'a node =
  { mutable cell : 'a cell
  ; mutable next : 'a node
  ; mutable prev : 'a node }

type 'a t =
  { mutable cur  : 'a node
  ; mutable size : int
  ; mutable idx  : int }

exception Empty

let create () =
  let rec cur = { cell = Zero
                ; prev = cur
                ; next = cur }
  in { cur
     ; size = 0
     ; idx  = 0 }

let clear q =
  let rec cur = { cell = Zero
                ; prev = cur
                ; next = cur }
  in q.cur  <- cur;
     q.size <- 0;
     ()

let length d = d.size

let incr_size d = d.size <- d.size + 1
let decr_size d = d.size <- d.size - 1

let incr_idx d = d.idx <- d.idx + 1
let decr_idx d = d.idx <- d.idx - 1

let is_zero n = match n.cell with
  | Zero -> true
  | One _ | Two _ | Three _ -> false

let is_empty d =
  let res = d.size = 0 in
  assert (res = is_zero d.cur);
  res

let unsafe_front_index d = d.idx

let front_index d =
  if is_empty d
  then None
  else Some (unsafe_front_index d)

let unsafe_back_index d =
  d.idx + length d - 1

let back_index d =
  if is_empty d
  then None
  else Some (unsafe_back_index d)

let push_front d x =
  incr_size d;
  decr_idx  d;

  match d.cur.cell with
  | Zero       -> d.cur.cell <- One x
  | One y      -> d.cur.cell <- Two (x, y)
  | Two (y, z) -> d.cur.cell <- Three (x, y, z)
  | Three _ ->
    let node = { cell = One x
               ; prev = d.cur.prev
               ; next = d.cur } in
    d.cur.prev.next <- node;
    d.cur.prev      <- node;
    d.cur           <- node;
    ()

let push_back d x =
  incr_size d;

  let n = d.cur.prev in
  match n.cell with
  | Zero       -> n.cell <- One x
  | One y      -> n.cell <- Two (y, x)
  | Two (y, z) -> n.cell <- Three (y, z, x)
  | Three _ ->
    let node = { cell = One x
               ; next = d.cur
               ; prev = n } in
    n.next     <- node;
    d.cur.prev <- node;
    ()

let peek_front d = match d.cur.cell with
  | Zero -> raise Empty
  | One x | Two (x, _) | Three (x, _, _) -> x

let peek_back d =
  if is_empty d
  then raise Empty
  else match d.cur.prev.cell with
    | Zero -> assert false
    | One x | Two (_, x) | Three (_, _, x) -> x

let take_back_node n = match n.cell with
  | Zero -> assert false
  | One x -> n.cell <- Zero; x
  | Two (x, y) -> n.cell <- One x; y
  | Three (x, y, z) -> n.cell <- Two (x, y); z


let take_back d =
  if is_empty d
  then raise Empty
  else if d.cur == d.cur.prev
  then begin
    decr_size d;
    take_back_node d.cur
  end else begin
    let n = d.cur.prev in
    let x = take_back_node n in

    decr_size d;

    if is_zero n
    then begin
      d.cur.prev <- n.prev;
      n.prev.next <- d.cur;
    end;

    x
  end

let take_front_node n = match n.cell with
  | Zero -> assert false
  | One x -> n.cell <- Zero; x
  | Two (x, y) -> n.cell <- One y; x
  | Three (x, y, z) -> n.cell <- Two (y, z); x

let take_front d =
  incr_idx d;

  if is_empty d
  then raise Empty
  else if d.cur.prev == d.cur
  then begin
    decr_size d;
    take_front_node d.cur;
  end else begin
    decr_size d;

    let x = take_front_node d.cur in

    if is_zero d.cur
    then begin
      d.cur.prev.next <- d.cur.next;
      d.cur.next.prev <- d.cur.prev;
      d.cur <- d.cur.next;
    end;

    x
  end

let iter f d =
  let rec iter f ~first n =
    begin match n.cell with
      | Zero -> ()
      | One x -> f x
      | Two (x, y) -> f x; f y
      | Three (x, y, z) -> f x; f y; f z
    end;

    if n.next != first then iter f ~first n.next
  in
  iter f ~first:d.cur d.cur

let append_front ~into q = iter (push_front into) q
let append_back ~into q = iter (push_back into) q

let fold f acc d =
  let rec aux ~first f acc n =
    let acc = match n.cell with
      | Zero -> acc
      | One x -> f acc x
      | Two (x, y) -> f (f acc x) y
      | Three (x, y, z) -> f (f (f acc x) y) z
    in
    if n.next == first
    then acc
    else aux ~first f acc n.next
  in
  aux ~first:d.cur f acc d.cur

let to_rev_list q = fold (fun l x -> x :: l) [] q

let to_list q = List.rev (to_rev_list q)

let true_index_exn d idx =
  let idx_from_zero = idx - d.idx in

  if idx_from_zero < 0 || length d <= idx_from_zero
  then raise (Invalid_argument "Dequeue.true_index_exn")
  else
    let true_idx = d.idx + idx_from_zero in

    if true_idx >= length d
    then true_idx - length d
    else true_idx

let nth d i =
  if i >= length d
  then raise (Invalid_argument "Dequeue.nth")
  else
    let i_node = i / 3 in
    let i_cell = i mod 3 in

    let rec iter_node rest node =
      if rest = 0
      then match node.cell, i_cell with
        | (One x | Two (x, _) | Three (x, _, _)), 0 -> x
        | (Two (_, x) | Three (_, x, _)), 1 -> x
        | Three (_, _, x), 2 -> x
        | _ -> raise (Invalid_argument "Dequeue.nth")
      else iter_node (rest - 1) node.next
    in

    iter_node i_node d.cur

let nth_exn d i = nth d (true_index_exn d i)

let nth d i = try Some (nth_exn d i) with exn -> None

let pp pp_data fmt d =
  let pp_list ?(sep = (fun fmt -> ())) pp_data fmt lst =
    let rec aux = function
      | [] -> ()
      | [ x ] -> pp_data fmt x
      | x :: r -> pp_data fmt x; sep fmt; aux r
    in
    aux lst
  in

  Format.fprintf fmt "{| (idx:%d) @[<hov>%a@] |}"
    d.idx
    (pp_list ~sep:(fun fmt -> Format.fprintf fmt ";@ ") pp_data)
    (to_list d)
