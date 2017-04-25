module Option =
struct
  let is_some = function
    | Some _ -> true
    | None -> false

  let value ~default = function
    | Some v -> v
    | None -> default
end

type 'a t =
  { mutable arr        : 'a CheapArray.t
  ; mutable a_idx      : int
  ; mutable b_idx      : int
  ; mutable ext_idx    : int
  ; mutable ext_length : int
  ; mutable arr_length : int
  ; never_shrink       : bool }

let create ?init ?never_shrink () =
  let never_shrink =
    match never_shrink with
    | None -> Option.is_some init
    | Some v -> v
  in

  let init = Option.value ~default:7 init in

  if init < 0
  then raise (Invalid_argument "Deque.create");

  let arr_length = init + 1 in

  { arr = CheapArray.create arr_length
  ; a_idx = 0
  ; b_idx = 1
  ; ext_idx = 0
  ; ext_length = 0
  ; arr_length
  ; never_shrink }

let length { ext_length; _ } = ext_length

let is_empty t = length t = 0

let clear t =
  if t.never_shrink
  then CheapArray.clear t.arr
  else t.arr <- CheapArray.create 8;

  t.a_idx <- 0;
  t.b_idx <- 1;
  t.ext_length <- 0;
  t.arr_length <- CheapArray.length t.arr

let front_ { ext_idx; _ } = ext_idx
let back_  ({ ext_idx; _ } as t) = ext_idx + length t - 1

let actual_front t =
  if t.ext_idx = t.arr_length - 1
  then 0
  else t.ext_idx + 1

let actual_back t =
  if t.b_idx = 0
  then t.arr_length - 1
  else t.b_idx - 1

let checked t f =
  if is_empty t
  then None
  else Some (f t)

let front t = checked t front_
let back  t = checked t back_

let foldi' t direction init f =
  if is_empty t
  then init
  else
    let front        = front_ t in
    let back         = back_ t in
    let actual_front = actual_front t in
    let actual_back  = actual_back  t in

    let rec loop acc ext_idx idx stop ~step =
      if idx = stop
      then (acc, ext_idx)
      else
        loop
          (f ext_idx acc (CheapArray.get_some_exn t.arr idx))
          (ext_idx + step)
          (idx + step)
          stop
          step
    in

    match direction with
    | `front_to_back ->
      if actual_front <= actual_back
      then
        let acc, _ =
          loop init front actual_front (actual_back + 1) 1
        in acc
      else
        let acc, ext_idx =
          loop init front actual_front t.arr_length 1
        in
        let acc, _ = loop acc ext_idx 0 (actual_back + 1) 1 in
        acc
    | `back_to_front ->
      if actual_front <= actual_back
      then
        let acc, _ = loop init back actual_back (-1) (-1) in
        acc
      else
        let acc, ext_idx = loop init back actual_back (-1) (-1) in
        let acc, _ = loop acc ext_idx (t.arr_length - 1) (actual_front - 1) (-1) in
        acc

let fold'  t direction init f = foldi' t direction init (fun _ acc v -> f acc v)
let iteri' t direction f      = foldi' t direction () (fun i () v -> f i v)
let iter'  t direction f      = foldi' t direction () (fun _ () v -> f v)

let fold  t init f = fold'  t `front_to_back init f
let foldi t init f = foldi' t `front_to_back init f
let iteri t f      = iteri' t `front_to_back f

let iter t f =
  if not (is_empty t)
  then begin
    let actual_front = actual_front t in
    let actual_back  = actual_back  t in

    let rec loop idx stop =
      if idx < stop
      then begin
        f (CheapArray.get_some_exn t.arr idx);
        loop (idx + 1) stop
      end
    in

    if actual_front <= actual_back
    then loop actual_front (actual_back + 1)
    else begin
      loop actual_front t.arr_length;
      loop 0 (actual_back + 1)
    end
  end

let blit arr' t =
  assert (not (is_empty t));

  let actual_front = actual_front t in
  let actual_back  = actual_back t in

  let old = t.arr in

  if actual_front <= actual_back
  then CheapArray.blit old actual_front arr' 0 (length t)
  else begin
    let break = CheapArray.length old - actual_front in

    CheapArray.blit old actual_front arr' 0 break;
    CheapArray.blit old 0 arr' break (actual_back - 1);
  end;

  t.b_idx <- length t;
  t.arr <- arr';
  t.arr_length <- CheapArray.length arr';
  t.a_idx <- CheapArray.length arr' - 1;

  assert (t.a_idx > t.b_idx)

let grow t =
  let arr' = CheapArray.create (t.arr_length * 2) in
  blit arr' t

let maybe_shrink t =
  if not t.never_shrink
     && t.arr_length > 10
     && t.arr_length / 3 > length t
  then begin
    let arr' = CheapArray.create (t.arr_length / 2) in
    blit arr' t
  end

let enqueue_back t v =
  if t.a_idx = t.b_idx then grow t;
  CheapArray.set_some t.arr t.b_idx v;
  t.b_idx <- if t.b_idx = t.arr_length - 1 then 0 else t.b_idx + 1;
  t.ext_length <- t.ext_length + 1

let enqueue_front t v =
  if t.a_idx = t.b_idx then grow t;
  CheapArray.set_some t.arr t.a_idx v;
  t.a_idx <- if t.a_idx = 0 then t.arr_length - 1 else t.a_idx - 1;
  t.ext_idx <- t.ext_idx - 1;
  t.ext_length <- t.ext_length + 1

let enqueue t direction v =
  match direction with
  | `front -> enqueue_front t v
  | `back -> enqueue_back t v

let peek_front_ t =
  CheapArray.get_some_exn t.arr (actual_front t)

let peek_front t =
  if is_empty t
  then None
  else Some (peek_front_ t)

let peek_back_ t =
  CheapArray.get_some_exn t.arr (actual_back t)

let peek_back t =
  if is_empty t
  then None
  else Some (peek_back_ t)

let peek t = function
  | `front -> peek_front t
  | `back -> peek_back t

let dequeue_front_ t =
  let idx = actual_front t in
  let res = CheapArray.get_some_exn t.arr idx in

  CheapArray.set_none t.arr idx;
  t.a_idx <- idx;
  t.ext_idx <- t.ext_idx + 1;
  t.ext_length <- t.ext_length - 1;

  maybe_shrink t;

  res

let dequeue_front t =
  if is_empty t
  then None
  else Some (dequeue_front_ t)

let dequeue_back_ t =
  let idx = actual_back t in
  let res = CheapArray.get_some_exn t.arr idx in

  CheapArray.set_none t.arr idx;
  t.b_idx <- idx;
  t.ext_length <- t.ext_length - 1;

  maybe_shrink t;

  res

let dequeue_back t =
  if is_empty t
  then None
  else Some (dequeue_back_ t)

let dequeue t = function
  | `front -> dequeue_front t
  | `back -> dequeue_back t

let true_index t idx =
  let idx_from_zero = idx - t.ext_idx in

  if idx_from_zero < 0 || length t <= idx_from_zero
  then raise (Invalid_argument "Deque.true_index: invalid index");

  let true_idx = t.a_idx + 1 + idx_from_zero in

  if true_idx >= t.arr_length
  then true_idx - t.arr_length
  else true_idx

let get t idx = CheapArray.get_some_exn t.arr (true_index t idx)

let get_opt t idx = try Some (get t idx) with _ -> None

let pp pp_data fmt t =
  Format.fprintf fmt "[ @[<hov>";
  iter t (fun x -> Format.fprintf fmt "%a;@ " pp_data x);
  Format.fprintf fmt "@] ]"
