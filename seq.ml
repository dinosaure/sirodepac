type 'a t = ('a -> unit) -> unit

let empty _ = ()

let cons x l k = k x; l k
let snoc l x k = l k; k x

module MList =
struct
  type 'a node =
    | Nil
    | Cons of 'a array * int ref * 'a node ref

  let of_seq_with seq k =
    let start = ref Nil in
    let chunk_size = ref 8 in
    (* XXX: fill the list. pref: tail-reference from previous node *)
    let prev, cur = ref start, ref Nil in
    seq
      (fun x ->
         k x; (* callback *)
         match !cur with
         | Nil ->
           let n = !chunk_size in
           if n < 4096 then chunk_size := 2 * !chunk_size;
           cur := Cons (Array.make n x, ref 1, ref Nil)
         | Cons (a, n, next) ->
           assert (!n < Array.length a);
           a.(!n) <- x;
           incr n;
           if !n = Array.length a
           then begin
             !prev := !cur;
             prev := next;
             cur := Nil
           end);
    !prev := !cur;
    !start

  let of_seq seq =
    of_seq_with seq (fun _ -> ())

  let rec iter f l = match l with
    | Nil -> ()
    | Cons (a, n, tl) ->
      for i = 0 to !n - 1 do f a.(i) done;
      iter f !tl

  let to_seq l k = iter k l
end

let persistent seq =
  let l = MList.of_seq seq in
  MList.to_seq l

let fold f init seq =
  let r = ref init in
  seq (fun elt -> r := f !r elt);
  !r

exception Break

let fold_while f s seq =
  let state = ref s in
  let consume x =
    let acc, cont = f (!state) x in
    state := acc;
    match cont with
    | `Stop -> raise Break
    | `Continue -> ()
  in
  try seq consume; !state
  with Break -> !state

let iter f seq = seq f

let concat s k = s (fun s' -> s' k)

let append s1 s2 k = s1 k; s2 k

let to_queue q seq = seq (fun x -> Queue.push x q)

let of_queue q k = Queue.iter k q

let to_list seq =
  List.rev (fold (fun y x -> x :: y) [] seq)

let of_list l k = List.iter k l
