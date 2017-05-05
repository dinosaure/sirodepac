type 'a t = ObjArray.t

let empty = ObjArray.empty
let get arr i = Obj.obj (ObjArray.get arr i)
let set arr i x = ObjArray.set arr i (Obj.repr x)
let length = ObjArray.length
let blit = ObjArray.blit
let copy = ObjArray.copy

let create len v =
  let res = ObjArray.create len in
  for i = 0 to len - 1
  do set res i v done;
  res

let iter a f =
  for i = 0 to length a - 1
  do f (get a i) done

let pp pp_data fmt arr =
  let len = length arr in

  let rec aux fmt idx =
    if idx = len
    then ()
    else if idx = len - 1
    then pp_data fmt (get arr idx)
    else begin
      Format.fprintf fmt "%05d: %a;@\n" idx pp_data (get arr idx);
      aux fmt (idx + 1)
    end
  in

  Format.fprintf fmt "[| @[<hov>%a@] |]" aux 0

