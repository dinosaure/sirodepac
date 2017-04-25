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
