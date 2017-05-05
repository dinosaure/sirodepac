type 'a t = 'a CheapOption.t UniformArray.t

let empty       = UniformArray.empty
let length t    = UniformArray.length t
let is_none t i = CheapOption.is_none (UniformArray.get t i)
let is_some t i = CheapOption.is_some (UniformArray.get t i)
let set t i x   = UniformArray.set t i (CheapOption.of_option x)
let set_some t i x = UniformArray.set t i (CheapOption.of_option (Some x))
let set_none t i = UniformArray.set t i CheapOption.none
let get t i     = CheapOption.to_option (UniformArray.get t i)
let get_some_exn t i = CheapOption.value_exn (UniformArray.get t i)

let clear t =
  for i = 0 to length t - 1
  do set t i None done

let create len = UniformArray.create len CheapOption.none
let blit src src_pos dst dst_pos len = UniformArray.blit src src_pos dst dst_pos len

let pp pp_data fmt arr =
  let len = length arr in

  let pp_data fmt = function
    | Some x -> pp_data fmt x
    | None -> Format.fprintf fmt "<none>"
  in

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

