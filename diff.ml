external identity : 'a -> 'a = "%identity"

let pp_list ?(sep = (fun fmt () -> ()) )pp_data fmt lst =
  let rec aux = function
    | [] -> ()
    | [ x ] -> pp_data fmt x
    | x :: r -> Format.fprintf fmt "%a%a" pp_data x sep (); aux r
  in aux lst

let pp_cstruct fmt cs =
  Format.fprintf fmt "\"";
  for i = 0 to Cstruct.len cs - 1
  do if Cstruct.get_uint8 cs i > 32 && Cstruct.get_uint8 cs i < 127
    then Format.fprintf fmt "%c" (Cstruct.get_char cs i)
    else Format.fprintf fmt "."
  done;
  Format.fprintf fmt "\""

let pp_array pp_data fmt arr =
  Format.fprintf fmt "[| @[<hov>%a@] |]"
    (pp_list ~sep:(fun fmt () -> Format.fprintf fmt ";@ ") pp_data) (Array.to_list arr)

let () =
  if Array.length Sys.argv = 3
  then
    let a = LoadFile.array_of_filename Sys.argv.(1) in
    let b = LoadFile.array_of_filename Sys.argv.(2) in

    let _ =
      Patience_diff.get_hunks
        ~transform:identity
        ~compare:Cstruct.compare
        ~context:(-1)
        ~mine:a ~other:b
    in

    let hunks =
      Pdiff.get_hunks
        ~transform:identity
        ~compare:Cstruct.compare
        ~context:(-1)
        ~a ~b
    in

    Format.printf "[ @[<hov>%a@] ]\n%!"
      (Pdiff.Hunk.pp_list ~sep:(fun fmt -> Format.fprintf fmt ";@ ") (Pdiff.Hunk.pp (pp_array pp_cstruct)))
      hunks;

    ()
  else Format.eprintf "%s filename filename\n%!" Sys.argv.(0)
