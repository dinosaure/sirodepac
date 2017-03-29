external identity : 'a -> 'a = "%identity"

let pp_string_array fmt arr =
  for i = 0 to Array.length arr - 1
  do Format.fprintf fmt "%s" (Array.get arr i) done

open Patience_diff_lib

let () =
  if Array.length Sys.argv = 3
  then
    let a = LoadFile.array_of_filename Sys.argv.(1) in
    let b = LoadFile.array_of_filename Sys.argv.(2) in

    let _ =
      Patience_diff.get_hunks
        ~transform:identity
        ~compare:String.compare
        ~context:0
        ~mine:a ~other:b
    in

    let hunks =
      Pdiff.get_hunks
        ~transform:identity
        ~compare:String.compare
        ~context:0
        ~a ~b
    in

    Format.printf "[ @[<hov>%a@] ]\n%!"
      (Pdiff.Hunk.pp_list ~sep:(fun fmt -> Format.fprintf fmt ";@ ") (Pdiff.Hunk.pp pp_string_array))
      hunks;

    ()
  else Format.eprintf "%s filename filename\n%!" Sys.argv.(0)
