let array_of_filename filename =
  let ch  = open_in filename in

  let rec aux acc = match input_line ch with
    | line -> aux (Cstruct.of_string (line ^ "\n") :: acc)
    | exception End_of_file -> List.rev acc
  in

Array.of_list (aux [])
