open Ocamlbuild_plugin

let () = dispatch @@ function
  | After_hygiene as x ->
    pdep [ "link" ] "linkdep" (fun param -> [ param ])
  | _ -> ()
