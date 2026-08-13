(* Arrow IPC codec spike — the round-trip crosses OCaml -> C++ -> Arrow -> IPC
   bytes -> Arrow -> C++ -> OCaml. Proves the one gap in native OCaml Arrow support
   (DESIGN.md §8.5.2). Compiled-AND-RUN, real libarrow linked. *)

let rows =
  [
    Arrow_ipc.{ id = 10; name = "alpha" };
    Arrow_ipc.{ id = 20; name = "beta" };
    Arrow_ipc.{ id = 30; name = "gamma-with-a-longer-value" };
    Arrow_ipc.{ id = -7; name = "" };
    Arrow_ipc.{ id = 9999; name = "δ-unicode-ok" };
  ]

let check name cond = if not cond then failwith ("FAILED: " ^ name)

let () =
  match Arrow_ipc.encode rows with
  | Error m -> failwith ("encode error: " ^ m)
  | Ok ipc ->
      let n = Bytes.length ipc in
      (* Arrow IPC *stream* format begins with the 0xFFFFFFFF continuation
         marker — assert the real format, not just "non-empty". *)
      let magic_ok =
        n >= 4
        && Char.code (Bytes.get ipc 0) = 0xff
        && Char.code (Bytes.get ipc 1) = 0xff
        && Char.code (Bytes.get ipc 2) = 0xff
        && Char.code (Bytes.get ipc 3) = 0xff
      in
      check "IPC bytes non-empty" (n > 0);
      check "Arrow IPC stream magic (0xFFFFFFFF)" magic_ok;
      (match Arrow_ipc.decode ipc with
      | Error m -> failwith ("decode error: " ^ m)
      | Ok got ->
          check "row count round-trips"
            (List.length got = List.length rows);
          List.iter2
            (fun (a : Arrow_ipc.row) (b : Arrow_ipc.row) ->
              check
                (Printf.sprintf "id %d = %d" a.id b.id)
                (a.id = b.id);
              check
                (Printf.sprintf "name %S = %S" a.name b.name)
                (String.equal a.name b.name))
            rows got);
      Printf.printf
        "ZEROBUS OCAML ARROW IPC CODEC SPIKE -- evidence\n\
         ocaml_version          : %s\n\
         arrow_cpp              : linked libarrow (real IPC codec)\n\
         rows                   : %d\n\
         ipc_bytes              : %d\n\
         ipc_stream_magic       : 0x%02X%02X%02X%02X (stream continuation)\n\
         roundtrip              : FAITHFUL (ids + utf8 strings, incl. empty + unicode)\n\
         %!"
        Sys.ocaml_version (List.length rows) n
        (Char.code (Bytes.get ipc 0))
        (Char.code (Bytes.get ipc 1))
        (Char.code (Bytes.get ipc 2))
        (Char.code (Bytes.get ipc 3));
      print_endline "ALL CHECKS PASSED"
