(** Example — the REST interface: one POST per batch (Eio).

    The Eio counterpart of {!rest_insert_lwt}. {!Zerobus_rest_eio} is the
    stateless, low-frequency alternative to the gRPC stream: no persistent
    connection, no offsets, no recovery — just a table-scoped [POST .../insert]
    with a JSON array of records. Use it for edge devices, webhooks, or
    infrequent reporting. For high volume, use the gRPC {!Zerobus_eio} stream
    (loop-then-flush) instead.

    [env]/[sw] come from the caller's [Eio_main.run] / [Switch.run]. *)

let getenv k = try Sys.getenv k with Not_found -> failwith ("set env " ^ k)

let run ~env ~sw : (unit, Zerobus_core.Error.t) result =
  let workspace_url = getenv "DATABRICKS_WORKSPACE_URL" in
  let table = getenv "ZEROBUS_TABLE" in
  let client_id = getenv "ZEROBUS_CLIENT_ID" in
  let client_secret = getenv "ZEROBUS_CLIENT_SECRET" in

  match
    Zerobus_rest_eio.create ~endpoint:"" ~workspace_url ~client_id
      ~client_secret ()
  with
  | Error e -> Error e
  | Ok client ->
      (* One POST carries the whole batch as a JSON array. *)
      let records =
        List.init 10 (fun i ->
            `Assoc
              [ ("id", `Int i); ("msg", `String (Printf.sprintf "row-%d" i)) ])
      in
      Zerobus_rest_eio.insert ~env ~sw client ~table records

let () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  match run ~env:(env :> Eio_unix.Stdenv.base) ~sw with
  | Ok () -> print_endline "OK — batch inserted via REST (Eio)"
  | Error e ->
      prerr_endline ("FAILED: " ^ Zerobus_core.Error.to_string e);
      exit 1
