(** Example 4 — the REST interface: one POST per batch (Lwt).

    {!Zerobus_rest} is the stateless, low-frequency alternative to the gRPC
    stream: no persistent connection, no offsets, no recovery — just a
    table-scoped [POST .../insert] with a JSON array of records. Use it for edge
    devices, webhooks, or infrequent reporting where the "throughput tax" of a
    round-trip per batch is fine. For high volume, use the gRPC {!Zerobus}
    stream (loop-then-flush) instead. *)

let ( let* ) = Lwt.bind
let env k = try Sys.getenv k with Not_found -> failwith ("set env " ^ k)

let main () : (unit, Zerobus_core.Error.t) result Lwt.t =
  let workspace_url = env "DATABRICKS_WORKSPACE_URL" in
  let table = env "ZEROBUS_TABLE" in
  let client_id = env "ZEROBUS_CLIENT_ID" in
  let client_secret = env "ZEROBUS_CLIENT_SECRET" in

  let* client_r =
    Zerobus_rest.create ~endpoint:"" ~workspace_url ~client_id ~client_secret ()
  in
  match client_r with
  | Error e -> Lwt.return (Error e)
  | Ok client ->
      (* One POST carries the whole batch as a JSON array. *)
      let records =
        List.init 10 (fun i ->
            `Assoc
              [ ("id", `Int i); ("msg", `String (Printf.sprintf "row-%d" i)) ])
      in
      Zerobus_rest.insert client ~table records

let () =
  match Lwt_main.run (main ()) with
  | Ok () -> print_endline "OK — batch inserted via REST"
  | Error e ->
      prerr_endline ("FAILED: " ^ Zerobus_core.Error.to_string e);
      exit 1
