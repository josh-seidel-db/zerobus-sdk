(** Example — the REST interface: one POST per batch (Async).

    The Async counterpart of {!rest_insert_lwt}. {!Zerobus_rest_async} is the
    stateless, low-frequency alternative to the gRPC stream: no persistent
    connection, no offsets, no recovery — just a table-scoped [POST .../insert]
    with a JSON array of records. Use it for edge devices, webhooks, or
    infrequent reporting. For high volume, use the gRPC {!Zerobus_async} stream
    (loop-then-flush) instead. *)

open! Core
open! Async

let env k =
  match Sys.getenv k with Some v -> v | None -> failwith ("set env " ^ k)

let main () : (unit, Zerobus_core.Error.t) result Deferred.t =
  let workspace_url = env "DATABRICKS_WORKSPACE_URL" in
  let table = env "ZEROBUS_TABLE" in
  let client_id = env "ZEROBUS_CLIENT_ID" in
  let client_secret = env "ZEROBUS_CLIENT_SECRET" in

  match
    Zerobus_rest_async.create ~endpoint:"" ~workspace_url ~client_id
      ~client_secret ()
  with
  | Error e -> return (Error e)
  | Ok client ->
      (* One POST carries the whole batch as a JSON array. *)
      let records =
        List.init 10 ~f:(fun i ->
            `Assoc
              [ ("id", `Int i); ("msg", `String (Printf.sprintf "row-%d" i)) ])
      in
      Zerobus_rest_async.insert client ~table records

let () =
  don't_wait_for
    ( main () >>| fun r ->
      (match r with
      | Ok () -> print_endline "OK — batch inserted via REST (Async)"
      | Error e ->
          prerr_endline ("FAILED: " ^ Zerobus_core.Error.to_string e);
          Core.exit 1);
      Shutdown.shutdown 0 );
  never_returns (Scheduler.go ())
