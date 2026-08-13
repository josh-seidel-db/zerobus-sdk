(** Example — the canonical ingestion pattern: loop then flush (Async, JSON).

    The Async counterpart of {!json_loop_then_flush_lwt}. The same cardinal rule
    applies: [ingest] only QUEUES a record and returns its offset handle
    immediately; sending and acknowledgement happen on background jobs. So:

    for record in records: ingest record (* queue only — never wait *) flush ()
    (* wait ONCE at the end *)

    The Async façade is bracket-shaped ([with_stream_oauth] owns the stream's
    scope for the body and tears it down on exit), because the ack-reader runs
    as a background job inside that scope.

    Build: dune build examples/ Run (needs a real workspace + service
    principal): DATABRICKS_WORKSPACE_URL=https://adb-....azuredatabricks.net \
    ZEROBUS_TABLE=main.default.my_table \ ZEROBUS_CLIENT_ID=...
    ZEROBUS_CLIENT_SECRET=... \ dune exec
    examples/json_loop_then_flush_async.exe *)

open! Core
open! Async

let env k =
  match Sys.getenv k with Some v -> v | None -> failwith ("set env " ^ k)

let main () : (unit, Zerobus_core.Error.t) result Deferred.t =
  let workspace_url = env "DATABRICKS_WORKSPACE_URL" in
  let table_name = env "ZEROBUS_TABLE" in
  let client_id = env "ZEROBUS_CLIENT_ID" in
  let client_secret = env "ZEROBUS_CLIENT_SECRET" in

  (* [endpoint:""] derives the gRPC endpoint from the workspace URL. *)
  Zerobus_async.create ~endpoint:"" ~workspace_url () >>= function
  | Error e -> return (Error e)
  | Ok client ->
      (* JSON needs no descriptor; Proto would pass [descriptor = Some ...]. *)
      let table = { Zerobus_core.Options.table_name; descriptor = None } in
      let options =
        {
          Zerobus_async.default_stream_options with
          record_type = Zerobus_core.Options.Json;
        }
      in
      (* Bracket: the stream is valid only inside this body. *)
      Zerobus_async.with_stream_oauth client table ~client_id ~client_secret
        ~options (fun stream ->
          (* 1) QUEUE in a loop — do NOT wait per record. *)
          let records =
            List.init 1000 ~f:(fun i ->
                Bytes.of_string
                  (Printf.sprintf {|{"id": %d, "msg": "row-%d"}|} i i))
          in
          Deferred.List.iter ~how:`Sequential records ~f:(fun r ->
              Zerobus_async.ingest stream r >>| fun (_ : (_, _) result) -> ())
          >>= fun () ->
          (* 2) FLUSH once — waits for every queued record to be durable. *)
          Zerobus_async.flush stream >>| function
          | Ok () -> ()
          | Error e -> failwith (Zerobus_core.Error.to_string e))

let () =
  don't_wait_for
    ( main () >>| fun r ->
      (match r with
      | Ok () ->
          print_endline "OK — 1000 records ingested and flushed durably (Async)"
      | Error e ->
          prerr_endline ("FAILED: " ^ Zerobus_core.Error.to_string e);
          Core.exit 1);
      Shutdown.shutdown 0 );
  never_returns (Scheduler.go ())
