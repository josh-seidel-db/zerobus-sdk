(** Example 1 — the canonical ingestion pattern: loop then flush (Lwt, JSON).

    THIS IS THE PATTERN TO COPY. Ingestion is asynchronous and pipelined:
    [ingest] only QUEUES a record and returns its offset handle immediately;
    sending and acknowledgement happen on background tasks. So the throughput
    recipe is:

      for record in records: stream.ingest record   (* queue only — never wait *)
      stream.flush ()                                (* wait ONCE at the end *)

    Calling [wait_for_offset] after every [ingest] would force one full server
    round-trip per record and collapse throughput by orders of magnitude. Don't.

    Build:  dune build examples/
    Run (needs a real workspace + service principal):
      DATABRICKS_WORKSPACE_URL=https://adb-....azuredatabricks.net \
      ZEROBUS_TABLE=main.default.my_table \
      ZEROBUS_CLIENT_ID=... ZEROBUS_CLIENT_SECRET=... \
      dune exec examples/json_loop_then_flush_lwt.exe *)

let ( let* ) = Lwt.bind

let env k = try Sys.getenv k with Not_found -> failwith ("set env " ^ k)

let main () : (unit, Zerobus_core.Error.t) result Lwt.t =
  let workspace_url = env "DATABRICKS_WORKSPACE_URL" in
  let table_name = env "ZEROBUS_TABLE" in
  let client_id = env "ZEROBUS_CLIENT_ID" in
  let client_secret = env "ZEROBUS_CLIENT_SECRET" in

  (* [endpoint:""] derives the gRPC endpoint from the workspace URL. *)
  let* client_r = Zerobus.create ~endpoint:"" ~workspace_url () in
  match client_r with
  | Error e -> Lwt.return (Error e)
  | Ok client -> (
      (* JSON needs no descriptor; Proto would pass [descriptor = Some ...]. *)
      let table = { Zerobus_core.Options.table_name; descriptor = None } in
      let options =
        { Zerobus.default_stream_options with record_type = Zerobus_core.Options.Json }
      in
      let* stream_r =
        Zerobus.create_stream client table ~client_id ~client_secret ~options ()
      in
      match stream_r with
      | Error e -> Lwt.return (Error e)
      | Ok stream ->
          (* 1) QUEUE in a loop — do NOT wait per record. *)
          let records =
            List.init 1000 (fun i ->
                Bytes.of_string (Printf.sprintf {|{"id": %d, "msg": "row-%d"}|} i i))
          in
          let* () =
            Lwt_list.iter_s
              (fun r ->
                let* _off = Zerobus.ingest stream r in
                Lwt.return_unit)
              records
          in
          (* 2) FLUSH once — waits for every queued record to be durable. *)
          let* flush_r = Zerobus.flush stream in
          let* _ = Zerobus.close stream in
          Lwt.return flush_r)

let () =
  match Lwt_main.run (main ()) with
  | Ok () -> print_endline "OK — 1000 records ingested and flushed durably"
  | Error e ->
      prerr_endline ("FAILED: " ^ Zerobus_core.Error.to_string e);
      exit 1
