(** Example 2 — async ack notification via a callback (Lwt, JSON).

    For continuous / unbounded streams you don't want to block on [flush] at all.
    Register an [ack_callback]: [on_ack] fires (from the ack-reader) as the server
    confirms each offset durable, [on_error] if a record ultimately fails. You
    still [ingest] in a tight loop and never wait inline — the callback is how you
    learn about durability, out of band.

    This is the right shape for a long-lived producer: queue continuously, react
    to acks asynchronously, and [flush] only periodically (e.g. every N records)
    or at shutdown — never per record. *)

let ( let* ) = Lwt.bind

let env k = try Sys.getenv k with Not_found -> failwith ("set env " ^ k)

let main () : (unit, Zerobus_core.Error.t) result Lwt.t =
  let workspace_url = env "DATABRICKS_WORKSPACE_URL" in
  let table_name = env "ZEROBUS_TABLE" in
  let client_id = env "ZEROBUS_CLIENT_ID" in
  let client_secret = env "ZEROBUS_CLIENT_SECRET" in

  let* client_r = Zerobus.create ~endpoint:"" ~workspace_url () in
  match client_r with
  | Error e -> Lwt.return (Error e)
  | Ok client -> (
      let acked = ref 0 in
      let ack_callback =
        {
          Zerobus_core.Options.on_ack =
            (fun _off -> incr acked);
          on_error =
            (fun off msg ->
              Printf.eprintf "ack error at offset %Ld: %s\n%!"
                (Zerobus_core.Options.int64_of_offset off) msg);
        }
      in
      let table = { Zerobus_core.Options.table_name; descriptor = None } in
      let options =
        {
          Zerobus.default_stream_options with
          record_type = Zerobus_core.Options.Json;
          ack_callback = Some ack_callback;
        }
      in
      let* stream_r =
        Zerobus.create_stream client table ~client_id ~client_secret ~options ()
      in
      match stream_r with
      | Error e -> Lwt.return (Error e)
      | Ok stream ->
          (* Queue continuously; the callback tracks durability out of band. *)
          let* () =
            Lwt_list.iter_s
              (fun i ->
                let r = Bytes.of_string (Printf.sprintf {|{"id": %d}|} i) in
                let* _off = Zerobus.ingest stream r in
                Lwt.return_unit)
              (List.init 500 Fun.id)
          in
          (* A periodic / shutdown flush still bounds how far behind acks can lag. *)
          let* flush_r = Zerobus.flush stream in
          let* _ = Zerobus.close stream in
          Printf.printf "acked %d records via callback\n%!" !acked;
          Lwt.return flush_r)

let () =
  match Lwt_main.run (main ()) with
  | Ok () -> print_endline "OK"
  | Error e ->
      prerr_endline ("FAILED: " ^ Zerobus_core.Error.to_string e);
      exit 1
