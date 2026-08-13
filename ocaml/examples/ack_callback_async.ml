(** Example — async ack notification via a callback (Async, JSON).

    The Async counterpart of {!ack_callback_lwt}. For continuous / unbounded
    streams you don't want to block on [flush] at all. Register an
    [ack_callback]: [on_ack] fires (from the ack-reader) as the server confirms
    each offset durable, [on_error] if a record ultimately fails. You still
    [ingest] in a tight loop and never wait inline — the callback is how you
    learn about durability, out of band.

    This is the right shape for a long-lived producer: queue continuously, react
    to acks asynchronously, and [flush] only periodically or at shutdown. *)

open! Core
open! Async

let env k =
  match Sys.getenv k with Some v -> v | None -> failwith ("set env " ^ k)

let main () : (unit, Zerobus_core.Error.t) result Deferred.t =
  let workspace_url = env "DATABRICKS_WORKSPACE_URL" in
  let table_name = env "ZEROBUS_TABLE" in
  let client_id = env "ZEROBUS_CLIENT_ID" in
  let client_secret = env "ZEROBUS_CLIENT_SECRET" in

  Zerobus_async.create ~endpoint:"" ~workspace_url () >>= function
  | Error e -> return (Error e)
  | Ok client ->
      let acked = ref 0 in
      let ack_callback =
        {
          Zerobus_core.Options.on_ack = (fun _off -> incr acked);
          on_error =
            (fun off msg ->
              Core.eprintf "ack error at offset %Ld: %s\n%!"
                (Zerobus_core.Options.int64_of_offset off)
                msg);
        }
      in
      let table = { Zerobus_core.Options.table_name; descriptor = None } in
      let options =
        {
          Zerobus_async.default_stream_options with
          record_type = Zerobus_core.Options.Json;
          ack_callback = Some ack_callback;
        }
      in
      Zerobus_async.with_stream_oauth client table ~client_id ~client_secret
        ~options (fun stream ->
          (* Queue continuously; the callback tracks durability out of band. *)
          Deferred.List.iter ~how:`Sequential (List.init 500 ~f:Fn.id)
            ~f:(fun i ->
              let r = Bytes.of_string (Printf.sprintf {|{"id": %d}|} i) in
              Zerobus_async.ingest stream r >>| fun (_ : (_, _) result) -> ())
          >>= fun () ->
          (* A periodic / shutdown flush bounds how far behind acks can lag. *)
          Zerobus_async.flush stream >>| fun flush_r ->
          Core.printf "acked %d records via callback\n%!" !acked;
          match flush_r with
          | Ok () -> ()
          | Error e -> failwith (Zerobus_core.Error.to_string e))

let () =
  don't_wait_for
    ( main () >>| fun r ->
      (match r with
      | Ok () -> print_endline "OK"
      | Error e ->
          prerr_endline ("FAILED: " ^ Zerobus_core.Error.to_string e);
          Core.exit 1);
      Shutdown.shutdown 0 );
  never_returns (Scheduler.go ())
