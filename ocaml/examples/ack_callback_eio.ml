(** Example — async ack notification via a callback (Eio, JSON).

    The Eio counterpart of {!ack_callback_lwt}. For continuous / unbounded
    streams you don't want to block on [flush] at all. Register an
    [ack_callback]: [on_ack] fires (from the ack-reader fiber) as the server
    confirms each offset durable, [on_error] if a record ultimately fails. You
    still [ingest] in a tight loop and never wait inline — the callback is how
    you learn about durability, out of band.

    [with_stream_oauth] owns the stream's [Switch] for the body; [env]/[sw] come
    from the caller's [Eio_main.run] / [Switch.run]. *)

let getenv k = try Sys.getenv k with Not_found -> failwith ("set env " ^ k)

let run ~env ~sw : (unit, Zerobus_core.Error.t) result =
  let workspace_url = getenv "DATABRICKS_WORKSPACE_URL" in
  let table_name = getenv "ZEROBUS_TABLE" in
  let client_id = getenv "ZEROBUS_CLIENT_ID" in
  let client_secret = getenv "ZEROBUS_CLIENT_SECRET" in

  match Zerobus_eio.create ~endpoint:"" ~workspace_url () with
  | Error e -> Error e
  | Ok client ->
      let acked = ref 0 in
      let ack_callback =
        {
          Zerobus_core.Options.on_ack = (fun _off -> incr acked);
          on_error =
            (fun off msg ->
              Printf.eprintf "ack error at offset %Ld: %s\n%!"
                (Zerobus_core.Options.int64_of_offset off)
                msg);
        }
      in
      let table = { Zerobus_core.Options.table_name; descriptor = None } in
      let options =
        {
          Zerobus_eio.default_stream_options with
          record_type = Zerobus_core.Options.Json;
          ack_callback = Some ack_callback;
        }
      in
      Zerobus_eio.with_stream_oauth ~env ~sw client table ~client_id
        ~client_secret ~options (fun stream ->
          (* Queue continuously; the callback tracks durability out of band. *)
          List.iter
            (fun i ->
              let r = Bytes.of_string (Printf.sprintf {|{"id": %d}|} i) in
              let (_ : (Zerobus_eio.offset, _) result) =
                Zerobus_eio.ingest stream r
              in
              ())
            (List.init 500 Fun.id);
          (* A periodic / shutdown flush bounds how far behind acks can lag. *)
          (match Zerobus_eio.flush stream with
          | Ok () -> ()
          | Error e -> failwith (Zerobus_core.Error.to_string e));
          Printf.printf "acked %d records via callback\n%!" !acked)

let () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  match run ~env:(env :> Eio_unix.Stdenv.base) ~sw with
  | Ok () -> print_endline "OK (Eio)"
  | Error e ->
      prerr_endline ("FAILED: " ^ Zerobus_core.Error.to_string e);
      exit 1
