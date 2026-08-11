(** Example 3 — loop then flush on Eio (direct-style, JSON).

    The same cardinal rule as the Lwt example — queue in a loop, flush once — but
    in Eio's direct style (no monad) and bracket shape: [with_stream_oauth] owns
    the stream's [Switch] for the duration of the body and tears it down on exit,
    because the ack-reader runs as a fiber inside that switch.

    [env] and [sw] come from the caller's [Eio_main.run] / [Switch.run]. *)

let getenv k = try Sys.getenv k with Not_found -> failwith ("set env " ^ k)

let run ~env ~sw : (unit, Zerobus_core.Error.t) result =
  let workspace_url = getenv "DATABRICKS_WORKSPACE_URL" in
  let table_name = getenv "ZEROBUS_TABLE" in
  let client_id = getenv "ZEROBUS_CLIENT_ID" in
  let client_secret = getenv "ZEROBUS_CLIENT_SECRET" in

  match Zerobus_eio.create ~endpoint:"" ~workspace_url () with
  | Error e -> Error e
  | Ok client ->
      let table = { Zerobus_core.Options.table_name; descriptor = None } in
      let options =
        {
          Zerobus_eio.default_stream_options with
          record_type = Zerobus_core.Options.Json;
        }
      in
      (* Bracket: the stream is valid only inside this body. *)
      Zerobus_eio.with_stream_oauth ~env ~sw client table ~client_id
        ~client_secret ~options (fun stream ->
          List.iter
            (fun i ->
              let r = Bytes.of_string (Printf.sprintf {|{"id": %d}|} i) in
              (* queue only — ignore the offset, do NOT wait here *)
              let (_ : (Zerobus_eio.offset, _) result) =
                Zerobus_eio.ingest stream r
              in
              ())
            (List.init 1000 Fun.id);
          (* flush once at the end *)
          match Zerobus_eio.flush stream with
          | Ok () -> ()
          | Error e -> failwith (Zerobus_core.Error.to_string e))

let () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  match run ~env:(env :> Eio_unix.Stdenv.base) ~sw with
  | Ok () -> print_endline "OK — 1000 records ingested and flushed durably (Eio)"
  | Error e ->
      prerr_endline ("FAILED: " ^ Zerobus_core.Error.to_string e);
      exit 1
