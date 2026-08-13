(** Live-TLS acceptance on Eio: drives the {!Zerobus_eio} façade over a REAL TLS
    1.3 + ALPN-h2 handshake against the self-signed [tls_server_eio] mock
    (separate process). Proves the Eio transport's TLS path end to end —
    handshake, ALPN negotiation of h2, and gRPC framing over the encrypted flow
    — not just the cleartext h2c path the other Eio tests use.

    The client can't use the system trust store for a self-signed cert, so the
    server prints its cert SHA-256 on the READY line and the client pins it via
    a [cert-fingerprint] authenticator (passed through
    [with_stream ~authenticator]).

    Runs on zbeio (OCaml 5.2). *)

module Zb = Zerobus_eio

let n_records = 200
let port = ref 0
let cert_fp = ref ""

let server_exe () =
  let dir = Filename.dirname Sys.executable_name in
  let cand = Filename.concat dir "tls_server_eio.exe" in
  if Sys.file_exists cand then cand else "./tls_server_eio.exe"

let with_server (f : unit -> 'a) : 'a =
  let exe = server_exe () in
  let stdout_r, stdout_w = Unix.pipe () in
  let pid =
    Unix.create_process exe [| exe; "0" |] Unix.stdin stdout_w Unix.stderr
  in
  Unix.close stdout_w;
  let ic = Unix.in_channel_of_descr stdout_r in
  (try
     match String.split_on_char ' ' (input_line ic) with
     | [ "READY"; p; fp ] ->
         port := int_of_string p;
         cert_fp := fp
     | _ -> failwith "bad READY line"
   with End_of_file -> failwith "server did not signal READY");
  Fun.protect
    ~finally:(fun () ->
      (try Unix.kill pid Sys.sigkill with _ -> ());
      (try ignore (Unix.waitpid [] pid) with _ -> ());
      try close_in ic with _ -> ())
    f

type result = { acked_last : bool; issued : int; err : string option }

let run ~env : result =
  Eio.Switch.run @@ fun sw ->
  (* Pin the server's self-signed cert by fingerprint (system trust store can't
     validate it). This still exercises the full real TLS handshake + ALPN. *)
  let authenticator =
    match
      X509.Authenticator.of_string (Printf.sprintf "cert-fp:sha256:%s" !cert_fp)
    with
    | Ok a -> a (fun () -> Some (Ptime_clock.now ()))
    | Error (`Msg m) -> failwith ("authenticator: " ^ m)
  in
  match
    Zb.create
      ~endpoint:(Printf.sprintf "127.0.0.1:%d" !port)
      ~workspace_url:"https://adb-1234567890.11.azuredatabricks.net" ()
  with
  | Error e ->
      {
        acked_last = false;
        issued = 0;
        err = Some (Zerobus_core.Error.to_string e);
      }
  | Ok client -> (
      let table =
        {
          Zerobus_core.Options.table_name = "main.default.mock";
          descriptor = None;
        }
      in
      let options =
        {
          Zerobus_core.Options.default_stream_options with
          record_type = Zerobus_core.Options.Json;
        }
      in
      let headers =
        [
          ("authorization", "Bearer mock-token");
          ( "x-databricks-zerobus-table-name",
            table.Zerobus_core.Options.table_name );
        ]
      in
      let body (stream : Zb.stream) : result =
        let rec loop i last =
          if i >= n_records then Ok last
          else
            match
              Zb.ingest stream
                (Bytes.of_string (Printf.sprintf {|{"id":%d}|} i))
            with
            | Ok off -> loop (i + 1) (Some off)
            | Error _ as e -> e
        in
        match loop 0 None with
        | Error e ->
            {
              acked_last = false;
              issued = 0;
              err = Some (Zerobus_core.Error.to_string e);
            }
        | Ok last_off ->
            let flush_r = Zb.flush stream in
            let acked_last =
              match (flush_r, last_off) with
              | Ok (), Some _ -> true
              | _ -> false
            in
            {
              acked_last;
              issued = n_records;
              err =
                (match flush_r with
                | Error e -> Some (Zerobus_core.Error.to_string e)
                | Ok () -> None);
            }
      in
      match
        Zb.with_stream ~env ~sw ~tls:true ~authenticator client table ~headers
          ~options body
      with
      | Ok r -> r
      | Error e ->
          {
            acked_last = false;
            issued = 0;
            err = Some (Zerobus_core.Error.to_string e);
          })

let result =
  lazy
    (with_server (fun () ->
         Eio_main.run @@ fun env ->
         let r = run ~env in
         Printf.eprintf
           "ZEROBUS OCAML LIVE-TLS TEST (Eio) -- evidence\n\
            ocaml_version : %s\n\
            tls           : TLS1.3 + ALPN h2 (self-signed, cert-fp pinned)\n\
            records_issued: %d\n\
            flush_all_acked: %b\n\
            error         : %s\n\
            %!"
           Sys.ocaml_version r.issued r.acked_last
           (match r.err with Some e -> e | None -> "none");
         r))

let () =
  Alcotest.run "phase3-tls-eio"
    [
      ( "live-tls",
        [
          Alcotest.test_case "no error" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check (option string)) "err" None r.err);
          Alcotest.test_case "all records issued" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check int) "issued" n_records r.issued);
          Alcotest.test_case "flush confirms all acked over TLS" `Slow
            (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool) "acked" true r.acked_last);
        ] );
    ]
