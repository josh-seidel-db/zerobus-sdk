(** Eio (OCaml 5.x) companion to bidi_spike.ml.

    Proves the SAME three properties as the Lwt spike -- all records acked, acks
    interleave with sends, offsets monotonic -- but on grpc-eio + h2-eio under
    OCaml 5.x, written direct-style (effects/fibers, no Lwt monad). This is the
    runtime the design review's findings #1/#2 were about, so it is the one most
    worth proving.

    TOPOLOGY: the server runs as a SEPARATE PROCESS (ingest_server_eio.exe),
    spawned by this driver. That is not incidental -- it is a finding:
    grpc-eio 0.2.0's client send path does not flush the h2 body, so an
    in-process client+server (even across Eio domains) deadlocks, while separate
    processes interoperate correctly. Separate processes is also the real-world
    topology. See README "Eio finding" for the full diagnosis and evidence.

    In-process concerns still out of scope, same as the Lwt spike: no TLS/ALPN,
    no OAuth, no recovery, no real Zerobus service. Written against the installed
    grpc-eio/h2-eio .mli, then compiled and run. *)

module Ingest = Ingest_proto.Ingest

let n_records = 200

(* Set from the server's READY line (OS-assigned port -> no collisions). *)
let port = ref 0

let encode_request (r : Ingest.ingest_request) : string =
  let enc = Pbrt.Encoder.create () in
  Ingest.encode_pb_ingest_request r enc;
  Pbrt.Encoder.to_string enc

let decode_response (s : string) : Ingest.ingest_response =
  Ingest.decode_pb_ingest_response (Pbrt.Decoder.of_string s)

(* -------- client: send-fiber + ack-fiber under one Switch -------- *)
module Client = struct
  type result = {
    acks : int64 list;
    sent_when_first_ack : int;
    total_sent : int;
  }

  let run ~sw ~net () : result =
    let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, !port) in
    let socket = Eio.Net.connect ~sw net addr in
    let connection =
      H2_eio.Client.create_connection ~sw ~error_handler:(fun _ -> ()) socket
    in
    let sent = ref 0 in
    let acks = ref [] in
    let first_ack_at = ref (-1) in

    let handler =
      Grpc_eio.Client.Rpc.bidirectional_streaming ~f:(fun writer responses ->
          let sender () =
            for i = 0 to n_records - 1 do
              let req =
                Ingest.make_ingest_request
                  ~payload:(Bytes.of_string (Printf.sprintf "row-%d" i))
                  ~client_seq:(Int64.of_int i) ()
              in
              Grpc_eio.Seq.write writer (encode_request req);
              incr sent;
              Eio.Fiber.yield () (* let the ack fiber interleave *)
            done;
            Grpc_eio.Seq.close_writer writer
          in
          let receiver () =
            Seq.iter
              (fun raw_resp ->
                let resp = decode_response raw_resp in
                if !first_ack_at < 0 then first_ack_at := !sent;
                acks := resp.Ingest.offset :: !acks)
              responses
          in
          Eio.Fiber.both sender receiver)
    in
    let do_request =
      H2_eio.Client.request connection ~error_handler:(fun _ -> ())
    in
    let result, _status =
      Grpc_eio.Client.call ~service:"ingest.Ingest" ~rpc:"IngestStream"
        ~scheme:"http" ~handler ~do_request ()
      |> Result.get_ok
    in
    ignore result;
    (* Shut the h2 connection down, else the switch never resolves and the
       client fiber (hence Eio_main.run) would not return. *)
    (try ignore (Eio.Promise.await (H2_eio.Client.shutdown connection))
     with _ -> ());
    {
      acks = List.rev !acks;
      sent_when_first_ack = !first_ack_at;
      total_sent = !sent;
    }
end

(* -------- spawn the server process, wait for READY, run client -------- *)

(* Locate ingest_server_eio.exe next to this test exe (dune puts them in the
   same _build dir; the (deps ...) stanza guarantees it is built). *)
let server_exe () =
  let dir = Filename.dirname Sys.executable_name in
  let candidate = Filename.concat dir "ingest_server_eio.exe" in
  if Sys.file_exists candidate then candidate
  else (* fall back to PATH-relative name *) "./ingest_server_eio.exe"

let with_server (f : unit -> 'a) : 'a =
  let exe = server_exe () in
  let stdout_r, stdout_w = Unix.pipe () in
  (* port 0 -> OS assigns a free port; server reports it back on READY *)
  let pid =
    Unix.create_process exe [| exe; "0" |] Unix.stdin stdout_w Unix.stderr
  in
  Unix.close stdout_w;
  let ic = Unix.in_channel_of_descr stdout_r in
  (* wait for "READY <port>" and adopt the reported port *)
  (try
     let line = input_line ic in
     match String.split_on_char ' ' line with
     | [ "READY"; p ] -> port := int_of_string p
     | _ -> failwith ("unexpected server line: " ^ line)
   with End_of_file -> failwith "server did not signal READY");
  Fun.protect
    ~finally:(fun () ->
      (try Unix.kill pid Sys.sigkill with _ -> ());
      (try ignore (Unix.waitpid [] pid) with _ -> ());
      (try close_in ic with _ -> ()))
    f

let run_client () : Client.result =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  Client.run ~sw ~net ()

(* -------- evidence + assertions -------- *)
let result =
  lazy
    (with_server (fun () ->
         let r = run_client () in
         let min_off = List.fold_left min Int64.max_int r.Client.acks
         and max_off = List.fold_left max Int64.min_int r.Client.acks in
         Printf.eprintf
           "ZEROBUS OCAML SPIKE (EIO) -- transport evidence\n\
            timestamp_utc          : %s\n\
            ocaml_version          : %s\n\
            runtime                : Eio (server in separate process)\n\
            records_sent           : %d\n\
            acks_received          : %d\n\
            sends_done_at_first_ack: %d (of %d)  <- proves ack arrived mid-send\n\
            first_offset           : %Ld\n\
            last_offset            : %Ld\n\
            offsets_contiguous     : %b\n%!"
           (let t = Unix.gmtime (Unix.time ()) in
            Printf.sprintf "%04d-%02d-%02dT%02d:%02d:%02dZ" (t.tm_year + 1900)
              (t.tm_mon + 1) t.tm_mday t.tm_hour t.tm_min t.tm_sec)
           Sys.ocaml_version r.Client.total_sent
           (List.length r.Client.acks)
           r.Client.sent_when_first_ack n_records min_off max_off
           (Int64.sub max_off min_off
           = Int64.of_int (List.length r.Client.acks - 1));
         r))

let () =
  ignore (Lazy.force result);
  Alcotest.run "bidi-transport-spike-eio"
    [
      ( "bidi streaming (eio)",
        [
          Alcotest.test_case "all records acked" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check int) "acked" n_records (List.length r.Client.acks);
              Alcotest.(check int) "sent" n_records r.Client.total_sent);
          Alcotest.test_case "acks interleave with sends" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool) "first ack before all sends" true
                (r.Client.sent_when_first_ack >= 0
                && r.Client.sent_when_first_ack < n_records));
          Alcotest.test_case "offsets are monotonic" `Slow (fun () ->
              let r = Lazy.force result in
              let rec mono = function
                | a :: (b :: _ as tl) -> Int64.compare a b < 0 && mono tl
                | _ -> true
              in
              Alcotest.(check bool) "increasing" true (mono r.Client.acks));
        ] );
    ]
