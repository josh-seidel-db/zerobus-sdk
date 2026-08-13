(** Arrow Flight DoPut spike — the "native, free half" of Arrow support.

    Proves that the REAL Apache Arrow Flight protobuf messages
    (arrow.flight.protocol.FlightData / PutResult) flow over a client-initiated
    bidirectional gRPC stream in native OCaml — with the Arrow IPC payload carried
    as an OPAQUE `data_body` (NO Arrow library linked) and the Zerobus per-batch
    offset riding in `app_metadata`. This mirrors rust/sdk/src/stream/arrow, which
    uses Flight `DoPut` and only DoPut.

    It asserts:
      1. every batch is acked (schema message excluded, per Arrow IPC framing),
      2. acks interleave with sends (real bidi concurrency),
      3. ack offsets are strictly increasing (per-stream ordering),
      4. the opaque data_body bytes round-trip intact end to end (the SDK can
         carry arbitrary Arrow IPC bytes without understanding them).

    Topology: server in a separate process (the proven pattern from spike-eio /
    spike-async). Runs on the Lwt switch. Scope: transport + proto only — no real
    Arrow encoding, no TLS/OAuth/recovery (covered by other spikes). *)

module F = Flight_proto.Flight

let n_batches = 200
let port = ref 0

(* A stand-in "Arrow IPC" body: opaque bytes the SDK never interprets. Each batch
   gets distinct content so we can prove it round-trips unmodified. *)
let fake_ipc_body i =
  Bytes.of_string (Printf.sprintf "ARROW-IPC-BATCH-%d:%s" i (String.make (i mod 17) 'x'))

(* Same checksum the server computes over data_body, so the client can confirm
   the opaque payload arrived byte-exact through the proto encoding. *)
let body_len_sum b =
  let sum = ref 0 in
  Bytes.iter (fun c -> sum := (!sum + Char.code c) land 0xffffff) b;
  (Bytes.length b, !sum)

let encode_flight_data (r : F.flight_data) : string =
  let enc = Pbrt.Encoder.create () in
  F.encode_pb_flight_data r enc;
  Pbrt.Encoder.to_string enc

let decode_put_result (s : string) : F.put_result =
  F.decode_pb_put_result (Pbrt.Decoder.of_string s)

type result = {
  acks : int64 list;
  sent_when_first_ack : int;
  total_batches_sent : int;
  bodies_intact : bool; (* server would reject corrupt bodies; see note below *)
}

let run_client () : result Lwt.t =
  let open Lwt.Syntax in
  let* addrs =
    Lwt_unix.getaddrinfo "127.0.0.1" (string_of_int !port)
      [ Unix.(AI_SOCKTYPE SOCK_STREAM) ]
  in
  let* sockaddr =
    match addrs with
    | ai :: _ -> Lwt.return ai.Unix.ai_addr
    | [] -> Lwt.fail_with "no addr"
  in
  let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let* () = Lwt_unix.connect fd sockaddr in
  let* connection =
    H2_lwt_unix.Client.create_connection ~error_handler:(fun _ -> ()) fd
  in

  let sent = ref 0 in
  let acks = ref [] in
  let first_ack_at = ref (-1) in
  let bodies_ok = ref true in
  (* expected (len,sum) per offset, computed from what we send *)
  let expected = Hashtbl.create n_batches in

  let handler =
    Grpc_lwt.Client.Rpc.bidirectional_streaming ~f:(fun write responses ->
        let sender () =
          (* 1) schema message: flight_descriptor + data_header, no data_body. *)
          let schema =
            F.make_flight_data
              ~flight_descriptor:
                (F.make_flight_descriptor ~type_:F.Cmd
                   ~cmd:(Bytes.of_string "main.default.zerobus_spike") ~path:[] ())
              ~data_header:(Bytes.of_string "ARROW-SCHEMA")
              ~app_metadata:Bytes.empty ~data_body:Bytes.empty ()
          in
          write (Some (encode_flight_data schema));
          (* 2) N record-batch messages, offset in app_metadata. *)
          let rec loop i =
            if i >= n_batches then (write None; Lwt.return_unit)
            else begin
              let body = fake_ipc_body i in
              Hashtbl.replace expected (Int64.of_int i) (body_len_sum body);
              let fd_msg =
                F.make_flight_data
                  ~data_header:(Bytes.of_string "ARROW-BATCH-HDR")
                  ~app_metadata:(Bytes.of_string (string_of_int i))
                  ~data_body:body ()
              in
              write (Some (encode_flight_data fd_msg));
              incr sent;
              let* () = Lwt.pause () in
              loop (i + 1)
            end
          in
          loop 0
        in
        let receiver () =
          Lwt_stream.iter
            (fun raw ->
              let pr = decode_put_result raw in
              if !first_ack_at < 0 then first_ack_at := !sent;
              (* ack is "<offset>:<len>:<sum>" *)
              match String.split_on_char ':' (Bytes.to_string pr.F.app_metadata) with
              | [ o; l; s ] ->
                  let off = Int64.of_string o in
                  acks := off :: !acks;
                  (match Hashtbl.find_opt expected off with
                   | Some (elen, esum) ->
                       if int_of_string l <> elen || int_of_string s <> esum then
                         bodies_ok := false
                   | None -> bodies_ok := false)
              | _ -> bodies_ok := false)
            responses
        in
        let* (), () = Lwt.both (sender ()) (receiver ()) in
        Lwt.return_unit)
  in
  let do_request = H2_lwt_unix.Client.request connection ~error_handler:(fun _ -> ()) in
  let* res =
    Grpc_lwt.Client.call ~service:"arrow.flight.protocol.FlightService"
      ~rpc:"DoPut" ~scheme:"http" ~handler ~do_request ()
  in
  match res with
  | Ok ((), _status) ->
      Lwt.return
        {
          acks = List.rev !acks;
          sent_when_first_ack = !first_ack_at;
          total_batches_sent = !sent;
          bodies_intact = !bodies_ok;
        }
  | Error _ -> Lwt.fail_with "DoPut call failed"

(* -------- spawn server subprocess, await READY, run client -------- *)

let server_exe () =
  let dir = Filename.dirname Sys.executable_name in
  let cand = Filename.concat dir "flight_server.exe" in
  if Sys.file_exists cand then cand else "./flight_server.exe"

let with_server (f : unit -> 'a Lwt.t) : 'a Lwt.t =
  let exe = server_exe () in
  let stdout_r, stdout_w = Unix.pipe () in
  let pid = Unix.create_process exe [| exe; "0" |] Unix.stdin stdout_w Unix.stderr in
  Unix.close stdout_w;
  let ic = Unix.in_channel_of_descr stdout_r in
  (try
     match String.split_on_char ' ' (input_line ic) with
     | [ "READY"; p ] -> port := int_of_string p
     | _ -> failwith "bad READY line"
   with End_of_file -> failwith "server did not signal READY");
  Lwt.finalize f (fun () ->
      (try Unix.kill pid Sys.sigkill with _ -> ());
      (try ignore (Unix.waitpid [] pid) with _ -> ());
      (try close_in ic with _ -> ());
      Lwt.return_unit)

(* -------- evidence + assertions -------- *)

let result =
  lazy
    (Lwt_main.run
       (with_server (fun () ->
            let open Lwt.Syntax in
            let* r = run_client () in
            let mn = List.fold_left min Int64.max_int r.acks
            and mx = List.fold_left max Int64.min_int r.acks in
            let t = Unix.gmtime (Unix.time ()) in
            Printf.eprintf
              "ZEROBUS OCAML SPIKE (ARROW FLIGHT / DoPut) -- transport evidence\n\
               timestamp_utc          : %04d-%02d-%02dT%02d:%02d:%02dZ\n\
               ocaml_version          : %s\n\
               rpc                    : arrow.flight.protocol.FlightService/DoPut (bidi)\n\
               proto                  : real Flight FlightData/PutResult (data_body=1000)\n\
               arrow_library_linked   : NONE (data_body carried as opaque bytes)\n\
               batches_sent           : %d\n\
               acks_received          : %d\n\
               sends_done_at_first_ack: %d (of %d)  <- ack arrived mid-send\n\
               first_offset           : %Ld\n\
               last_offset            : %Ld\n\
               offsets_contiguous     : %b\n\
               opaque_data_body       : %s (server verified len+checksum of every batch body)\n%!"
              (t.tm_year + 1900) (t.tm_mon + 1) t.tm_mday t.tm_hour t.tm_min
              t.tm_sec Sys.ocaml_version r.total_batches_sent
              (List.length r.acks) r.sent_when_first_ack n_batches mn mx
              (Int64.sub mx mn = Int64.of_int (List.length r.acks - 1))
              (if r.bodies_intact then "INTACT" else "CORRUPTED");
            Lwt.return r)))

let () =
  ignore (Lazy.force result);
  Alcotest.run "arrow-flight-doput-spike"
    [
      ( "flight DoPut (bidi)",
        [
          Alcotest.test_case "every batch acked" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check int) "acked" n_batches (List.length r.acks);
              Alcotest.(check int) "sent" n_batches r.total_batches_sent);
          Alcotest.test_case "acks interleave with sends" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool) "first ack before all sends" true
                (r.sent_when_first_ack >= 0 && r.sent_when_first_ack < n_batches));
          Alcotest.test_case "offsets monotonic" `Slow (fun () ->
              let r = Lazy.force result in
              let rec mono = function
                | a :: (b :: _ as tl) -> Int64.compare a b < 0 && mono tl
                | _ -> true
              in
              Alcotest.(check bool) "increasing" true (mono r.acks));
          Alcotest.test_case "opaque data_body carried" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool) "bodies intact" true r.bodies_intact);
        ] );
    ]
