(** Async (Jane Street) companion to spike/ (Lwt) and spike-eio/ (Eio).

    Proves the SAME three properties on grpc-async + h2-async, so zerobus-async
    is validated v1 scope:
      1. all 200 records acked,
      2. acks interleave with sends (concurrency, not send-then-receive),
      3. offsets strictly increasing.

    TOPOLOGY: server runs as a separate process (ingest_server_async.exe),
    matching the grpc-eio finding that in-process client+server on one scheduler
    is fragile; separate processes is the real-world shape anyway.

    Written against the installed grpc-async 0.2.0 / h2-async 0.12.0 .mli, then
    compiled and run. In-process concerns (TLS/OAuth/recovery/live service) are
    out of scope here, same as the other transport spikes. *)

open! Core
open! Async

module Ingest = Ingest_proto.Ingest

let n_records = 200
let port = ref 0

let encode_request (r : Ingest.ingest_request) : string =
  let enc = Pbrt.Encoder.create () in
  Ingest.encode_pb_ingest_request r enc;
  Pbrt.Encoder.to_string enc

let decode_response (s : string) : Ingest.ingest_response =
  Ingest.decode_pb_ingest_response (Pbrt.Decoder.of_string s)

type result = {
  acks : int list; (* offsets, arrival order *)
  sent_when_first_ack : int;
  total_sent : int;
}

let run_client () : result Deferred.t =
  let where =
    Tcp.Where_to_connect.of_host_and_port
      (Host_and_port.create ~host:"127.0.0.1" ~port:!port)
  in
  let%bind socket = Tcp.connect_sock where in
  let%bind connection =
    H2_async.Client.create_connection ~error_handler:(fun _ -> ()) socket
  in
  let sent = ref 0 in
  let acks = ref [] in
  let first_ack_at = ref (-1) in

  let handler =
    Grpc_async.Client.Rpc.bidirectional_streaming
      ~handler:(fun (writer : string Pipe.Writer.t) (responses : string Pipe.Reader.t) ->
        (* send: push N requests then close the writer pipe (half-close). *)
        let sender () =
          let%bind () =
            Deferred.repeat_until_finished 0 (fun i ->
                if i >= n_records then return (`Finished ())
                else begin
                  let req =
                    Ingest.make_ingest_request
                      ~payload:(Bytes.of_string (Printf.sprintf "row-%d" i))
                      ~client_seq:(Int64.of_int i) ()
                  in
                  let%bind () = Pipe.write writer (encode_request req) in
                  incr sent;
                  (* yield so the ack reader can interleave *)
                  let%bind () = Scheduler.yield () in
                  return (`Repeat (i + 1))
                end)
          in
          Pipe.close writer;
          return ()
        in
        (* receive: record offsets as they arrive. *)
        let receiver () =
          Pipe.iter responses ~f:(fun raw ->
              let resp = decode_response raw in
              if !first_ack_at < 0 then first_ack_at := !sent;
              acks := Int64.to_int_exn resp.Ingest.offset :: !acks;
              return ())
        in
        let%bind () = Deferred.all_unit [ sender (); receiver () ] in
        return ())
  in
  let do_request = H2_async.Client.request connection ~error_handler:(fun _ -> ()) in
  let%bind res =
    Grpc_async.Client.call ~service:"ingest.Ingest" ~rpc:"IngestStream"
      ~scheme:"http" ~handler ~do_request ()
  in
  let%bind () = H2_async.Client.shutdown connection in
  match res with
  | Ok ((), _status) ->
      return
        {
          acks = List.rev !acks;
          sent_when_first_ack = !first_ack_at;
          total_sent = !sent;
        }
  | Error _ -> failwith "grpc-async call failed"

(* -------- spawn server subprocess, await READY, run client -------- *)

let server_exe () =
  let dir = Filename.dirname (Sys.get_argv ()).(0) in
  let cand = Filename.concat dir "ingest_server_async.exe" in
  if Sys_unix.file_exists_exn cand then cand else "./ingest_server_async.exe"

let with_server (f : unit -> 'a Deferred.t) : 'a Deferred.t =
  let%bind process =
    Process.create_exn ~prog:(server_exe ()) ~args:[ "0" ] ()
  in
  let stdout_pipe = Reader.lines (Process.stdout process) in
  (* read the "READY <port>" line *)
  let%bind ready_line = Pipe.read stdout_pipe in
  (match ready_line with
   | `Ok line -> (
       match String.split ~on:' ' line with
       | [ "READY"; p ] -> port := Int.of_string p
       | _ -> failwithf "unexpected server line: %s" line ())
   | `Eof -> failwith "server closed before READY");
  Monitor.protect f ~finally:(fun () ->
      Process.send_signal process Signal.kill;
      let%bind _ = Process.wait process in
      return ())

(* -------- evidence + assertions -------- *)

let result_ivar : result option ref = ref None

let get_result () : result Deferred.t =
  match !result_ivar with
  | Some r -> return r
  | None ->
      let%bind r = with_server run_client in
      let offs = r.acks in
      let mn = List.fold offs ~init:Int.max_value ~f:Int.min in
      let mx = List.fold offs ~init:Int.min_value ~f:Int.max in
      let t = Unix.gmtime (Unix.time ()) in
      Core.eprintf
        "ZEROBUS OCAML SPIKE (ASYNC) -- transport evidence\n\
         timestamp_utc          : %04d-%02d-%02dT%02d:%02d:%02dZ\n\
         ocaml_version          : %s\n\
         runtime                : Async (server in separate process)\n\
         records_sent           : %d\n\
         acks_received          : %d\n\
         sends_done_at_first_ack: %d (of %d)  <- proves ack arrived mid-send\n\
         first_offset           : %d\n\
         last_offset            : %d\n\
         offsets_contiguous     : %b\n%!"
        (t.Unix.tm_year + 1900) (t.tm_mon + 1) t.tm_mday t.tm_hour t.tm_min
        t.tm_sec Sys.ocaml_version r.total_sent (List.length r.acks)
        r.sent_when_first_ack n_records mn mx
        (mx - mn = List.length r.acks - 1);
      result_ivar := Some r;
      return r

(* Run the whole exchange once, synchronously, before Alcotest. *)
let result : result =
  Thread_safe.block_on_async_exn (fun () -> get_result ())

let () =
  Alcotest.run "bidi-transport-spike-async"
    [
      ( "bidi streaming (async)",
        [
          Alcotest.test_case "all records acked" `Slow (fun () ->
              Alcotest.(check int) "acked" n_records (List.length result.acks);
              Alcotest.(check int) "sent" n_records result.total_sent);
          Alcotest.test_case "acks interleave with sends" `Slow (fun () ->
              Alcotest.(check bool) "first ack before all sends" true
                (result.sent_when_first_ack >= 0
                && result.sent_when_first_ack < n_records));
          Alcotest.test_case "offsets are monotonic" `Slow (fun () ->
              let rec mono = function
                | a :: (b :: _ as tl) -> a < b && mono tl
                | _ -> true
              in
              Alcotest.(check bool) "increasing" true (mono result.acks));
        ] );
    ]
