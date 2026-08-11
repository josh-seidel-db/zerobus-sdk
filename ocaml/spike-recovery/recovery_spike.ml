(** Recovery spike for the OCaml Zerobus SDK — the last unproven transport
    mechanic (PLAN.md Phase 6, DESIGN.md §12.3).

    Proves that when a bidi gRPC ingest stream breaks mid-stream, the client:
      1. detects the non-OK stream end (server returns a RETRYABLE status),
      2. reconnects with exponential backoff (bounded by a wall-clock budget),
      3. replays only the UN-ACKED tail of records, in order,
    and that the end state is: every record acked exactly once, offsets
    contiguous, no duplicates.

    Topology: the mock server runs in a SEPARATE PROCESS (the proven pattern from
    spike-flight / spike-eio / spike-async), spawned here; we read its `READY
    <port>` line, then drive the recovery client. The server
    (`ingest_server_recovery.ml`) acks by the record's own client_seq and breaks
    the first stream after 50 acks with status Unavailable.

    Runs on the Lwt switch (fl414), where bidi is green (spike/). *)

module Ingest = Ingest_proto.Ingest

let n_records = 200
let port = ref 0

let encode_request (r : Ingest.ingest_request) : string =
  let enc = Pbrt.Encoder.create () in
  Ingest.encode_pb_ingest_request r enc;
  Pbrt.Encoder.to_string enc

let decode_response (s : string) : Ingest.ingest_response =
  Ingest.decode_pb_ingest_response (Pbrt.Decoder.of_string s)

(* ------------------------------------------------------------------ *)
(* Retry: exponential backoff + jitter + wall-clock budget.           *)
(* Mirrors the sister SDK's Retry (databricks-sdk-ocaml lib/retry.ml); *)
(* production reuses that module rather than this local copy.          *)
(* ------------------------------------------------------------------ *)

let with_retry ?(backoff_ms = 100.) ?(max_backoff_ms = 2000.)
    ?(budget_ms = 30000.) (f : unit -> 'a Lwt.t) : 'a Lwt.t =
  let open Lwt.Syntax in
  let start = Unix.gettimeofday () in
  let rec loop delay =
    Lwt.catch f (fun exn ->
        let elapsed_ms = (Unix.gettimeofday () -. start) *. 1000. in
        if elapsed_ms > budget_ms then Lwt.fail exn
        else
          (* full jitter over [0, delay] *)
          let jittered = Random.float delay in
          let* () = Lwt_unix.sleep (jittered /. 1000.) in
          loop (Float.min (delay *. 2.) max_backoff_ms))
  in
  loop backoff_ms

(* Raised when a stream ends with a retryable (non-OK) status — triggers a retry. *)
exception Stream_broke

(* ------------------------------------------------------------------ *)
(* Client: one bidi stream attempt over a fresh connection.           *)
(* Sends exactly [pending] (the un-acked record indices), drains acks  *)
(* into [on_ack]. Raises [Stream_broke] if the stream ends non-OK.     *)
(* ------------------------------------------------------------------ *)

let run_stream_once ~(pending : int list) ~(on_ack : int64 -> unit) : unit Lwt.t
    =
  let open Lwt.Syntax in
  let* addrs =
    Lwt_unix.getaddrinfo "127.0.0.1" (string_of_int !port)
      [ Unix.(AI_SOCKTYPE SOCK_STREAM) ]
  in
  let* sockaddr =
    match addrs with
    | ai :: _ -> Lwt.return ai.Unix.ai_addr
    | [] -> Lwt.fail_with "no address for 127.0.0.1"
  in
  let socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let* () = Lwt_unix.connect socket sockaddr in
  let* connection =
    H2_lwt_unix.Client.create_connection ~error_handler:(fun _ -> ()) socket
  in
  let handler =
    Grpc_lwt.Client.Rpc.bidirectional_streaming ~f:(fun write responses ->
        let sender () =
          let* () =
            Lwt_list.iter_s
              (fun idx ->
                let req =
                  Ingest.make_ingest_request
                    ~payload:(Bytes.of_string (Printf.sprintf "row-%d" idx))
                    ~client_seq:(Int64.of_int idx) ()
                in
                write (Some (encode_request req));
                Lwt.pause ())
              pending
          in
          write None;
          Lwt.return_unit
        in
        let receiver () =
          Lwt_stream.iter
            (fun raw -> on_ack (decode_response raw).Ingest.offset)
            responses
        in
        let* (), () = Lwt.both (sender ()) (receiver ()) in
        Lwt.return_unit)
  in
  let do_request = H2_lwt_unix.Client.request connection ~error_handler:(fun _ -> ()) in
  let* res =
    Grpc_lwt.Client.call ~service:"ingest.Ingest" ~rpc:"IngestStream"
      ~scheme:"http" ~handler ~do_request ()
  in
  let* () = Lwt.catch
      (fun () -> H2_lwt_unix.Client.shutdown connection)
      (fun _ -> Lwt.return_unit)
  in
  match res with
  | Ok ((), status) ->
      (* A bidi stream that the server aborts still returns Ok here, but with a
         non-OK gRPC status in the trailers. Treat non-OK as a retryable break. *)
      if Grpc.Status.code status = Grpc.Status.OK then Lwt.return_unit
      else Lwt.fail Stream_broke
  | Error _ -> Lwt.fail Stream_broke

(* ------------------------------------------------------------------ *)
(* Recovery driver: replay the un-acked tail across reconnects.       *)
(* ------------------------------------------------------------------ *)

type result = {
  acks_in_order : int64 list; (* every ack seen, arrival order, across streams *)
  reconnects : int; (* number of stream attempts beyond the first *)
  all_offsets_acked : bool;
  no_duplicates : bool;
  offsets_contiguous : bool;
}

let run_client () : result Lwt.t =
  let open Lwt.Syntax in
  let acked = Array.make n_records false in
  let all_acks = ref [] in
  let attempts = ref 0 in

  let pending () =
    let rec build i acc =
      if i < 0 then acc
      else build (i - 1) (if acked.(i) then acc else i :: acc)
    in
    build (n_records - 1) []
  in

  let on_ack off =
    all_acks := off :: !all_acks;
    let i = Int64.to_int off in
    if i >= 0 && i < n_records then acked.(i) <- true
  in

  (* One "attempt" = one stream over a fresh connection. We retry (with backoff)
     until no records remain un-acked. Each attempt sends only the current
     un-acked tail; a broken stream simply leaves its tail un-acked for the next. *)
  let rec drive () =
    match pending () with
    | [] -> Lwt.return_unit
    | tail ->
        incr attempts;
        let* () =
          Lwt.catch
            (fun () -> run_stream_once ~pending:tail ~on_ack)
            (function Stream_broke -> Lwt.return_unit | exn -> Lwt.fail exn)
        in
        (* If the stream broke before fully acking, retry with backoff. *)
        if pending () = [] then Lwt.return_unit
        else Lwt.fail Stream_broke
  in
  let* () = with_retry (fun () -> drive ()) in

  let all_present = Array.for_all (fun b -> b) acked in
  let acks = List.rev !all_acks in
  let no_dups =
    let counts = Hashtbl.create 256 in
    List.iter
      (fun o ->
        Hashtbl.replace counts o (1 + try Hashtbl.find counts o with Not_found -> 0))
      acks;
    Hashtbl.fold (fun _ c ok -> ok && c = 1) counts true
  in
  (* Distinct acked offsets, sorted, must be exactly 0..n-1. *)
  let distinct_sorted =
    List.sort_uniq Int64.compare acks
  in
  let contiguous =
    List.length distinct_sorted = n_records
    && List.for_all2
         (fun i o -> Int64.of_int i = o)
         (List.init n_records (fun i -> i))
         distinct_sorted
  in
  Lwt.return
    {
      acks_in_order = acks;
      reconnects = !attempts - 1;
      all_offsets_acked = all_present;
      no_duplicates = no_dups;
      offsets_contiguous = contiguous;
    }

(* ------------------------------------------------------------------ *)
(* Spawn the server subprocess, await READY, run the client.          *)
(* ------------------------------------------------------------------ *)

let server_exe () =
  let dir = Filename.dirname Sys.executable_name in
  let cand = Filename.concat dir "ingest_server_recovery.exe" in
  if Sys.file_exists cand then cand else "./ingest_server_recovery.exe"

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

(* ------------------------------------------------------------------ *)
(* Evidence + assertions.                                             *)
(* ------------------------------------------------------------------ *)

let result =
  lazy
    (Lwt_main.run
       (with_server (fun () ->
            let open Lwt.Syntax in
            let* r = run_client () in
            let t = Unix.gmtime (Unix.time ()) in
            Printf.eprintf
              "ZEROBUS OCAML RECOVERY SPIKE -- transport evidence\n\
               timestamp_utc          : %04d-%02d-%02dT%02d:%02d:%02dZ\n\
               ocaml_version          : %s\n\
               runtime                : Lwt (server in separate process)\n\
               records_target         : %d\n\
               reconnects             : %d  <- >=1 proves recovery triggered\n\
               acks_observed          : %d (incl. replays)\n\
               all_offsets_acked      : %b\n\
               no_duplicates          : %b\n\
               offsets_contiguous     : %b\n%!"
              (t.tm_year + 1900) (t.tm_mon + 1) t.tm_mday t.tm_hour t.tm_min
              t.tm_sec Sys.ocaml_version n_records r.reconnects
              (List.length r.acks_in_order) r.all_offsets_acked r.no_duplicates
              r.offsets_contiguous;
            Lwt.return r)))

let () =
  ignore (Lazy.force result);
  Alcotest.run "recovery-spike"
    [
      ( "recovery scenario",
        [
          Alcotest.test_case "recovery was triggered (>=1 reconnect)" `Slow
            (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool)
                "at least one reconnect" true (r.reconnects >= 1));
          Alcotest.test_case "all records ultimately acked" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool) "all acked" true r.all_offsets_acked);
          Alcotest.test_case "no duplicate acks" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool) "no dups" true r.no_duplicates);
          Alcotest.test_case "offsets contiguous 0..n-1" `Slow (fun () ->
              let r = Lazy.force result in
              Alcotest.(check bool) "contiguous" true r.offsets_contiguous);
        ] );
    ]
