(** Mock Zerobus [EphemeralStream] server that sends a graceful [CloseStreamSignal]
    mid-way on the FIRST stream, to exercise the driver's graceful-close-then-recover
    path ([stream_paused_max_wait_time_ms]). Separate process, cleartext h2c.

    Behavior:
    - On the FIRST bidi stream: reply to CreateIngestStream, ack records by their
      offset_id, but after [close_after_n] acked records send a [CloseStreamSignal]
      (with a short server duration) and then END THE BODY cleanly (Grpc.Status.OK).
      This is the graceful case (NOT a retryable error status): the driver should
      drain the acks it already sent, then reconnect+replay.
    - A process-global flag ensures only the first stream closes gracefully; the
      reconnected stream is served to completion (acks every replayed + new record).

    Acks carry [durability_ack_up_to_offset] = the record's own offset_id, so the
    driver's monotonic watermark advances across the reconnect. *)

module Z = Zerobus_proto.Zerobus_service

let close_after_n = 50
let already_closed = ref false

(* argv.(2) = "slow" keeps the body OPEN for [slow_hold_s] after sending the
   CloseStreamSignal (server duration advertised as 10s), so a finite
   stream_paused_max_wait_time_ms cap must fire BEFORE the body ends — proving the
   cap, not body-end, drives the reconnect. Default (no flag) ends the body at once. *)
let slow_mode =
  Array.length Sys.argv > 2 && String.equal Sys.argv.(2) "slow"
let slow_hold_s = 10.0

let decode_req (s : string) : Z.ephemeral_stream_request =
  Z.decode_pb_ephemeral_stream_request (Pbrt.Decoder.of_string s)

let encode_resp (r : Z.ephemeral_stream_response) : string =
  let e = Pbrt.Encoder.create () in
  Z.encode_pb_ephemeral_stream_response r e;
  Pbrt.Encoder.to_string e

(* Raised to end the handler (and thus the response body) at once, in fast mode. *)
exception Close_now

let handle (requests : string Lwt_stream.t) (respond : string -> unit) :
    Grpc.Status.t Lwt.t =
  let open Lwt.Syntax in
  let will_close = not !already_closed in
  let acked_this_conn = ref 0 in
  let* () =
    Lwt.catch
      (fun () ->
        Lwt_stream.iter_s
          (fun raw ->
            match decode_req raw with
            | Z.Create_stream _ ->
                respond
                  (encode_resp
                     (Z.Create_stream_response
                        (Z.make_create_ingest_stream_response
                           ~stream_id:"mock-close-0001" ())));
                Lwt.return_unit
            | Z.Ingest_record r ->
                if will_close && !acked_this_conn >= close_after_n then begin
                  already_closed := true;
                  (* Graceful close: advertise a close duration, then either HOLD the
                     body open past it (slow mode → a finite client cap must fire
                     first) or end the body at once (fast mode → client drains to
                     body-end). *)
                  let secs = if slow_mode then 10L else 1L in
                  let dur =
                    Zerobus_proto.Duration.make_duration ~seconds:secs ~nanos:0l ()
                  in
                  respond
                    (encode_resp
                       (Z.Close_stream_signal
                          (Z.make_close_stream_signal ~duration:dur ())));
                  if slow_mode then
                    (* Block the handler so the response body stays OPEN; the client's
                       finite cap should reconnect before this returns. *)
                    Lwt_unix.sleep slow_hold_s
                  else Lwt.fail Close_now
                end
                else begin
                  respond
                    (encode_resp
                       (Z.Ingest_record_response
                          (Z.make_ingest_record_response
                             ~durability_ack_up_to_offset:r.Z.offset_id ())));
                  incr acked_this_conn;
                  Lwt.return_unit
                end
            | Z.Ingest_record_batch _ -> Lwt.return_unit)
          requests)
      (function Close_now -> Lwt.return_unit | e -> Lwt.fail e)
  in
  (* Clean end-of-body (OK) — the graceful signal, not a transport error. *)
  Lwt.return (Grpc.Status.v Grpc.Status.OK)

let grpc_server () =
  let service =
    Grpc_lwt.Server.Service.(
      v ()
      |> add_rpc ~name:"EphemeralStream"
           ~rpc:(Grpc_lwt.Server.Rpc.Bidirectional_streaming handle)
      |> handle_request)
  in
  Grpc_lwt.Server.(
    v () |> add_service ~name:"databricks.zerobus.Zerobus" ~service)

let () =
  let requested =
    if Array.length Sys.argv > 1 then int_of_string Sys.argv.(1) else 0
  in
  Lwt_main.run
    (let open Lwt.Syntax in
     let server = grpc_server () in
     let listen_fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
     Lwt_unix.setsockopt listen_fd Unix.SO_REUSEADDR true;
     let* () =
       Lwt_unix.bind listen_fd Unix.(ADDR_INET (inet_addr_loopback, requested))
     in
     Lwt_unix.listen listen_fd 128;
     let actual_port =
       match Lwt_unix.getsockname listen_fd with
       | Unix.ADDR_INET (_, p) -> p
       | _ -> requested
     in
     let handler =
       H2_lwt_unix.Server.create_connection_handler ?config:None
         ~request_handler:(fun _ reqd -> Grpc_lwt.Server.handle_request server reqd)
         ~error_handler:(fun _ ?request:_ _ _ -> ())
     in
     Printf.printf "READY %d\n%!" actual_port;
     let rec accept_loop () =
       let* client_fd, client_addr = Lwt_unix.accept listen_fd in
       Lwt.async (fun () -> handler client_addr client_fd);
       accept_loop ()
     in
     accept_loop ())
