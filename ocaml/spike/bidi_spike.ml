(** Transport-risk spike for the OCaml Zerobus SDK.

    Proves that the OCaml gRPC stack (grpc-lwt 0.2.0 + h2 0.12.0) can sustain a
    client-initiated *bidirectional* streaming RPC where the client pushes N
    records while concurrently draining acknowledgments -- the exact shape of the
    Zerobus ingest hot path (see ../DESIGN.md §8.7 and spike/README.md).

    Everything runs in-process over a real HTTP/2 loopback socket: no Databricks
    credentials, no network egress, safe for CI.

    Structure:
      - [Server]         : a mock ingest.Ingest/IngestStream bidi handler.
      - [Client]         : send-fiber + ack-drain-fiber (the real SDK driver
                           pattern in miniature), run under Lwt.both.
      - [Raw_h2_framing] : the fallback -- gRPC length-prefix framing that the
                           core transport would use if grpc-lwt's bidi client
                           were inadequate. Self-tested independently.
      - alcotest suite   : the go/no-go assertions.

    This file was written against the interfaces actually installed on this
    machine (verified by reading the .mli files), then compiled and run. *)

(* The generated protobuf module lives in the [ingest_proto] wrapper library. *)
module Ingest = Ingest_proto.Ingest

let n_records = 200
let port = 50551

(* ------------------------------------------------------------------ *)
(* Shared proto helpers (generated module is [Ingest]).                *)
(* ------------------------------------------------------------------ *)

let encode_request (r : Ingest.ingest_request) : string =
  let enc = Pbrt.Encoder.create () in
  Ingest.encode_pb_ingest_request r enc;
  Pbrt.Encoder.to_string enc

let decode_request (s : string) : Ingest.ingest_request =
  Ingest.decode_pb_ingest_request (Pbrt.Decoder.of_string s)

let encode_response (r : Ingest.ingest_response) : string =
  let enc = Pbrt.Encoder.create () in
  Ingest.encode_pb_ingest_response r enc;
  Pbrt.Encoder.to_string enc

let decode_response (s : string) : Ingest.ingest_response =
  Ingest.decode_pb_ingest_response (Pbrt.Decoder.of_string s)

(* ------------------------------------------------------------------ *)
(* Server: for each incoming request, emit one ack with a monotonic    *)
(* offset. grpc-lwt server bidi handler shape:                          *)
(*   string Lwt_stream.t -> (string -> unit) -> Grpc.Status.t Lwt.t     *)
(* ------------------------------------------------------------------ *)

module Server = struct
  let handle_ingest_stream (requests : string Lwt_stream.t)
      (send_response : string -> unit) : Grpc.Status.t Lwt.t =
    let open Lwt.Syntax in
    let offset = ref 0L in
    let* () =
      Lwt_stream.iter_s
        (fun raw_req ->
          let (_ : Ingest.ingest_request) = decode_request raw_req in
          let resp =
            Ingest.make_ingest_response ~offset:!offset ~durable:true ()
          in
          offset := Int64.add !offset 1L;
          send_response (encode_response resp);
          Lwt.return_unit)
        requests
    in
    Lwt.return (Grpc.Status.v Grpc.Status.OK)

  let grpc_server () =
    let service =
      Grpc_lwt.Server.Service.(
        v ()
        |> add_rpc ~name:"IngestStream"
             ~rpc:
               (Grpc_lwt.Server.Rpc.Bidirectional_streaming
                  handle_ingest_stream))
    in
    Grpc_lwt.Server.(
      v ()
      |> add_service ~name:"ingest.Ingest"
           ~service:(Grpc_lwt.Server.Service.handle_request service))

  (* Serve over h2 on loopback:[port]. Returns a shutdown thunk. *)
  let start () : (unit -> unit Lwt.t) Lwt.t =
    let open Lwt.Syntax in
    let server = grpc_server () in
    let request_handler _client_addr reqd =
      Grpc_lwt.Server.handle_request server reqd
    in
    let error_handler _client_addr ?request:_ _error start_response =
      let response_body = start_response H2.Headers.empty in
      H2.Body.Writer.close response_body
    in
    let listen_address = Unix.(ADDR_INET (inet_addr_loopback, port)) in
    let* server_socket =
      Lwt_io.establish_server_with_client_socket listen_address
        (fun client_addr socket ->
          H2_lwt_unix.Server.create_connection_handler ?config:None
            ~request_handler:(fun addr -> request_handler addr)
            ~error_handler:(fun addr -> error_handler addr)
            client_addr socket)
    in
    Lwt.return (fun () -> Lwt_io.shutdown_server server_socket)
end

(* ------------------------------------------------------------------ *)
(* Client: the miniature of the real SDK driver.                       *)
(*   grpc-lwt client bidi handler shape:                                *)
(*     f:((string option -> unit) -> string Lwt_stream.t -> 'a Lwt.t)   *)
(*   where write (Some s) sends a message and write None half-closes.   *)
(* ------------------------------------------------------------------ *)

module Client = struct
  type result = {
    acks : int64 list;          (* offsets, in arrival order *)
    sent_when_first_ack : int;  (* how many sends had completed at 1st ack *)
    total_sent : int;
  }

  let run () : result Lwt.t =
    let open Lwt.Syntax in
    (* connect a plain TCP socket to the loopback server *)
    let* addrs =
      Lwt_unix.getaddrinfo "127.0.0.1" (string_of_int port)
        [ Unix.(AI_FAMILY PF_INET) ]
    in
    (* pattern-match rather than List.hd (sister-SDK golden rule) *)
    let* sockaddr =
      match addrs with
      | { Unix.ai_addr; _ } :: _ -> Lwt.return ai_addr
      | [] -> Lwt.fail_with "no address for 127.0.0.1"
    in
    let socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    let* () = Lwt_unix.connect socket sockaddr in
    let error_handler _ = () in
    let* connection =
      H2_lwt_unix.Client.create_connection ~error_handler socket
    in

    let sent = ref 0 in
    let acks = ref [] in
    let first_ack_at = ref (-1) in

    let handler =
      Grpc_lwt.Client.Rpc.bidirectional_streaming
        ~f:(fun (write : string option -> unit) (responses : string Lwt_stream.t) ->
          (* send fiber: push N requests, then half-close with `None`. *)
          let sender () =
            let rec loop i =
              if i >= n_records then (
                write None;
                Lwt.return_unit)
              else begin
                let req =
                  Ingest.make_ingest_request
                    ~payload:(Bytes.of_string (Printf.sprintf "row-%d" i))
                    ~client_seq:(Int64.of_int i) ()
                in
                write (Some (encode_request req));
                incr sent;
                (* yield so the ack fiber can interleave *)
                let* () = Lwt.pause () in
                loop (i + 1)
              end
            in
            loop 0
          in
          (* ack-drain fiber: record offsets as they arrive. *)
          let receiver () =
            Lwt_stream.iter
              (fun raw_resp ->
                let resp = decode_response raw_resp in
                if !first_ack_at < 0 then first_ack_at := !sent;
                acks := resp.Ingest.offset :: !acks)
              responses
          in
          let* (), () = Lwt.both (sender ()) (receiver ()) in
          Lwt.return_unit)
    in

    let do_request = H2_lwt_unix.Client.request connection ~error_handler in
    let* res =
      Grpc_lwt.Client.call ~service:"ingest.Ingest" ~rpc:"IngestStream"
        ~scheme:"http" ~handler ~do_request ()
    in
    let* () = H2_lwt_unix.Client.shutdown connection in
    match res with
    | Ok ((), _status) ->
        Lwt.return
          {
            acks = List.rev !acks;
            sent_when_first_ack = !first_ack_at;
            total_sent = !sent;
          }
    | Error status ->
        Lwt.fail_with
          (Printf.sprintf "grpc call failed: h2 status %s"
             (H2.Status.to_string status))
end

(* ------------------------------------------------------------------ *)
(* Fallback: gRPC length-prefix framing over RAW h2.                   *)
(* Proven here so the workaround in README.md is code, not prose.      *)
(* ------------------------------------------------------------------ *)

module Raw_h2_framing = struct
  (* gRPC message framing: 1 byte compressed-flag (0) + 4 byte BE length +
     protobuf bytes. This is the entire on-the-wire codec. *)
  let frame (msg : string) : string =
    let len = String.length msg in
    let buf = Buffer.create (5 + len) in
    Buffer.add_char buf '\000';                       (* not compressed *)
    Buffer.add_char buf (Char.chr ((len lsr 24) land 0xff));
    Buffer.add_char buf (Char.chr ((len lsr 16) land 0xff));
    Buffer.add_char buf (Char.chr ((len lsr 8) land 0xff));
    Buffer.add_char buf (Char.chr (len land 0xff));
    Buffer.add_string buf msg;
    Buffer.contents buf

  (* Incremental deframer: feed bytes as they arrive off the h2 body, pull
     out complete messages. Returns (complete messages, leftover buffer). *)
  let deframe (acc : string) : string list * string =
    let rec loop acc msgs =
      if String.length acc < 5 then (List.rev msgs, acc)
      else begin
        let len =
          (Char.code acc.[1] lsl 24)
          lor (Char.code acc.[2] lsl 16)
          lor (Char.code acc.[3] lsl 8)
          lor Char.code acc.[4]
        in
        if String.length acc < 5 + len then (List.rev msgs, acc)
        else
          let msg = String.sub acc 5 len in
          loop
            (String.sub acc (5 + len) (String.length acc - 5 - len))
            (msg :: msgs)
      end
    in
    loop acc []

  (* The headers the real transport would send on the h2 request. Documented
     here to pin the contract. *)
  let request_headers ~table_name ~bearer =
    [
      (":method", "POST");
      (":scheme", "https");
      (":path", "/ingest.Ingest/IngestStream");
      ("content-type", "application/grpc+proto");
      ("te", "trailers");
      ("authorization", "Bearer " ^ bearer);
      ("x-databricks-zerobus-table-name", table_name);
    ]

  (* Round-trip self-test of the codec: frame then deframe must be identity,
     including a split-buffer case (bytes arriving in two chunks). *)
  let codec_roundtrips () : bool =
    let m1 =
      encode_request
        (Ingest.make_ingest_request ~payload:(Bytes.of_string "a")
           ~client_seq:1L ())
    in
    let m2 =
      encode_request
        (Ingest.make_ingest_request ~payload:(Bytes.of_string "bb")
           ~client_seq:2L ())
    in
    let wire = frame m1 ^ frame m2 in
    (* deliver in two arbitrary chunks to exercise the leftover path *)
    let chunk1 = String.sub wire 0 (String.length wire - 3) in
    let chunk2 = String.sub wire (String.length wire - 3) 3 in
    let msgs1, leftover = deframe chunk1 in
    let msgs2, leftover2 = deframe (leftover ^ chunk2) in
    let got = msgs1 @ msgs2 in
    let _ = request_headers ~table_name:"c.s.t" ~bearer:"x" in
    leftover2 = "" && got = [ m1; m2 ]
end

(* ------------------------------------------------------------------ *)
(* Assertions.                                                         *)
(* ------------------------------------------------------------------ *)

let with_server (f : unit -> 'a Lwt.t) : 'a Lwt.t =
  let open Lwt.Syntax in
  let* stop = Server.start () in
  Lwt.finalize f (fun () -> stop ())

(* Run the client once; memoize so the three assertions share one exchange
   (and one server), keeping the evidence a single reproducible run. Emit the
   raw measured facts to stderr, so the proof is the numbers themselves, not
   merely a green checkmark. (The canonical EVIDENCE.txt artifact is produced by
   capturing this stderr; see README.md.) *)
let result =
  lazy
    (let res = Lwt_main.run (with_server Client.run) in
     let min_off = List.fold_left min Int64.max_int res.Client.acks
     and max_off = List.fold_left max Int64.min_int res.Client.acks in
     Printf.eprintf
       "ZEROBUS OCAML SPIKE -- transport evidence\n\
        timestamp_utc          : %s\n\
        ocaml_version          : %s\n\
        records_sent           : %d\n\
        acks_received          : %d\n\
        sends_done_at_first_ack: %d (of %d)  <- proves ack arrived mid-send\n\
        first_offset           : %Ld\n\
        last_offset            : %Ld\n\
        offsets_contiguous     : %b\n%!"
       (let t = Unix.gmtime (Unix.time ()) in
        Printf.sprintf "%04d-%02d-%02dT%02d:%02d:%02dZ" (t.tm_year + 1900)
          (t.tm_mon + 1) t.tm_mday t.tm_hour t.tm_min t.tm_sec)
       Sys.ocaml_version res.Client.total_sent
       (List.length res.Client.acks)
       res.Client.sent_when_first_ack n_records min_off max_off
       (Int64.sub max_off min_off
       = Int64.of_int (List.length res.Client.acks - 1));
     res)

let test_all_acked () =
  let res = Lazy.force result in
  Alcotest.(check int) "every record acknowledged" n_records
    (List.length res.Client.acks);
  Alcotest.(check int) "every record sent" n_records res.Client.total_sent

let test_interleaved () =
  let res = Lazy.force result in
  (* The core property: at least one ack arrived BEFORE all sends finished.
     If the runtime serialized send-then-receive (the finding-#1 bug), the
     first ack would only land after all N sends -> this fails. *)
  Alcotest.(check bool) "first ack arrived before all sends completed" true
    (res.Client.sent_when_first_ack >= 0
    && res.Client.sent_when_first_ack < n_records)

let test_monotonic () =
  let res = Lazy.force result in
  let rec mono = function
    | a :: (b :: _ as tl) -> Int64.compare a b < 0 && mono tl
    | _ -> true
  in
  Alcotest.(check bool) "ack offsets strictly increasing" true
    (mono res.Client.acks)

let test_fallback_codec () =
  Alcotest.(check bool)
    "raw-h2 gRPC framing round-trips (incl. split buffers)" true
    (Raw_h2_framing.codec_roundtrips ())

let () =
  (* Force the exchange first, so the raw evidence reaches real stderr / the
     file rather than being captured into Alcotest's per-test log. *)
  ignore (Lazy.force result);
  Alcotest.run "bidi-transport-spike"
    [
      ( "bidi streaming",
        [
          Alcotest.test_case "all records acked" `Slow test_all_acked;
          Alcotest.test_case "acks interleave with sends" `Slow test_interleaved;
          Alcotest.test_case "offsets are monotonic" `Slow test_monotonic;
        ] );
      ( "fallback framing",
        [
          Alcotest.test_case "raw-h2 gRPC framing round-trips" `Quick
            test_fallback_codec;
        ] );
    ]
