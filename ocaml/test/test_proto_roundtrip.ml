(* Phase 1 acceptance: the vendored Zerobus wire types encode to binary protobuf
   and decode back identically. This proves the D1-gate-A codegen produces usable
   types and that the whole EphemeralStream message family — including the oneof
   payloads and the imported google.protobuf.Duration — round-trips. *)

module Wire = Zerobus_core.Wire
module Duration = Zerobus_core.Duration

(* Encode with [enc], decode with [dec], return the decoded value. *)
let roundtrip enc dec v =
  let e = Pbrt.Encoder.create () in
  enc v e;
  dec (Pbrt.Decoder.of_string (Pbrt.Encoder.to_string e))

let test_create_stream_request () =
  let req =
    Wire.make_create_ingest_stream_request ~table_name:"main.iot.telemetry"
      ~descriptor_proto:(Bytes.of_string "\x0a\x04demo") ~record_type:Wire.Proto ()
  in
  let out =
    roundtrip Wire.encode_pb_create_ingest_stream_request
      Wire.decode_pb_create_ingest_stream_request req
  in
  Alcotest.(check string)
    "table_name" "main.iot.telemetry" out.Wire.table_name;
  Alcotest.(check bytes)
    "descriptor_proto" (Bytes.of_string "\x0a\x04demo") out.Wire.descriptor_proto;
  Alcotest.(check bool)
    "record_type=Proto" true (out.Wire.record_type = Wire.Proto)

(* The ingest hot path: a proto-encoded record wrapped in the EphemeralStream
   request oneof, with its offset id. *)
let test_ingest_record_proto () =
  let payload = Bytes.of_string "\x08\x2a\x12\x03abc" in
  let req =
    Wire.make_ingest_record_request ~offset_id:42L
      ~record:(Wire.Proto_encoded_record payload) ()
  in
  let msg = Wire.Ingest_record req in
  let out =
    roundtrip Wire.encode_pb_ephemeral_stream_request
      Wire.decode_pb_ephemeral_stream_request msg
  in
  match out with
  | Wire.Ingest_record r -> (
      Alcotest.(check int64) "offset_id" 42L r.Wire.offset_id;
      match r.Wire.record with
      | Some (Wire.Proto_encoded_record b) ->
          Alcotest.(check bytes) "proto payload" payload b
      | _ -> Alcotest.fail "expected Proto_encoded_record")
  | _ -> Alcotest.fail "expected Ingest_record payload"

(* JSON records travel through the same envelope. *)
let test_ingest_record_json () =
  let req =
    Wire.make_ingest_record_request ~offset_id:7L
      ~record:(Wire.Json_record {|{"device":"sensor-1","temp":21.5}|}) ()
  in
  let out =
    roundtrip Wire.encode_pb_ingest_record_request
      Wire.decode_pb_ingest_record_request req
  in
  Alcotest.(check int64) "offset_id" 7L out.Wire.offset_id;
  match out.Wire.record with
  | Some (Wire.Json_record s) ->
      Alcotest.(check string) "json" {|{"device":"sensor-1","temp":21.5}|} s
  | _ -> Alcotest.fail "expected Json_record"

(* The server's durability watermark. *)
let test_ack_response () =
  let ack = Wire.make_ingest_record_response ~durability_ack_up_to_offset:1000L () in
  let out =
    roundtrip Wire.encode_pb_ingest_record_response
      Wire.decode_pb_ingest_record_response ack
  in
  Alcotest.(check int64)
    "ack watermark" 1000L out.Wire.durability_ack_up_to_offset

(* CloseStreamSignal carries the imported google.protobuf.Duration — proves the
   cross-file import resolves and round-trips. *)
let test_close_signal_duration () =
  let dur = Duration.make_duration ~seconds:30L ~nanos:500l () in
  let sig_ = Wire.make_close_stream_signal ~duration:dur () in
  let out =
    roundtrip Wire.encode_pb_close_stream_signal
      Wire.decode_pb_close_stream_signal sig_
  in
  match out.Wire.duration with
  | Some d ->
      Alcotest.(check int64) "seconds" 30L d.Duration.seconds;
      Alcotest.(check int32) "nanos" 500l d.Duration.nanos
  | None -> Alcotest.fail "expected duration"

let () =
  Alcotest.run "zerobus-core proto"
    [
      ( "roundtrip",
        [
          Alcotest.test_case "create_stream_request" `Quick
            test_create_stream_request;
          Alcotest.test_case "ingest_record (proto)" `Quick
            test_ingest_record_proto;
          Alcotest.test_case "ingest_record (json)" `Quick
            test_ingest_record_json;
          Alcotest.test_case "ack response" `Quick test_ack_response;
          Alcotest.test_case "close signal duration" `Quick
            test_close_signal_duration;
        ] );
    ]
