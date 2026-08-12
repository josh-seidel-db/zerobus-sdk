(** Unit test for Zerobus_core.Flight_protocol.decode_ack defensiveness (#5).

    A Flight [PutResult] whose [app_metadata] carries no parseable offset (empty, a
    schema-accept, or a heartbeat) must NOT be treated as a fatal Protocol_error —
    that would kill the whole Arrow stream on the first such frame. It must decode
    to [Created] (keep-reading, no watermark advance). A well-formed ack still
    decodes to [Watermark], and genuinely undecodable bytes are still an error. *)

module FP = Zerobus_core.Flight_protocol
module F = Zerobus_proto.Flight

let put_result_with (meta : string) : string =
  let pr = F.make_put_result ~app_metadata:(Bytes.of_string meta) () in
  let e = Pbrt.Encoder.create () in
  F.encode_pb_put_result pr e;
  Pbrt.Encoder.to_string e

let check_created label meta =
  Alcotest.test_case label `Quick (fun () ->
      match FP.decode_ack (put_result_with meta) with
      | Ok Zerobus_core.Stream.Created -> ()
      | Ok (Zerobus_core.Stream.Watermark w) ->
          Alcotest.failf "%s: expected Created, got Watermark %Ld" label w
      | Ok (Zerobus_core.Stream.Closed _) ->
          Alcotest.failf "%s: expected Created, got Closed" label
      | Error e ->
          Alcotest.failf "%s: expected Created (keep-reading), got Error %s" label
            (Zerobus_core.Error.to_string e))

let check_watermark label meta expected =
  Alcotest.test_case label `Quick (fun () ->
      match FP.decode_ack (put_result_with meta) with
      | Ok (Zerobus_core.Stream.Watermark w) ->
          Alcotest.(check int64) label expected w
      | other ->
          ignore other;
          Alcotest.failf "%s: expected Watermark %Ld" label expected)

let () =
  Alcotest.run "flight-decode"
    [
      ( "metadata-less PutResult is not fatal",
        [
          check_created "empty-metadata" "";
          check_created "whitespace-metadata" "   ";
          check_created "non-offset-json" {|{"note":"schema accepted"}|};
          check_created "garbage-text" "not-an-offset";
          (* A real ack still advances the watermark. *)
          check_watermark "ack-up-to-offset-json" {|{"ack_up_to_offset":42}|} 42L;
          check_watermark "offset-id-json" {|{"offset_id":7}|} 7L;
          check_watermark "bare-int" "123" 123L;
        ] );
    ]
