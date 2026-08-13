(** Runtime-agnostic core of the native OCaml Zerobus Ingest SDK.

    A peer implementation of the Zerobus gRPC ingest protocol — it speaks the
    wire protocol directly rather than binding the Rust core. Not useful on its
    own: the {!Io.IO} functor is instantiated by the per-runtime packages
    {b zerobus} (Lwt), {b zerobus-eio} (Eio), and {b zerobus-async} (Async). *)

module Wire = Zerobus_proto.Zerobus_service
(** The generated Zerobus gRPC wire types (messages, enums, [make_*] builders).
*)

module Duration = Zerobus_proto.Duration
(** The [google.protobuf.Duration] well-known type imported by the schema. *)

module Error = Error
(** The SDK error taxonomy (results-over-exceptions; retryable classification).
*)

module Grpc_transport = Grpc_transport
(** The abstract bidirectional gRPC transport the driver runs over (§6.5). *)

module Io = Io
(** The effect surface the whole SDK is functorized over (§6.2). *)

module Options = Options
(** Stream configuration, record types, and the §5 public value types. *)

module Config = Config
(** Configuration utilities: endpoint derivation and OAuth token-request
    building (DESIGN.md §5.2, §12.2). *)

module Stream = Stream
(** The streaming driver, its {!Stream.PROTOCOL} abstraction, the {!Stream.ack}
    type, and both [Make] (default [EphemeralStream]) and [Make_with_protocol]
    (for an alternative protocol, e.g. Arrow over Flight [DoPut]). *)

module Flight_protocol = Flight_protocol
(** The Arrow Flight [DoPut] {!Stream.PROTOCOL} (for [record_type = Arrow]).
    Pure protobuf — no libarrow; feed it to {!Stream.Make_with_protocol}. *)

module Make = Stream.Make
(** [Make (Io)] builds the streaming driver for a given runtime; the per-runtime
    packages apply it to their {!Io.IO} instantiation. *)
