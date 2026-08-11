(** Runtime-agnostic core of the native OCaml Zerobus Ingest SDK.

    A peer implementation of the Zerobus gRPC ingest protocol — it speaks the wire
    protocol directly rather than binding the Rust core. Not useful on its own: the
    {!Io.IO} functor is instantiated by the per-runtime packages {b zerobus} (Lwt),
    {b zerobus-eio} (Eio), and {b zerobus-async} (Async). *)

(** The generated Zerobus gRPC wire types (messages, enums, [make_*] builders). *)
module Wire = Zerobus_proto.Zerobus_service

(** The [google.protobuf.Duration] well-known type imported by the schema. *)
module Duration = Zerobus_proto.Duration

(** The SDK error taxonomy (results-over-exceptions; retryable classification). *)
module Error = Error

(** The abstract bidirectional gRPC transport the driver runs over (§6.5). *)
module Grpc_transport = Grpc_transport

(** The effect surface the whole SDK is functorized over (§6.2). *)
module Io = Io

(** Stream configuration, record types, and the §5 public value types. *)
module Options = Options

(** Configuration utilities: endpoint derivation and OAuth token-request building
    (DESIGN.md §5.2, §12.2). *)
module Config = Config

(** The streaming driver, its {!Stream.PROTOCOL} abstraction, the {!Stream.ack}
    type, and both [Make] (default [EphemeralStream]) and [Make_with_protocol]
    (for an alternative protocol, e.g. Arrow over Flight [DoPut]). *)
module Stream = Stream

(** The Arrow Flight [DoPut] {!Stream.PROTOCOL} (for [record_type = Arrow]). Pure
    protobuf — no libarrow; feed it to {!Stream.Make_with_protocol}. *)
module Flight_protocol = Flight_protocol

(** [Make (Io)] builds the streaming driver for a given runtime; the per-runtime
    packages apply it to their {!Io.IO} instantiation. *)
module Make = Stream.Make
