(** Async instantiation of {!Zerobus_core.Io.IO}.

    Mirrors the Lwt shim ({!Zerobus_io_lwt}) on Jane Street's Async: [Deferred.t]
    is the effect; the [Mailbox] is a bounded [Pipe]; [send] frames+flushes each
    message on the h2 request body, [recv] drains deframed responses.

    {b Transport.} We drive the runtime-agnostic {!H2.Client_connection} core over
    an Async [Reader]/[Writer] duplex ourselves (see {!H2_pump}) — NOT
    gluten-async / h2-async, whose TLS backend is a build-time [select] shipping a
    dummy on our switches (and which does not compile against current tls). The
    duplex is a plain [Tcp.connect] for cleartext h2c (mocks) or a [tls-async]
    channel for live TLS 1.3 + ALPN h2 (see {!Tls_connect}, a dune [select] on the
    optional [tls-async] dep). Live-proven against the real Zerobus endpoint
    (spike-async-tls/); cleartext shape proven in [spike-async/]. *)

open! Core
open! Async
module Error = Zerobus_core.Error

type 'a t = 'a Deferred.t
type 'a io = 'a t

let return = Deferred.return
let bind t f = Deferred.bind t ~f
let map t f = Deferred.map t ~f

module Scope = struct
  (* Async has no hard fiber cancellation, so a daemon blocked in [Pipe.read]
     cannot be force-killed. [stop] is a broadcast the daemon can race against
     (see [fork_daemon], which wires it in); the streaming driver's ack-reader
     also terminates naturally when its response pipe ends. On scope exit we fill
     [stop] and give daemons a scheduler turn to observe it, but we do NOT block
     scope exit on a daemon that ignores [stop] — that would hang [with_scope]. *)
  type t = { stop : unit Ivar.t; mutable daemons : unit Deferred.t list }

  let with_scope (f : t -> 'a io) : 'a io =
    let scope = { stop = Ivar.create (); daemons = [] } in
    Monitor.protect
      (fun () -> f scope)
      ~finally:(fun () ->
        Ivar.fill_if_empty scope.stop ();
        (* Best-effort join, bounded so an uncancellable daemon can't hang exit. *)
        Deferred.any
          [ Deferred.all_unit scope.daemons;
            (* [Time_ns]/[Clock_ns] (not [Time_float]) so the transport compiles on
               BOTH async v0.16 (fl414, canonical) and async v0.15 — the latter is
               forced by the tls-async 0.17.0 in-tree live-TLS test switch, where
               [Time_float] is absent (see doc/arch/tls_async_status.md). *)
            Async.Clock_ns.after (Time_ns.Span.of_ms 0.) ])

  let register (scope : t) (d : unit Deferred.t) =
    scope.daemons <- d :: scope.daemons

  let stopped (scope : t) : unit Deferred.t = Ivar.read scope.stop
end

let both (a : unit -> 'a t) (b : unit -> 'b t) : ('a * 'b) t =
  Deferred.both (a ()) (b ())

(* First-wins race: Deferred.any returns the first branch to become determined.
   Async has NO hard fiber cancellation, so the losing Deferred is simply detached
   (same discipline as Scope / fork_daemon) — callers keep loser side effects
   harmless when abandoned, as {!Io.IO.first} documents. *)
let first (a : unit -> 'a t) (b : unit -> 'a t) : 'a t =
  Deferred.any [ a (); b () ]

let fork_daemon (scope : Scope.t) (f : unit -> unit t) : unit =
  (* Race the daemon body against the scope's stop signal so scope exit can
     unblock a cooperative daemon; an uncancellable one is abandoned (see Scope). *)
  let d =
    Deferred.any
      [ (Monitor.try_with ~run:`Now (fun () -> f ()) |> Deferred.ignore_m);
        Scope.stopped scope ]
  in
  Scope.register scope d

module Mutex = struct
  (* Async has no bare mutex; a Sequencer is a throttle of concurrency 1, which
     serializes the token-cache critical section exactly as with_lock needs. *)
  type t = unit Sequencer.t

  let create () = Sequencer.create ()
  let with_lock t f = Throttle.enqueue t f
end

module Mailbox = struct
  (* Bounded blocking mailbox over an Async Pipe (pushback gives the bound). *)
  type 'a t = { r : 'a Pipe.Reader.t; w : 'a Pipe.Writer.t }

  let create ~capacity =
    let r, w = Pipe.create () in
    Pipe.set_size_budget w capacity;
    { r; w }

  let put t x =
    if Pipe.is_closed t.w then Deferred.return () else Pipe.write t.w x

  let take t =
    Pipe.read t.r >>| function `Ok x -> Some x | `Eof -> None

  let close t = Pipe.close t.w
end

(* gRPC length-prefix framing over raw h2 (1 compressed-flag byte + 4-byte BE
   length + payload). Identical wire format to the Lwt/Eio references; we drive h2
   directly and FLUSH each frame because grpc's client sender omits the flush
   (§6.5): against a real server an unflushed write stalls the RPC. *)
let grpc_frame (msg : string) : string =
  let len = String.length msg in
  let b = Buffer.create (5 + len) in
  Buffer.add_char b '\000';
  Buffer.add_char b (Char.of_int_exn ((len lsr 24) land 0xff));
  Buffer.add_char b (Char.of_int_exn ((len lsr 16) land 0xff));
  Buffer.add_char b (Char.of_int_exn ((len lsr 8) land 0xff));
  Buffer.add_char b (Char.of_int_exn (len land 0xff));
  Buffer.add_string b msg;
  Buffer.contents b

let grpc_deframe (acc : string) : string list * string =
  let rec loop acc msgs =
    if String.length acc < 5 then (List.rev msgs, acc)
    else
      let len =
        (Char.to_int acc.[1] lsl 24) lor (Char.to_int acc.[2] lsl 16)
        lor (Char.to_int acc.[3] lsl 8) lor Char.to_int acc.[4]
      in
      if String.length acc < 5 + len then (List.rev msgs, acc)
      else
        loop (String.sub acc ~pos:(5 + len) ~len:(String.length acc - 5 - len))
          (String.sub acc ~pos:5 ~len :: msgs)
  in
  loop acc []

module H2_client : Zerobus_core.Grpc_transport.S with type 'a io = 'a t = struct
  type 'a io = 'a t

  (* The connection is a record of thunks over the pumped h2 core: [make_request]
     opens a stream on it, [shutdown_conn] tears down the core + the duplex. This
     keeps the transport identical for the cleartext and TLS paths (they differ
     only in how the underlying [Reader]/[Writer] duplex was obtained). *)
  type connection = {
    make_request :
      H2.Request.t ->
      error_handler:(H2.Client_connection.error_handler) ->
      response_handler:(H2.Client_connection.response_handler) ->
      trailers_handler:(H2.Client_connection.trailers_handler) ->
      H2.Body.Writer.t;
    shutdown_conn : unit -> unit Deferred.t;
    scheme : string;
    conn_headers : (string * string) list;
  }

  (* A raw-h2 bidi call: the request body writer, a mailbox of deframed response
     messages, and the trailer/status — same shape as the Lwt/Eio references. *)
  type call = {
    body : H2.Body.Writer.t;
    responses : string Mailbox.t;
    mutable grpc_status : (int * string) option;
    mutable send_closed : bool;
  }

  (* Build a [connection] from a cleartext Async [Reader]/[Writer] duplex + the h2
     core pumped over it ({!H2_pump}). Shared by the cleartext (h2c) and TLS paths;
     only how the duplex is obtained differs. This drives the runtime-agnostic
     [H2.Client_connection] ourselves — no gluten-async / h2-async — which is what
     lets Async do live TLS (via {!Tls_connect}) at all. *)
  let connection_of_duplex ~reader ~writer ~shutdown ~scheme ~headers :
      connection =
    let error_handler _ = () in
    let h2 =
      H2.Client_connection.create ?config:None ?push_handler:None ~error_handler ()
    in
    H2_pump.start h2 ~reader ~writer;
    {
      make_request =
        (fun req ~error_handler ~response_handler ~trailers_handler ->
          H2.Client_connection.request h2 req ~error_handler ~trailers_handler
            ~response_handler);
      shutdown_conn =
        (fun () ->
          H2.Client_connection.shutdown h2;
          shutdown ());
      scheme;
      conn_headers = headers;
    }

  let connect ~host ~port ?(tls = true) ?(headers = []) () :
      (connection, Error.t) result io =
    if tls then
      (* Live TLS 1.3 + ALPN h2 via [Tls_connect] (real when tls-async is present,
         else an honest error — a dune [select], see doc/arch/tls_async_status.md).
         The h2 core is then pumped over the resulting cleartext duplex. *)
      match%map Tls_connect.connect ~host ~port with
      | Error msg -> Error (Error.Transport_error msg)
      | Ok (reader, writer, shutdown) ->
          Ok (connection_of_duplex ~reader ~writer ~shutdown ~scheme:"https"
                ~headers)
    else
      (* Cleartext h2c for loopback mocks: a plain TCP duplex. *)
      let where =
        Tcp.Where_to_connect.of_host_and_port
          (Host_and_port.create ~host ~port)
      in
      Monitor.try_with ~run:`Now (fun () -> Tcp.connect where)
      >>| function
      | Error exn -> Error (Error.Transport_error (Exn.to_string exn))
      | Ok (_socket, reader, writer) ->
          let shutdown () =
            Writer.close writer >>= fun () -> Reader.close reader
          in
          Ok (connection_of_duplex ~reader ~writer ~shutdown ~scheme:"http"
                ~headers)

  let start_bidi (conn : connection) ~service ~rpc ?(headers = []) () :
      (call, Error.t) result io =
    let authority =
      (* :authority is required by gRPC servers (gate B finding). *)
      match
        List.Assoc.find (conn.conn_headers @ headers) ":authority" ~equal:String.equal
      with
      | Some a -> a
      | None -> (
          match
            List.Assoc.find (conn.conn_headers @ headers) "host" ~equal:String.equal
          with
          | Some h -> h
          | None -> "")
    in
    (* Strip the keys we set ourselves from the caller-supplied headers, so we
       never emit a DUPLICATE [:authority] (or other) pseudo/synthetic header —
       h2 rejects a repeated pseudo-header with a stream/protocol error, which
       manifests as a silently stalled RPC. [:authority] is already folded into
       [authority] above; [host] is a synthetic key the caller uses to hint it. *)
    let ours = [ ":authority"; "te"; "content-type"; "grpc-encoding"; "host" ] in
    let caller =
      List.filter (conn.conn_headers @ headers) ~f:(fun (k, _) ->
          not (List.mem ours k ~equal:String.equal))
    in
    let base_headers =
      [ (":authority", authority);
        ("te", "trailers");
        ("content-type", "application/grpc+proto");
        ("grpc-encoding", "identity") ]
      @ caller
    in
    let req =
      H2.Request.create ~scheme:conn.scheme `POST
        (Printf.sprintf "/%s/%s" service rpc)
        ~headers:(H2.Headers.of_list base_headers)
    in
    let responses = Mailbox.create ~capacity:4096 in
    let call_ref = ref None in
    let acc = ref "" in
    let response_handler (_resp : H2.Response.t) body =
      let rec on_read bs ~off ~len =
        acc := !acc ^ Bigstringaf.substring bs ~off ~len;
        let msgs, rest = grpc_deframe !acc in
        acc := rest;
        List.iter msgs ~f:(fun m -> don't_wait_for (Mailbox.put responses m));
        H2.Body.Reader.schedule_read body ~on_read ~on_eof
      and on_eof () = Mailbox.close responses in
      H2.Body.Reader.schedule_read body ~on_read ~on_eof
    in
    let trailers_handler (h : H2.Headers.t) =
      match (H2.Headers.get h "grpc-status", !call_ref) with
      | Some s, Some c ->
          let msg = Option.value (H2.Headers.get h "grpc-message") ~default:"" in
          c.grpc_status <- Some ((try Int.of_string s with _ -> -1), msg)
      | _ -> ()
    in
    let body =
      conn.make_request req
        ~error_handler:(fun _ -> Mailbox.close responses)
        ~response_handler ~trailers_handler
    in
    let call = { body; responses; grpc_status = None; send_closed = false } in
    call_ref := Some call;
    return (Ok call)

  (* Write one framed message and flush it (the §6.5 fix), awaiting the flush via
     an Ivar so [send] returns only once the bytes have reached the wire. *)
  let send (call : call) (msg : string) : (unit, Error.t) result io =
    if call.send_closed then return (Error (Error.Stream_error "send after close"))
    else begin
      H2.Body.Writer.write_string call.body (grpc_frame msg);
      let flushed = Ivar.create () in
      H2.Body.Writer.flush call.body (fun () -> Ivar.fill_if_empty flushed ());
      Ivar.read flushed >>| fun () -> Ok ()
    end

  let close_send (call : call) : (unit, Error.t) result io =
    if call.send_closed then return (Ok ())
    else begin
      call.send_closed <- true;
      H2.Body.Writer.close call.body;
      return (Ok ())
    end

  let recv (call : call) : (string option, Error.t) result io =
    Mailbox.take call.responses >>| fun v -> Ok v

  let status (call : call) : (unit, Error.t) result io =
    match call.grpc_status with
    | None | Some (0, _) -> return (Ok ())
    | Some (code, message) ->
        return (Error (Error.Server_status { code; message }))

  let shutdown (conn : connection) : unit io = conn.shutdown_conn ()
end

(* [Time_ns] (not [Time_float]) for v0.15/v0.16 portability — see the note in
   [Scope.with_scope] above. [after] here is [Async.Clock_ns.after]. *)
let sleep secs = Async.Clock_ns.after (Time_ns.Span.of_sec secs)

(* The Async {!Zerobus_core.Io.IO} instantiation, named (not inlined) so it can
   drive BOTH the default EphemeralStream driver and the Arrow/Flight driver —
   mirrors {!Zerobus_io_lwt.Io_impl} / {!Zerobus_io_eio.Io_impl}. *)
module Io_impl = struct
  type nonrec 'a t = 'a t
  type 'a io = 'a t
  let return = return
  let bind = bind
  let map = map
  module Scope = Scope
  let both = both
  let first = first
  let fork_daemon = fork_daemon
  module Mutex = Mutex
  module Mailbox = Mailbox
  module H2_client = H2_client
  let sleep = sleep
end

(* Default driver: the [EphemeralStream] protocol (JSON/Proto). Counterpart of
   {!Zerobus_io_lwt.Stream} / {!Zerobus_io_eio.Stream}. *)
module Stream = Zerobus_core.Make (Io_impl)

(* Arrow/Flight driver: same Async IO, the Flight [DoPut] protocol. Used by the
   façade when [record_type = Arrow]; needs no libarrow ([Flight_protocol] carries
   the IPC bytes opaquely, the caller supplies them via the zerobus-arrow codec) —
   mirrors {!Zerobus_io_lwt.Stream_flight} / {!Zerobus_io_eio.Stream_flight}. *)
module Stream_flight =
  Zerobus_core.Stream.Make_with_protocol (Io_impl) (Zerobus_core.Flight_protocol)

type stream_handle = Stream.stream
