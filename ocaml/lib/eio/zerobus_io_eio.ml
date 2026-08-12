(** Eio instantiation of {!Zerobus_core.Io.IO} — the direct-style runtime.

    This is where the two design-review correctness findings (§6.2) bite, and this
    file is the proof they were handled:

    - {b Direct-style effect.} Eio is not monadic: ['a t = 'a], [return x = x],
      [bind x f = f x]. A value of type ['a t] is therefore an {e already-computed}
      ['a].

    - {b Thunked combinators (finding #1).} Because ['a t] is eager, [both] must
      take [unit -> _] thunks and fork them as Eio fibers ([Fiber.both]) — if it
      took values, both sides would already have run, sequentially, defeating the
      send/ack concurrency. Results are captured through refs since [Fiber.both]
      returns [unit].

    - {b Scoped daemons (finding #2).} Eio forbids unscoped background fibers, so
      {!Scope} wraps an Eio [Switch.t] and [fork_daemon] uses [Fiber.fork_daemon
      ~sw]. [with_scope] runs a [Switch.run]; leaving it cancels every daemon.

    {b Transport (Phase 3/5).} The runtime-generic [connect] signature deliberately
    does not carry Eio's [Eio.Env] (network capability) or a [Switch.t] — but the
    Eio socket connect needs both. The direct-style {!Zerobus_eio} public façade
    supplies them: it runs inside [Eio_main.run] + [Switch.run] and installs the
    live [net]/[sw] into {!Ctx} before driving the core [Make] functor. [connect]
    then reads them from {!Ctx}. This mirrors the [env]/[~sw] threading of §6.2.

    Like the Lwt reference ({!Zerobus_io_lwt}), the transport drives {b raw h2} and
    {b flushes each frame}: grpc-eio 0.2.0's client sender omits the h2 flush
    (§6.5), which stalls a real server / deadlocks an in-process mock. It also sets
    the [:authority] pseudo-header (required by gRPC servers — gate B finding). *)

module Error = Zerobus_core.Error

type 'a t = 'a
type 'a io = 'a t

let return x = x
let bind x f = f x
let map x f = f x

module Scope = struct
  (* A scope owns an Eio Switch; daemons forked into it are cancelled when the
     switch finishes. We stash the current switch so fork_daemon can reach it. *)
  type t = { sw : Eio.Switch.t }

  let with_scope (f : t -> 'a io) : 'a io =
    Eio.Switch.run (fun sw -> f { sw })
end

let both (a : unit -> 'a t) (b : unit -> 'b t) : 'a * 'b =
  let ra = ref None and rb = ref None in
  Eio.Fiber.both (fun () -> ra := Some (a ())) (fun () -> rb := Some (b ()));
  match (!ra, !rb) with
  | Some x, Some y -> (x, y)
  | _ -> failwith "Fiber.both did not set both results"

(* First-wins race: Eio.Fiber.first runs both and returns the first to finish,
   CANCELLING the loser — the abandon-the-loser semantics {!Io.IO.first} wants.
   Direct style: the thunks return values directly. *)
let first (a : unit -> 'a t) (b : unit -> 'a t) : 'a t =
  Eio.Fiber.first a b

let fork_daemon (scope : Scope.t) (f : unit -> unit t) : unit =
  Eio.Fiber.fork_daemon ~sw:scope.Scope.sw (fun () ->
      (try f () with _ -> ());
      `Stop_daemon)

module Mutex = struct
  type t = Eio.Mutex.t

  let create () = Eio.Mutex.create ()
  let with_lock t f = Eio.Mutex.use_rw ~protect:true t f
end

module Mailbox = struct
  (* Eio's Stream is a bounded blocking queue but has no "close" that wakes a
     consumer already blocked in [take]. So the element type is ['a option] and
     [close] pushes a [None] sentinel: a blocked [take] is woken by it and returns
     [None]. [closed] additionally short-circuits [put] and repeated [take]s.
     (Capacity is +1 so [close] can always enqueue the sentinel without blocking.) *)
  type 'a t = {
    stream : 'a option Eio.Stream.t;
    mutable closed : bool;
  }

  let create ~capacity =
    { stream = Eio.Stream.create (capacity + 1); closed = false }

  let put t x = if t.closed then () else Eio.Stream.add t.stream (Some x)

  let take t =
    if t.closed then
      (* drain any buffered real values, then report closed *)
      match Eio.Stream.take_nonblocking t.stream with
      | Some (Some x) -> Some x
      | _ -> None
    else
      match Eio.Stream.take t.stream with
      | Some x -> Some x
      | None ->
          (* sentinel: leave one in the stream so concurrent takers also unblock *)
          Eio.Stream.add t.stream None;
          None

  let close t =
    if not t.closed then begin
      t.closed <- true;
      Eio.Stream.add t.stream None
    end
end

(* The live Eio capabilities the transport needs but the runtime-generic [connect]
   signature can't carry. The {!Zerobus_eio} façade installs these inside
   [Eio_main.run] + [Switch.run] (see [with_env]); [connect] reads them here. *)
module Ctx = struct
  (* [net] is the standard [Eio_unix] network capability — exactly what
     [Eio_main.run]'s env yields (and what the façade installs). The transport
     only uses it via [getaddrinfo_stream]/[connect]. [authenticator], when set,
     overrides the default system trust store ([Ca_certs]) for the TLS handshake —
     used by the self-signed TLS test (fingerprint pinning); [None] in production. *)
  type t = {
    net : [ `Unix | `Generic ] Eio.Net.ty Eio.Resource.t;
    sw : Eio.Switch.t;
    authenticator : X509.Authenticator.t option;
  }

  let current : t option ref = ref None

  let with_env ?authenticator ~net ~sw (f : unit -> 'a) : 'a =
    let saved = !current in
    current := Some { net; sw; authenticator };
    Fun.protect ~finally:(fun () -> current := saved) f

  let get () =
    match !current with
    | Some c -> c
    | None ->
        failwith
          "Zerobus_io_eio: no Eio env in scope — drive via the Zerobus_eio \
           façade (Ctx.with_env), which installs net/sw"
end

(* gRPC length-prefix framing over raw h2 (1 compressed-flag byte + 4-byte BE
   length + payload). Identical wire format to the Lwt reference; we drive h2
   directly and FLUSH each frame because grpc-eio 0.2.0's client sender omits the
   flush (§6.5): against a real server an unflushed write stalls the RPC. *)
let grpc_frame (msg : string) : string =
  let len = String.length msg in
  let b = Buffer.create (5 + len) in
  Buffer.add_char b '\000';
  Buffer.add_char b (Char.chr ((len lsr 24) land 0xff));
  Buffer.add_char b (Char.chr ((len lsr 16) land 0xff));
  Buffer.add_char b (Char.chr ((len lsr 8) land 0xff));
  Buffer.add_char b (Char.chr (len land 0xff));
  Buffer.add_string b msg;
  Buffer.contents b

let grpc_deframe (acc : string) : string list * string =
  let rec loop acc msgs =
    if String.length acc < 5 then (List.rev msgs, acc)
    else
      let len =
        (Char.code acc.[1] lsl 24) lor (Char.code acc.[2] lsl 16)
        lor (Char.code acc.[3] lsl 8) lor Char.code acc.[4]
      in
      if String.length acc < 5 + len then (List.rev msgs, acc)
      else
        loop (String.sub acc (5 + len) (String.length acc - 5 - len))
          (String.sub acc 5 len :: msgs)
  in
  loop acc []

(* Adapt a [Tls_eio.t] (an [Eio.Flow.two_way] + close) into an
   [Eio.Net.stream_socket]. [h2-eio]/[gluten-eio]'s [create_connection] is typed to
   want a [stream_socket], but its IO loop only ever calls [Eio.Flow.single_read] /
   [Eio.Flow.write] / [Eio.Flow.shutdown] / close — all of which a TLS flow
   provides. The [stream_socket] constraint is therefore stricter than the code
   needs; this shim re-exposes the flow under that type by delegating every method
   to the underlying flow. This is what lets us run real TLS+ALPN-h2 on Eio without
   a newer h2-eio. *)
let stream_socket_of_flow (flow : Tls_eio.t) :
    [ `Generic ] Eio.Net.stream_socket_ty Eio.Resource.t =
  let module S = struct
    type t = Tls_eio.t
    type tag = [ `Generic ]

    let single_read t buf = Eio.Flow.single_read t buf
    let read_methods = []
    let single_write t bufs = Eio.Flow.single_write t bufs
    let copy t ~src = Eio.Flow.copy src t
    let shutdown t cmd = Eio.Flow.shutdown t cmd
    let close t = Eio.Resource.close t
  end in
  let handler = Eio.Net.Pi.stream_socket (module S) in
  Eio.Resource.T (flow, handler)

module H2_client : Zerobus_core.Grpc_transport.S with type 'a io = 'a t = struct
  type 'a io = 'a t

  type connection = {
    h2 : H2_eio.Client.t;
    scheme : string;
    conn_headers : (string * string) list;
  }

  (* A raw-h2 bidi call: the request body writer, a mailbox of deframed response
     messages, and the trailer/status — same shape as the Lwt reference. *)
  type call = {
    body : H2.Body.Writer.t;
    responses : string Mailbox.t;
    mutable grpc_status : (int * string) option;
    mutable send_closed : bool;
  }

  let connect ~host ~port ?(tls = true) ?(headers = []) () :
      (connection, Error.t) result io =
    let { Ctx.net; sw; authenticator = auth_override } = Ctx.get () in
    try
      (* Resolve + connect a TCP socket via the façade-supplied net capability. *)
      let addrs = Eio.Net.getaddrinfo_stream ~service:(string_of_int port) net host in
      match addrs with
      | [] -> Error (Error.Transport_error (Printf.sprintf "no address for %s" host))
      | addr :: _ ->
          let socket = Eio.Net.connect ~sw net addr in
          if tls then begin
            (* Real TLS 1.3 + ALPN h2, mirroring the live-verified Lwt reference
               (spike-live / D1 gate B). tls-eio's [client_of_flow] returns an
               [Eio.Flow.two_way]; [stream_socket_of_flow] re-types it as the
               [stream_socket] that [H2_eio.Client.create_connection] wants (its IO
               loop uses only flow ops — see the shim). The RNG is seeded from the
               OS via [Mirage_crypto_rng_unix.use_default] (idempotent). *)
            Mirage_crypto_rng_unix.use_default ();
            let authenticator =
              match auth_override with
              | Some a -> a
              | None -> (
                  match Ca_certs.authenticator () with
                  | Ok a -> a
                  | Error (`Msg m) -> failwith ("ca-certs: " ^ m))
            in
            let peer_name =
              match Domain_name.of_string host with
              | Ok d -> ( match Domain_name.host d with Ok h -> Some h | Error _ -> None)
              | Error _ -> None
            in
            let cfg =
              match
                Tls.Config.client ~authenticator ~alpn_protocols:[ "h2" ] ()
              with
              | Ok c -> c
              | Error (`Msg m) -> failwith ("tls config: " ^ m)
            in
            let tls_flow = Tls_eio.client_of_flow cfg ?host:peer_name socket in
            (* Fail fast unless ALPN negotiated h2 (mandatory for gRPC). *)
            (match Tls_eio.epoch tls_flow with
             | Ok { Tls.Core.alpn_protocol = Some "h2"; _ } -> ()
             | Ok { Tls.Core.alpn_protocol = Some other; _ } ->
                 failwith (Printf.sprintf "ALPN != h2 (got %s)" other)
             | Ok { Tls.Core.alpn_protocol = None; _ } ->
                 failwith "ALPN not negotiated (no h2)"
             | Error () -> failwith "no TLS epoch (handshake/ALPN failed)");
            let h2 =
              H2_eio.Client.create_connection ~sw
                ~error_handler:(fun _ -> ()) (stream_socket_of_flow tls_flow)
            in
            Ok { h2; scheme = "https"; conn_headers = headers }
          end
          else
            (* Cleartext h2c for loopback test mocks. *)
            let h2 =
              H2_eio.Client.create_connection ~sw
                ~error_handler:(fun _ -> ()) socket
            in
            Ok { h2; scheme = "http"; conn_headers = headers }
    with exn -> Error (Error.Transport_error (Printexc.to_string exn))

  let start_bidi (conn : connection) ~service ~rpc ?(headers = []) () :
      (call, Error.t) result io =
    let authority =
      (* :authority is required by gRPC servers (gate B finding). *)
      match List.assoc_opt ":authority" (conn.conn_headers @ headers) with
      | Some a -> a
      | None -> (
          match List.assoc_opt "host" (conn.conn_headers @ headers) with
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
      List.filter
        (fun (k, _) -> not (List.mem k ours))
        (conn.conn_headers @ headers)
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
        List.iter (fun m -> Mailbox.put responses m) msgs;
        H2.Body.Reader.schedule_read body ~on_read ~on_eof
      and on_eof () = Mailbox.close responses in
      H2.Body.Reader.schedule_read body ~on_read ~on_eof
    in
    let trailers_handler (h : H2.Headers.t) =
      match (H2.Headers.get h "grpc-status", !call_ref) with
      | Some s, Some c ->
          let msg = Option.value (H2.Headers.get h "grpc-message") ~default:"" in
          c.grpc_status <- Some ((try int_of_string s with _ -> -1), msg)
      | _ -> ()
    in
    let body =
      H2_eio.Client.request conn.h2 req
        ~error_handler:(fun _ -> Mailbox.close responses)
        ~trailers_handler ~response_handler
    in
    let call = { body; responses; grpc_status = None; send_closed = false } in
    call_ref := Some call;
    Ok call

  (* Write one framed message and flush it (the §6.5 fix), awaiting the flush via
     an Eio promise so [send] returns only once the bytes have reached the wire. *)
  let send (call : call) (msg : string) : (unit, Error.t) result io =
    if call.send_closed then Error (Error.Stream_error "send after close")
    else begin
      H2.Body.Writer.write_string call.body (grpc_frame msg);
      let p, u = Eio.Promise.create () in
      H2.Body.Writer.flush call.body (fun () ->
          if not (Eio.Promise.is_resolved p) then Eio.Promise.resolve u ());
      Eio.Promise.await p;
      Ok ()
    end

  let close_send (call : call) : (unit, Error.t) result io =
    if call.send_closed then Ok ()
    else begin
      call.send_closed <- true;
      H2.Body.Writer.close call.body;
      Ok ()
    end

  let recv (call : call) : (string option, Error.t) result io =
    Ok (Mailbox.take call.responses)

  let status (call : call) : (unit, Error.t) result io =
    match call.grpc_status with
    | None | Some (0, _) -> Ok ()
    | Some (code, message) -> Error (Error.Server_status { code; message })

  let shutdown (conn : connection) : unit io =
    try ignore (Eio.Promise.await (H2_eio.Client.shutdown conn.h2)) with _ -> ()
end

let sleep secs = Eio_unix.sleep secs

(* Apply the core Make functor to this Eio instantiation, exposing the stream
   driver — the direct-style counterpart of {!Zerobus_io_lwt.Stream}. *)
(* The Eio {!Zerobus_core.Io.IO} instantiation (direct-style: ['a t = 'a]), named
   so it can drive both the default and the Arrow/Flight protocol. *)
module Io_impl = struct
  type 'a t = 'a
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

(* Default driver: [EphemeralStream] protocol (JSON/Proto). *)
module Stream = Zerobus_core.Make (Io_impl)

(* Arrow/Flight driver: same Eio IO, the Flight [DoPut] protocol. Used by the
   façade when [record_type = Arrow]; needs no libarrow (Flight_protocol carries
   the IPC bytes opaquely, the caller supplies them via the zerobus-arrow codec). *)
module Stream_flight =
  Zerobus_core.Stream.Make_with_protocol (Io_impl) (Zerobus_core.Flight_protocol)

type stream_handle = Stream.stream
