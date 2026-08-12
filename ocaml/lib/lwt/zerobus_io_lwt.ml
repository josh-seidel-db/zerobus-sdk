(** Lwt instantiation of {!Zerobus_core.Io.IO} — the reference runtime.

    The monad primitives map directly onto Lwt. The interesting part is
    {!H2_client}: [grpc-lwt] inverts control — [Grpc_lwt.Client.call] invokes a
    handler [f write responses] where [write] pushes requests and [responses] is
    an [Lwt_stream]. The driver ({!Zerobus_core.Make}) wants the opposite: an
    imperative [send]/[close_send]/[recv]. We bridge by running the call as a
    background promise, handing [write] and the response stream back through
    [Lwt_mvar]s; [recv] then pulls straight from the [Lwt_stream]. This is the
    reusable core the real streaming driver builds on. Phase 2 wires cleartext h2c
    (loopback mock); TLS+ALPN (proven in [spike-live/]) layers on in Phase 3. *)

module Error = Zerobus_core.Error

let ( let* ) = Lwt.bind

type 'a t = 'a Lwt.t
type 'a io = 'a t

let return = Lwt.return
let bind = Lwt.bind
let map t f = Lwt.map f t

module Scope = struct
  (* A registry of forked daemons. [with_scope] cancels and joins them when the
     body finishes (or raises) — the structured-concurrency boundary. *)
  type t = { mutable daemons : unit Lwt.t list }

  let with_scope (f : t -> 'a io) : 'a io =
    let scope = { daemons = [] } in
    Lwt.finalize
      (fun () -> f scope)
      (fun () ->
        List.iter Lwt.cancel scope.daemons;
        Lwt.join
          (List.map
             (fun d -> Lwt.catch (fun () -> d) (fun _ -> Lwt.return_unit))
             scope.daemons))

  let register (scope : t) (d : unit Lwt.t) = scope.daemons <- d :: scope.daemons
end

let both (a : unit -> 'a t) (b : unit -> 'b t) : ('a * 'b) t =
  Lwt.both (a ()) (b ())

(* First-wins race: Lwt.pick returns the first promise to resolve and CANCELS the
   others — exactly the abandon-the-loser semantics {!Io.IO.first} wants. *)
let first (a : unit -> 'a t) (b : unit -> 'a t) : 'a t =
  Lwt.pick [ a (); b () ]

let fork_daemon (scope : Scope.t) (f : unit -> unit t) : unit =
  Scope.register scope (Lwt.catch f (fun _ -> Lwt.return_unit))

module Mutex = struct
  type t = Lwt_mutex.t

  let create = Lwt_mutex.create
  let with_lock t f = Lwt_mutex.with_lock t f
end

module Mailbox = struct
  (* Bounded blocking mailbox: producers block when full, consumers block when
     empty, [take] yields [None] once closed and drained. *)
  type 'a t = {
    q : 'a Queue.t;
    capacity : int;
    mutable closed : bool;
    non_empty : unit Lwt_condition.t;
    non_full : unit Lwt_condition.t;
  }

  let create ~capacity =
    {
      q = Queue.create ();
      capacity;
      closed = false;
      non_empty = Lwt_condition.create ();
      non_full = Lwt_condition.create ();
    }

  let rec put t x =
    if t.closed then Lwt.return_unit
    else if Queue.length t.q >= t.capacity then
      let* () = Lwt_condition.wait t.non_full in
      put t x
    else begin
      Queue.push x t.q;
      Lwt_condition.signal t.non_empty ();
      Lwt.return_unit
    end

  let rec take t =
    if not (Queue.is_empty t.q) then begin
      let x = Queue.pop t.q in
      Lwt_condition.signal t.non_full ();
      Lwt.return (Some x)
    end
    else if t.closed then Lwt.return None
    else
      let* () = Lwt_condition.wait t.non_empty in
      take t

  let close t =
    t.closed <- true;
    Lwt_condition.broadcast t.non_empty ();
    Lwt_condition.broadcast t.non_full ()
end

(* gRPC length-prefix framing over raw h2 (1 compressed-flag byte + 4-byte BE
   length + payload). We drive h2 directly and FLUSH each frame because grpc-lwt
   0.2.0's client sender omits the flush (§6.5 / gate B finding): against a real
   remote server that waits for the request, an unflushed write stalls the RPC. *)
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

module H2_client : Zerobus_core.Grpc_transport.S with type 'a io = 'a t = struct
  type 'a io = 'a t

  (* GADT to hold either a plain or TLS H2 client. Both have the same methods;
     this allows connection to be polymorphic over the underlying H2 type. *)
  type h2_client_wrapper =
    | Plain of H2_lwt_unix.Client.t
    | Tls of H2_lwt_unix.Client.TLS.t

  type connection = {
    h2 : h2_client_wrapper;
    conn_headers : (string * string) list;
  }

  (* A raw-h2 bidi call: the request body writer, a mailbox of deframed response
     messages, and the trailer/status. We drive h2 directly (not grpc-lwt's
     client) so we can flush each frame. *)
  type call = {
    body : H2.Body.Writer.t;
    responses : string Mailbox.t;  (* deframed gRPC messages *)
    mutable grpc_status : (int * string) option;  (* from trailers *)
    mutable send_closed : bool;
  }

  let connect ~host ~port ?(tls = true) ?(headers = []) () :
      (connection, Error.t) result io =
    (* TLS is an explicit flag (default on), never inferred from the port, so a
       bearer token is never sent over cleartext by accident. *)
    let* addrs =
      Lwt_unix.getaddrinfo host (string_of_int port)
        [ Unix.(AI_SOCKTYPE SOCK_STREAM) ]
    in
    match addrs with
    | [] ->
        Lwt.return
          (Error (Error.Transport_error (Printf.sprintf "no address for %s" host)))
    | ai :: _ ->
        let sockaddr = ai.Unix.ai_addr in
        (* Derive the socket family from the resolved address (IPv4/IPv6) rather
           than hardcoding PF_INET, so IPv6/dual-stack hosts connect. *)
        let fd = Lwt_unix.socket ai.Unix.ai_family ai.Unix.ai_socktype 0 in
        (* Close the fd on any failure after this point (no leak on the recovery
           reconnect loop). *)
        let close_fd () = Lwt.catch (fun () -> Lwt_unix.close fd) (fun _ -> Lwt.return_unit) in
        Lwt.catch
          (fun () ->
            let* () = Lwt_unix.connect fd sockaddr in
            let* h2_wrapper =
              if tls then begin
                (* TLS 1.3 + ALPN h2 (ported from spike-live/tls_alpn.ml §12.1):
                   system trust anchors + peer-name verification. *)
                let authenticator =
                  match Ca_certs.authenticator () with
                  | Ok a -> a
                  | Error (`Msg m) -> failwith ("ca-certs: " ^ m)
                in
                let peer_name =
                  match Domain_name.of_string host with
                  | Ok d -> ( match Domain_name.host d with Ok h -> Some h | Error _ -> None)
                  | Error _ -> None
                in
                let cfg =
                  Tls.Config.client ~authenticator ?peer_name
                    ~alpn_protocols:[ "h2" ] ()
                in
                let* tls_conn = Tls_lwt.Unix.client_of_fd cfg fd in
                (* Fail fast unless ALPN negotiated h2 (mandatory for gRPC). *)
                let epoch =
                  match Tls_lwt.Unix.epoch tls_conn with
                  | Ok e -> e
                  | Error () -> failwith "no TLS epoch (ALPN check failed)"
                in
                match epoch.Tls.Core.alpn_protocol with
                | Some "h2" ->
                    let* h2_tls =
                      H2_lwt_unix.Client.TLS.create_connection
                        ~error_handler:(fun _ -> ()) tls_conn
                    in
                    Lwt.return (Tls h2_tls)
                | Some other ->
                    Lwt.fail_with
                      (Printf.sprintf "ALPN != h2 (got %s)" other)
                | None -> Lwt.fail_with "ALPN not negotiated (no h2)"
              end
              else
                (* Cleartext h2c for loopback test mocks. *)
                let* h2_plain =
                  H2_lwt_unix.Client.create_connection ~error_handler:(fun _ -> ()) fd
                in
                Lwt.return (Plain h2_plain)
            in
            Lwt.return (Ok { h2 = h2_wrapper; conn_headers = headers }))
          (fun exn ->
            (* close the fd so failed connects/handshakes don't leak descriptors *)
            let* () = close_fd () in
            Lwt.return
              (Error (Error.Transport_error (Printexc.to_string exn))))

  let start_bidi (conn : connection) ~service ~rpc ?(headers = []) () :
      (call, Error.t) result io =
    let scheme = match conn.h2 with Tls _ -> "https" | Plain _ -> "http" in
    let authority =
      (* :authority is required by gRPC servers (gate B finding). Prefer an
         explicit header, else fall back to the connection's host if provided. *)
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
      H2.Request.create ~scheme `POST
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
        (* Mailbox.put returns unit io; on Lwt it's a promise — fire it. *)
        List.iter (fun m -> Lwt.async (fun () -> Mailbox.put responses m)) msgs;
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
      match conn.h2 with
      | Plain h2 ->
          H2_lwt_unix.Client.request h2 req ~error_handler:(fun _ ->
              Mailbox.close responses)
            ~trailers_handler ~response_handler
      | Tls h2 ->
          H2_lwt_unix.Client.TLS.request h2 req ~error_handler:(fun _ ->
              Mailbox.close responses)
            ~trailers_handler ~response_handler
    in
    let call = { body; responses; grpc_status = None; send_closed = false } in
    call_ref := Some call;
    Lwt.return (Ok call)

  (* Write one framed message and flush it (the §6.5 fix). *)
  let send (call : call) (msg : string) : (unit, Error.t) result io =
    if call.send_closed then
      Lwt.return (Error (Error.Stream_error "send after close"))
    else begin
      H2.Body.Writer.write_string call.body (grpc_frame msg);
      let flushed, u = Lwt.wait () in
      H2.Body.Writer.flush call.body (fun () ->
          if Lwt.is_sleeping flushed then Lwt.wakeup_later u ());
      let* () = flushed in
      Lwt.return (Ok ())
    end

  let close_send (call : call) : (unit, Error.t) result io =
    if call.send_closed then Lwt.return (Ok ())
    else begin
      call.send_closed <- true;
      H2.Body.Writer.close call.body;
      Lwt.return (Ok ())
    end

  let recv (call : call) : (string option, Error.t) result io =
    let* v = Mailbox.take call.responses in
    Lwt.return (Ok v)

  let status (call : call) : (unit, Error.t) result io =
    match call.grpc_status with
    | None | Some (0, _) -> Lwt.return (Ok ())
    | Some (code, message) ->
        Lwt.return (Error (Error.Server_status { code; message }))

  let shutdown (conn : connection) : unit io =
    Lwt.catch
      (fun () ->
        match conn.h2 with
        | Plain h2 -> H2_lwt_unix.Client.shutdown h2
        | Tls h2 -> H2_lwt_unix.Client.TLS.shutdown h2)
      (fun _ -> Lwt.return_unit)
end

let sleep = Lwt_unix.sleep

(* The Lwt {!Zerobus_core.Io.IO} instantiation, named so it can be applied to both
   the default driver and the Arrow/Flight driver below. *)
module Io_impl = struct
  type 'a t = 'a Lwt.t
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

(* Apply the core Make functor to this Lwt instantiation, exposing the stream driver
   (default [EphemeralStream] protocol — JSON/Proto). *)
module Stream = Zerobus_core.Make (Io_impl)

(* The Arrow/Flight driver: same Lwt IO, but the Flight [DoPut] protocol. Used by
   the façade when [record_type = Arrow]. Needs no libarrow (Flight_protocol carries
   the IPC bytes opaquely; the caller supplies them via the zerobus-arrow codec). *)
module Stream_flight =
  Zerobus_core.Stream.Make_with_protocol (Io_impl) (Zerobus_core.Flight_protocol)

(** A stream_handle is the opaque type returned by {!Stream.open_stream}. *)
type stream_handle = Stream.stream
