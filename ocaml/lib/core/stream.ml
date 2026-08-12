(** The runtime-agnostic streaming driver (DESIGN.md §5, §6.4), functorized over
    {!Io.IO}. This is the real Phase 5 data plane: [create_stream], [ingest] /
    [ingest_records] (queue-only, never wait — the cardinal rule), [wait_for_offset],
    [flush], [close], plus the async [ack_callback]. It runs the send side and an
    ack-reader daemon concurrently inside the stream's {!Io.IO.Scope}, coordinating
    through the mailbox/condition primitives the functor provides.

    {b Protocol abstraction (Phase 7b).} The offset/ack/watermark/recovery control
    plane is wire-agnostic; only four operations are specific to the on-the-wire
    protocol — the RPC name, how a create/schema frame and a record frame are
    built, and how a response frame decodes to an ack watermark. Those live behind
    {!PROTOCOL}. The default {!Ephemeral} instance speaks the Zerobus
    [EphemeralStream] RPC (JSON/Proto, ack in [durability_ack_up_to_offset]); an
    Arrow instance can speak Flight [DoPut] (IPC bytes in [FlightData.data_body],
    ack in [PutResult.app_metadata]) by supplying a different {!PROTOCOL} to
    {!Make_with_protocol} — WITHOUT this module (or [lib/core]) depending on the
    Flight proto or libarrow. {!Make} = {!Make_with_protocol} over {!Ephemeral},
    so the existing single-arg [Zerobus_core.Make (Io)] API is unchanged.

    Recovery (§12.3) is Phase 6: reconnect-and-replay of the un-acked buffer. *)

module Wire = Zerobus_proto.Zerobus_service

(** What a decoded response frame means to the driver's control plane. *)
type ack =
  | Watermark of int64  (** durability watermark: all offsets <= n are durable *)
  | Created  (** stream/schema accepted; not an ack, keep reading *)
  | Closed of int
      (** server signalled it will close the stream in [n] ms (0 if the signal
          carried no duration). Honored via [stream_paused_max_wait_time_ms]. *)

(** The on-the-wire protocol the driver runs over. Everything else in the driver
    (offsets, watermark, waiters, recovery, replay buffer) is protocol-agnostic. *)
module type PROTOCOL = sig
  val service : string
  val rpc : string

  (** Build the framed create/schema request sent first (and re-sent on
      reconnect), from the table + options. *)
  val create_frame :
    table:Options.table_properties -> options:Options.stream_options -> string

  (** Build the framed record request for one record at [offset]. *)
  val record_frame :
    record_type:Options.record_type -> offset:int64 -> bytes -> string

  (** Decode a response frame into an {!ack}, or an error on a malformed frame. *)
  val decode_ack : string -> (ack, Error.t) result
end

(** The Zerobus [EphemeralStream] protocol (JSON/Proto). This is the default and
    keeps [lib/core] free of any Flight/Arrow dependency. *)
module Ephemeral : PROTOCOL = struct
  let service = "databricks.zerobus.Zerobus"
  let rpc = "EphemeralStream"

  let encode_request (r : Wire.ephemeral_stream_request) : string =
    let enc = Pbrt.Encoder.create () in
    Wire.encode_pb_ephemeral_stream_request r enc;
    Pbrt.Encoder.to_string enc

  let create_frame ~(table : Options.table_properties)
      ~(options : Options.stream_options) : string =
    let record_type =
      match options.Options.record_type with
      | Options.Json -> Wire.Json
      | Options.Proto | Options.Arrow -> Wire.Proto
    in
    encode_request
      (Wire.Create_stream
         (Wire.make_create_ingest_stream_request
            ~table_name:table.Options.table_name ~record_type
            ?descriptor_proto:
              (Option.map Options.descriptor_to_bytes table.Options.descriptor)
            ()))

  let record_frame ~(record_type : Options.record_type) ~offset (b : bytes) :
      string =
    let record =
      match record_type with
      | Options.Json -> Wire.Json_record (Bytes.to_string b)
      | Options.Proto | Options.Arrow -> Wire.Proto_encoded_record b
    in
    encode_request
      (Wire.Ingest_record
         (Wire.make_ingest_record_request ~offset_id:offset ~record ()))

  let decode_ack (s : string) : (ack, Error.t) result =
    match Wire.decode_pb_ephemeral_stream_response (Pbrt.Decoder.of_string s) with
    | Wire.Ingest_record_response ack ->
        Ok (Watermark ack.Wire.durability_ack_up_to_offset)
    | Wire.Create_stream_response _ -> Ok Created
    | Wire.Close_stream_signal sig_ ->
        (* The server may attach a Duration: how long until it closes. Surface it
           as ms so the driver can honor [stream_paused_max_wait_time_ms]. *)
        let ms =
          match sig_.Wire.duration with
          | Some d ->
              Int64.to_int
                (Int64.add
                   (Int64.mul d.Zerobus_proto.Duration.seconds 1000L)
                   (Int64.of_int32 (Int32.div d.Zerobus_proto.Duration.nanos 1_000_000l)))
          | None -> 0
        in
        Ok (Closed ms)
    | exception Pbrt.Decoder.Failure _ ->
        Error (Error.Protocol_error "could not decode EphemeralStreamResponse")
end

module Make_with_protocol (Io : Io.IO) (P : PROTOCOL) = struct
  open Io

  let ( let* ) = bind

  (* Race a result-producing computation against a timer: return [f]'s result if it
     finishes within [ms], else [Error (Timeout ...)]. [ms <= 0] disables the timer
     (runs [f] unbounded). Built on {!Io.first} (abandons the loser — on Lwt/Eio the
     loser is cancelled; on Async detached, so [f] must be safe to abandon). *)
  let with_timeout ~ms ~(label : string) (f : unit -> ('a, Error.t) result Io.t) :
      ('a, Error.t) result Io.t =
    if ms <= 0 then f ()
    else
      Io.first f (fun () ->
          let* () = Io.sleep (float_of_int ms /. 1000.) in
          return
            (Error
               (Error.Timeout
                  (Printf.sprintf "%s timed out after %dms" label ms))))

  type offset = Options.offset

  (* A one-shot notification a waiter blocks on; the ack-reader completes it by
     putting () once the watermark reaches the waiter's target. Built from the
     IO.Mailbox (capacity 1) since the IO signature has no bare condition var. *)
  type waiter = { target : int64; mbox : unit Mailbox.t }

  type stream = {
    options : Options.stream_options;
    scope : Scope.t;  (* the stream's lifetime scope; timeout timers fork here *)
    state_lock : Mutex.t;  (* guards the mutable fields below *)
    (* The active bidi call; swapped by recovery on a retryable failure. Guarded
       against concurrent send during a reconnect by [send_lock]. *)
    mutable call : Io.H2_client.call;
    send_lock : Mutex.t;  (* serializes sends + the call swap during recovery *)
    (* Reconnect inputs, captured at open (§12.3): enough to re-establish the
       stream and replay. Headers/create are re-sent verbatim on reconnect. *)
    conn : Io.H2_client.connection;
    create_request : string;  (* framed create/schema request, re-sent first *)
    service : string;
    rpc : string;
    reconnect_headers : (string * string) list;
    mutable next_offset : int64;  (* next offset to assign *)
    mutable last_acked : int64;  (* highest durably-acked offset; -1 = none *)
    mutable last_issued : int64;  (* highest offset handed to ingest; -1 = none *)
    mutable fatal : Error.t option;  (* set on a fatal stream/transport error *)
    mutable closed : bool;
    mutable waiters : waiter list;  (* pending wait_for_offset / flush waiters *)
    (* Un-acked replay buffer for recovery (§12.3): (offset, framed-bytes) from
       last_acked+1 forward, newest first. Bounded by [max_inflight_requests] AND
       [inflight_limit_bytes] — but NEVER dropped (see [overflow_policy]). *)
    mutable unacked : (int64 * string) list;  (* newest first *)
    mutable unacked_bytes : int;  (* running byte total of [unacked] (O(1) upkeep) *)
    inflight_limit_bytes : int;  (* byte ceiling, resolved at open from options/mem *)
    (* Producers parked by [overflow_policy = Block] until the ack-reader drains the
       buffer below the bound; woken (capacity-1 box each) when space frees up. *)
    mutable space_waiters : unit Mailbox.t list;
  }

  (* Drop replay-buffer entries at or below [watermark] (they are durable now) and
     keep [unacked_bytes] in sync. Runs under state_lock. *)
  let prune_unacked s watermark =
    let kept, freed =
      List.fold_left
        (fun (kept, freed) (off, framed) ->
          if off > watermark then ((off, framed) :: kept, freed)
          else (kept, freed + String.length framed))
        ([], 0) s.unacked
    in
    (* fold reverses order; restore newest-first *)
    s.unacked <- List.rev kept;
    s.unacked_bytes <- s.unacked_bytes - freed

  (* Collect the space-waiter boxes to wake (called after pruning frees room). Runs
     under state_lock; the boxes are [put] outside the lock by the caller. *)
  let take_space_waiters s =
    let ws = s.space_waiters in
    s.space_waiters <- [];
    ws

  (* Wake every waiter whose target the watermark has now reached, and drop them.
     Runs under state_lock. Returns the boxes to notify (notify outside the lock). *)
  let collect_ready_waiters s =
    let ready, pending =
      List.partition (fun w -> w.target <= s.last_acked) s.waiters
    in
    s.waiters <- pending;
    List.map (fun w -> w.mbox) ready

  (* On a fatal error: record it, wake all waiters (they observe [fatal]). Returns
     the (waiter-target, box) list so the caller can both notify the boxes AND fire
     [on_error] per pending offset OUTSIDE the state lock (the callback is user code;
     never run it under a lock — same discipline as [on_ack]). *)
  let fail_all s err =
    if s.fatal = None then s.fatal <- Some err;
    let failed = List.map (fun w -> (w.target, w.mbox)) s.waiters in
    s.waiters <- [];
    failed

  (* put () to each waiter box; capacity-1 mailbox, never blocks *)
  let notify_all boxes =
    let rec go = function
      | [] -> return ()
      | b :: rest ->
          let* () = Mailbox.put b () in
          go rest
    in
    go boxes

  (* Handle the (target, box) list from [fail_all]: fire the [on_error] callback for
     each failed offset (if registered), then wake every waiter box AND every
     backpressure space-waiter (a [Block]-parked producer must un-park on failure,
     re-check [s.fatal], and return the error instead of hanging forever). Callback
     runs here, OUTSIDE the state lock. *)
  let notify_failed s err (failed : (int64 * unit Mailbox.t) list) =
    (match s.options.Options.ack_callback with
     | Some cb ->
         List.iter
           (fun (target, _) ->
             cb.Options.on_error (Options.offset_of_int64 target) (Error.to_string err))
           failed
     | None -> ());
    let* space_boxes =
      Mutex.with_lock s.state_lock (fun () ->
          let ws = s.space_waiters in
          s.space_waiters <- [];
          return ws)
    in
    let* () = notify_all (List.map snd failed) in
    notify_all space_boxes

  (* Re-open the bidi RPC and replay the un-acked tail in order (§12.3). Runs
     under [send_lock] so no concurrent [ingest] send races the swap. Returns Ok
     once the fresh call is live and the tail re-sent, Error if reconnect fails. *)
  let reconnect_and_replay (s : stream) : (unit, Error.t) result Io.t =
    let* call_r =
      Io.H2_client.start_bidi s.conn ~service:s.service ~rpc:s.rpc
        ~headers:s.reconnect_headers ()
    in
    match call_r with
    | Error _ as e -> return e
    | Ok call -> (
        (* create/schema first, then the un-acked tail oldest-first. *)
        let tail =
          Mutex.with_lock s.state_lock (fun () ->
              return (List.rev_map snd s.unacked))
        in
        let* tail = tail in
        let rec send_all = function
          | [] -> return (Ok ())
          | frame :: rest -> (
              let* r = Io.H2_client.send call frame in
              match r with Ok () -> send_all rest | Error _ as e -> return e)
        in
        let* create_r = Io.H2_client.send call s.create_request in
        match create_r with
        | Error _ as e -> return e
        | Ok () -> (
            let* replay_r = send_all tail in
            match replay_r with
            | Error _ as e -> return e
            | Ok () ->
                (* swap the active call so subsequent ingests use it *)
                s.call <- call;
                return (Ok ())))

  (* The ack-reader daemon: drain the transport, advance the durability watermark
     (monotonic — an ack of N implies all <= N), fire the callback, wake waiters.
     On a retryable failure it reconnects+replays (bounded by recovery_retries,
     with backoff via IO.sleep); a fatal or exhausted failure wakes waiters with
     the error. Ends cleanly on recv None / a Closed frame. *)
  let ack_reader (s : stream) : unit -> unit Io.t =
   fun () ->
    let o = s.options in
    let rec try_recover attempt backoff_ms err : bool Io.t =
      if not o.Options.recovery then return false
      else if attempt > o.Options.recovery_retries then return false
      else if not (Error.is_retryable err) then return false
      else if s.closed then return false
      else
        let* () = Io.sleep (float_of_int backoff_ms /. 1000.) in
        let* rr =
          Mutex.with_lock s.send_lock (fun () -> reconnect_and_replay s)
        in
        match rr with
        | Ok () -> return true
        | Error err' ->
            let next = min (backoff_ms * 2) o.Options.recovery_timeout_ms in
            try_recover (attempt + 1) next err'
    in
    let advance_watermark watermark =
      let* advanced, boxes, space_boxes =
        Mutex.with_lock s.state_lock (fun () ->
            (* Guard monotonicity: a duplicate or out-of-order (<=) watermark must
               NOT move [last_acked] backward nor re-fire the callback — callers
               rely on "durable up to N" being monotonically non-decreasing. *)
            let advanced = watermark > s.last_acked in
            if advanced then s.last_acked <- watermark;
            prune_unacked s s.last_acked;
            (* Pruning freed replay-buffer room; wake any producers parked by the
               Block overflow policy so they can re-check and proceed. *)
            return (advanced, collect_ready_waiters s, take_space_waiters s))
      in
      (* Fire [on_ack] only when the watermark actually advanced, and with the new
         [last_acked] (== watermark here) — never a stale/decreasing value. *)
      (match (advanced, s.options.Options.ack_callback) with
       | true, Some cb -> cb.Options.on_ack (Options.offset_of_int64 s.last_acked)
       | _ -> ());
      let* () = notify_all boxes in
      notify_all space_boxes
    in
    let rec loop () : unit Io.t =
      (* capture the call we're reading so its final status is the one we check on
         end-of-stream (recovery may swap s.call afterward) *)
      let this_call = s.call in
      let* r = Io.H2_client.recv this_call in
      match r with
      | Ok None ->
          (* End of the response body. Distinguish a clean OK end from a
             retryable non-OK status (which lives in the trailer, not in recv):
             a broken stream ends the body too, so [recv]=None alone is ambiguous. *)
          let* st = Io.H2_client.status this_call in
          (match st with
           | Ok () ->
               (* Clean server end. Any waiter still pending (target > last_acked)
                  will NEVER be satisfied — the ack-reader is terminating and no more
                  acks are coming. Fail those fast (firing on_error) instead of
                  leaving flush/wait_for_offset to block until their own timeout. If
                  none pend, this is a normal clean close (empty fail list). *)
               let err =
                 Error.Stream_error "server closed the stream before all records acked"
               in
               let* failed =
                 Mutex.with_lock s.state_lock (fun () ->
                     (* Only mark fatal when waiters actually pend; a clean close with
                        nothing outstanding must NOT poison [s.fatal]. *)
                     if s.waiters = [] then return []
                     else return (fail_all s err))
               in
               notify_failed s err failed
           | Error err -> on_failure err)
      | Ok (Some raw) -> (
          match P.decode_ack raw with
          | Ok (Watermark watermark) ->
              let* () = advance_watermark watermark in
              loop ()
          | Ok Created -> loop ()
          | Ok (Closed server_ms) -> on_close server_ms
          | Error err -> on_failure err)
      | Error err -> on_failure err
    and on_failure err =
      let* recovered =
        try_recover 1 s.options.Options.recovery_backoff_ms err
      in
      if recovered then loop ()
      else
        let* failed = Mutex.with_lock s.state_lock (fun () -> return (fail_all s err)) in
        notify_failed s err failed
    (* Reconnect+replay after a graceful close, through the SAME bounded, retryable
       recovery path as [on_failure] (capped by [recovery_retries], gated on
       [is_retryable]) so a server that repeatedly closes freshly-opened streams
       cannot drive an unbounded reconnect loop. If recovery is exhausted, fail ALL
       waiters so nobody hangs. *)
    and reconnect_after_close () =
      let* recovered =
        try_recover 1 s.options.Options.recovery_backoff_ms
          (Error.Transport_error "server graceful close")
      in
      if recovered then loop ()
      else
        let err = Error.Stream_error "server closed the stream" in
        let* failed =
          Mutex.with_lock s.state_lock (fun () -> return (fail_all s err))
        in
        notify_failed s err failed
    (* Drain acks on the closing stream until the server ends the body (recv=None),
       advancing the (monotonic, idempotent) watermark as they arrive. Returns when
       the body ends. Triggers NO reconnect itself, so it is safe to abandon (Async)
       or cancel (Lwt/Eio) when the grace timer wins the race in [on_close]. *)
    and drain_until_close () : (unit, Error.t) result Io.t =
      let this_call = s.call in
      let* r = Io.H2_client.recv this_call in
      match r with
      | Ok None -> return (Ok ())
      | Ok (Some raw) -> (
          match P.decode_ack raw with
          | Ok (Watermark watermark) ->
              let* () = advance_watermark watermark in
              drain_until_close ()
          | Ok Created -> drain_until_close ()
          | Ok (Closed _) -> drain_until_close () (* already closing; keep draining *)
          | Error _ -> return (Ok ()))
      | Error _ -> return (Ok ())
    (* Graceful close: the server signalled it will close the stream in [server_ms].
       Honor [stream_paused_max_wait_time_ms]:
       - recovery off (or a caller-initiated close): terminal — we cannot re-open, so
         fail ALL outstanding waiters (not just the already-ready ones) with a
         stream-closed error, so [wait_for_offset]/[flush] don't hang forever.
       - [Some 0]: reconnect immediately (skip the grace window).
       - [None]: drain in-flight acks until the server ends the body, then reconnect
         (most graceful — bounded by the server's own close duration).
       - [Some x > 0]: drain, but cap the wait at exactly [min (x, server_ms)] — race
         the drain against a timer via {!with_timeout}/{!Io.first} (the reader is
         single-threaded, so we cannot both keep reading AND time out without the
         race). Either way we reconnect+replay once afterward via the bounded path.
       Draining keeps the watermark advancing for acks the server sends before it ends
       the body (genuine drain, not a blind sleep). *)
    and on_close server_ms =
      if (not o.Options.recovery) || s.closed then
        let err =
          Error.Stream_error "server closed the stream (recovery disabled)"
        in
        let* failed = Mutex.with_lock s.state_lock (fun () -> return (fail_all s err)) in
        notify_failed s err failed
      else
        match o.Options.stream_paused_max_wait_time_ms with
        | Some 0 -> reconnect_after_close ()
        | None ->
            let* _ = drain_until_close () in
            reconnect_after_close ()
        | Some x ->
            let cap = if server_ms > 0 then min x server_ms else x in
            (* Race the drain against a [cap]-ms deadline; whichever wins, reconnect.
               [with_timeout] returns the drain's [Ok ()] or [Error Timeout] — both
               lead to the same reconnect, so the timeout marker is just discarded. *)
            let* _ =
              with_timeout ~ms:cap ~label:"graceful-close drain" drain_until_close
            in
            reconnect_after_close ()
    in
    loop ()

  (* ---- construction ---- *)

  (* Open the stream: connect, start the bidi RPC, send the create/schema frame,
     fork the ack-reader into the stream's scope. [headers] carry auth + table. *)
  let open_stream ~host ~port ~tls ~headers ~(options : Options.stream_options)
      ~(table : Options.table_properties) (scope : Scope.t) :
      (stream, Error.t) result Io.t =
    (* Bound connection establishment by [connection_timeout_ms] (a hard deadline
       via {!with_timeout}/{!Io.first}); on expiry the connect is abandoned and we
       return [Timeout]. *)
    let* conn_r =
      with_timeout ~ms:options.Options.connection_timeout_ms ~label:"connect"
        (fun () -> Io.H2_client.connect ~host ~port ~tls ~headers ())
    in
    match conn_r with
    | Error _ as e -> return e
    | Ok conn -> (
        let* call_r =
          Io.H2_client.start_bidi conn ~service:P.service ~rpc:P.rpc ()
        in
        match call_r with
        | Error _ as e -> return e
        | Ok call ->
            let create_request = P.create_frame ~table ~options in
            let* send_r = Io.H2_client.send call create_request in
            match send_r with
            | Error _ as e -> return e
            | Ok () ->
                (* Resolve the byte ceiling once, at open: explicit [max_inflight_bytes]
                   or a smart budget from available memory ({!Mem_budget}). Never
                   unbounded, never lossy. *)
                let inflight_limit_bytes =
                  match options.Options.max_inflight_bytes with
                  | Some n when n > 0 -> n
                  | _ -> Mem_budget.default_budget_bytes ()
                in
                let s =
                  {
                    call;
                    options;
                    scope;
                    state_lock = Mutex.create ();
                    send_lock = Mutex.create ();
                    conn;
                    create_request;
                    service = P.service;
                    rpc = P.rpc;
                    reconnect_headers = headers;
                    next_offset = 0L;
                    last_acked = -1L;
                    last_issued = -1L;
                    fatal = None;
                    closed = false;
                    waiters = [];
                    unacked = [];
                    unacked_bytes = 0;
                    inflight_limit_bytes;
                    space_waiters = [];
                  }
                in
                fork_daemon scope (ack_reader s);
                return (Ok s))

  (* ---- ingestion (queue-only; never wait — the cardinal rule) ---- *)

  (* Would adding a record of [n] bytes exceed EITHER inflight bound? (count / bytes) *)
  let would_overflow s n =
    List.length s.unacked >= s.options.Options.max_inflight_requests
    || s.unacked_bytes + n > s.inflight_limit_bytes

  (* Reserve a slot for a new [framed] record: assign its offset, append to the
     replay buffer, bump byte total. Runs under state_lock. Returns the offset. *)
  let reserve_slot s framed =
    let off = s.next_offset in
    s.next_offset <- Int64.add off 1L;
    s.last_issued <- off;
    s.unacked <- (off, framed) :: s.unacked;
    s.unacked_bytes <- s.unacked_bytes + String.length framed;
    off

  (* Queue one record for sending. ALL of the fatal/closed guard, the no-loss
     overflow decision, and the offset assignment happen atomically UNDER state_lock
     (fixing the prior race where fatal/closed were read BEFORE locking, letting a
     record slip onto an already-failed stream). Framing needs the offset and the
     byte bound needs the frame size, so we peek the offset, frame, size-check, then
     commit — all inside the one lock. Overflow NEVER drops: [Fail] returns
     [Backpressure]; [Block] parks on a space-waiter and retries when the ack-reader
     frees room. *)
  let rec ingest_encoded (s : stream) (make : int64 -> string) :
      (offset, Error.t) result Io.t =
    let* decision =
      Mutex.with_lock s.state_lock (fun () ->
          match s.fatal with
          | Some e -> return (`Err e)
          | None ->
              if s.closed then return (`Err (Error.Stream_error "ingest after close"))
              else
                (* We must know the size to check the byte bound, but size depends on
                   the frame which depends on the offset. Peek the offset without
                   committing, frame, then decide. *)
                let off = s.next_offset in
                let framed = make off in
                let n = String.length framed in
                if would_overflow s n then
                  match s.options.Options.overflow_policy with
                  | Options.Fail ->
                      return
                        (`Err
                          (Error.Backpressure
                             (Printf.sprintf
                                "un-acked buffer full (%d records / %d bytes, limit \
                                 %d records / %d bytes) — flush or retry"
                                (List.length s.unacked) s.unacked_bytes
                                s.options.Options.max_inflight_requests
                                s.inflight_limit_bytes)))
                  | Options.Block ->
                      if s.unacked = [] then (
                        (* oversized-but-empty: admit to avoid deadlock *)
                        let off = reserve_slot s framed in
                        return (`Send (off, framed)))
                      else begin
                        let box = Mailbox.create ~capacity:1 in
                        s.space_waiters <- box :: s.space_waiters;
                        return (`Wait box)
                      end
                else
                  let off = reserve_slot s framed in
                  return (`Send (off, framed)))
    in
    match decision with
    | `Err e -> return (Error e)
    | `Wait box ->
        let* _ = Mailbox.take box in
        ingest_encoded s make (* retry after room frees (or the stream failed) *)
    | `Send (off, framed) ->
        (* send under send_lock so a concurrent recovery call-swap doesn't race the
           write; [s.call] is read inside the lock. *)
        let* send_r =
          Mutex.with_lock s.send_lock (fun () -> Io.H2_client.send s.call framed)
        in
        (match send_r with
         | Ok () -> return (Ok (Options.offset_of_int64 off))
         | Error e -> return (Error e))

  let ingest (s : stream) (record : bytes) : (offset, Error.t) result Io.t =
    ingest_encoded s (fun off ->
        P.record_frame ~record_type:s.options.Options.record_type ~offset:off
          record)

  let ingest_records (s : stream) (records : bytes list) :
      (offset, Error.t) result Io.t =
    let rec loop last = function
      | [] -> (
          match last with
          | Some off -> return (Ok off)
          | None -> return (Error (Error.Stream_error "ingest_records: empty")))
      | r :: rest -> (
          let* res = ingest s r in
          match res with Ok off -> loop (Some off) rest | Error _ as e -> return e)
    in
    loop None records

  (* ---- waiting ---- *)

  (* Try to claim (remove) a still-pending waiter under the state lock. Returns
     its box iff this caller is the one that removed it — the ack-reader,
     fail_all, and the timeout timer all race to claim, and the single claimer is
     the only one that [put]s (a capacity-1 box would block a second putter, so
     exactly-one-put matters). *)
  let claim_waiter s w =
    Mutex.with_lock s.state_lock (fun () ->
        if List.memq w s.waiters then begin
          s.waiters <- List.filter (fun w' -> w' != w) s.waiters;
          return (Some w.mbox)
        end
        else return None)

  (* Wait until the durability watermark reaches [target], the stream fails, or
     [timeout_ms] elapses (<= 0 disables the timeout). The timer forks into the
     stream's scope, so [close] cancels any still-pending timer. *)
  let wait_for_offset_timeout (s : stream) ~timeout_ms (o : offset) :
      (unit, Error.t) result Io.t =
    let target = Options.int64_of_offset o in
    let* already =
      Mutex.with_lock s.state_lock (fun () ->
          match s.fatal with
          | Some e -> return (`Fatal e)
          | None ->
              if s.last_acked >= target then return `Done
              else begin
                let w = { target; mbox = Mailbox.create ~capacity:1 } in
                s.waiters <- w :: s.waiters;
                return (`Wait w)
              end)
    in
    match already with
    | `Done -> return (Ok ())
    | `Fatal e -> return (Error e)
    | `Wait w ->
        (* Fork a one-shot timeout timer into the stream scope: on expiry, if the
           waiter is still pending, claim it and wake it (it will observe a
           timeout below since neither the watermark nor [fatal] was reached). *)
        if timeout_ms > 0 then
          fork_daemon s.scope (fun () ->
              let* () = Io.sleep (float_of_int timeout_ms /. 1000.) in
              let* box = claim_waiter s w in
              match box with Some mbox -> Mailbox.put mbox () | None -> return ());
        let* _ = Mailbox.take w.mbox in
        (* Waking reason, in priority order: fatal error, target reached, else the
           only remaining cause is the timeout timer. *)
        (match s.fatal with
         | Some e -> return (Error e)
         | None ->
             if s.last_acked >= target then return (Ok ())
             else
               return
                 (Error
                    (Error.Timeout
                       (Printf.sprintf
                          "no durability ack for offset %Ld within %dms"
                          target timeout_ms))))

  (* Wait until the durability watermark reaches [target], or the stream fails,
     bounded by [server_lack_of_ack_timeout_ms] (the ack watchdog). *)
  let wait_for_offset (s : stream) (o : offset) : (unit, Error.t) result Io.t =
    wait_for_offset_timeout s
      ~timeout_ms:s.options.Options.server_lack_of_ack_timeout_ms o

  (* Flush: wait once for all records issued so far to be durable, bounded by
     [flush_timeout_ms] so a non-acking / stalled server yields [Timeout] rather
     than hanging forever. *)
  let flush (s : stream) : (unit, Error.t) result Io.t =
    match s.fatal with
    | Some e -> return (Error e)
    | None ->
        if s.last_issued < 0L then return (Ok ())
        else
          wait_for_offset_timeout s
            ~timeout_ms:s.options.Options.flush_timeout_ms
            (Options.offset_of_int64 s.last_issued)

  (* ---- teardown ---- *)

  let close (s : stream) : (unit, Error.t) result Io.t =
    if s.closed then return (Ok ())
    else begin
      s.closed <- true;
      let* _ = Io.H2_client.close_send s.call in
      (* best-effort: flush pending acks before tearing down the scope *)
      let* _ = flush s in
      (* Shut the transport connection down. Required on direct-style runtimes
         (Eio): the h2 connection is registered in the caller's Switch, which
         won't resolve — hanging the whole [Eio_main.run] — until the connection
         is closed. Idempotent / best-effort on all runtimes. *)
      let* () = Io.H2_client.shutdown s.conn in
      return (Ok ())
    end
end

(* The default driver: [Make_with_protocol] over the [EphemeralStream] protocol,
   so the existing single-arg [Zerobus_core.Make (Io)] API is unchanged. *)
module Make (Io : Io.IO) = Make_with_protocol (Io) (Ephemeral)
