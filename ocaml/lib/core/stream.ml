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
  | Closed  (** server signalled end-of-stream *)

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
    | Wire.Close_stream_signal _ -> Ok Closed
    | exception Pbrt.Decoder.Failure _ ->
        Error (Error.Protocol_error "could not decode EphemeralStreamResponse")
end

module Make_with_protocol (Io : Io.IO) (P : PROTOCOL) = struct
  open Io

  let ( let* ) = bind

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
    (* Un-acked replay buffer for recovery (§12.3), bounded by
       max_inflight_requests: (offset, framed-bytes) from last_acked+1 forward. *)
    mutable unacked : (int64 * string) list;  (* newest first *)
  }

  (* Drop replay-buffer entries at or below [watermark] (they are durable now). *)
  let prune_unacked s watermark =
    s.unacked <- List.filter (fun (off, _) -> off > watermark) s.unacked

  (* Wake every waiter whose target the watermark has now reached, and drop them.
     Runs under state_lock. Returns the boxes to notify (notify outside the lock). *)
  let collect_ready_waiters s =
    let ready, pending =
      List.partition (fun w -> w.target <= s.last_acked) s.waiters
    in
    s.waiters <- pending;
    List.map (fun w -> w.mbox) ready

  (* On a fatal error: record it, wake all waiters (they observe [fatal]). *)
  let fail_all s err =
    if s.fatal = None then s.fatal <- Some err;
    let boxes = List.map (fun w -> w.mbox) s.waiters in
    s.waiters <- [];
    boxes

  (* put () to each waiter box; capacity-1 mailbox, never blocks *)
  let notify_all boxes =
    let rec go = function
      | [] -> return ()
      | b :: rest ->
          let* () = Mailbox.put b () in
          go rest
    in
    go boxes

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
      let* boxes =
        Mutex.with_lock s.state_lock (fun () ->
            if watermark > s.last_acked then s.last_acked <- watermark;
            prune_unacked s s.last_acked;
            return (collect_ready_waiters s))
      in
      (match s.options.Options.ack_callback with
       | Some cb -> cb.Options.on_ack (Options.offset_of_int64 watermark)
       | None -> ());
      notify_all boxes
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
               let* boxes = Mutex.with_lock s.state_lock (fun () -> return (collect_ready_waiters s)) in
               notify_all boxes
           | Error err -> on_failure err)
      | Ok (Some raw) -> (
          match P.decode_ack raw with
          | Ok (Watermark watermark) ->
              let* () = advance_watermark watermark in
              loop ()
          | Ok Created -> loop ()
          | Ok Closed ->
              let* boxes = Mutex.with_lock s.state_lock (fun () -> return (collect_ready_waiters s)) in
              notify_all boxes
          | Error err -> on_failure err)
      | Error err -> on_failure err
    and on_failure err =
      let* recovered =
        try_recover 1 s.options.Options.recovery_backoff_ms err
      in
      if recovered then loop ()
      else
        let* boxes = Mutex.with_lock s.state_lock (fun () -> return (fail_all s err)) in
        notify_all boxes
    in
    loop ()

  (* ---- construction ---- *)

  (* Open the stream: connect, start the bidi RPC, send the create/schema frame,
     fork the ack-reader into the stream's scope. [headers] carry auth + table. *)
  let open_stream ~host ~port ~tls ~headers ~(options : Options.stream_options)
      ~(table : Options.table_properties) (scope : Scope.t) :
      (stream, Error.t) result Io.t =
    let* conn_r = Io.H2_client.connect ~host ~port ~tls ~headers () in
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
                  }
                in
                fork_daemon scope (ack_reader s);
                return (Ok s))

  (* ---- ingestion (queue-only; never wait — the cardinal rule) ---- *)

  let ingest_encoded (s : stream) (make : int64 -> string) :
      (offset, Error.t) result Io.t =
    match s.fatal with
    | Some e -> return (Error e)
    | None ->
        if s.closed then return (Error (Error.Stream_error "ingest after close"))
        else
          let* off, framed =
            Mutex.with_lock s.state_lock (fun () ->
                let off = s.next_offset in
                s.next_offset <- Int64.add off 1L;
                s.last_issued <- off;
                let framed = make off in
                (* retain for replay, bounded by max_inflight_requests *)
                s.unacked <- (off, framed) :: s.unacked;
                if List.length s.unacked > s.options.Options.max_inflight_requests
                then
                  s.unacked <-
                    (let rec take n = function
                       | x :: xs when n > 0 -> x :: take (n - 1) xs
                       | _ -> []
                     in
                     take s.options.Options.max_inflight_requests s.unacked);
                return (off, framed))
          in
          (* send under send_lock so a concurrent recovery call-swap doesn't race
             the write; [s.call] is read inside the lock. *)
          let* send_r =
            Mutex.with_lock s.send_lock (fun () ->
                Io.H2_client.send s.call framed)
          in
          match send_r with
          | Ok () -> return (Ok (Options.offset_of_int64 off))
          | Error e -> return (Error e)

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
