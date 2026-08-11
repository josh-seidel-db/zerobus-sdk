(** Drive the runtime-agnostic {!H2.Client_connection} core over an Async
    [Reader.t] / [Writer.t] duplex — the same job gluten does over a socket, but
    without gluten-async (whose TLS backend is a build-time [select] that ships a
    dummy on our switches and doesn't compile against current tls). This lets the
    Async transport run h2 over BOTH a plain TCP duplex (cleartext h2c, for mocks)
    and a [tls-async] duplex (live TLS + ALPN h2) — see {!Tls_connect}. Proven live
    against the real Zerobus endpoint (spike-async-tls/). *)

open! Core
open! Async

(* Pump the WRITE state machine: drain h2's IOVecs to the Writer; on [`Yield]
   register to resume; on [`Close] stop. *)
let rec run_writer (conn : H2.Client_connection.t) (w : Writer.t) : unit =
  match H2.Client_connection.next_write_operation conn with
  | `Write iovecs ->
      let n =
        List.fold iovecs ~init:0 ~f:(fun acc { Faraday.buffer; off; len } ->
            Writer.write_bigstring w buffer ~pos:off ~len;
            acc + len)
      in
      H2.Client_connection.report_write_result conn (`Ok n);
      run_writer conn w
  | `Yield ->
      H2.Client_connection.yield_writer conn (fun () -> run_writer conn w)
  | `Close _ -> ()

(* Pump the READ state machine: feed each chunk the Reader delivers into h2's
   [read]; on EOF tell h2 via [read_eof]. Runs for the connection's life. *)
let run_reader (conn : H2.Client_connection.t) (r : Reader.t) : unit Deferred.t =
  let%map _reason =
    Reader.read_one_chunk_at_a_time r ~handle_chunk:(fun buf ~pos ~len ->
        let consumed =
          match H2.Client_connection.next_read_operation conn with
          | `Read -> H2.Client_connection.read conn buf ~off:pos ~len
          | `Close -> 0
        in
        return (`Consumed (consumed, `Need_unknown)))
  in
  ignore
    (H2.Client_connection.read_eof conn Bigstringaf.empty ~off:0 ~len:0 : int)

(* Start both pumps for a fresh connection over [reader]/[writer]. The write pump
   runs inline (it immediately [`Yield]s until there is something to send); the
   read pump is detached for the connection's life. *)
let start (conn : H2.Client_connection.t) ~(reader : Reader.t)
    ~(writer : Writer.t) : unit =
  don't_wait_for (run_reader conn reader);
  run_writer conn writer
