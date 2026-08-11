(** Async live-TLS transport spike — drives the runtime-agnostic [H2.Client_connection]
    core over a [Tls_async] duplex, WITHOUT gluten-async / h2-async.

    Why: gluten-async's [tls_io] is a build-time [select] that ships a dummy on our
    switches, and gluten-async 0.5.2 doesn't even compile against tls 0.17 — an
    upstream packaging dead-end (see doc/arch/tls_async_status.md). But [h2]'s core
    [Client_connection] is runtime-agnostic (that's what gluten drives over a
    socket). So we do TLS with [Tls_async.connect] directly — exactly as the Lwt
    transport uses [Tls_lwt] — get a cleartext Async [Reader]/[Writer] with ALPN
    [h2], and pump the h2 core over them ourselves.

    This module is the reusable pump; [ingest_live.ml] exercises it end to end. *)

open! Core
open! Async
module Rw = Tls_async

(* A connected h2 client over a TLS duplex: the core connection + the writer we
   flush frames to. *)
type t = {
  conn : H2.Client_connection.t;
  writer : Writer.t;
  reader : Reader.t;
}

(* Drive the h2 WRITE state machine: whenever the core has bytes, pull the IOVecs
   and write them to the Async [Writer]; on [`Yield] register to resume; on
   [`Close] stop. Runs as a detached loop for the connection's life. *)
let rec run_writer (conn : H2.Client_connection.t) (w : Writer.t) : unit =
  match H2.Client_connection.next_write_operation conn with
  | `Write iovecs ->
      let n =
        List.fold iovecs ~init:0 ~f:(fun acc { Faraday.buffer; off; len } ->
            (* buffer is a Bigstringaf.t; Writer.write_bigstring wants a bigstring. *)
            Writer.write_bigstring w buffer ~pos:off ~len;
            acc + len)
      in
      H2.Client_connection.report_write_result conn (`Ok n);
      run_writer conn w
  | `Yield ->
      H2.Client_connection.yield_writer conn (fun () -> run_writer conn w)
  | `Close _ -> ()

(* Drive the h2 READ state machine: feed every chunk the Async [Reader] delivers
   into [Client_connection.read]. Returns when the reader hits EOF. *)
let run_reader (conn : H2.Client_connection.t) (r : Reader.t) : unit Deferred.t =
  Reader.read_one_chunk_at_a_time r
    ~handle_chunk:(fun buf ~pos ~len ->
      (* buf is a Bigstring; feed the whole available window to h2. It returns how
         many bytes it consumed. *)
      let consumed =
        match H2.Client_connection.next_read_operation conn with
        | `Read -> H2.Client_connection.read conn buf ~off:pos ~len
        | `Close ->
            (* reader side is done; consume nothing more *)
            0
      in
      return (`Consumed (consumed, `Need_unknown)))
  >>| fun _reason ->
  (* Tell the core the read side hit EOF so it can finish/close. *)
  ignore (H2.Client_connection.read_eof conn Bigstringaf.empty ~off:0 ~len:0 : int)

(* Establish TLS 1.3 + ALPN h2 to [host]:[port] and start pumping. Fails if ALPN
   didn't negotiate h2. *)
let connect ~host ~port : t Deferred.Or_error.t =
  let open Deferred.Or_error.Let_syntax in
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
  let cfg = Tls.Config.client ~authenticator ?peer_name ~alpn_protocols:[ "h2" ] () in
  let where = Tcp.Where_to_connect.of_host_and_port (Host_and_port.create ~host ~port) in
  let%bind session, reader, writer = Rw.connect cfg where ~host:peer_name in
  (* Verify ALPN negotiated h2 (read it off the TLS epoch, as the Lwt path does). *)
  let%bind () =
    match Rw.Session.epoch session with
    | Error e -> Deferred.return (Error e)
    | Ok epoch -> (
        match epoch.Tls.Core.alpn_protocol with
        | Some "h2" -> return ()
        | other ->
            Deferred.Or_error.error_string
              (Printf.sprintf "ALPN != h2 (got %s)"
                 (Option.value other ~default:"none")))
  in
  let error_handler _ = () in
  let conn =
    H2.Client_connection.create ?config:None ?push_handler:None ~error_handler ()
  in
  (* Fork the read + write pumps for the life of the connection. *)
  don't_wait_for (run_reader conn reader);
  run_writer conn writer;
  return { conn; writer; reader }
