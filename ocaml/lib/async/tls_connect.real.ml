(** Real TLS connect for the Async transport — used when [tls-async] is present
    (dune [select] picks this over [tls_connect.dummy.ml]). Establishes TLS 1.3 +
    ALPN [h2] with system trust anchors + peer-name verification (mirrors the Lwt
    reference), returning a cleartext Async [Reader]/[Writer] duplex the h2 core
    pump ({!H2_pump}) drives. No gluten-async / h2-async TLS involved. *)

open! Core
open! Async

let available = true

(* Returns (reader, writer, shutdown) on success. [shutdown] closes the writer. *)
let connect ~host ~port :
    (Reader.t * Writer.t * (unit -> unit Deferred.t), string) Deferred.Result.t =
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
    Tls.Config.client ~authenticator ?peer_name ~alpn_protocols:[ "h2" ] ()
  in
  let where =
    Tcp.Where_to_connect.of_host_and_port (Host_and_port.create ~host ~port)
  in
  match%bind Tls_async.connect cfg where ~host:peer_name with
  | Error e -> return (Error (Error.to_string_hum e))
  | Ok (session, reader, writer) -> (
      (* Fail fast unless ALPN negotiated h2 (mandatory for gRPC). *)
      match Tls_async.Session.epoch session with
      | Error e -> return (Error (Error.to_string_hum e))
      | Ok epoch -> (
          match epoch.Tls.Core.alpn_protocol with
          | Some "h2" ->
              let shutdown () =
                Writer.close writer >>= fun () -> Reader.close reader
              in
              return (Ok (reader, writer, shutdown))
          | other ->
              return
                (Error
                   (Printf.sprintf "ALPN != h2 (got %s)"
                      (Option.value other ~default:"none")))))
