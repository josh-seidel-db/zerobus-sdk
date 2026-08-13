(** Live spike — DESIGN.md §12.1: native OCaml TLS 1.3 + ALPN "h2" to the real
    Zerobus gRPC endpoint.

    Uses pure-OCaml [tls-lwt] with system trust anchors from [ca-certs] (the one
    dependency the design says is genuinely required, §12.1). Opens a real TCP
    connection to the endpoint, performs the TLS handshake offering
    [~alpn_protocols:["h2"]], and asserts:

      1. the handshake succeeds and the certificate chain verifies against the
         OS trust store (ca-certs authenticator), and
      2. the server negotiated ALPN protocol "h2" — the precondition for gRPC.

    This is exactly what the loopback spikes (spike/, spike-eio/) could NOT prove.
    Endpoint host/port come from argv or env; no secret involved.

    Exit 0 = proven; non-zero = failure with a diagnostic. *)

let getenv_default k d = try Sys.getenv k with Not_found -> d

let host =
  if Array.length Sys.argv > 1 then Sys.argv.(1)
  else getenv_default "ZEROBUS_ENDPOINT"
         "984752964297111.zerobus.eastus2.azuredatabricks.net"

let port =
  if Array.length Sys.argv > 2 then int_of_string Sys.argv.(2) else 443

let () =
  Lwt_main.run
    (let open Lwt.Syntax in
     Printf.printf "== §12.1 TLS+ALPN spike ==\nendpoint: %s:%d\n%!" host port;

     (* System trust anchors — never a custom/none authenticator. *)
     let authenticator =
       match Ca_certs.authenticator () with
       | Ok a -> a
       | Error (`Msg m) -> failwith ("ca-certs: " ^ m)
     in
     let peer_name =
       match Domain_name.of_string host with
       | Ok d -> ( match Domain_name.host d with Ok h -> Some h | Error _ -> None )
       | Error _ -> None
     in
     let cfg =
       Tls.Config.client ~authenticator ?peer_name
         ~alpn_protocols:[ "h2" ] ()
     in

     (* Resolve + TCP connect. *)
     let* addrs =
       Lwt_unix.getaddrinfo host (string_of_int port) [ Unix.(AI_SOCKTYPE SOCK_STREAM) ]
     in
     let* sockaddr =
       match addrs with
       | ai :: _ -> Lwt.return ai.Unix.ai_addr
       | [] -> Lwt.fail_with ("no DNS for " ^ host)
     in
     let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
     let* () = Lwt_unix.connect fd sockaddr in
     Printf.printf "tcp: connected\n%!";

     (* TLS handshake offering ALPN h2. *)
     let* tls =
       Lwt.catch
         (fun () -> Tls_lwt.Unix.client_of_fd cfg fd)
         (fun exn -> Lwt.fail_with ("TLS handshake failed: " ^ Printexc.to_string exn))
     in
     Printf.printf "tls: handshake OK (cert chain verified vs ca-certs)\n%!";

     (* Inspect the negotiated epoch. *)
     let epoch =
       match Tls_lwt.Unix.epoch tls with
       | Ok e -> e
       | Error () -> failwith "no TLS epoch after handshake"
     in
     let alpn = epoch.Tls.Core.alpn_protocol in
     let version =
       match epoch.Tls.Core.protocol_version with
       | `TLS_1_3 -> "TLS1.3"
       | `TLS_1_2 -> "TLS1.2"
       | `TLS_1_1 -> "TLS1.1"
       | `TLS_1_0 -> "TLS1.0"
     in
     let* () = Tls_lwt.Unix.close tls in

     let t = Unix.gmtime (Unix.time ()) in
     Printf.printf
       "\n-- EVIDENCE (§12.1) --\n\
        timestamp_utc  : %04d-%02d-%02dT%02d:%02d:%02dZ\n\
        endpoint       : %s:%d\n\
        tls_version    : %s\n\
        cert_verified  : true (ca-certs system trust anchors)\n\
        alpn_negotiated: %s\n%!"
       (t.tm_year + 1900) (t.tm_mon + 1) t.tm_mday t.tm_hour t.tm_min t.tm_sec
       host port version
       (match alpn with Some p -> p | None -> "(none)");

     match alpn with
     | Some "h2" ->
         Printf.printf "RESULT: PASS — native OCaml negotiated ALPN h2 over TLS.\n%!";
         Lwt.return_unit
     | other ->
         Printf.printf "RESULT: FAIL — expected ALPN h2, got %s\n%!"
           (match other with Some p -> p | None -> "(none)");
         exit 1)
