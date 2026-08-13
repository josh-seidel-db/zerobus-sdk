(** Real OAuth token-endpoint HTTP backend for the Async runtime — used when
    [tls-async] (+ its deps) is installed (dune [select] picks this over
    [oauth.dummy.ml]). Does the client-credentials POST to
    [<workspace_url>/oidc/v1/token] and returns the parsed
    [(access_token, expiry)]; the façade ({!Zerobus_async}) owns the token cache
    \+ orchestration.

    {2 Why tls-async and not cohttp-async}

    The token POST is plain HTTPS/1.1, but it must use {b ocaml-tls} — the same
    pure-OCaml TLS the gRPC transport uses ([tls_connect.real.ml]) — not
    OpenSSL. [cohttp-async] routes HTTPS through [conduit-async], whose only TLS
    backend is [async_ssl] (OpenSSL via [conf-libssl]); the SDK deliberately
    avoids that C dependency (DESIGN §12.5). So this backend connects with
    [Tls_async.connect] (system trust store + peer-name verification) and speaks
    a minimal HTTP/1.1 request/response by hand — exactly the pure-TLS posture
    of the Lwt reference (whose [conduit-lwt-unix] uses [tls-lwt]) and the Eio
    façade (which threads [tls-eio]). No cohttp/conduit/OpenSSL on this path.

    Kept behind the [select] so the Async package does NOT force these deps on
    switches that only want the gRPC transport — like the tls-async live-TLS
    [select]. See doc/arch/tls_async_status.md for the packaging rationale. *)

open! Core
open! Async

let available = true

(* --- minimal URL split: <scheme>://<host>[:<port>]<path> --- *)
type url = { tls : bool; host : string; port : int; path : string }

let parse_url (u : string) : url option =
  let strip_scheme prefix rest_tls =
    if String.is_prefix u ~prefix then
      Some (String.drop_prefix u (String.length prefix), rest_tls)
    else None
  in
  match
    match strip_scheme "https://" true with
    | Some x -> Some x
    | None -> strip_scheme "http://" false
  with
  | None -> None
  | Some (rest, tls) ->
      let authority, path =
        match String.lsplit2 rest ~on:'/' with
        | Some (a, p) -> (a, "/" ^ p)
        | None -> (rest, "/")
      in
      let host, port =
        match String.lsplit2 authority ~on:':' with
        | Some (h, p) ->
            (h, try Int.of_string p with _ -> if tls then 443 else 80)
        | None -> (authority, if tls then 443 else 80)
      in
      Some { tls; host; port; path }

(* Build a raw HTTP/1.1 POST with Connection: close so the server ends the body
   at EOF (lets us read the whole response with Reader.contents). *)
let http_post_request ~host ~path ~headers ~body : string =
  let buf = Buffer.create 256 in
  Buffer.add_string buf (Printf.sprintf "POST %s HTTP/1.1\r\n" path);
  Buffer.add_string buf (Printf.sprintf "Host: %s\r\n" host);
  Buffer.add_string buf "Connection: close\r\n";
  Buffer.add_string buf
    (Printf.sprintf "Content-Length: %d\r\n" (String.length body));
  List.iter headers ~f:(fun (k, v) ->
      Buffer.add_string buf (Printf.sprintf "%s: %s\r\n" k v));
  Buffer.add_string buf "\r\n";
  Buffer.add_string buf body;
  Buffer.contents buf

(* Parse "HTTP/1.1 <code> ..." + split headers/body on the blank line. *)
let parse_response (raw : string) : (int * string) option =
  match String.substr_index raw ~pattern:"\r\n\r\n" with
  | None -> None
  | Some i ->
      let head = String.sub raw ~pos:0 ~len:i in
      let body = String.sub raw ~pos:(i + 4) ~len:(String.length raw - i - 4) in
      let status_line =
        match String.lsplit2 head ~on:'\r' with
        | Some (s, _) -> s
        | None -> head
      in
      let code =
        match String.split status_line ~on:' ' with
        | _ :: c :: _ -> ( try Int.of_string c with _ -> 0)
        | _ -> 0
      in
      Some (code, body)

(* General HTTPS/HTTP POST over ocaml-tls (system trust store + peer-name
   verification for https; plain TCP for http, used by the cleartext test mock).
   Writes the request, reads the whole response to EOF (we send Connection: close),
   tears down, and returns (http_status, body). This is the SHARED HTTP layer the
   token mint AND the sibling REST/OTLP Async packages use, so no code path pulls
   cohttp-async's OpenSSL conduit. *)
let https_post ~url:(u : string) ~headers ~body :
    (int * string, exn) result Deferred.t =
  match parse_url u with
  | None -> return (Error (Failure ("bad URL: " ^ u)))
  | Some url ->
      let req =
        http_post_request ~host:url.host ~path:url.path ~headers ~body
      in
      let exchange reader writer =
        Writer.write writer req;
        let%bind () = Writer.flushed writer in
        let%bind raw = Reader.contents reader in
        let%bind () = Writer.close writer in
        let%bind () = Reader.close reader in
        match parse_response raw with
        | Some pair -> return pair
        | None -> failwith "malformed HTTP response"
      in
      Monitor.try_with ~run:`Now (fun () ->
          if url.tls then
            (* Pure-OCaml TLS, system anchors + peer name; no ALPN needed. *)
            let authenticator =
              match Ca_certs.authenticator () with
              | Ok a -> a
              | Error (`Msg m) -> failwith ("ca-certs: " ^ m)
            in
            let peer_name =
              match Domain_name.of_string url.host with
              | Ok d -> (
                  match Domain_name.host d with
                  | Ok h -> Some h
                  | Error _ -> None)
              | Error _ -> None
            in
            let cfg = Tls.Config.client ~authenticator ?peer_name () in
            let where =
              Tcp.Where_to_connect.of_host_and_port
                (Host_and_port.create ~host:url.host ~port:url.port)
            in
            match%bind Tls_async.connect cfg where ~host:peer_name with
            | Error e -> failwith (Error.to_string_hum e)
            | Ok (_session, reader, writer) -> exchange reader writer
          else
            let where =
              Tcp.Where_to_connect.of_host_and_port
                (Host_and_port.create ~host:url.host ~port:url.port)
            in
            let%bind _sock, reader, writer = Tcp.connect where in
            exchange reader writer)

(* The client-credentials token POST: Basic auth + form body, on the shared HTTP
   layer above. Returns (http_status, body). *)
let post_token ~token_url ~client_id ~client_secret ~body :
    (int * string, exn) result Deferred.t =
  let basic = Base64.encode_string (client_id ^ ":" ^ client_secret) in
  https_post ~url:token_url
    ~headers:
      [
        ("Content-Type", "application/x-www-form-urlencoded");
        ("Authorization", "Basic " ^ basic);
      ]
    ~body

(* Parse the token endpoint's JSON reply: pull [access_token] and [expires_in]
   (default 3600s). [now] is the base for the absolute expiry. Returns
   [(token, expiry)] or [None] on malformed / missing-token responses. *)
let parse_token_response ~now (body : string) : (string * float) option =
  try
    let json = Yojson.Safe.from_string body in
    let field k =
      match json with
      | `Assoc l -> List.Assoc.find l k ~equal:String.equal
      | _ -> None
    in
    match field "access_token" with
    | Some (`String t) ->
        let expires_in =
          match field "expires_in" with
          | Some (`Int n) -> Float.of_int n
          | Some (`Float f) -> f
          | Some (`String s) -> ( try Float.of_string s with _ -> 3600.0)
          | _ -> 3600.0
        in
        Some (t, now +. expires_in)
    | _ -> None
  with _ -> None
