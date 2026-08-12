(** Real OAuth token-endpoint HTTP backend for the Async runtime — used when
    [cohttp-async] (+ yojson/uri) is installed (dune [select] picks this over
    [oauth.dummy.ml]). Does the raw client-credentials POST to
    [<workspace_url>/oidc/v1/token] and returns the parsed [(access_token,
    expiry)]; the façade ({!Zerobus_async}) owns the token cache + orchestration.

    Kept behind the [select] so the Async package does NOT force a [cohttp-async]
    (and its [cohttp] 6.0->5.3) dependency on switches that only want the gRPC
    transport — exactly like the tls-async live-TLS [select]. See
    doc/arch/tls_async_status.md for the packaging rationale. *)

open! Core
open! Async

let available = true

(* POST the client-credentials grant. [body] is the URL-encoded request body
   (built by the caller via Zerobus_core.Config); [client_id]/[client_secret] go
   in the HTTP Basic header. Returns (http_status, response_body). *)
let post_token ~token_url ~client_id ~client_secret ~body :
    (int * string, exn) result Deferred.t =
  let basic = Base64.encode_string (client_id ^ ":" ^ client_secret) in
  let headers =
    Cohttp.Header.of_list
      [ ("Content-Type", "application/x-www-form-urlencoded");
        ("Authorization", "Basic " ^ basic) ]
  in
  match%map
    Monitor.try_with ~run:`Now (fun () ->
        let%bind resp, resp_body =
          Cohttp_async.Client.post ~headers
            ~body:(Cohttp_async.Body.of_string body)
            (Uri.of_string token_url)
        in
        let%map body_str = Cohttp_async.Body.to_string resp_body in
        let code = Cohttp.Response.status resp |> Cohttp.Code.code_of_status in
        (code, body_str))
  with
  | Ok pair -> Ok pair
  | Error exn -> Error exn

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
