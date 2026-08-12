type t =
  | Transport_error of string
  | Stream_error of string
  | Auth_error of string
  | Server_status of { code : int; message : string }
  | Protocol_error of string
  | Timeout of string
  | Backpressure of string

let to_string = function
  | Transport_error s -> Printf.sprintf "Transport_error: %s" s
  | Stream_error s -> Printf.sprintf "Stream_error: %s" s
  | Auth_error s -> Printf.sprintf "Auth_error: %s" s
  | Server_status { code; message } ->
      Printf.sprintf "Server_status(%d): %s" code message
  | Protocol_error s -> Printf.sprintf "Protocol_error: %s" s
  | Timeout s -> Printf.sprintf "Timeout: %s" s
  | Backpressure s -> Printf.sprintf "Backpressure: %s" s

(* gRPC status codes that the service/transport may surface transiently.
   14 = UNAVAILABLE, 4 = DEADLINE_EXCEEDED, 8 = RESOURCE_EXHAUSTED,
   10 = ABORTED. These mirror the Rust core's retryable classification. *)
let retryable_status_code = function
  | 14 | 4 | 8 | 10 -> true
  | _ -> false

let is_retryable = function
  | Transport_error _ | Timeout _ -> true
  | Server_status { code; _ } -> retryable_status_code code
  (* Backpressure is a producer-side [Fail]-policy signal, not a stream fault: it
     must NOT trigger recovery (the stream is healthy; the caller over-produced). *)
  | Auth_error _ | Protocol_error _ | Stream_error _ | Backpressure _ -> false
