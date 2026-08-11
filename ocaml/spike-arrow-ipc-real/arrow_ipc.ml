(* OCaml binding to the real Arrow C++ IPC codec (arrow_ipc_stubs.cc).

   Proves DESIGN.md §8.5.2: OCaml can turn columnar data into Arrow IPC bytes and
   back via a thin C-ABI binding to Arrow C++ (RecordBatchStreamWriter / Reader).
   Results-over-exceptions at the OCaml surface, per the golden rules.

   The C side builds/parses the RecordBatch and hands IPC bytes across; the OCaml
   side owns the [bytes] and the round-trip assertions. In the real SDK these IPC
   bytes become the opaque FlightData.data_body (native Flight DoPut, spike-flight/). *)

(* --- external C stubs (see arrow_ipc_stubs.cc) --- *)

(* Encode: takes the columns as OCaml arrays; returns IPC bytes or an error msg.
   Implemented as a C stub that allocates, which we copy into OCaml [bytes]. *)
external c_encode :
  int array -> string array -> (bytes, string) result
  = "zbi_ocaml_encode_int_str"

(* Decode: takes IPC bytes; returns (ids, names) or an error msg. *)
external c_decode :
  bytes -> (int array * string array, string) result
  = "zbi_ocaml_decode_int_str"

type row = { id : int; name : string }

let encode (rows : row list) : (bytes, string) result =
  let ids = Array.of_list (List.map (fun r -> r.id) rows) in
  let names = Array.of_list (List.map (fun r -> r.name) rows) in
  c_encode ids names

let decode (ipc : bytes) : (row list, string) result =
  match c_decode ipc with
  | Error _ as e -> e
  | Ok (ids, names) ->
      Ok
        (List.map2
           (fun id name -> { id; name })
           (Array.to_list ids) (Array.to_list names))
