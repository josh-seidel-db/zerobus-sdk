(** The Arrow Flight [DoPut] {!Stream.PROTOCOL} instance for
    [record_type = Arrow] (DESIGN §8.5). Encodes/decodes only the Flight
    protobuf messages, carrying the Arrow IPC payload as an opaque
    [FlightData.data_body] — so it needs no libarrow and can drive
    {!Stream.Make_with_protocol} from any runtime without pulling the Arrow
    codec into the JSON/Proto closure. The caller supplies the IPC bytes (via
    the optional [zerobus-arrow] codec). *)

include Stream.PROTOCOL
