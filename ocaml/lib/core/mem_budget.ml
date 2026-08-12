(** A smart, portable byte budget for the un-acked replay buffer (DESIGN §12.3,
    backpressure). The un-acked buffer must never drop records (that is silent data
    loss on recovery), but it also must not grow until it OOMs the process. So when
    [max_inflight_bytes] is left [None], we derive a ceiling from how much memory is
    actually available to the process right now.

    Best-effort and fully portable: every probe is wrapped so a failure (unsupported
    OS, sandbox, missing tool) falls back to a safe conservative default rather than
    raising. Modern machines handle far more than 1,000,000 small records, so the
    default budget deliberately scales with available RAM instead of a hard count. *)

(* Conservative floor/ceiling so we never pick something absurd on a probe failure
   or a tiny/huge machine. *)
let min_budget_bytes = 64 * 1024 * 1024 (* 64 MiB: always allow a healthy buffer *)
let max_budget_bytes = 8 * 1024 * 1024 * 1024 (* 8 GiB: never reserve more than this *)

(* Fraction of *available* (not total) memory we are willing to hold as un-acked
   replay buffer. Leaves the large majority for the rest of the process + OS. *)
let budget_fraction = 0.25

let clamp lo hi x = if x < lo then lo else if x > hi then hi else x

(* Read an integer value (in kB) for [key] from a "key: N kB" style file line, e.g.
   Linux /proc/meminfo "MemAvailable:   12345 kB". Returns bytes. *)
let read_meminfo_kb (path : string) (key : string) : int option =
  try
    let ic = open_in path in
    Fun.protect
      ~finally:(fun () -> try close_in ic with _ -> ())
      (fun () ->
        let rec loop () =
          match input_line ic with
          | line ->
              if String.length line >= String.length key
                 && String.sub line 0 (String.length key) = key
              then
                (* parse the first integer on the line *)
                let n = ref 0 and seen = ref false and i = ref 0 in
                let len = String.length line in
                while !i < len && not (!seen && (line.[!i] < '0' || line.[!i] > '9')) do
                  let c = line.[!i] in
                  if c >= '0' && c <= '9' then (n := (!n * 10) + (Char.code c - 48); seen := true);
                  incr i
                done;
                if !seen then Some (!n * 1024) else loop ()
              else loop ()
          | exception End_of_file -> None
        in
        loop ())
  with _ -> None

(* Available bytes to the process, best-effort and dependency-free:
   - Linux: /proc/meminfo MemAvailable (a plain file read, no [unix] needed).
   Other OSes (macOS/BSD/Windows) have no equally-portable file probe without
   spawning a tool or binding C, so we return [None] and let the caller fall back to
   the conservative floor. This is intentional: the budget must be SAFE everywhere,
   and a 64 MiB floor already dwarfs typical un-acked working sets while never
   risking an OOM. Callers who want a larger explicit budget set [max_inflight_bytes]. *)
let available_bytes () : int option =
  read_meminfo_kb "/proc/meminfo" "MemAvailable"

(* The smart default byte budget: [budget_fraction] of available memory, clamped to
   [[min_budget_bytes, max_budget_bytes]]. If the probe fails, fall back to the min
   floor (64 MiB) — never unbounded, never zero. *)
let default_budget_bytes () : int =
  match available_bytes () with
  | Some avail when avail > 0 ->
      clamp min_budget_bytes max_budget_bytes
        (int_of_float (float_of_int avail *. budget_fraction))
  | _ -> min_budget_bytes
