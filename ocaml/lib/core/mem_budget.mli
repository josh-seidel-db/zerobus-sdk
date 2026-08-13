(** A smart, portable byte budget for the un-acked replay buffer (DESIGN §12.3,
    backpressure). The un-acked buffer must never drop records (silent data loss
    on recovery), but must also not grow until it OOMs the process. So when
    [max_inflight_bytes] is left [None], the stream derives its ceiling from how
    much memory is actually available to the process right now, via this module.

    Every probe is best-effort and fully portable: a failure (unsupported OS,
    sandbox, missing file) falls back to a safe conservative default rather than
    raising. *)

val default_budget_bytes : unit -> int
(** The smart default byte budget for the un-acked buffer: a fraction (25%) of
    the memory currently available to the process, clamped to [[64 MiB, 8 GiB]].
    On platforms without a dependency-free availability probe (anything but
    Linux [/proc/meminfo]), or on any probe failure, returns the 64 MiB floor —
    never unbounded, never zero. Callers wanting a larger ceiling set
    [max_inflight_bytes] explicitly. *)
