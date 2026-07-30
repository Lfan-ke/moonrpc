// gRPC hedging policy (the `hedgingPolicy` of a service config's method config): unlike
// retry, hedging fires attempts *in parallel* — an attempt every `hedging_delay_millis`
// without waiting for the previous to fail — and commits the call on the first attempt
// whose status is fatal (OK, or an error outside `non_fatal`). A non-fatal failure lets
// hedging keep going. This is pure decision logic — all-backend and total — that the async
// `Channel` drives: it decides *whether another attempt may start* and *whether a finished
// attempt commits the call*; the socket layer originates the parallel attempts, waits the
// delay, and cancels the losers.

///|
/// A method's hedging policy. `max_attempts` counts every parallel attempt (gRPC caps it at
/// 5); a new attempt originates every `hedging_delay_millis` while attempts remain. A
/// finished attempt commits the whole call unless its status is one of `non_fatal`, in which
/// case hedging continues.
pub(all) struct HedgingPolicy {
  max_attempts : Int
  hedging_delay_millis : Int
  non_fatal : Array[Int]
}

///|
/// A conventional default: up to 3 parallel attempts, a fresh one every 500 ms, treating
/// only `UNAVAILABLE` as non-fatal — so a transient transport failure keeps hedging while a
/// real application error commits at once.
pub fn HedgingPolicy::default() -> HedgingPolicy {
  {
    max_attempts: 3,
    hedging_delay_millis: 500,
    non_fatal: [Status::code(Unavailable)],
  }
}

///|
/// The effective attempt cap: the configured `max_attempts`, but never above gRPC's ceiling
/// of 5.
pub fn HedgingPolicy::attempt_cap(self : HedgingPolicy) -> Int {
  if self.max_attempts > 5 {
    5
  } else {
    self.max_attempts
  }
}

///|
/// Whether another hedged attempt may originate given `attempts_started` (the number already
/// launched): only while attempts remain under the cap.
pub fn HedgingPolicy::can_start_another(
  self : HedgingPolicy,
  attempts_started : Int,
) -> Bool {
  attempts_started < self.attempt_cap()
}

///|
/// Whether a finished attempt with `status_code` commits the whole call — stopping hedging
/// and returning this result. OK commits (success), and so does any error *not* listed as
/// non-fatal (a real failure retrying won't fix). A non-fatal error does not commit; hedging
/// continues.
pub fn HedgingPolicy::should_commit(
  self : HedgingPolicy,
  status_code : Int,
) -> Bool {
  if status_code == 0 {
    return true
  }
  for code in self.non_fatal {
    if code == status_code {
      return false
    }
  }
  true
}

///|
/// The delay between originating hedged attempts. The async driver waits this long before
/// launching the next parallel attempt; `0` fires them all at once.
pub fn HedgingPolicy::delay_millis(self : HedgingPolicy) -> Int {
  if self.hedging_delay_millis < 0 {
    0
  } else {
    self.hedging_delay_millis
  }
}
