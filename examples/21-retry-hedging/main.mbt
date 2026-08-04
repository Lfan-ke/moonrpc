// The client resilience policies, as pure decision logic: retry re-issues a failed call
// after an exponential backoff; hedging fires parallel attempts on a delay and commits on
// the first fatal status. This walks a retry policy's should-retry decisions and backoff
// schedule, then a hedging policy's start / commit decisions — the whole surface the async
// Channel drives (it does the sleeping and the socket work; these say whether and when).
//
//     moon run examples/21-retry-hedging

///|
fn status_label(code : Int) -> String {
  match code {
    0 => "OK"
    4 => "DEADLINE_EXCEEDED"
    14 => "UNAVAILABLE"
    _ => "code \{code}"
  }
}

///|
fn run() -> Unit {
  // Retry: the conventional default retries only UNAVAILABLE, up to 3 attempts.
  let retry = @moonrpc.RetryPolicy::default()
  let unavailable = @moonrpc.Status::code(Unavailable)
  let deadline = @moonrpc.Status::code(DeadlineExceeded)
  println("retry policy (default):")
  println(
    "  UNAVAILABLE after attempt 1: \{retry.should_retry(unavailable, 1)}",
  )
  println(
    "  UNAVAILABLE after attempt 3: \{retry.should_retry(unavailable, 3)} (attempts exhausted)",
  )
  println(
    "  DEADLINE_EXCEEDED after 1:   \{retry.should_retry(deadline, 1)} (not a retryable code)",
  )
  println("  OK after attempt 1:          \{retry.should_retry(0, 1)}")

  // A policy with a longer schedule shows the exponential backoff (the deterministic cap
  // the jitter is drawn under).
  let long : @moonrpc.RetryPolicy = {
    max_attempts: 5,
    initial_backoff_millis: 100,
    max_backoff_millis: 1000,
    backoff_multiplier: 2.0,
    retryable: [unavailable],
  }
  println("backoff schedule (100ms, x2, cap 1000ms):")
  for i = 1; i <= 5; i = i + 1 {
    println("  before retry \{i}: \{long.backoff_millis(i)} ms")
  }

  // Hedging: parallel attempts every delay, committing on the first fatal status.
  let hedge = @moonrpc.HedgingPolicy::default()
  println("hedging policy (default):")
  println("  attempt cap: \{hedge.attempt_cap()}")
  println("  delay between attempts: \{hedge.delay_millis()} ms")
  println("  can start a 3rd attempt: \{hedge.can_start_another(2)}")
  println(
    "  can start a 4th attempt: \{hedge.can_start_another(3)} (cap reached)",
  )
  println(
    "  commit on \{status_label(unavailable)}: \{hedge.should_commit(unavailable)} (non-fatal, keep hedging)",
  )
  println(
    "  commit on \{status_label(deadline)}: \{hedge.should_commit(deadline)} (fatal, stop)",
  )
  println("  commit on OK: \{hedge.should_commit(0)}")
}

///|
#coverage.skip
fn main {
  run()
}
