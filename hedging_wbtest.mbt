// gRPC hedging policy (← the `hedgingPolicy` of a method config).

///|
test "hedging: attempts originate up to the cap, honouring gRPC's ceiling of 5" {
  let policy = HedgingPolicy::default()
  // Default is 3 attempts: three may start, the fourth may not.
  assert_eq(policy.can_start_another(0), true)
  assert_eq(policy.can_start_another(2), true)
  assert_eq(policy.can_start_another(3), false)
  // A policy configured above gRPC's ceiling is clamped to 5.
  let greedy = { ..policy, max_attempts: 8 }
  assert_eq(greedy.attempt_cap(), 5)
  assert_eq(greedy.can_start_another(4), true)
  assert_eq(greedy.can_start_another(5), false)
}

///|
test "hedging: OK and fatal errors commit; a non-fatal error keeps hedging" {
  let policy = HedgingPolicy::default()
  // OK commits — take the success.
  assert_eq(policy.should_commit(0), true)
  // UNAVAILABLE is non-fatal by default: don't commit, keep hedging.
  assert_eq(policy.should_commit(Status::code(Unavailable)), false)
  // INVALID_ARGUMENT is fatal: commit at once, retrying won't help.
  assert_eq(policy.should_commit(Status::code(InvalidArgument)), true)
  // DEADLINE_EXCEEDED is fatal too under the default (not listed non-fatal).
  assert_eq(policy.should_commit(Status::code(DeadlineExceeded)), true)
}

///|
test "hedging: delay is exposed and never negative" {
  let policy = HedgingPolicy::default()
  assert_eq(policy.delay_millis(), 500)
  // Fire-all-at-once.
  assert_eq({ ..policy, hedging_delay_millis: 0 }.delay_millis(), 0)
  // A misconfigured negative delay is floored at zero.
  assert_eq({ ..policy, hedging_delay_millis: -5 }.delay_millis(), 0)
}
