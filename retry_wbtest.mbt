///|
test "retry: should_retry honours attempts, retryable codes, and OK" {
  let p = RetryPolicy::default()
  // a retryable status with attempts left is retried.
  assert_eq(p.should_retry(Status::code(Unavailable), 1), true)
  assert_eq(p.should_retry(Status::code(Unavailable), 2), true)
  // the last allowed attempt does not retry again (max_attempts = 3).
  assert_eq(p.should_retry(Status::code(Unavailable), 3), false)
  // a non-retryable status is never retried.
  assert_eq(p.should_retry(Status::code(NotFound), 1), false)
  // OK is never retried.
  assert_eq(p.should_retry(0, 1), false)
}

///|
test "retry: custom retryable set" {
  let p = RetryPolicy::{
    max_attempts: 5,
    initial_backoff_millis: 10,
    max_backoff_millis: 100,
    backoff_multiplier: 3.0,
    retryable: [Status::code(ResourceExhausted), Status::code(Unavailable)],
  }
  assert_eq(p.should_retry(Status::code(ResourceExhausted), 1), true)
  assert_eq(p.should_retry(Status::code(Unavailable), 4), true)
  assert_eq(p.should_retry(Status::code(Internal), 1), false)
}

///|
test "retry: the effective attempt count is capped at 5" {
  let p = RetryPolicy::{
    max_attempts: 100,
    initial_backoff_millis: 10,
    max_backoff_millis: 100,
    backoff_multiplier: 2.0,
    retryable: [Status::code(Unavailable)],
  }
  // Even with max_attempts 100, the 5th attempt is the last.
  assert_eq(p.should_retry(Status::code(Unavailable), 4), true)
  assert_eq(p.should_retry(Status::code(Unavailable), 5), false)
}

///|
test "retry: backoff grows geometrically and saturates at the cap" {
  let p = RetryPolicy::default()
  assert_eq(p.backoff_millis(1), 100)
  assert_eq(p.backoff_millis(2), 200)
  assert_eq(p.backoff_millis(3), 400)
  assert_eq(p.backoff_millis(4), 800)
  // 1600 would exceed the 1000 ms cap.
  assert_eq(p.backoff_millis(5), 1000)
  assert_eq(p.backoff_millis(20), 1000)
}
