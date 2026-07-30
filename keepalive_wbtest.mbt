// The gRPC client keepalive state machine (← `keepalive_time` / `keepalive_timeout`).

///|
test "keepalive: pings after idle, activity/ack reset, and a missing ack times out" {
  let ka = Keepalive::new(keepalive_time=100L, keepalive_timeout=20L, now=0L)
  // Not idle long enough yet.
  assert_eq(ka.should_ping(50L), false)
  // Idle at least keepalive_time → ping is due.
  assert_eq(ka.should_ping(100L), true)
  // Activity resets the idle timer.
  ka.on_activity(100L)
  assert_eq(ka.should_ping(150L), false) // idle 50 < 100
  assert_eq(ka.should_ping(200L), true) // idle 100 >= 100
  // Once a ping is outstanding, no new ping is sent.
  ka.on_ping_sent(200L)
  assert_eq(ka.should_ping(400L), false)
  // Before the ack deadline the connection is not dead.
  assert_eq(ka.is_timed_out(215L), false) // 15 < 20
  // Past the deadline with no ack, the connection is declared dead.
  assert_eq(ka.is_timed_out(220L), true) // 20 >= 20
  // An ack clears the outstanding ping and counts as activity.
  ka.on_ping_ack(218L)
  assert_eq(ka.is_timed_out(300L), false)
  assert_eq(ka.should_ping(250L), false) // idle since ack (218) = 32 < 100
  assert_eq(ka.should_ping(318L), true) // idle 100 → ping again
}
