// The gRPC sub-connection connectivity state machine (← gRPC `connectivity.State`).

///|
test "connectivity: a subconn walks IDLE→CONNECTING→READY, fails, retries, and shuts down" {
  let sc = SubConn::new("host:1")
  assert_eq(sc.state, Idle)
  assert_eq(sc.is_ready(), false)
  // Connect and come up ready.
  sc.connect()
  assert_eq(sc.state, Connecting)
  sc.on_connected()
  assert_eq(sc.state, Ready)
  assert_eq(sc.is_ready(), true)
  // A ready connection going idle returns to IDLE.
  sc.on_idle()
  assert_eq(sc.state, Idle)
  // Connect again, but this time it fails → transient failure.
  sc.connect()
  sc.on_failure()
  assert_eq(sc.state, TransientFailure)
  assert_eq(sc.is_ready(), false)
  // Transient failure retries via CONNECTING.
  sc.connect()
  assert_eq(sc.state, Connecting)
  // Shutdown is terminal: further connects are no-ops.
  sc.shutdown()
  assert_eq(sc.state, Shutdown)
  sc.connect()
  assert_eq(sc.state, Shutdown)
  sc.on_failure()
  assert_eq(sc.state, Shutdown)
}
