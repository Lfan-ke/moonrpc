///|
/// Hedging exists to race attempts, so the parallel path is the one that matters.
/// A `Channel` drives one call at a time, so two attempts landing on the same
/// backend would put two readers on one socket — which aborts the event loop rather
/// than failing a call. With one backend there is nothing to hedge onto, so exactly
/// one attempt must run and the call must still answer normally.
async test "hedging with no delay does not double up on a single backend" {
  @async.with_task_group(g => {
    let count = [0]
    let backend = GrpcServer::new()
    backend.register("/echo.E/Say", req => {
      count[0] = count[0] + 1
      cat(b"A:", req)
    })
    let tb = g.spawn(() => backend.serve(port=18193))
    @async.sleep(300)
    let chan = ManagedChannel::connect([("127.0.0.1", 18193)])
    let policy = {
      ..@moonrpc.HedgingPolicy::default(),
      max_attempts: 3,
      hedging_delay_millis: 0,
    }
    let r = chan.unary_hedged("/echo.E/Say", b"x", policy)
    assert_eq(r.grpc_status, 0)
    assert_eq(r.messages[0] == b"A:x", true)
    assert_eq(count[0], 1)
    chan.close()
    tb.cancel()
  })
}

///|
/// Two backends and no delay is the case the fix exists for: both attempts start at
/// once, land on different channels, and the call answers normally instead of putting
/// two readers on one socket.
async test "hedging runs two attempts in parallel across two backends" {
  @async.with_task_group(g => {
    let a_hits = [0]
    let b_hits = [0]
    let a = GrpcServer::new()
    a.register("/echo.E/Say", req => {
      a_hits[0] = a_hits[0] + 1
      cat(b"A:", req)
    })
    let b = GrpcServer::new()
    b.register("/echo.E/Say", req => {
      b_hits[0] = b_hits[0] + 1
      cat(b"B:", req)
    })
    let ta = g.spawn(() => a.serve(port=18194))
    let tb2 = g.spawn(() => b.serve(port=18195))
    @async.sleep(300)
    let chan = ManagedChannel::connect([
      ("127.0.0.1", 18194),
      ("127.0.0.1", 18195),
    ])
    let policy = {
      ..@moonrpc.HedgingPolicy::default(),
      max_attempts: 2,
      hedging_delay_millis: 0,
    }
    let r = chan.unary_hedged("/echo.E/Say", b"x", policy)
    assert_eq(r.grpc_status, 0)
    // Whichever committed, at most one attempt reached each backend — never two on one.
    assert_eq(a_hits[0] <= 1, true)
    assert_eq(b_hits[0] <= 1, true)
    assert_eq(a_hits[0] + b_hits[0] >= 1, true)
    chan.close()
    ta.cancel()
    tb2.cancel()
  })
}
