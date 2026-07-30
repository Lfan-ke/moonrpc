// End-to-end load balancing: a `ManagedChannel` dials two real `GrpcServer`s over actual
// sockets and its picker spreads calls across them. Each backend stamps its own identity on
// the reply, so the distribution is observable — this is the native proof that the pure pool
// + picker actually drive real connections, and the mutation-verification target for it.

///|
async test "managed channel: round-robin spreads calls across two live backends" {
  @async.with_task_group(g => {
    let a = GrpcServer::new()
    a.register("/echo.E/Say", req => cat(b"A:", req))
    let b = GrpcServer::new()
    b.register("/echo.E/Say", req => cat(b"B:", req))
    let ta = g.spawn(() => a.serve(port=18101))
    let tb = g.spawn(() => b.serve(port=18102))
    @async.sleep(300)
    let chan = ManagedChannel::connect(
      [("127.0.0.1", 18101), ("127.0.0.1", 18102)],
      policy=RoundRobin,
    )
    // Both backends dialed and READY, in resolver order.
    assert_eq(chan.ready_addresses(), ["127.0.0.1:18101", "127.0.0.1:18102"])
    // Four calls alternate A, B, A, B — the picker cycles the ready set.
    let r1 = chan.unary("/echo.E/Say", b"x")
    let r2 = chan.unary("/echo.E/Say", b"x")
    let r3 = chan.unary("/echo.E/Say", b"x")
    let r4 = chan.unary("/echo.E/Say", b"x")
    assert_eq(r1.messages[0] == b"A:x", true)
    assert_eq(r2.messages[0] == b"B:x", true)
    assert_eq(r3.messages[0] == b"A:x", true)
    assert_eq(r4.messages[0] == b"B:x", true)
    chan.close()
    ta.cancel()
    tb.cancel()
  })
}

///|
async test "managed channel: pick_first pins every call to the first backend" {
  @async.with_task_group(g => {
    let a = GrpcServer::new()
    a.register("/echo.E/Say", req => cat(b"A:", req))
    let b = GrpcServer::new()
    b.register("/echo.E/Say", req => cat(b"B:", req))
    let ta = g.spawn(() => a.serve(port=18103))
    let tb = g.spawn(() => b.serve(port=18104))
    @async.sleep(300)
    // Default policy is PickFirst.
    let chan = ManagedChannel::connect([
      ("127.0.0.1", 18103),
      ("127.0.0.1", 18104),
    ])
    let r1 = chan.unary("/echo.E/Say", b"x")
    let r2 = chan.unary("/echo.E/Say", b"x")
    assert_eq(r1.messages[0] == b"A:x", true)
    assert_eq(r2.messages[0] == b"A:x", true)
    chan.close()
    ta.cancel()
    tb.cancel()
  })
}

///|
async test "managed channel: a backend that fails to dial is skipped; calls route to the live one" {
  @async.with_task_group(g => {
    let a = GrpcServer::new()
    a.register("/echo.E/Say", req => cat(b"A:", req))
    let ta = g.spawn(() => a.serve(port=18105))
    @async.sleep(300)
    // 18999 has nothing listening: its dial fails and its sub-connection never goes READY.
    let chan = ManagedChannel::connect(
      [("127.0.0.1", 18999), ("127.0.0.1", 18105)],
      policy=RoundRobin,
    )
    // Only the live backend is ready — the dead one is filtered out.
    assert_eq(chan.ready_addresses(), ["127.0.0.1:18105"])
    let r1 = chan.unary("/echo.E/Say", b"x")
    let r2 = chan.unary("/echo.E/Say", b"x")
    assert_eq(r1.messages[0] == b"A:x", true)
    assert_eq(r2.messages[0] == b"A:x", true)
    chan.close()
    ta.cancel()
  })
}
