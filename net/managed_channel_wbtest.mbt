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
// A DNS A response resolving example.com -> 127.0.0.1 (0x7f000001), id 0x2a2a.
let mock_localhost_response : Bytes = b"\x2a\x2a\x81\x80\x00\x01\x00\x01\x00\x00\x00\x00\x07example\x03com\x00\x00\x01\x00\x01\xc0\x0c\x00\x01\x00\x01\x00\x00\x01\x2c\x00\x04\x7f\x00\x00\x01"

///|
async test "managed channel: dns_connect resolves a name over UDP then calls the resolved backend" {
  @async.with_task_group(g => {
    let backend = GrpcServer::new()
    backend.register("/echo.E/Say", req => cat(b"A:", req))
    let tb = g.spawn(() => backend.serve(port=18106))
    let dns = @socket.UdpServer(@socket.Addr::parse("127.0.0.1:15355"))
    let td = g.spawn(() => {
      let buf = FixedArray::make(512, b'\x00')
      let (_, addr) = dns.recvfrom(buf)
      dns.sendto(mock_localhost_response, addr)
    })
    @async.sleep(300)
    // Resolve example.com -> 127.0.0.1 and dial it at the backend's port.
    let chan = ManagedChannel::dns_connect(
      "127.0.0.1", 15355, "example.com", 18106,
    )
    assert_eq(chan.ready_addresses(), ["127.0.0.1:18106"])
    let r = chan.unary("/echo.E/Say", b"x")
    assert_eq(r.messages[0] == b"A:x", true)
    chan.close()
    dns.close()
    tb.cancel()
    td.cancel()
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
