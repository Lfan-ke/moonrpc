// The multiplexed channel: concurrent calls sharing one h2c connection through the shared
// read pump. This is the native proof that the demux loop routes each stream's reply to
// the right waiting call.

///|
async test "mux channel: two concurrent unary calls multiplex on one connection" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register("/echo.E/Say", req => cat(b"echo:", req))
    let ts = g.spawn(() => server.serve(port=18108))
    @async.sleep(300)
    let ch = MuxChannel::connect("127.0.0.1", 18108)
    let tp = g.spawn(() => ch.read_pump())
    // Both calls are in flight at once — distinct streams on the same connection.
    let t1 = g.spawn(() => ch.unary("/echo.E/Say", b"one"))
    let t2 = g.spawn(() => ch.unary("/echo.E/Say", b"two"))
    let r1 = t1.wait()
    let r2 = t2.wait()
    assert_eq(r1.grpc_status, 0)
    assert_eq(r2.grpc_status, 0)
    assert_eq(r1.messages[0] == b"echo:one", true)
    assert_eq(r2.messages[0] == b"echo:two", true)
    // Cancelling the server closes the connection, so the pump's blocked read gets EOF
    // and exits on its own (a local close doesn't interrupt a pending native read).
    ts.cancel()
    let _ = tp.wait()
    ch.close()
  })
}

///|
async test "mux channel: keepalive keeps a live connection healthy by pinging" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register("/echo.E/Say", req => cat(b"echo:", req))
    let ts = g.spawn(() => server.serve(port=18110))
    @async.sleep(300)
    let ch = MuxChannel::connect("127.0.0.1", 18110)
    let tp = g.spawn(() => ch.read_pump())
    // Ping after 120 ms idle; a missing ACK within 300 ms would declare the connection dead.
    let tk = g.spawn(() => {
      ch.run_keepalive(keepalive_time=120L, keepalive_timeout=300L)
    })
    // Stay idle across several ping/pong cycles — the live server ACKs every PING.
    @async.sleep(600)
    // Still healthy: a call succeeds because the ACKs kept the connection from timing out.
    // Bounded so that if keepalive wrongly closed the connection, this fails cleanly.
    let r = (
      @async.with_timeout_opt(2000, () => ch.unary("/echo.E/Say", b"x")) :
      @moonrpc.CallReply?)
    match r {
      Some(reply) => {
        assert_eq(reply.grpc_status, 0)
        assert_eq(reply.messages[0] == b"echo:x", true)
      }
      None => fail("keepalive closed a healthy connection")
    }
    tk.cancel()
    ts.cancel()
    let _ = tp.wait()
    ch.close()
  })
}

///|
async test "mux channel: many concurrent calls on one connection each get their own reply" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register("/echo.E/Say", req => cat(b"r:", req))
    let ts = g.spawn(() => server.serve(port=18109))
    @async.sleep(300)
    let ch = MuxChannel::connect("127.0.0.1", 18109)
    let tp = g.spawn(() => ch.read_pump())
    // Five concurrent calls; each reply must match its own request.
    let payloads = [b"a", b"b", b"c", b"d", b"e"]
    let tasks = []
    for p in payloads {
      tasks.push(g.spawn(() => ch.unary("/echo.E/Say", p)))
    }
    for i in 0..<payloads.length() {
      let r = tasks[i].wait()
      assert_eq(r.messages[0] == cat(b"r:", payloads[i]), true)
    }
    // Cancelling the server closes the connection, so the pump's blocked read gets EOF
    // and exits on its own (a local close doesn't interrupt a pending native read).
    ts.cancel()
    let _ = tp.wait()
    ch.close()
  })
}
