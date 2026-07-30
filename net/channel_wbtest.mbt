// End-to-end: the real Channel client calls a real GrpcServer over an actual TCP
// socket — unary, server-streaming, client-streaming, a Health/Check, and deadline
// cancellation against a deliberately slow server. This is the "can actually run"
// milestone and the native mutation-verification target.

///|
async test "channel: unary and Health/Check over a real socket, multiplexed on one connection" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register("/echo.Echo/Say", req => cat(b"echo:", req))
    let health = @moonrpc.HealthService::new()
    health.set_status("pkg.Svc", @moonrpc.Serving)
    server.register_health(health)
    let task = g.spawn(() => server.serve(port=18081))
    @async.sleep(300)
    let chan = Channel::connect("127.0.0.1", 18081)
    // first call: stream id 1.
    let r1 = chan.unary("/echo.Echo/Say", b"world")
    assert_eq(r1.status == b"200", true)
    assert_eq(r1.grpc_status, 0)
    assert_eq(r1.messages[0] == b"echo:world", true)
    // second call on the same long-lived connection: stream id 3.
    let r2 = chan.unary("/echo.Echo/Say", b"again")
    assert_eq(r2.messages[0] == b"echo:again", true)
    // a Health/Check for a serving service.
    let hr = chan.unary(
      @moonrpc.health_check_path,
      @moonrpc.encode_health_request(b"pkg.Svc"),
    )
    assert_eq(hr.grpc_status, 0)
    assert_eq(
      @moonrpc.decode_health_response(hr.messages[0]) == @moonrpc.Serving,
      true,
    )
    chan.close()
    task.cancel()
  })
}

///|
async test "channel: a server-streaming call receives every framed reply in order over a socket" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register_server_streaming("/count.C/Up", (_ctx, req) => {
      [cat(req, b"-1"), cat(req, b"-2"), cat(req, b"-3")]
    })
    let task = g.spawn(() => server.serve(port=18082))
    @async.sleep(300)
    let chan = Channel::connect("127.0.0.1", 18082)
    let reply = chan.server_streaming("/count.C/Up", b"n")
    assert_eq(reply.grpc_status, 0)
    assert_eq(reply.messages.length(), 3)
    assert_eq(reply.messages[0] == b"n-1", true)
    assert_eq(reply.messages[1] == b"n-2", true)
    assert_eq(reply.messages[2] == b"n-3", true)
    chan.close()
    task.cancel()
  })
}

///|
async test "channel: a client-streaming call sends many messages for one reply over a socket" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register_client_streaming("/sum.S/Add", (_ctx, msgs) => {
      let buf = @buffer.Buffer()
      buf.write_bytes(b"got")
      for m in msgs {
        buf.write_byte(b'/')
        buf.write_bytes(m)
      }
      buf.to_bytes()
    })
    let task = g.spawn(() => server.serve(port=18083))
    @async.sleep(300)
    let chan = Channel::connect("127.0.0.1", 18083)
    let reply = chan.client_streaming("/sum.S/Add", [b"a", b"b", b"c"])
    assert_eq(reply.grpc_status, 0)
    assert_eq(reply.messages.length(), 1)
    assert_eq(reply.messages[0] == b"got/a/b/c", true)
    chan.close()
    task.cancel()
  })
}

///|
async test "channel: a bidi-streaming call exchanges a run of messages over a socket" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register_bidi("/chat.C/Talk", _ctx => {
      on_message: m => [cat(b"re:", m)],
      on_end: () => [b"bye"],
    })
    let task = g.spawn(() => server.serve(port=18085))
    @async.sleep(300)
    let chan = Channel::connect("127.0.0.1", 18085)
    let reply = chan.bidi("/chat.C/Talk", [b"hi", b"there"])
    assert_eq(reply.grpc_status, 0)
    // one reply per request (on_message) plus the on_end reply, in order.
    assert_eq(reply.messages.length(), 3)
    assert_eq(reply.messages[0] == b"re:hi", true)
    assert_eq(reply.messages[1] == b"re:there", true)
    assert_eq(reply.messages[2] == b"bye", true)
    chan.close()
    task.cancel()
  })
}

///|
/// Drive one h2c connection but sleep `delay` milliseconds before writing any
/// non-empty batch of response frames — a server slow enough to blow a short client
/// deadline.
async fn drive_slow(
  conn : @socket.Tcp,
  engine : @moonrpc.H2Server,
  delay : Int,
) -> Unit {
  let preface = conn.read_exactly(@moonrpc.connection_preface.length())
  if !@moonrpc.has_connection_preface(preface) {
    return
  }
  write_frames(conn, engine.preface())
  for ;; {
    let frame = read_frame(conn)
    let out = engine.feed(frame)
    if out.length() > 0 {
      @async.sleep(delay)
    }
    write_frames(conn, out)
    if engine.goaway_received() {
      break
    }
  }
}

///|
async test "channel: a call past its deadline is cancelled with DEADLINE_EXCEEDED" {
  @async.with_task_group(g => {
    let server = @socket.TcpServer::TcpServer(
      @socket.Addr::parse("127.0.0.1:18084"),
      reuse_addr=true,
    )
    let task = g.spawn(() => {
      server.run_forever((conn, _addr) => {
        let engine = @moonrpc.H2Server::new()
        engine.register("/slow.S/Wait", req => cat(b"ok:", req))
        drive_slow(conn, engine, 400) catch {
          _ => ()
        }
      })
    })
    @async.sleep(300)
    let chan = Channel::connect("127.0.0.1", 18084)
    // the server takes ~400ms to answer; the client's deadline is 120ms.
    let reply = chan.unary("/slow.S/Wait", b"x", timeout_millis=Some(120))
    assert_eq(reply.grpc_status, @moonrpc.Status::code(DeadlineExceeded))
    chan.close()
    task.cancel()
  })
}

///|
async test "channel: -bin metadata base64s on the wire and round-trips as raw bytes" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    let raw = b"\x00\x01\xff\x10"
    server.register_unary("/m.M/Echo", (ctx, _req) => {
      // The request's -bin metadata is surfaced already base64-decoded; echo it back
      // as a -bin trailer, which the server re-encodes on the wire.
      match ctx.metadata_get(b"x-bin") {
        Some(v) => ctx.add_trailer(b"echo-bin", v)
        None => ()
      }
      b"ok"
    })
    let task = g.spawn(() => server.serve(port=18086))
    @async.sleep(300)
    let chan = Channel::connect("127.0.0.1", 18086)
    let reply = chan.unary("/m.M/Echo", b"", metadata=[
      { name: b"x-bin", value: raw },
    ])
    assert_eq(reply.grpc_status, 0)
    let mut echoed = b""
    for t in reply.trailers {
      if t.name == b"echo-bin" {
        echoed = t.value
      }
    }
    // Raw bytes, not the base64 text that travelled on the wire.
    assert_eq(echoed == raw, true)
    chan.close()
    task.cancel()
  })
}

///|
async test "channel: a failing call carries a google.rpc.Status in grpc-status-details-bin" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register_unary("/e.E/Fail", (ctx, _req) => {
      ctx.fail(@moonrpc.Status::code(NotFound), "no such row")
      ctx.add_error_detail(b"detail-any-bytes")
      b""
    })
    let task = g.spawn(() => server.serve(port=18087))
    @async.sleep(300)
    let chan = Channel::connect("127.0.0.1", 18087)
    let reply = chan.unary("/e.E/Fail", b"")
    assert_eq(reply.grpc_status, @moonrpc.Status::code(NotFound))
    // The grpc-status-details-bin trailer carries a serialized google.rpc.Status,
    // arriving already base64-decoded (the -bin convention).
    // For a trailers-only failure the status rides the single HEADERS frame, which
    // the client surfaces as initial metadata, so look in both header sets.
    let mut details = b""
    for t in reply.headers {
      if t.name == b"grpc-status-details-bin" {
        details = t.value
      }
    }
    for t in reply.trailers {
      if t.name == b"grpc-status-details-bin" {
        details = t.value
      }
    }
    let r = @moonrpc.PbReader::new(details)
    let mut code = 0
    let mut msg = ""
    let mut detail = b""
    while !r.eof() {
      let (field, wire) = r.read_tag()
      match field {
        1 => code = r.read_int32()
        2 => msg = r.read_string()
        3 => detail = r.read_bytes()
        _ => r.skip(wire)
      }
    }
    assert_eq(code, @moonrpc.Status::code(NotFound))
    assert_eq(msg == "no such row", true)
    assert_eq(detail == b"detail-any-bytes", true)
    chan.close()
    task.cancel()
  })
}
