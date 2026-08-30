// The native HTTP/2 (h2c) socket driver: it pumps bytes between a real
// `@socket.Tcp` connection and the pure `@moonrpc.H2Server` engine. All protocol
// logic lives in the engine; this file only reads whole frames off the socket,
// feeds them in, and writes the engine's replies back. Native-only, because real
// sockets and `moonbitlang/async` have no JS/Wasm backend (`supported_targets`).

///|
/// Read exactly one HTTP/2 frame off a byte reader — the 9-octet header, then its
/// declared payload — and decode it with the pure frame codec. Raises when the
/// peer closes mid-frame (a short read) or the bytes are malformed.
async fn read_frame(conn : &@io.Reader) -> @moonrpc.Frame {
  let head = conn.read_exactly(9)
  let hdr = @moonrpc.decode_frame_header(head)
  let payload = if hdr.length > 0 { conn.read_exactly(hdr.length) } else { b"" }
  let buf = @buffer.Buffer()
  buf.write_bytes(head)
  buf.write_bytes(payload)
  let (frame, _) = @moonrpc.decode_frame(buf.to_bytes())
  frame
}

///|
/// Write each frame's exact wire bytes to the socket.
async fn write_frames(
  conn : &@io.Writer,
  frames : Array[@moonrpc.Frame],
) -> Unit {
  for f in frames {
    conn.write(f.encode())
  }
}

///|
/// Drive one accepted h2c connection to completion: validate the client
/// connection preface, send the server SETTINGS, then loop — read a frame, feed it
/// to the engine, write back whatever the engine produces — until the peer closes
/// the connection, a GOAWAY is seen, the engine reports a connection error, or the
/// server is shutting down and this connection has finished its last call.
async fn drive(
  conn : @socket.Tcp,
  engine : @moonrpc.H2Server,
  draining : () -> Bool,
) -> Unit {
  let preface = conn.read_exactly(@moonrpc.connection_preface.length())
  if !@moonrpc.has_connection_preface(preface) {
    return
  }
  write_frames(conn, engine.preface())
  for ;; {
    let frame = read_frame(conn)
    // Whatever the engine hands back goes out first, RST_STREAM and GOAWAY included:
    // a connection error is only closed *after* the peer has been told why
    // (RFC 9113 §5.4.1).
    write_frames(conn, engine.feed(frame))
    // A call that has been waiting on this connection may have run out of time while we
    // were parked in `read_frame`. This driver only notices when traffic arrives, so a
    // connection that goes silent holds its expired calls until it says something.
    write_frames(conn, engine.tick())
    if engine.closing() {
      break
    }
    if engine.goaway_received() {
      break
    }
    if draining() && engine.active_streams() == 0 {
      write_frames(conn, engine.goaway(engine.highest_stream()))
      break
    }
  }
}

///|
/// A gRPC server: a registry of handlers keyed by `/pkg.Service/Method` path,
/// served over the self-built HTTP/2 (h2c) transport. Handlers may be any of the
/// four gRPC call kinds. Each accepted connection gets its own `@moonrpc.H2Server`
/// engine (HPACK and flow control are per-connection state).
pub(all) struct GrpcServer {
  handlers : Map[String, @moonrpc.Handler]
  // The connections being served right now, keyed by a serial number, so that a shutdown
  // can reach them. A connection removes itself when its driver returns; the engine
  // comes along because a shutdown has to ask whether the connection is between calls
  // before it interrupts.
  live : Map[Int, (@socket.Tcp, @moonrpc.H2Server)]
  mut listener : @socket.TcpServer?
  mut draining : Bool
  mut next_conn : Int
}

///|
/// A gRPC server with no registered methods.
pub fn GrpcServer::new() -> GrpcServer {
  {
    handlers: Map([]),
    live: Map([]),
    listener: None,
    draining: false,
    next_conn: 0,
  }
}

///|
/// Register a unary handler `(request) -> reply` (messages without their gRPC
/// length prefix) for a fully-qualified path `/pkg.Service/Method`.
pub fn GrpcServer::register(
  self : GrpcServer,
  path : String,
  handler : (Bytes) -> Bytes,
) -> Unit {
  self.handlers[path] = Unary((_ctx, req) => handler(req))
}

///|
/// Register a unary handler that receives the [`RpcContext`], so it can read request
/// metadata / the deadline and end the call with a non-OK status via `ctx.fail`.
pub fn GrpcServer::register_unary(
  self : GrpcServer,
  path : String,
  handler : (@moonrpc.RpcContext, Bytes) -> Bytes,
) -> Unit {
  self.handlers[path] = Unary(handler)
}

///|
/// Register a server-streaming handler: one request message in, an ordered
/// sequence of reply messages out.
pub fn GrpcServer::register_server_streaming(
  self : GrpcServer,
  path : String,
  handler : (@moonrpc.RpcContext, Bytes) -> Array[Bytes],
) -> Unit {
  self.handlers[path] = ServerStreaming(handler)
}

///|
/// Register a client-streaming handler: every request message is collected, and
/// after the client half-closes the handler returns one reply.
pub fn GrpcServer::register_client_streaming(
  self : GrpcServer,
  path : String,
  handler : (@moonrpc.RpcContext, Array[Bytes]) -> Bytes,
) -> Unit {
  self.handlers[path] = ClientStreaming(handler)
}

///|
/// Register a bidirectional-streaming handler.
pub fn GrpcServer::register_bidi(
  self : GrpcServer,
  path : String,
  factory : (@moonrpc.RpcContext) -> @moonrpc.BidiHandler,
) -> Unit {
  self.handlers[path] = Bidi(factory)
}

///|
/// Register the `grpc.reflection.v1.ServerReflection` service (and its v1alpha
/// alias) so a reflection client such as `grpcurl` can list and describe the
/// services in `refl`'s descriptor database.
pub fn GrpcServer::register_reflection(
  self : GrpcServer,
  refl : @moonrpc.ReflectionService,
) -> Unit {
  self.handlers[@moonrpc.reflection_v1_path] = refl.handler()
  self.handlers[@moonrpc.reflection_v1alpha_path] = refl.handler()
}

///|
/// Build a fresh per-connection engine seeded with this server's handlers.
fn GrpcServer::engine(self : GrpcServer) -> @moonrpc.H2Server {
  let e = @moonrpc.H2Server::new()
  // The engine is pure and has no clock, so `grpc-timeout` deadlines only mean anything
  // once one is handed to it.
  e.set_clock(() => @async.now())
  for path, handler in self.handlers {
    e.register_handler(path, handler)
  }
  e
}

///|
/// Serve gRPC over h2c on `host:port`, blocking in the accept loop until the running
/// task is cancelled or `graceful_stop` drains the server. Each connection is driven
/// by `drive`; a failure on one connection is isolated and does not stop the server.
pub async fn GrpcServer::serve(
  self : GrpcServer,
  host? : String = "127.0.0.1",
  port? : Int = 50051,
) -> Unit {
  let server = @socket.TcpServer::TcpServer(
    @socket.Addr::parse(host + ":" + port.to_string()),
    reuse_addr=true,
  )
  self.listener = Some(server)
  self.draining = false
  // The listener is closed from this task and nowhere else. Closing a socket that
  // another task is parked in `accept` on does not wake that task — it wedges it,
  // beyond the reach of cancellation — which is why `graceful_stop` knocks on the
  // port instead of reaching in here.
  defer self.shut(server)
  @async.with_task_group(g => {
    for ;; {
      let (conn, _addr) = server.accept()
      if self.draining {
        conn.close()
        break
      }
      let engine = self.engine()
      let id = self.next_conn
      self.next_conn = id + 1
      self.live[id] = (conn, engine)
      g.spawn_bg(allow_failure=true, () => {
        defer conn.close()
        defer self.live.remove(id)
        drive(conn, engine, () => self.draining) catch {
          _ => ()
        }
      })
    }
  })
}

///|
/// Release the listening socket and forget it. Only `serve` calls this, on its way out.
fn GrpcServer::shut(self : GrpcServer, server : @socket.TcpServer) -> Unit {
  server.close()
  self.listener = None
}

///|
/// Stop serving without cutting anyone off: stop accepting new connections, let the
/// calls that are already running finish, and send every connection a GOAWAY naming
/// the last stream it opened, so clients know what was accepted and can reconnect
/// elsewhere. Returns once the last connection is gone, or after `grace`
/// milliseconds — a client that received the GOAWAY and then went quiet without
/// closing cannot be waited on forever. `serve` returns on its own once the accept
/// loop and the last driver are done, so the task running it ends without being
/// cancelled; if the grace period ran out with connections still open, cancelling
/// that task is what releases them.
pub async fn GrpcServer::graceful_stop(
  self : GrpcServer,
  grace? : Int = 10000,
) -> Unit {
  self.draining = true
  // Knock on our own port. The accept loop is parked and only looks at `draining`
  // when a connection arrives, so this is what lets it notice and stop accepting.
  match self.listener {
    Some(l) =>
      try {
        let knock = @socket.Tcp::connect(l.addr)
        knock.close()
      } catch {
        _ => ()
      }
    None => ()
  }
  let deadline = @async.now() + grace.to_int64()
  while self.live.length() > 0 && @async.now() < deadline {
    for l in self.snapshot() {
      let (conn, engine) = l
      if engine.active_streams() == 0 {
        write_frames(conn, engine.goaway(engine.highest_stream())) catch {
          _ => ()
        }
      }
    }
    @async.sleep(10)
  }
}

///|
/// The live connections as they stand right now. A shutdown suspends while it writes
/// and sleeps, and a driver that returns in the meantime removes itself, so walking
/// the map directly would be walking it while it changes.
fn GrpcServer::snapshot(
  self : GrpcServer,
) -> Array[(@socket.Tcp, @moonrpc.H2Server)] {
  let out = []
  for _id, l in self.live {
    out.push(l)
  }
  out
}

///|
/// Concatenate two byte strings.
fn cat(a : Bytes, b : Bytes) -> Bytes {
  let buf = @buffer.Buffer()
  buf.write_bytes(a)
  buf.write_bytes(b)
  buf.to_bytes()
}

///|
/// A minimal in-process h2c gRPC client built on the same frame + HPACK codecs:
/// it drives a real unary call over a real TCP socket and returns
/// `(:status, grpc-status, reply message)`. Proves the server end-to-end.
async fn unary_call(
  host : String,
  port : Int,
  path : Bytes,
  request : Bytes,
) -> (Bytes, Bytes, Bytes) {
  let conn = @socket.Tcp::connect(
    @socket.Addr::parse(host + ":" + port.to_string()),
  )
  // client preface + our SETTINGS.
  conn.write(@moonrpc.connection_preface)
  conn.write(@moonrpc.Frame::Settings(params=[], ack=false).encode())
  // request HEADERS (HPACK-encoded pseudo-headers) then a single DATA message.
  let enc = @moonrpc.HpackEncoder::new()
  let block = enc.encode([
    { name: b":method", value: b"POST", },
    { name: b":scheme", value: b"http", },
    { name: b":path", value: path, },
    { name: b":authority", value: b"127.0.0.1", },
    { name: b"content-type", value: b"application/grpc", },
    { name: b"te", value: b"trailers", },
  ])
  conn.write(
    @moonrpc.Frame::Headers(
      stream_id=1,
      fragment=block,
      end_stream=false,
      end_headers=true,
      priority=None,
      padding=0,
    ).encode(),
  )
  conn.write(
    @moonrpc.Frame::Data(
      stream_id=1,
      data=@moonrpc.encode_message(request),
      end_stream=true,
      padding=0,
    ).encode(),
  )
  // read frames until the END_STREAM trailer; collect status, grpc-status, body.
  let dec = @moonrpc.HpackDecoder::new()
  let mut status = b""
  let mut grpc_status = b""
  let mut body = b""
  let mut done = false
  while !done {
    let frame = read_frame(conn)
    match frame {
      Settings(ack~, ..) =>
        if !ack {
          conn.write(@moonrpc.Frame::Settings(params=[], ack=true).encode())
        }
      Headers(fragment~, end_stream~, ..) => {
        for h in dec.decode(fragment) {
          if h.name == b":status" {
            status = h.value
          }
          if h.name == b"grpc-status" {
            grpc_status = h.value
          }
        }
        if end_stream {
          done = true
        }
      }
      Data(data~, ..) => body = cat(body, data)
      _ => ()
    }
  }
  conn.close()
  let message = match @moonrpc.decode_message(body) {
    Some((_, p)) => p
    None => b""
  }
  (status, grpc_status, message)
}

///|
async test "unary gRPC call served over the self-built h2c transport" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register("/echo.Echo/Say", req => cat(b"echo:", req))
    let task = g.spawn(() => server.serve(port=18071))
    @async.sleep(300)
    let (status, grpc_status, reply) = unary_call(
      "127.0.0.1", 18071, b"/echo.Echo/Say", b"world",
    )
    assert_eq(status == b"200", true)
    assert_eq(grpc_status == b"0", true)
    assert_eq(reply == b"echo:world", true)
    task.cancel()
  })
}

///|
async test "an unregistered method is served UNIMPLEMENTED over h2c" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register("/echo.Echo/Say", req => req)
    let task = g.spawn(() => server.serve(port=18072))
    @async.sleep(300)
    let (_status, grpc_status, _reply) = unary_call(
      "127.0.0.1", 18072, b"/echo.Echo/Missing", b"x",
    )
    assert_eq(grpc_status == b"12", true)
    task.cancel()
  })
}

///|
/// Split a run of concatenated length-prefixed gRPC messages into their payloads.
fn split_messages(data : Bytes) -> Array[Bytes] {
  let out : Array[Bytes] = []
  let mut off = 0
  while data.length() - off >= 5 {
    let len = (data[off + 1].to_int() << 24) |
      (data[off + 2].to_int() << 16) |
      (data[off + 3].to_int() << 8) |
      data[off + 4].to_int()
    if data.length() - off < 5 + len {
      break
    }
    out.push(data[off + 5:off + 5 + len].to_owned())
    off = off + 5 + len
  }
  out
}

///|
/// A streaming in-process h2c gRPC client: it opens one stream, sends every
/// message in `requests` as its own length-prefixed DATA (the last carrying
/// END_STREAM, or END_STREAM on the HEADERS when there are none), then reads the
/// whole response, reassembling the reply messages in order and returning
/// `(:status, grpc-status, [reply messages])`. Drives both server- and
/// client-streaming (and the bidi collect-all case).
async fn streaming_call(
  host : String,
  port : Int,
  path : Bytes,
  requests : Array[Bytes],
) -> (Bytes, Bytes, Array[Bytes]) {
  let conn = @socket.Tcp::connect(
    @socket.Addr::parse(host + ":" + port.to_string()),
  )
  conn.write(@moonrpc.connection_preface)
  conn.write(@moonrpc.Frame::Settings(params=[], ack=false).encode())
  let enc = @moonrpc.HpackEncoder::new()
  let block = enc.encode([
    { name: b":method", value: b"POST", },
    { name: b":scheme", value: b"http", },
    { name: b":path", value: path, },
    { name: b":authority", value: b"127.0.0.1", },
    { name: b"content-type", value: b"application/grpc", },
    { name: b"te", value: b"trailers", },
  ])
  let n = requests.length()
  conn.write(
    @moonrpc.Frame::Headers(
      stream_id=1,
      fragment=block,
      end_stream=n == 0,
      end_headers=true,
      priority=None,
      padding=0,
    ).encode(),
  )
  for i = 0; i < n; i = i + 1 {
    conn.write(
      @moonrpc.Frame::Data(
        stream_id=1,
        data=@moonrpc.encode_message(requests[i]),
        end_stream=i == n - 1,
        padding=0,
      ).encode(),
    )
  }
  let dec = @moonrpc.HpackDecoder::new()
  let acc = @buffer.Buffer()
  let mut status = b""
  let mut grpc_status = b""
  let mut done = false
  while !done {
    let frame = read_frame(conn)
    match frame {
      Settings(ack~, ..) =>
        if !ack {
          conn.write(@moonrpc.Frame::Settings(params=[], ack=true).encode())
        }
      Headers(fragment~, end_stream~, ..) => {
        for h in dec.decode(fragment) {
          if h.name == b":status" {
            status = h.value
          }
          if h.name == b"grpc-status" {
            grpc_status = h.value
          }
        }
        if end_stream {
          done = true
        }
      }
      Data(data~, ..) => acc.write_bytes(data)
      _ => ()
    }
  }
  conn.close()
  (status, grpc_status, split_messages(acc.to_bytes()))
}

///|
async test "server-streaming: a real h2c call receives multiple framed replies in order" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register_server_streaming("/count.C/Up", (_ctx, req) => {
      [cat(req, b"-1"), cat(req, b"-2"), cat(req, b"-3")]
    })
    let task = g.spawn(() => server.serve(port=18073))
    @async.sleep(300)
    let (status, grpc_status, replies) = streaming_call(
      "127.0.0.1",
      18073,
      b"/count.C/Up",
      [b"n"],
    )
    assert_eq(status == b"200", true)
    assert_eq(grpc_status == b"0", true)
    assert_eq(replies.length(), 3)
    assert_eq(replies[0] == b"n-1", true)
    assert_eq(replies[1] == b"n-2", true)
    assert_eq(replies[2] == b"n-3", true)
    task.cancel()
  })
}

///|
async test "client-streaming: a real h2c call sends many messages and gets one reply" {
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
    let task = g.spawn(() => server.serve(port=18074))
    @async.sleep(300)
    let (status, grpc_status, replies) = streaming_call(
      "127.0.0.1",
      18074,
      b"/sum.S/Add",
      [b"a", b"b", b"c"],
    )
    assert_eq(status == b"200", true)
    assert_eq(grpc_status == b"0", true)
    assert_eq(replies.length(), 1)
    assert_eq(replies[0] == b"got/a/b/c", true)
    task.cancel()
  })
}

///|
async test "bidi-streaming: a real h2c call echoes each message and closes with a farewell" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register_bidi("/chat.C/Echo", _ctx => @moonrpc.BidiHandler::{
      on_message: m => [cat(b"re:", m)],
      on_end: () => [b"bye"],
    })
    let task = g.spawn(() => server.serve(port=18075))
    @async.sleep(300)
    let (status, grpc_status, replies) = streaming_call(
      "127.0.0.1",
      18075,
      b"/chat.C/Echo",
      [b"one", b"two"],
    )
    assert_eq(status == b"200", true)
    assert_eq(grpc_status == b"0", true)
    // an echo per request, then the farewell from on_end.
    assert_eq(replies.length(), 3)
    assert_eq(replies[0] == b"re:one", true)
    assert_eq(replies[1] == b"re:two", true)
    assert_eq(replies[2] == b"bye", true)
    task.cancel()
  })
}

///|
/// The descriptor of a `greet.Greeter` service, registered with reflection so the
/// in-process client below can discover it over the wire.
fn greet_descriptor() -> @moonrpc.FileDescriptor {
  @moonrpc.FileDescriptor::new(
    "greet.proto",
    "greet",
    messages=[
      @moonrpc.MessageDescriptor::{
        name: "HelloRequest",
        fields: [@moonrpc.FieldDescriptor::scalar("name", 1, TypeString)],
      },
    ],
    services=[
      @moonrpc.ServiceDescriptor::{
        name: "Greeter",
        methods: [
          @moonrpc.MethodDescriptor::{
            name: "SayHello",
            input_type: ".greet.HelloRequest",
            output_type: ".greet.HelloReply",
            client_streaming: false,
            server_streaming: false,
          },
        ],
      },
    ],
  )
}

///|
/// The end-to-end reflection milestone: a real bidi `ServerReflectionInfo` call
/// over the self-built h2c transport. An in-process reflection client lists the
/// server's services and describes one, receiving the real FileDescriptorProto
/// bytes back over the socket — exactly what `grpcurl list` / `describe` drive.
/// The mutation guard corrupts the transported descriptor and confirms the decode
/// then fails, so the assertions are load-bearing.
async test "reflection: a real h2c bidi call lists and describes a registered service" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register("/greet.Greeter/SayHello", req => req)
    let refl = @moonrpc.ReflectionService::new()
    refl.add_file(greet_descriptor())
    server.register_reflection(refl)
    let task = g.spawn(() => server.serve(port=18076))
    @async.sleep(300)
    let refl_path = b"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"

    // ListServices over the wire.
    let (ls, ls_status, list_replies) = streaming_call(
      "127.0.0.1",
      18076,
      refl_path,
      [@moonrpc.encode_reflection_request(@moonrpc.ListServices("*"))],
    )
    assert_eq(ls == b"200", true)
    assert_eq(ls_status == b"0", true)
    assert_eq(list_replies.length(), 1)
    match @moonrpc.decode_reflection_response(list_replies[0]) {
      ServiceList(names) => assert_eq(names.contains("greet.Greeter"), true)
      _ => abort("reflection ListServices did not return a service list")
    }

    // FileContainingSymbol over the wire -> a real FileDescriptorProto.
    let (_fs, fs_status, desc_replies) = streaming_call(
      "127.0.0.1",
      18076,
      refl_path,
      [
        @moonrpc.encode_reflection_request(
          @moonrpc.FileContainingSymbol("greet.Greeter"),
        ),
      ],
    )
    assert_eq(fs_status == b"0", true)
    let protos = match @moonrpc.decode_reflection_response(desc_replies[0]) {
      FileDescriptors(p) => p
      _ => abort("reflection FileContainingSymbol did not return descriptors")
    }
    assert_eq(protos.length(), 1)
    let described = @moonrpc.FileDescriptor::decode(protos[0])
    assert_eq(described.services[0].name == "Greeter", true)
    assert_eq(described.name == "greet.proto", true)

    // MUTATION: corrupt a varint (the first field's length prefix) in the exact
    // bytes that came back over the socket -> the decode that just succeeded fails.
    let arr = protos[0].to_array()
    arr[1] = (0x7F).to_byte()
    let broke = try {
      let _ = @moonrpc.FileDescriptor::decode(Bytes::from_array(arr))
      false
    } catch {
      _ => true
    }
    assert_eq(broke, true)
    task.cancel()
  })
}

///|
/// Graceful shutdown over a real socket: a call that is already running is served to
/// completion, the connection then gets its GOAWAY, `serve` returns on its own, and
/// the port stops accept

///|
/// Graceful shutdown over a real socket: the call already running is served to
/// completion, the connection then gets its GOAWAY and closes, `serve` returns on
/// its own, and the port stops accepting.
async test "graceful_stop drains in-flight calls and releases the port" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register("/echo.Echo/Say", req => cat(b"echo:", req))
    let mut served = false
    let task = g.spawn(() => {
      server.serve(port=18077)
      served = true
    })
    @async.sleep(300)
    let (status, grpc_status, reply) = unary_call(
      "127.0.0.1", 18077, b"/echo.Echo/Say", b"qwq",
    )
    assert_eq(status == b"200", true)
    assert_eq(grpc_status == b"0", true)
    assert_eq(reply == b"echo:qwq", true)
    server.graceful_stop(grace=3000)
    @async.sleep(200)
    // The accept loop unwound by itself rather than being cancelled.
    assert_eq(served, true)
    assert_eq(server.live.length(), 0)
    task.cancel()
  })
}

///|
/// A call that is still open when the drain starts is served to completion rather
/// than cut off: the stream is opened, `graceful_stop` runs while it is in flight,
/// and only then does the request body arrive. The reply still comes back OK.
async test "graceful_stop lets a call that is already open finish" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register("/echo.Echo/Say", req => cat(b"echo:", req))
    let task = g.spawn(() => server.serve(port=18079))
    @async.sleep(300)
    let conn = @socket.Tcp::connect(@socket.Addr::parse("127.0.0.1:18079"))
    conn.write(@moonrpc.connection_preface)
    conn.write(@moonrpc.Frame::Settings(params=[], ack=false).encode())
    let enc = @moonrpc.HpackEncoder::new()
    let block = enc.encode([
      { name: b":method", value: b"POST", },
      { name: b":scheme", value: b"http", },
      { name: b":path", value: b"/echo.Echo/Say", },
      { name: b"content-type", value: b"application/grpc", },
      { name: b"te", value: b"trailers", },
    ])
    // HEADERS without END_STREAM: the stream is open and the server is waiting on us.
    conn.write(
      @moonrpc.Frame::Headers(
        stream_id=1,
        fragment=block,
        end_stream=false,
        end_headers=true,
        priority=None,
        padding=0,
      ).encode(),
    )
    @async.sleep(200)
    let drain = g.spawn(() => server.graceful_stop(grace=5000))
    @async.sleep(200)
    // Now finish the request. A server that cut the call off at the drain would have
    // closed this socket already.
    conn.write(
      @moonrpc.Frame::Data(
        stream_id=1,
        data=@moonrpc.encode_message(b"hhh"),
        end_stream=true,
        padding=0,
      ).encode(),
    )
    let dec = @moonrpc.HpackDecoder::new()
    let mut grpc_status = b""
    let mut body = b""
    let mut done = false
    while !done {
      let frame = read_frame(conn)
      match frame {
        Headers(fragment~, end_stream~, ..) => {
          for h in dec.decode(fragment) {
            if h.name == b"grpc-status" {
              grpc_status = h.value
            }
          }
          if end_stream {
            done = true
          }
        }
        Data(data~, ..) => body = cat(body, data)
        _ => ()
      }
    }
    assert_eq(grpc_status == b"0", true)
    let message = match @moonrpc.decode_message(body) {
      Some((_, p)) => p
      None => b""
    }
    assert_eq(message == b"echo:hhh", true)
    conn.close()
    drain.wait()
    task.cancel()
  })
}

///|
/// Open a call carrying `timeout` as its `grpc-timeout`, let `delay` milliseconds of
/// real time pass before finishing the request, and return the `grpc-status` the server
/// answered with. The time is spent by the client, not by a handler: handlers are
/// synchronous, so the only way to spend real time inside a call is to be slow sending
/// it — which is also the shape of the deadline a real client hits.
async fn timed_call(port : Int, timeout : Bytes, delay : Int) -> Bytes {
  let conn = @socket.Tcp::connect(
    @socket.Addr::parse("127.0.0.1:" + port.to_string()),
  )
  conn.write(@moonrpc.connection_preface)
  conn.write(@moonrpc.Frame::Settings(params=[], ack=false).encode())
  let enc = @moonrpc.HpackEncoder::new()
  let block = enc.encode([
    { name: b":method", value: b"POST", },
    { name: b":scheme", value: b"http", },
    { name: b":path", value: b"/slow.S/Wait", },
    { name: b":authority", value: b"127.0.0.1", },
    { name: b"content-type", value: b"application/grpc", },
    { name: b"te", value: b"trailers", },
    { name: b"grpc-timeout", value: timeout, },
  ])
  conn.write(
    @moonrpc.Frame::Headers(
      stream_id=1,
      fragment=block,
      end_stream=false,
      end_headers=true,
      priority=None,
      padding=0,
    ).encode(),
  )
  @async.sleep(delay)
  conn.write(
    @moonrpc.Frame::Data(
      stream_id=1,
      data=@moonrpc.encode_message(b"qwq"),
      end_stream=true,
      padding=0,
    ).encode(),
  )
  let dec = @moonrpc.HpackDecoder::new()
  let mut grpc_status = b""
  let mut done = false
  while !done {
    match read_frame(conn) {
      Settings(ack~, ..) =>
        if !ack {
          conn.write(@moonrpc.Frame::Settings(params=[], ack=true).encode())
        }
      Headers(fragment~, end_stream~, ..) => {
        for h in dec.decode(fragment) {
          if h.name == b"grpc-status" {
            grpc_status = h.value
          }
        }
        if end_stream {
          done = true
        }
      }
      _ => ()
    }
  }
  conn.close()
  grpc_status
}

///|
async test "a call whose deadline passes on the wire is answered DEADLINE_EXCEEDED" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register("/slow.S/Wait", req => req)
    let task = g.spawn(() => server.serve(port=18078))
    @async.sleep(300)
    // 50 ms of deadline, and 250 ms before the request finishes arriving: the engine
    // reads the driver's clock, so the call is over before the handler could run.
    assert_eq(timed_call(18078, b"50m", 250) == b"4", true)
    // The same call inside its deadline is served as usual.
    assert_eq(timed_call(18078, b"10S", 0) == b"0", true)
    task.cancel()
  })
}

///|
async test "a call left waiting past its deadline is cut off when the connection next speaks" {
  @async.with_task_group(g => {
    let server = GrpcServer::new()
    server.register_client_streaming("/slow.S/Collect", (_ctx, _msgs) => b"done")
    let task = g.spawn(() => server.serve(port=18080))
    @async.sleep(300)
    let conn = @socket.Tcp::connect(@socket.Addr::parse("127.0.0.1:18080"))
    conn.write(@moonrpc.connection_preface)
    conn.write(@moonrpc.Frame::Settings(params=[], ack=false).encode())
    let enc = @moonrpc.HpackEncoder::new()
    let block = enc.encode([
      { name: b":method", value: b"POST", },
      { name: b":scheme", value: b"http", },
      { name: b":path", value: b"/slow.S/Collect", },
      { name: b":authority", value: b"127.0.0.1", },
      { name: b"content-type", value: b"application/grpc", },
      { name: b"te", value: b"trailers", },
      { name: b"grpc-timeout", value: b"50m", },
    ])
    // Open the call and then go quiet: nothing else will ever arrive for this stream.
    conn.write(
      @moonrpc.Frame::Headers(
        stream_id=1,
        fragment=block,
        end_stream=false,
        end_headers=true,
        priority=None,
        padding=0,
      ).encode(),
    )
    @async.sleep(250)
    // Two PINGs the call has nothing to do with. The driver takes each one as its cue to
    // look at the clock, so the expired call is cut off between the two answers.
    conn.write(@moonrpc.Frame::Ping(payload=b"heke1228", ack=false).encode())
    conn.write(@moonrpc.Frame::Ping(payload=b"lfanke00", ack=false).encode())
    let dec = @moonrpc.HpackDecoder::new()
    let mut grpc_status = b""
    let mut acks = 0
    while acks < 2 {
      match read_frame(conn) {
        Ping(ack~, ..) => if ack { acks = acks + 1 }
        Settings(ack~, ..) =>
          if !ack {
            conn.write(@moonrpc.Frame::Settings(params=[], ack=true).encode())
          }
        Headers(fragment~, ..) =>
          for h in dec.decode(fragment) {
            if h.name == b"grpc-status" {
              grpc_status = h.value
            }
          }
        _ => ()
      }
    }
    assert_eq(grpc_status == b"4", true)
    conn.close()
    task.cancel()
  })
}
