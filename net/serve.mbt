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
/// the connection or a GOAWAY is seen.
async fn drive(conn : @socket.Tcp, engine : @moonrpc.H2Server) -> Unit {
  let preface = conn.read_exactly(@moonrpc.connection_preface.length())
  if !@moonrpc.has_connection_preface(preface) {
    return
  }
  write_frames(conn, engine.preface())
  for ;; {
    let frame = read_frame(conn)
    write_frames(conn, engine.feed(frame))
    if engine.goaway_received() {
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
}

///|
/// A gRPC server with no registered methods.
pub fn GrpcServer::new() -> GrpcServer {
  { handlers: Map([]) }
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
  for path, handler in self.handlers {
    e.register_handler(path, handler)
  }
  e
}

///|
/// Serve gRPC over h2c on `host:port`, blocking in the accept loop until the
/// running task is cancelled. Each connection is driven by `drive`; a failure on
/// one connection is isolated and does not stop the server.
pub async fn GrpcServer::serve(
  self : GrpcServer,
  host? : String = "127.0.0.1",
  port? : Int = 50051,
) -> Unit {
  let server = @socket.TcpServer::TcpServer(
    @socket.Addr::parse(host + ":" + port.to_string()),
    reuse_addr=true,
  )
  server.run_forever((conn, _addr) => {
    drive(conn, self.engine()) catch {
      _ => ()
    }
  })
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
    { name: b":method", value: b"POST" },
    { name: b":scheme", value: b"http" },
    { name: b":path", value: path },
    { name: b":authority", value: b"127.0.0.1" },
    { name: b"content-type", value: b"application/grpc" },
    { name: b"te", value: b"trailers" },
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
    { name: b":method", value: b"POST" },
    { name: b":scheme", value: b"http" },
    { name: b":path", value: path },
    { name: b":authority", value: b"127.0.0.1" },
    { name: b"content-type", value: b"application/grpc" },
    { name: b"te", value: b"trailers" },
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
