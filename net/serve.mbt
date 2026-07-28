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
/// A gRPC server: a registry of unary handlers keyed by `/pkg.Service/Method`
/// path, served over the self-built HTTP/2 (h2c) transport. Each accepted
/// connection gets its own `@moonrpc.H2Server` engine (HPACK and flow control are
/// per-connection state).
pub(all) struct GrpcServer {
  handlers : Map[String, (Bytes) -> Bytes]
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
  self.handlers[path] = handler
}

///|
/// Build a fresh per-connection engine seeded with this server's handlers.
fn GrpcServer::engine(self : GrpcServer) -> @moonrpc.H2Server {
  let e = @moonrpc.H2Server::new()
  for path, handler in self.handlers {
    e.register(path, handler)
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
    let task = g.spawn(() => server.serve(port=50071))
    @async.sleep(300)
    let (status, grpc_status, reply) = unary_call(
      "127.0.0.1", 50071, b"/echo.Echo/Say", b"world",
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
    let task = g.spawn(() => server.serve(port=50072))
    @async.sleep(300)
    let (_status, grpc_status, _reply) = unary_call(
      "127.0.0.1", 50072, b"/echo.Echo/Missing", b"x",
    )
    assert_eq(grpc_status == b"12", true)
    task.cancel()
  })
}
