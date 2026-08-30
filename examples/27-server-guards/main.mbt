// What a gRPC server refuses. A request is only a call if its request line says so
// (gRPC PROTOCOL-HTTP2), a connection only runs as many streams at once as it advertised
// (RFC 9113 §5.1.2), DATA only arrives inside the receive window (§6.9.1), a field block
// arrives contiguously (§6.10), and a call only lasts as long as its `grpc-timeout`. Each
// guard answers in the peer's own language: an HTTP status, a RST_STREAM, or a GOAWAY.
//
//     moon run examples/27-server-guards

///|
fn ascii(bs : Bytes) -> String {
  let buf = StringBuilder()
  for b in bs {
    buf.write_char((b.to_int() & 0xFF).unsafe_to_char())
  }
  buf.to_string()
}

///|
/// A well-formed gRPC request block plus whatever `extra` the demonstration needs.
fn request(
  enc : @moonrpc.HpackEncoder,
  path : Bytes,
  extra? : Array[@moonrpc.Header] = [],
) -> Bytes {
  let headers : Array[@moonrpc.Header] = [
    { name: b":method", value: b"POST", },
    { name: b":scheme", value: b"http", },
    { name: b":path", value: path, },
    { name: b"content-type", value: b"application/grpc", },
    { name: b"te", value: b"trailers", },
  ]
  for h in extra {
    headers.push(h)
  }
  enc.encode(headers)
}

///|
fn open_call(
  server : @moonrpc.H2Server,
  block : Bytes,
  id : Int,
  end_stream : Bool,
) -> Array[@moonrpc.Frame] {
  server.feed(
    Headers(
      stream_id=id,
      fragment=block,
      end_stream~,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
}

///|
fn describe(f : @moonrpc.Frame) -> String {
  match f {
    RstStream(stream_id~, error_code~) =>
      "RST_STREAM(stream \{stream_id}, code \{error_code})"
    GoAway(error_code~, debug~, ..) =>
      "GOAWAY(code \{error_code}, \"\{ascii(debug)}\")"
    WindowUpdate(stream_id~, increment~) =>
      "WINDOW_UPDATE(stream \{stream_id}, +\{increment})"
    Headers(stream_id~, ..) => "HEADERS(stream \{stream_id})"
    Data(stream_id~, ..) => "DATA(stream \{stream_id})"
    _ => "other"
  }
}

///|
/// The `:status` and `grpc-status` of a response, for the answers that are one HEADERS.
fn answer(
  frames : Array[@moonrpc.Frame],
  dec : @moonrpc.HpackDecoder,
) -> String raise {
  let mut http = b"-"
  let mut grpc = b"-"
  for f in frames {
    if f is Headers(fragment~, ..) {
      for h in dec.decode(fragment) {
        if h.name == b":status" {
          http = h.value
        }
        if h.name == b"grpc-status" {
          grpc = h.value
        }
      }
    }
  }
  ":status \{ascii(http)}, grpc-status \{ascii(grpc)}"
}

///|
/// A request is a gRPC call only if `:method` is POST, `content-type` begins
/// `application/grpc`, and `te` carries `trailers`. Anything else gets an HTTP status,
/// because a 200 is what a plain HTTP client reads as success.
fn request_line() -> Unit raise {
  let cases : Array[(String, Array[@moonrpc.Header])] = [
    (
      "a browser form post",
      [
        { name: b":method", value: b"POST", },
        { name: b":path", value: b"/echo.Echo/Say", },
        { name: b"content-type", value: b"application/x-www-form-urlencoded", },
        { name: b"te", value: b"trailers", },
      ],
    ),
    (
      "a health-checking GET",
      [
        { name: b":method", value: b"GET", },
        { name: b":path", value: b"/echo.Echo/Say", },
        { name: b"content-type", value: b"application/grpc", },
        { name: b"te", value: b"trailers", },
      ],
    ),
    (
      "a gRPC call with no te: trailers",
      [
        { name: b":method", value: b"POST", },
        { name: b":path", value: b"/echo.Echo/Say", },
        { name: b"content-type", value: b"application/grpc", },
      ],
    ),
    (
      "a real call, content-type application/grpc+proto",
      [
        { name: b":method", value: b"POST", },
        { name: b":path", value: b"/echo.Echo/Say", },
        { name: b"content-type", value: b"application/grpc+proto", },
        { name: b"te", value: b"trailers", },
      ],
    ),
  ]
  for c in cases {
    let (what, headers) = c
    let server = @moonrpc.H2Server::new()
    server.register("/echo.Echo/Say", req => req)
    let enc = @moonrpc.HpackEncoder::new()
    let dec = @moonrpc.HpackDecoder::new()
    let _ = server.feed(Settings(params=[], ack=false))
    let out = open_call(server, enc.encode(headers), 1, true)
    println("\{what} -> \{answer(out, dec)}")
  }
}

///|
/// The peer is told how many streams it may run at once, and the one over that is
/// refused rather than served: REFUSED_STREAM says it was never processed.
fn stream_limit() -> Unit {
  let server = @moonrpc.H2Server::new()
  server.register("/echo.Echo/Say", req => req)
  server.set_max_streams(2)
  let enc = @moonrpc.HpackEncoder::new()
  for f in server.preface() {
    if f is Settings(params~, ..) {
      for p in params {
        if p.0 == @moonrpc.settings_max_concurrent_streams {
          println("advertised SETTINGS_MAX_CONCURRENT_STREAMS = \{p.1}")
        }
      }
    }
  }
  let _ = server.feed(Settings(params=[], ack=false))
  for id in [1, 3, 5] {
    let out = open_call(server, request(enc, b"/echo.Echo/Say"), id, false)
    let said = if out.length() == 0 { "accepted" } else { describe(out[0]) }
    println("stream \{id} -> \{said} (\{server.active_streams()} running)")
  }
}

///|
/// Flow control is a promise: the peer may send what the receive window allows and no
/// more. Overrunning it is not something a receiver can absorb, so the connection ends.
fn receive_window() -> Unit {
  let server = @moonrpc.H2Server::new()
  server.register("/echo.Echo/Say", req => req)
  let enc = @moonrpc.HpackEncoder::new()
  let _ = server.feed(Settings(params=[], ack=false))
  let _ = open_call(server, request(enc, b"/echo.Echo/Say"), 1, false)
  let buf = Buffer()
  for _i = 0; _i < @moonrpc.default_window_size + 1; _i = _i + 1 {
    buf.write_byte(b'q')
  }
  println("advertised receive window: \{@moonrpc.default_window_size} octets")
  for
    f in server.feed(
      Data(stream_id=1, data=buf.to_bytes(), end_stream=false, padding=0),
    ) {
    println("one octet more than that -> \{describe(f)}")
  }
}

///|
/// A field block is a contiguous run of HEADERS then CONTINUATION. Nothing may be
/// interleaved into it, on this stream or any other.
fn field_block() -> Unit {
  let server = @moonrpc.H2Server::new()
  server.register("/echo.Echo/Say", req => req)
  let enc = @moonrpc.HpackEncoder::new()
  let _ = server.feed(Settings(params=[], ack=false))
  let _ = server.feed(
    Headers(
      stream_id=1,
      fragment=request(enc, b"/echo.Echo/Say"),
      end_stream=false,
      end_headers=false,
      priority=None,
      padding=0,
    ),
  )
  for f in server.feed(WindowUpdate(stream_id=1, increment=100)) {
    println(
      "a WINDOW_UPDATE between HEADERS and CONTINUATION -> \{describe(f)}",
    )
  }
}

///|
/// A deadline is generated at both ends. The engine has no clock of its own, so the
/// driver installs one; here the handler moves it by hand to spend more of the call than
/// it was given, and the reply it produced is not sendable.
fn deadline() -> Unit raise {
  let server = @moonrpc.H2Server::new()
  let now = Ref(0L)
  server.set_clock(() => now.val)
  server.register_unary("/slow.S/Wait", (_ctx, req) => {
    now.val = now.val + 5000L
    req
  })
  let enc = @moonrpc.HpackEncoder::new()
  let dec = @moonrpc.HpackDecoder::new()
  let _ = server.feed(Settings(params=[], ack=false))
  let block = request(enc, b"/slow.S/Wait", extra=[
    { name: b"grpc-timeout", value: b"2S", },
  ])
  let out = open_call(server, block, 1, true)
  println("a 2-second call whose handler took 5 -> \{answer(out, dec)}")
  println("  frames: \{out.length()} (the reply never left)")
  println("  stream 1 is \{server.stream_state(1)}")
}

///|
fn run() -> Unit raise {
  println("-- what counts as a gRPC request (PROTOCOL-HTTP2) --")
  request_line()
  println("")
  println("-- how many streams at once (§5.1.2) --")
  stream_limit()
  println("")
  println("-- the receive window (§6.9.1) --")
  receive_window()
  println("")
  println("-- field-block contiguity (§6.10) --")
  field_block()
  println("")
  println("-- the call's deadline --")
  deadline()
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
