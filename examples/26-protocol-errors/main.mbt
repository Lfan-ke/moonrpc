// Protocol errors and their scope (RFC 9113 §5.4). A stream error kills one stream with
// RST_STREAM and the connection keeps serving (§5.4.2); a connection error ends everything
// after a GOAWAY that names the last stream the peer can count on (§5.4.1). The engine
// classifies the fault where it sees it and returns the frames that report it, so a driver
// never has to guess. On the other side, a reset arrives instead of trailers, so the client
// builds the call's status out of the HTTP/2 error code (gRPC PROTOCOL-HTTP2).
//
//     moon run examples/26-protocol-errors

///|
fn ascii(bs : Bytes) -> String {
  let buf = StringBuilder()
  for b in bs {
    buf.write_char((b.to_int() & 0xFF).unsafe_to_char())
  }
  buf.to_string()
}

///|
fn open_call(
  server : @moonrpc.H2Server,
  enc : @moonrpc.HpackEncoder,
  id : Int,
  end_stream : Bool,
) -> Array[@moonrpc.Frame] {
  let block = enc.encode([
    { name: b":method", value: b"POST", },
    { name: b":scheme", value: b"http", },
    { name: b":path", value: b"/echo.Echo/Say", },
    { name: b"content-type", value: b"application/grpc", },
    { name: b"te", value: b"trailers", },
  ])
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
    GoAway(last_stream_id~, error_code~, debug~) =>
      "GOAWAY(last \{last_stream_id}, code \{error_code}, \"\{ascii(debug)}\")"
    Headers(stream_id~, ..) => "HEADERS(stream \{stream_id})"
    Data(stream_id~, ..) => "DATA(stream \{stream_id})"
    _ => "other"
  }
}

///|
fn stream_error() -> Unit {
  let server = @moonrpc.H2Server::new()
  server.register("/echo.Echo/Say", req => req)
  let enc = @moonrpc.HpackEncoder::new()
  let _ = server.feed(Settings(params=[], ack=false))
  let _ = open_call(server, enc, 1, false)

  // §6.9: a zero increment credits nothing. On a stream it costs only that stream.
  for f in server.feed(WindowUpdate(stream_id=1, increment=0)) {
    println("zero-increment WINDOW_UPDATE on stream 1 -> \{describe(f)}")
  }
  println("connection still open: \{!server.closing()}")

  // The reset closed stream 1, so the request finishing after it earns another reset
  // rather than the reply the handler would have produced (§5.4.2).
  for
    f in server.feed(
      Data(
        stream_id=1,
        data=@moonrpc.encode_message(b"qwq"),
        end_stream=true,
        padding=0,
      ),
    ) {
    println("late DATA on the reset stream -> \{describe(f)}")
  }

  // A fresh stream on the same connection runs to completion.
  let _ = open_call(server, enc, 3, false)
  let out = server.feed(
    Data(
      stream_id=3,
      data=@moonrpc.encode_message(b"heke1228"),
      end_stream=true,
      padding=0,
    ),
  )
  println("stream 3 on the same connection -> \{out.length()} frames:")
  for f in out {
    println("  \{describe(f)}")
  }
}

///|
fn conn_error() -> Unit {
  let server = @moonrpc.H2Server::new()
  let _ = server.feed(Settings(params=[], ack=false))

  // §6.1: DATA belongs to a stream, never to the connection control stream.
  for
    f in server.feed(
      Data(stream_id=0, data=b"hhh", end_stream=false, padding=0),
    ) {
    println("DATA on stream 0 -> \{describe(f)}")
  }
  println("connection closing: \{server.closing()}")

  // §5.4.1: the GOAWAY was the last thing this connection had to say.
  let after = server.feed(Ping(payload=b"heke1228", ack=false))
  println("a PING after the GOAWAY -> \{after.length()} frames (no pong)")
}

///|
fn client_status() -> Unit raise {
  // A reset carries no trailers, so the HTTP/2 error code is the only thing left to
  // build a status from. REFUSED_STREAM means the server never processed the call.
  let codes = [
    (@moonrpc.error_refused_stream, "REFUSED_STREAM"),
    (@moonrpc.error_enhance_your_calm, "ENHANCE_YOUR_CALM"),
    (@moonrpc.error_inadequate_security, "INADEQUATE_SECURITY"),
    (@moonrpc.error_cancel, "CANCEL"),
    (@moonrpc.error_protocol_error, "PROTOCOL_ERROR"),
  ]
  for pair in codes {
    let (code, name) = pair
    let client = @moonrpc.H2Client::new()
    let (id, _req) = client.unary("/echo.Echo/Say", b"qwq")
    let _ = client.feed(RstStream(stream_id=id, error_code=code))
    let status = @moonrpc.Status::from_h2_error(code)
    println(
      "RST_STREAM(\{name}) -> grpc-status \{client.reply(id).grpc_status} \{status.name()}",
    )
  }

  // A proxy answering on the gRPC port sends an HTTP status and no gRPC trailers at
  // all (gRPC http-grpc-status-mapping).
  let enc = @moonrpc.HpackEncoder::new()
  for http in [b"401", b"404", b"503"] {
    let client = @moonrpc.H2Client::new()
    let (id, _req) = client.unary("/echo.Echo/Say", b"emm")
    let _ = client.feed(
      Headers(
        stream_id=id,
        fragment=enc.encode([{ name: b":status", value: http, }]),
        end_stream=true,
        end_headers=true,
        priority=None,
        padding=0,
      ),
    )
    let reply = client.reply(id)
    println(
      ":status \{ascii(http)} with no grpc-status -> grpc-status \{reply.grpc_status}",
    )
  }
}

///|
fn run() -> Unit raise {
  println("-- a stream error (§5.4.2) --")
  stream_error()
  println("")
  println("-- a connection error (§5.4.1) --")
  conn_error()
  println("")
  println("-- what the client makes of a call with no trailers --")
  client_status()
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
