// In-memory drive of the client engine against the server engine: real request
// frames out of `H2Client`, through the actual frame + HPACK codecs, into
// `H2Server`, and its response frames back — no sockets, every backend. This is
// the client-side mutation-verification target: break request framing, response
// reassembly, or client flow control and these go red.

///|
/// Shuttle frames between a client and server engine until the call `id` completes:
/// deliver the client's queued frames to the server, its replies back to the
/// client, and repeat. The preface handshake is seeded first.
fn drive_pair(
  server : H2Server,
  client : H2Client,
  id : Int,
  initial : Array[Frame],
) -> Unit raise {
  let to_server : Array[Frame] = []
  let to_client : Array[Frame] = []
  for f in client.preface() {
    to_server.push(f)
  }
  for f in server.preface() {
    to_client.push(f)
  }
  for f in initial {
    to_server.push(f)
  }
  let mut si = 0
  let mut ci = 0
  let mut steps = 0
  while !client.is_done(id) && steps < 100000 {
    steps = steps + 1
    if si < to_server.length() {
      let f = to_server[si]
      si = si + 1
      for r in server.feed(f) {
        to_client.push(r)
      }
    } else if ci < to_client.length() {
      let f = to_client[ci]
      ci = ci + 1
      for r in client.feed(f) {
        to_server.push(r)
      }
    } else {
      break
    }
  }
}

///|
test "client: a gzip-encoded response message is inflated for the caller" {
  // Our own server only replies with identity, so hand-feed the client the response
  // a gzip-compressing peer (e.g. grpc-go with the gzip compressor) would send: a
  // `grpc-encoding: gzip` header and one reply message with the compression flag set.
  let client = H2Client::new()
  let (id, _req) = client.unary("/echo.Echo/Say", b"x")
  let _ = client.feed(Settings(params=[], ack=false))
  let enc = HpackEncoder::new()
  let hblock = enc.encode([
    { name: b":status", value: b"200", },
    { name: b"content-type", value: b"application/grpc", },
    { name: b"grpc-encoding", value: b"gzip", },
  ])
  let _ = client.feed(
    Headers(
      stream_id=id,
      fragment=hblock,
      end_stream=false,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  let gz = gz_unhex(
    "1f8b0800cb9b6a6a02ffcb48cdc9c9d75148afca2c5028cf2fca490100eb83468211000000",
  )
  let _ = client.feed(
    Data(
      stream_id=id,
      data=encode_message(gz, compressed=true),
      end_stream=false,
      padding=0,
    ),
  )
  let tblock = enc.encode([{ name: b"grpc-status", value: b"0", }])
  let _ = client.feed(
    Headers(
      stream_id=id,
      fragment=tblock,
      end_stream=true,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  let reply = client.reply(id)
  assert_eq(reply.grpc_status, 0)
  assert_eq(reply.messages.length(), 1)
  // The caller sees the plaintext, not the gzip bytes.
  assert_eq(
    reply.messages[0] == gz_unhex("68656c6c6f2c20677a697020776f726c64"),
    true,
  )
}

///|
test "client: a response header block split across HEADERS + CONTINUATION is reassembled" {
  let client = H2Client::new()
  let (id, _req) = client.unary("/echo.Echo/Say", b"x")
  let _ = client.feed(Settings(params=[], ack=false))
  let enc = HpackEncoder::new()
  let block = enc.encode([
    { name: b":status", value: b"200", },
    { name: b"content-type", value: b"application/grpc", },
    { name: b"x-custom", value: b"hello", },
  ])
  // Deliver the block in two frames: HEADERS(end_headers=false) then CONTINUATION.
  // The custom header is only recoverable if the fragments are concatenated before
  // the stateful HPACK decode — decoding each fragment alone desyncs or panics.
  let mid = block.length() / 2
  let _ = client.feed(
    Headers(
      stream_id=id,
      fragment=block[0:mid].to_owned(),
      end_stream=false,
      end_headers=false,
      priority=None,
      padding=0,
    ),
  )
  let _ = client.feed(
    Continuation(
      stream_id=id,
      fragment=block[mid:block.length()].to_owned(),
      end_headers=true,
    ),
  )
  let tblock = enc.encode([{ name: b"grpc-status", value: b"0", }])
  let _ = client.feed(
    Headers(
      stream_id=id,
      fragment=tblock,
      end_stream=true,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  let reply = client.reply(id)
  assert_eq(reply.status == b"200", true)
  assert_eq(reply.grpc_status, 0)
  assert_eq(header_present(reply.headers, b"x-custom", b"hello"), true)
}

///|
test "client: a compressed reply under an unsupported encoding fails INTERNAL, not raw bytes" {
  let client = H2Client::new()
  let (id, _req) = client.unary("/echo.Echo/Say", b"x")
  let _ = client.feed(Settings(params=[], ack=false))
  let enc = HpackEncoder::new()
  let hblock = enc.encode([
    { name: b":status", value: b"200", },
    { name: b"content-type", value: b"application/grpc", },
    { name: b"grpc-encoding", value: b"deflate", },
  ])
  let _ = client.feed(
    Headers(
      stream_id=id,
      fragment=hblock,
      end_stream=false,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  // A message with the compression flag set under "deflate", which the client never
  // advertised and does not decode.
  let _ = client.feed(
    Data(
      stream_id=id,
      data=encode_message(b"rawbytes", compressed=true),
      end_stream=false,
      padding=0,
    ),
  )
  let tblock = enc.encode([{ name: b"grpc-status", value: b"0", }])
  let _ = client.feed(
    Headers(
      stream_id=id,
      fragment=tblock,
      end_stream=true,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  let reply = client.reply(id)
  // The RPC fails INTERNAL — the trailer's grpc-status 0 does not override it — and
  // the undecodable bytes are not surfaced as a message.
  assert_eq(reply.grpc_status, Status::code(Internal))
  assert_eq(reply.messages.length(), 0)
}

///|
test "client+server: a unary call round-trips over the two pure engines" {
  let server = H2Server::new()
  server.register("/echo.Echo/Say", req => cat(b"echo:", req))
  let client = H2Client::new()
  let (id, frames) = client.unary("/echo.Echo/Say", b"world")
  drive_pair(server, client, id, frames)
  let reply = client.reply(id)
  assert_eq(reply.status == b"200", true)
  assert_eq(reply.grpc_status, 0)
  assert_eq(reply.messages.length(), 1)
  assert_eq(reply.messages[0] == b"echo:world", true)
}

///|
test "client+server: a server-streaming call reassembles every framed reply in order" {
  let server = H2Server::new()
  server.register_server_streaming("/count.C/Up", (_ctx, req) => {
    [cat(req, b"-1"), cat(req, b"-2"), cat(req, b"-3")]
  })
  let client = H2Client::new()
  let id = client.open("/count.C/Up")
  let frames = client.send(id, b"n", end=true)
  drive_pair(server, client, id, frames)
  let reply = client.reply(id)
  assert_eq(reply.grpc_status, 0)
  assert_eq(reply.messages.length(), 3)
  assert_eq(reply.messages[0] == b"n-1", true)
  assert_eq(reply.messages[1] == b"n-2", true)
  assert_eq(reply.messages[2] == b"n-3", true)
}

///|
test "client+server: a client-streaming call sends many messages for one reply" {
  let server = H2Server::new()
  server.register_client_streaming("/sum.S/Add", (_ctx, msgs) => {
    let buf = Buffer()
    buf.write_bytes(b"got")
    for m in msgs {
      buf.write_byte(b'/')
      buf.write_bytes(m)
    }
    buf.to_bytes()
  })
  let client = H2Client::new()
  let id = client.open("/sum.S/Add")
  let all : Array[Frame] = []
  for f in client.send(id, b"a") {
    all.push(f)
  }
  for f in client.send(id, b"b") {
    all.push(f)
  }
  for f in client.send(id, b"c") {
    all.push(f)
  }
  for f in client.close_send(id) {
    all.push(f)
  }
  drive_pair(server, client, id, all)
  let reply = client.reply(id)
  assert_eq(reply.grpc_status, 0)
  assert_eq(reply.messages.length(), 1)
  assert_eq(reply.messages[0] == b"got/a/b/c", true)
}

///|
test "client+server: request metadata reaches the handler; response metadata reaches the client" {
  let server = H2Server::new()
  server.register_unary("/meta.M/Do", (ctx, _req) => {
    let token = match ctx.metadata_get(b"x-token") {
      Some(v) => v
      None => b"none"
    }
    ctx.add_header(b"x-server", b"on")
    ctx.add_trailer(b"x-count", b"1")
    cat(b"token:", token)
  })
  let client = H2Client::new()
  let id = client.open("/meta.M/Do", metadata=[
    { name: b"x-token", value: b"abc", },
  ])
  let frames = client.send(id, b"", end=true)
  drive_pair(server, client, id, frames)
  let reply = client.reply(id)
  assert_eq(reply.messages[0] == b"token:abc", true)
  assert_eq(header_present(reply.headers, b"x-server", b"on"), true)
  assert_eq(header_present(reply.trailers, b"x-count", b"1"), true)
}

///|
/// Whether `headers` contains an exact `(name, value)` pair.
fn header_present(headers : Array[Header], name : Bytes, value : Bytes) -> Bool {
  for h in headers {
    if h.name == name && h.value == value {
      return true
    }
  }
  false
}

///|
/// Count DATA octets in a batch and note whether an END_STREAM DATA frame appears.
fn tally_data(
  frames : Array[Frame],
  octets : Ref[Int],
  fin : Ref[Bool],
) -> Unit {
  for f in frames {
    match f {
      Data(data~, end_stream~, ..) => {
        octets.val = octets.val + data.length()
        if end_stream {
          fin.val = true
        }
      }
      _ => ()
    }
  }
}

///|
test "client: a small send window splits the request and a WINDOW_UPDATE releases the rest" {
  let client = H2Client::new()
  // peer advertises a tiny 5-octet initial window before the call opens.
  let _ = client.feed(
    Settings(params=[(settings_initial_window_size, 5)], ack=false),
  )
  let id = client.open("/big.Svc/M")
  let octets = Ref(0)
  let fin = Ref(false)
  // a 20-byte body -> 25 octets framed; only 5 escape the window, no END_STREAM.
  tally_data(client.send(id, b"0123456789ABCDEFGHIJ", end=true), octets, fin)
  assert_eq(octets.val, 5)
  assert_eq(fin.val, false)
  // a zero increment releases nothing.
  tally_data(client.feed(WindowUpdate(stream_id=id, increment=0)), octets, fin)
  assert_eq(octets.val, 5)
  // grow the stream window and the remaining 20 octets plus END_STREAM flow.
  tally_data(
    client.feed(WindowUpdate(stream_id=id, increment=1000)),
    octets,
    fin,
  )
  assert_eq(octets.val, 25)
  assert_eq(fin.val, true)
}

///|
test "client: a reply split across two DATA frames is reassembled into one message" {
  let client = H2Client::new()
  let id = client.open("/x.Y/Z")
  let _ = client.send(id, b"q", end=true)
  let enc = HpackEncoder::new()
  // initial response HEADERS.
  let _ = client.feed(
    Headers(
      stream_id=id,
      fragment=enc.encode([{ name: b":status", value: b"200", }]),
      end_stream=false,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  // one length-prefixed message "hello" split mid-way across two DATA frames.
  let framed = encode_message(b"hello")
  let _ = client.feed(
    Data(stream_id=id, data=framed[0:3].to_owned(), end_stream=false, padding=0),
  )
  assert_eq(client.reply(id).messages.length(), 0)
  let _ = client.feed(
    Data(
      stream_id=id,
      data=framed[3:framed.length()].to_owned(),
      end_stream=false,
      padding=0,
    ),
  )
  // trailers close the stream.
  let _ = client.feed(
    Headers(
      stream_id=id,
      fragment=enc.encode([{ name: b"grpc-status", value: b"0", }]),
      end_stream=true,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  let reply = client.reply(id)
  assert_eq(reply.messages.length(), 1)
  assert_eq(reply.messages[0] == b"hello", true)
  assert_eq(reply.grpc_status, 0)
}

///|
test "client: an odd stream id is allocated per call" {
  let client = H2Client::new()
  assert_eq(client.open("/a/b"), 1)
  assert_eq(client.open("/a/b"), 3)
  assert_eq(client.open("/a/b"), 5)
}

///|
test "client: a GOAWAY drains the connection — refuses new streams, marks unprocessed calls retryable" {
  let client = H2Client::new()
  // Three in-flight calls straddling the peer's last-processed id 233: one well below,
  // one exactly at it, one above.
  let below = client.open("/qwq/hhh")
  client.next_id = 233
  let at = client.open("/emm/qwe")
  let above = client.open("/hhh/emm")
  assert_eq(below, 1)
  assert_eq(at, 233)
  assert_eq(above, 235)

  // The peer starts draining at stream 233.
  let out = client.feed(
    GoAway(last_stream_id=233, error_code=error_no_error, debug=b""),
  )
  // A GOAWAY is answered with no frames.
  assert_eq(out.length(), 0)
  assert_eq(client.goaway_received(), true)
  assert_eq(client.goaway_last_stream_id(), 233)

  // Only the call above the last-processed id is safe to retry; the one exactly at it
  // (the server may have handled it) and the one below are not.
  assert_eq(client.is_retryable(above), true)
  assert_eq(client.is_retryable(at), false)
  assert_eq(client.is_retryable(below), false)

  // No new RPC starts on a draining connection: open is refused with the reserved id 0
  // and no stream id is consumed.
  assert_eq(client.open("/qwe/qwq"), 0)
  assert_eq(client.next_id, 237)
}

///|
/// A header block larger than the peer's MAX_FRAME_SIZE goes out as HEADERS plus
/// CONTINUATION. Emitting it as one frame is a FRAME_SIZE_ERROR at the far end, and
/// a request carrying a large `-bin` value gets there easily. The floor the RFC puts
/// on MAX_FRAME_SIZE is 2^14, so the block has to actually exceed that.
test "an oversized request header block is split into CONTINUATION frames" {
  let e = H2Client::new()
  let big = Bytes::from_array(Array::make(20000, b'x'))
  let (_id, frames) = e.unary("/echo.E/Say", b"hi", metadata=[
    { name: b"x-big-bin", value: big, },
  ])
  let mut headers = 0
  let mut continuations = 0
  let mut last_end_headers = false
  for f in frames {
    match f {
      Headers(end_headers~, fragment~, ..) => {
        headers = headers + 1
        assert_eq(fragment.length() <= default_max_frame_size, true)
        last_end_headers = end_headers
      }
      Continuation(fragment~, end_headers~, ..) => {
        continuations = continuations + 1
        assert_eq(fragment.length() <= default_max_frame_size, true)
        last_end_headers = end_headers
      }
      _ => ()
    }
  }
  assert_eq(headers, 1)
  assert_eq(continuations > 0, true)
  assert_eq(last_end_headers, true)
}

// -- a call that ends without trailers ---------------------------------------

///|
/// Feed `client` a complete response header block for call `id`, ending the stream.
fn respond(client : H2Client, id : Int, headers : Array[Header]) -> Unit raise {
  let _ = client.feed(
    Headers(
      stream_id=id,
      fragment=HpackEncoder::new().encode(headers),
      end_stream=true,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
}

///|
test "client: a RST_STREAM ends the call with the status its code maps to" {
  // gRPC PROTOCOL-HTTP2, "HTTP2 Error Code -> Status". A reset arrives instead of
  // trailers, so the error code is the only thing left to build a status from; without
  // the mapping every reset reads as `-1` and a caller cannot tell a stream the server
  // refused (retry elsewhere) from one it dropped mid-flight.
  let cases = [
    (error_refused_stream, Unavailable),
    (error_enhance_your_calm, ResourceExhausted),
    (error_inadequate_security, PermissionDenied),
    (error_cancel, Cancelled),
    (error_protocol_error, Internal),
    (error_stream_closed, Internal),
  ]
  for pair in cases {
    let (code, want) = pair
    let client = H2Client::new()
    let (id, _req) = client.unary("/echo.Echo/Say", b"qwq")
    assert_eq(client.feed(RstStream(stream_id=id, error_code=code)).length(), 0)
    assert_eq(client.is_done(id), true)
    assert_eq(client.reply(id).grpc_status, Status::code(want))
  }
}

///|
test "client: a grpc-status already received outranks a later RST_STREAM code" {
  let client = H2Client::new()
  let (id, _req) = client.unary("/echo.Echo/Say", b"emm")
  respond(client, id, [
    { name: b":status", value: b"200", },
    { name: b"grpc-status", value: b"5", },
  ])
  // The server named NOT_FOUND itself; a reset code is a transport-level guess and
  // must not overwrite it.
  let _ = client.feed(RstStream(stream_id=id, error_code=error_cancel))
  assert_eq(client.reply(id).grpc_status, Status::code(NotFound))
}

///|
test "client: a non-200 :status with no grpc-status maps per the HTTP mapping" {
  // gRPC http-grpc-status-mapping: a proxy or a plain HTTP server answering on the
  // gRPC port sends an HTTP status and no gRPC trailers at all.
  let cases = [
    (b"400", Internal),
    (b"401", Unauthenticated),
    (b"403", PermissionDenied),
    (b"404", Unimplemented),
    (b"429", Unavailable),
    (b"502", Unavailable),
    (b"503", Unavailable),
    (b"504", Unavailable),
    (b"418", Unknown),
  ]
  for pair in cases {
    let (http, want) = pair
    let client = H2Client::new()
    let (id, _req) = client.unary("/echo.Echo/Say", b"hhh")
    respond(client, id, [{ name: b":status", value: http, }])
    assert_eq(client.is_done(id), true)
    assert_eq(client.reply(id).grpc_status, Status::code(want))
  }
}

///|
test "client: a 200 with no grpc-status anywhere still reports -1" {
  // The mapping covers HTTP-level refusals only. A 200 that never sends a trailer is
  // a broken gRPC server, and `-1` keeps saying so rather than inventing a status.
  let client = H2Client::new()
  let (id, _req) = client.unary("/echo.Echo/Say", b"qwe")
  respond(client, id, [
    { name: b":status", value: b"200", },
    { name: b"content-type", value: b"application/grpc", },
  ])
  assert_eq(client.reply(id).grpc_status, -1)
}
