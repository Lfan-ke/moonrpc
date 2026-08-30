// In-memory drive of the server engine: real request frames in, real response
// frames out, through the actual frame + HPACK codecs — all backends. This is the
// mutation-verification target: break `dispatch`/`pump_stream` and these go red.

///|
fn concat(a : Bytes, b : Bytes) -> Bytes {
  let buf = Buffer()
  buf.write_bytes(a)
  buf.write_bytes(b)
  buf.to_bytes()
}

///|
test "engine: a unary request dispatches and is framed as HEADERS + DATA + trailers" {
  let engine = H2Server::new()
  engine.register("/echo.Echo/Say", req => concat(b"echo:", req))
  let enc = HpackEncoder::new()
  let block = enc.encode([
    { name: b":method", value: b"POST", },
    { name: b":scheme", value: b"http", },
    { name: b":path", value: b"/echo.Echo/Say", },
    { name: b":authority", value: b"localhost", },
    { name: b"content-type", value: b"application/grpc", },
    { name: b"te", value: b"trailers", },
  ])
  // client preface (SETTINGS) then the request stream.
  let _ = engine.feed(Settings(params=[], ack=false))
  let _ = engine.feed(
    Headers(
      stream_id=1,
      fragment=block,
      end_stream=false,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  let out = engine.feed(
    Data(stream_id=1, data=encode_message(b"world"), end_stream=true, padding=0),
  )
  // the response is exactly HEADERS, DATA, trailer-HEADERS.
  assert_eq(out.length(), 3)
  let dec = HpackDecoder::new()
  let mut status = b""
  let mut grpc_status = b""
  let mut ctype = b""
  let mut body = b""
  let mut header_frames = 0
  for f in out {
    match f {
      Headers(fragment~, ..) => {
        header_frames = header_frames + 1
        for h in dec.decode(fragment) {
          if h.name == b":status" {
            status = h.value
          }
          if h.name == b"content-type" {
            ctype = h.value
          }
          if h.name == b"grpc-status" {
            grpc_status = h.value
          }
        }
      }
      Data(data~, ..) => body = concat(body, data)
      _ => ()
    }
  }
  assert_eq(header_frames, 2)
  assert_eq(status == b"200", true)
  assert_eq(ctype == b"application/grpc", true)
  assert_eq(grpc_status == b"0", true)
  let msg = match decode_message(body) {
    Some((_, p)) => p
    None => b""
  }
  assert_eq(msg == b"echo:world", true)
  // the stream is fully closed after the END_STREAM trailer.
  assert_eq(engine.stream_state(1), Closed)
}

///|
test "engine: SETTINGS is acknowledged; PING is answered" {
  let engine = H2Server::new()
  let acks = engine.feed(
    Settings(params=[(settings_max_frame_size, 32768)], ack=false),
  )
  assert_eq(acks.length(), 1)
  match acks[0] {
    Settings(ack~, ..) => assert_eq(ack, true)
    _ => abort("expected SETTINGS ack")
  }
  let pong = engine.feed(
    Ping(payload=b"\x01\x02\x03\x04\x05\x06\x07\x08", ack=false),
  )
  assert_eq(pong.length(), 1)
  match pong[0] {
    Ping(ack~, payload~) => {
      assert_eq(ack, true)
      assert_eq(payload == b"\x01\x02\x03\x04\x05\x06\x07\x08", true)
    }
    _ => abort("expected PING pong")
  }
}

///|
test "engine: an unknown method gets a trailers-only UNIMPLEMENTED response" {
  let engine = H2Server::new()
  let enc = HpackEncoder::new()
  let block = enc.encode([{ name: b":path", value: b"/nope.Svc/Method", }])
  let _ = engine.feed(Settings(params=[], ack=false))
  let out = engine.feed(
    Headers(
      stream_id=1,
      fragment=block,
      end_stream=true,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  // trailers-only: a single END_STREAM HEADERS, no DATA.
  assert_eq(out.length(), 1)
  let dec = HpackDecoder::new()
  let mut grpc_status = b""
  match out[0] {
    Headers(fragment~, end_stream~, ..) => {
      assert_eq(end_stream, true)
      for h in dec.decode(fragment) {
        if h.name == b"grpc-status" {
          grpc_status = h.value
        }
      }
    }
    _ => abort("expected a HEADERS frame")
  }
  // 12 == UNIMPLEMENTED.
  assert_eq(grpc_status == b"12", true)
}

///|
/// Count DATA octets and whether an END_STREAM (trailer) HEADERS appears.
fn tally(frames : Array[Frame], acc : Ref[Int], trailer : Ref[Bool]) -> Unit {
  for f in frames {
    match f {
      Data(data~, ..) => acc.val = acc.val + data.length()
      Headers(end_stream~, ..) => if end_stream { trailer.val = true }
      _ => ()
    }
  }
}

///|
test "engine: a small send window splits the reply and a WINDOW_UPDATE releases it" {
  let engine = H2Server::new()
  // a 20-byte reply, larger than the window we advertise.
  engine.register("/big.Svc/M", _req => b"0123456789ABCDEFGHIJ")
  let enc = HpackEncoder::new()
  let block = enc.encode([{ name: b":path", value: b"/big.Svc/M", }])
  // peer advertises a tiny 5-octet initial window before opening the stream.
  let _ = engine.feed(
    Settings(params=[(settings_initial_window_size, 5)], ack=false),
  )
  let data_sent = Ref(0)
  let trailer_seen = Ref(false)
  // dispatch fires on the END_STREAM HEADERS: HEADERS + only 5 DATA octets escape
  // the tiny window, and no trailer yet.
  let dispatch_out = engine.feed(
    Headers(
      stream_id=1,
      fragment=block,
      end_stream=true,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  tally(dispatch_out, data_sent, trailer_seen)
  assert_eq(data_sent.val, 5)
  assert_eq(trailer_seen.val, false)
  // a connection-level update alone pumps nothing more: the stream window is the
  // exhausted one.
  tally(
    engine.feed(WindowUpdate(stream_id=0, increment=1000)),
    data_sent,
    trailer_seen,
  )
  assert_eq(data_sent.val, 5)
  // growing the stream window too releases the rest of the body and the trailer.
  tally(
    engine.feed(WindowUpdate(stream_id=1, increment=1000)),
    data_sent,
    trailer_seen,
  )
  // the full length-prefixed message (5 prefix + 20 body = 25 octets) got out,
  // and only after the body drained did the END_STREAM trailer follow.
  assert_eq(data_sent.val, 25)
  assert_eq(trailer_seen.val, true)
  assert_eq(engine.stream_state(1), Closed)
}

///|
test "percent_encode follows the grpc-message spec" {
  // Printable ASCII (incl. space) passes through unchanged.
  assert_eq(percent_encode("name is required") == b"name is required", true)
  // '%' itself is escaped.
  assert_eq(percent_encode("50%") == b"50%25", true)
  // Non-ASCII is escaped byte-by-byte over its UTF-8 encoding (é = C3 A9).
  assert_eq(percent_encode("é") == b"%C3%A9", true)
  // Control characters are escaped (newline 0x0A).
  assert_eq(percent_encode("a\nb") == b"a%0Ab", true)
}

///|
test "engine: a compressed request under an unsupported encoding gets UNIMPLEMENTED" {
  let engine = H2Server::new()
  engine.register("/echo.Echo/Say", req => req)
  let enc = HpackEncoder::new()
  let block = enc.encode([
    { name: b":path", value: b"/echo.Echo/Say", },
    { name: b"grpc-encoding", value: b"snappy", },
  ])
  let _ = engine.feed(Settings(params=[], ack=false))
  let _ = engine.feed(
    Headers(
      stream_id=1,
      fragment=block,
      end_stream=false,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  // A gRPC message whose compression flag (byte 0) is set under an encoding we do not
  // decode: the server must not read the body as-is, and answers UNIMPLEMENTED since
  // it accepts only identity and gzip.
  let compressed = b"\x01\x00\x00\x00\x03abc"
  let out = engine.feed(
    Data(stream_id=1, data=compressed, end_stream=true, padding=0),
  )
  let dec = HpackDecoder::new()
  let mut grpc_status = b""
  for f in out {
    match f {
      Headers(fragment~, ..) =>
        for h in dec.decode(fragment) {
          if h.name == b"grpc-status" {
            grpc_status = h.value
          }
        }
      _ => ()
    }
  }
  assert_eq(grpc_status == b"12", true)
}

///|
test "add_window saturates at 2^31-1 instead of wrapping negative" {
  // A WINDOW_UPDATE that pushes the window past 2^31-1 must not wrap to a large
  // negative window (which would stall every send); it saturates.
  assert_eq(add_window(0x7FFFFFFF, 0x7FFFFFFF), 0x7FFFFFFF)
  assert_eq(add_window(default_window_size, 1000), default_window_size + 1000)
  assert_eq(add_window(100, -40), 60)
}

///|
test "engine: a CONTINUATION past the header-size cap is rejected, not buffered forever" {
  let engine = H2Server::new()
  let enc = HpackEncoder::new()
  let block = enc.encode([{ name: b":path", value: b"/e.E/M", }])
  let _ = engine.feed(Settings(params=[], ack=false))
  let _ = engine.feed(
    Headers(
      stream_id=1,
      fragment=block,
      end_stream=false,
      end_headers=false,
      priority=None,
      padding=0,
    ),
  )
  let buf = Buffer()
  for _i = 0; _i < max_header_list_size + 16; _i = _i + 1 {
    buf.write_byte(b'\x00')
  }
  let out = engine.feed(
    Continuation(stream_id=1, fragment=buf.to_bytes(), end_headers=false),
  )
  assert_eq(out.length(), 1)
  match out[0] {
    GoAway(error_code~, ..) => assert_eq(error_code, error_frame_size_error)
    _ => abort("expected GOAWAY")
  }
  assert_eq(engine.closing(), true)
}

///|
test "engine: a HEADERS on an even (server-initiated) stream id is a protocol error" {
  let engine = H2Server::new()
  engine.register("/e.E/M", req => req)
  let enc = HpackEncoder::new()
  let block = enc.encode([{ name: b":path", value: b"/e.E/M", }])
  let _ = engine.feed(Settings(params=[], ack=false))
  // Stream id 2 is even, so server-initiated — a client must not open it.
  let out = engine.feed(
    Headers(
      stream_id=2,
      fragment=block,
      end_stream=true,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  assert_eq(out.length(), 1)
  match out[0] {
    GoAway(error_code~, ..) => assert_eq(error_code, error_protocol_error)
    _ => abort("expected GOAWAY")
  }
  assert_eq(engine.closing(), true)
}

///|
test "engine: a message length prefix past the cap gets RESOURCE_EXHAUSTED, not a crash" {
  let engine = H2Server::new()
  engine.register("/echo.Echo/Say", req => req)
  let enc = HpackEncoder::new()
  let block = enc.encode([{ name: b":path", value: b"/echo.Echo/Say", }])
  let _ = engine.feed(Settings(params=[], ack=false))
  let _ = engine.feed(
    Headers(
      stream_id=1,
      fragment=block,
      end_stream=false,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  // A gRPC message prefix of 0x80000000: naively read as a signed Int this is negative,
  // and `data[5:5+len]` would slice with end < start and abort the whole process.
  let hostile = b"\x00\x80\x00\x00\x00\x01\x02"
  let out = engine.feed(
    Data(stream_id=1, data=hostile, end_stream=true, padding=0),
  )
  let dec = HpackDecoder::new()
  let mut grpc_status = b""
  for f in out {
    match f {
      Headers(fragment~, ..) =>
        for h in dec.decode(fragment) {
          if h.name == b"grpc-status" {
            grpc_status = h.value
          }
        }
      _ => ()
    }
  }
  // 8 == RESOURCE_EXHAUSTED.
  assert_eq(grpc_status == b"8", true)
}

///|
test "engine: a gzip-compressed request is inflated and reaches the handler" {
  let engine = H2Server::new()
  // The handler echoes whatever bytes it receives; a correct decode means the reply
  // equals the original plaintext, not the gzip bytes.
  engine.register("/echo.Echo/Say", req => req)
  let enc = HpackEncoder::new()
  let block = enc.encode([
    { name: b":path", value: b"/echo.Echo/Say", },
    { name: b"grpc-encoding", value: b"gzip", },
  ])
  let _ = engine.feed(Settings(params=[], ack=false))
  let _ = engine.feed(
    Headers(
      stream_id=1,
      fragment=block,
      end_stream=false,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  // A real gzip member of "hello, gzip world" (CPython gzip.compress), framed as one
  // gRPC message with the compression flag set.
  let plaintext = gz_unhex("68656c6c6f2c20677a697020776f726c64")
  let gz = gz_unhex(
    "1f8b0800cb9b6a6a02ffcb48cdc9c9d75148afca2c5028cf2fca490100eb83468211000000",
  )
  let out = engine.feed(
    Data(
      stream_id=1,
      data=encode_message(gz, compressed=true),
      end_stream=true,
      padding=0,
    ),
  )
  let dec = HpackDecoder::new()
  let mut grpc_status = b""
  let mut body = b""
  for f in out {
    match f {
      Headers(fragment~, ..) =>
        for h in dec.decode(fragment) {
          if h.name == b"grpc-status" {
            grpc_status = h.value
          }
        }
      Data(data~, ..) => body = concat(body, data)
      _ => ()
    }
  }
  assert_eq(grpc_status == b"0", true)
  // The reply frames the decoded plaintext as an identity gRPC message.
  assert_eq(
    decode_message(body) is Some((false, msg)) && msg == plaintext,
    true,
  )
}

///|
test "emit_header_frames floors an illegal zero MAX_FRAME_SIZE instead of looping forever" {
  let buf = Buffer()
  for _i = 0; _i < 20000; _i = _i + 1 {
    buf.write_byte(b'\x00')
  }
  let block = buf.to_bytes()
  // A peer MAX_FRAME_SIZE of 0 previously made the split loop advance by zero bytes
  // and emit empty CONTINUATION frames forever; it now floors to the default and the
  // 20000-byte block splits into a finite HEADERS + one CONTINUATION.
  let frames = emit_header_frames(1, block, true, 0)
  assert_eq(frames.length(), 2)
}

///|
test "emit_header_frames splits a block larger than max_frame into HEADERS + CONTINUATION" {
  // 10-byte block, max_frame 4 -> HEADERS(4, end_headers=false) + CONTINUATION(4) +
  // CONTINUATION(2, end_headers=true).
  let frames = emit_header_frames(1, b"0123456789", true, 4)
  assert_eq(frames.length(), 3)
  match frames[0] {
    Headers(fragment~, end_headers~, end_stream~, ..) => {
      assert_eq(end_headers, false)
      assert_eq(end_stream, true)
      assert_eq(fragment == b"0123", true)
    }
    _ => abort("expected HEADERS")
  }
  match frames[1] {
    Continuation(fragment~, end_headers~, ..) => {
      assert_eq(end_headers, false)
      assert_eq(fragment == b"4567", true)
    }
    _ => abort("expected CONTINUATION")
  }
  match frames[2] {
    Continuation(fragment~, end_headers~, ..) => {
      assert_eq(end_headers, true)
      assert_eq(fragment == b"89", true)
    }
    _ => abort("expected CONTINUATION")
  }
  // A block within max_frame stays a single end_headers=true HEADERS.
  assert_eq(emit_header_frames(1, b"ab", false, 4).length(), 1)
}

///|
test "goaway emits a NO_ERROR GOAWAY carrying the last processed stream id" {
  let out = H2Server::new().goaway(7)
  assert_eq(out.length(), 1)
  match out[0] {
    GoAway(last_stream_id~, error_code~, ..) => {
      assert_eq(last_stream_id, 7)
      assert_eq(error_code, error_no_error)
    }
    _ => abort("expected GOAWAY")
  }
}

// -- RFC 9113 §5.4 fault reporting ------------------------------------------

///|
/// Drive one complete unary call on stream `id` and return the `grpc-status` it came
/// back with, so a test can show the connection is still serving after a stream error.
fn call_once(
  engine : H2Server,
  enc : HpackEncoder,
  dec : HpackDecoder,
  id : Int,
  path : Bytes,
  body : Bytes,
) -> Bytes raise {
  let block = enc.encode([{ name: b":path", value: path, }])
  let _ = engine.feed(
    Headers(
      stream_id=id,
      fragment=block,
      end_stream=false,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  let out = engine.feed(
    Data(stream_id=id, data=encode_message(body), end_stream=true, padding=0),
  )
  let mut status = b""
  for f in out {
    match f {
      Headers(fragment~, ..) =>
        for h in dec.decode(fragment) {
          if h.name == b"grpc-status" {
            status = h.value
          }
        }
      _ => ()
    }
  }
  status
}

///|
test "engine: a zero-increment WINDOW_UPDATE resets that stream and spares the connection" {
  let engine = H2Server::new()
  engine.register("/e.E/M", req => concat(b"re:", req))
  let enc = HpackEncoder::new()
  let dec = HpackDecoder::new()
  let _ = engine.feed(Settings(params=[], ack=false))
  // RFC 9113 §6.9: a zero increment credits nothing, and on a stream it costs only
  // that stream.
  let out = engine.feed(WindowUpdate(stream_id=1, increment=0))
  assert_eq(out.length(), 1)
  match out[0] {
    RstStream(stream_id~, error_code~) => {
      assert_eq(stream_id, 1)
      assert_eq(error_code, error_protocol_error)
    }
    _ => abort("expected RST_STREAM")
  }
  assert_eq(engine.closing(), false)
  // The connection is untouched: a fresh stream runs a call to completion on it.
  assert_eq(call_once(engine, enc, dec, 3, b"/e.E/M", b"qwq") == b"0", true)
}

///|
test "engine: a frame on a finished stream is a stream error, not a connection one" {
  let engine = H2Server::new()
  engine.register("/e.E/M", req => req)
  let enc = HpackEncoder::new()
  let dec = HpackDecoder::new()
  let _ = engine.feed(Settings(params=[], ack=false))
  assert_eq(call_once(engine, enc, dec, 1, b"/e.E/M", b"hhh") == b"0", true)
  // Stream 1 is closed; §5.1 makes more DATA on it a STREAM_CLOSED stream error.
  let out = engine.feed(
    Data(stream_id=1, data=encode_message(b"late"), end_stream=true, padding=0),
  )
  assert_eq(out.length(), 1)
  match out[0] {
    RstStream(stream_id~, error_code~) => {
      assert_eq(stream_id, 1)
      assert_eq(error_code, error_stream_closed)
    }
    _ => abort("expected RST_STREAM")
  }
  assert_eq(engine.closing(), false)
  assert_eq(call_once(engine, enc, dec, 3, b"/e.E/M", b"qwq") == b"0", true)
}

///|
test "engine: DATA on stream 0 ends the connection with a PROTOCOL_ERROR GOAWAY" {
  let engine = H2Server::new()
  engine.register("/e.E/M", req => req)
  let _ = engine.feed(Settings(params=[], ack=false))
  let out = engine.feed(
    Data(stream_id=0, data=b"hhh", end_stream=false, padding=0),
  )
  assert_eq(out.length(), 1)
  match out[0] {
    GoAway(last_stream_id~, error_code~, debug~) => {
      assert_eq(last_stream_id, 0)
      assert_eq(error_code, error_protocol_error)
      assert_eq(debug == b"DATA on stream 0", true)
    }
    _ => abort("expected GOAWAY")
  }
  // §5.4.1: the connection is over once the GOAWAY is out, so nothing else runs.
  assert_eq(engine.closing(), true)
  assert_eq(engine.feed(Ping(payload=b"heke1228", ack=false)).length(), 0)
}

///|
test "engine: a CONTINUATION with no field block open is a connection error" {
  let engine = H2Server::new()
  let _ = engine.feed(Settings(params=[], ack=false))
  // §6.10: nothing left END_HEADERS clear, so there is no block to continue.
  let out = engine.feed(
    Continuation(stream_id=1, fragment=b"", end_headers=true),
  )
  assert_eq(out.length(), 1)
  match out[0] {
    GoAway(error_code~, ..) => assert_eq(error_code, error_protocol_error)
    _ => abort("expected GOAWAY")
  }
  assert_eq(engine.closing(), true)
}

///|
test "engine: a WINDOW_UPDATE past the 2^31-1 window ceiling is a FLOW_CONTROL_ERROR" {
  let engine = H2Server::new()
  let _ = engine.feed(Settings(params=[], ack=false))
  // §6.9.1: both windows start at 65535, so 2^31-1 more overflows either one.
  let stream_out = engine.feed(WindowUpdate(stream_id=1, increment=0x7FFFFFFF))
  assert_eq(stream_out.length(), 1)
  match stream_out[0] {
    RstStream(stream_id~, error_code~) => {
      assert_eq(stream_id, 1)
      assert_eq(error_code, error_flow_control_error)
    }
    _ => abort("expected RST_STREAM")
  }
  let conn_out = engine.feed(WindowUpdate(stream_id=0, increment=0x7FFFFFFF))
  assert_eq(conn_out.length(), 1)
  match conn_out[0] {
    GoAway(error_code~, ..) => assert_eq(error_code, error_flow_control_error)
    _ => abort("expected GOAWAY")
  }
}

///|
test "engine: a field block that will not decode is a COMPRESSION_ERROR" {
  let engine = H2Server::new()
  let _ = engine.feed(Settings(params=[], ack=false))
  // An indexed field naming index 0 is illegal HPACK (RFC 7541 §6.1). The block never
  // decodes, so our compression context stops matching the peer's for good.
  let out = engine.feed(
    Headers(
      stream_id=1,
      fragment=b"",
      end_stream=true,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  assert_eq(out.length(), 1)
  match out[0] {
    GoAway(error_code~, ..) => assert_eq(error_code, error_compression_error)
    _ => abort("expected GOAWAY")
  }
  assert_eq(engine.closing(), true)
}

///|
test "engine: a stream opened after our own GOAWAY is refused, not served" {
  let engine = H2Server::new()
  engine.register("/e.E/M", req => req)
  let enc = HpackEncoder::new()
  let dec = HpackDecoder::new()
  let _ = engine.feed(Settings(params=[], ack=false))
  assert_eq(call_once(engine, enc, dec, 1, b"/e.E/M", b"hhh") == b"0", true)
  // The drain names stream 1 as the last one we will handle.
  let _ = engine.goaway(engine.highest_stream())
  let block = enc.encode([{ name: b":path", value: b"/e.E/M", }])
  let out = engine.feed(
    Headers(
      stream_id=3,
      fragment=block,
      end_stream=true,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  assert_eq(out.length(), 1)
  match out[0] {
    RstStream(stream_id~, error_code~) => {
      assert_eq(stream_id, 3)
      // §6.8: it was never processed, so the client is free to re-issue it elsewhere.
      assert_eq(error_code, error_refused_stream)
    }
    _ => abort("expected RST_STREAM")
  }
  assert_eq(engine.closing(), false)
}

///|
test "engine: a reset stream stops mid-call and sends nothing more" {
  let engine = H2Server::new()
  engine.register("/e.E/M", req => concat(b"re:", req))
  let enc = HpackEncoder::new()
  let dec = HpackDecoder::new()
  let _ = engine.feed(Settings(params=[], ack=false))
  let block = enc.encode([{ name: b":path", value: b"/e.E/M", }])
  let _ = engine.feed(
    Headers(
      stream_id=1,
      fragment=block,
      end_stream=false,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  // A window past the ceiling resets stream 1 while the call is still open.
  let out = engine.feed(WindowUpdate(stream_id=1, increment=0x7FFFFFFF))
  assert_eq(out.length(), 1)
  match out[0] {
    RstStream(stream_id~, error_code~) => {
      assert_eq(stream_id, 1)
      assert_eq(error_code, error_flow_control_error)
    }
    _ => abort("expected RST_STREAM")
  }
  // §5.4.2: the reset closed the stream, so the request that finishes arriving after
  // it earns another reset — never the response the handler would have produced.
  assert_eq(engine.stream_state(1), Closed)
  let late = engine.feed(
    Data(stream_id=1, data=encode_message(b"qwq"), end_stream=true, padding=0),
  )
  assert_eq(late.length(), 1)
  match late[0] {
    RstStream(stream_id~, error_code~) => {
      assert_eq(stream_id, 1)
      assert_eq(error_code, error_stream_closed)
    }
    _ => abort("expected RST_STREAM")
  }
  assert_eq(engine.closing(), false)
  assert_eq(call_once(engine, enc, dec, 3, b"/e.E/M", b"hhh") == b"0", true)
}

///|
/// A peer's RST_STREAM has to clear the half-read header block, not just mark the
/// stream closed. A CONTINUATION arriving afterwards is the peer's protocol error —
/// it is continuing a block on a stream it just reset — and blaming ourselves with
/// INTERNAL_ERROR would send the wrong diagnostic to the wrong side.
test "a peer's RST_STREAM abandons the header block it was in the middle of" {
  let e = H2Server::new()
  e.register("/echo.E/Say", req => req)
  let enc = HpackEncoder::new()
  let block = enc.encode([
    { name: b":method", value: b"POST", },
    { name: b":scheme", value: b"http", },
    { name: b":path", value: b"/echo.E/Say", },
    { name: b"content-type", value: b"application/grpc", },
  ])
  let _ = e.feed(Settings(params=[], ack=false))
  // Open a block and deliberately leave it open.
  let _ = e.feed(
    Headers(
      stream_id=1,
      fragment=block,
      end_stream=false,
      end_headers=false,
      priority=None,
      padding=0,
    ),
  )
  let _ = e.feed(RstStream(stream_id=1, error_code=error_cancel))
  assert_eq(e.stream_state(1), StreamState::Closed)
  // Continuing the abandoned block is the peer's fault, and the GOAWAY has to say so.
  let after = e.feed(
    Continuation(stream_id=1, fragment=block, end_headers=true),
  )
  let mut code = -1
  for f in after {
    if f is GoAway(error_code~, ..) {
      code = error_code
    }
  }
  assert_eq(code, error_protocol_error)
}
