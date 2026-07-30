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
    { name: b":method", value: b"POST" },
    { name: b":scheme", value: b"http" },
    { name: b":path", value: b"/echo.Echo/Say" },
    { name: b":authority", value: b"localhost" },
    { name: b"content-type", value: b"application/grpc" },
    { name: b"te", value: b"trailers" },
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
  let block = enc.encode([{ name: b":path", value: b"/nope.Svc/Method" }])
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
  let block = enc.encode([{ name: b":path", value: b"/big.Svc/M" }])
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
  // a zero-increment update pumps nothing more (window still exhausted).
  tally(
    engine.feed(WindowUpdate(stream_id=0, increment=0)),
    data_sent,
    trailer_seen,
  )
  assert_eq(data_sent.val, 5)
  // grow both windows generously and the rest of the body plus the trailer flow.
  tally(
    engine.feed(WindowUpdate(stream_id=0, increment=1000)),
    data_sent,
    trailer_seen,
  )
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
    { name: b":path", value: b"/echo.Echo/Say" },
    { name: b"grpc-encoding", value: b"gzip" },
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
  // A gRPC message whose compression flag (byte 0) is set: the server must not read
  // the body as-is, and answers UNIMPLEMENTED since it only supports identity.
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
