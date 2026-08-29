// In-memory drive of the streaming engine: real request frames in, real response
// frames out, through the actual frame + HPACK + message codecs on every backend.
// This is the mutation-verification target for the streaming modes — break the
// stream terminator or the per-message framing and these go red.

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
/// Collect a response: the reassembled reply messages (in order), the `grpc-status`
/// value, whether a trailer (END_STREAM HEADERS) was seen, and the response initial
/// and trailing metadata. A single decoder threads through every HEADERS block in
/// emission order so the HPACK dynamic table stays in sync with the encoder.
struct Collected {
  messages : Array[Bytes]
  mut grpc_status : Bytes
  mut trailer_seen : Bool
  mut header_meta : Array[Header]
  mut trailer_meta : Array[Header]
}

///|
fn collect(frames : Array[Frame], dec : HpackDecoder) -> Collected raise {
  let acc = Buffer()
  let c : Collected = {
    messages: [],
    grpc_status: b"",
    trailer_seen: false,
    header_meta: [],
    trailer_meta: [],
  }
  for f in frames {
    match f {
      Data(data~, ..) => acc.write_bytes(data)
      Headers(fragment~, end_stream~, ..) => {
        let hs = dec.decode(fragment)
        for h in hs {
          if h.name == b"grpc-status" {
            c.grpc_status = h.value
          }
        }
        if end_stream {
          c.trailer_seen = true
          c.trailer_meta = hs
        } else {
          c.header_meta = hs
        }
      }
      _ => ()
    }
  }
  for m in split_messages(acc.to_bytes()) {
    c.messages.push(m)
  }
  c
}

///|
/// Drive a whole request stream against a fresh decoder, accumulating every
/// response frame the engine emits across all the fed frames.
fn drive_request(
  engine : H2Server,
  request_frames : Array[Frame],
) -> (Collected, HpackDecoder) raise {
  let dec = HpackDecoder::new()
  let all : Array[Frame] = []
  for f in request_frames {
    for out in engine.feed(f) {
      all.push(out)
    }
  }
  (collect(all, dec), dec)
}

///|
fn req_headers(enc : HpackEncoder, path : Bytes, end_stream~ : Bool) -> Frame {
  let block = enc.encode([
    { name: b":method", value: b"POST", },
    { name: b":scheme", value: b"http", },
    { name: b":path", value: path, },
    { name: b":authority", value: b"localhost", },
    { name: b"content-type", value: b"application/grpc", },
    { name: b"te", value: b"trailers", },
  ])
  Headers(
    stream_id=1,
    fragment=block,
    end_stream~,
    end_headers=true,
    priority=None,
    padding=0,
  )
}

///|
fn data_msg(payload : Bytes, end_stream~ : Bool) -> Frame {
  Data(stream_id=1, data=encode_message(payload), end_stream~, padding=0)
}

///|
test "server-streaming: one request fans out to many ordered reply messages" {
  let engine = H2Server::new()
  engine.register_server_streaming("/count.C/Up", (_ctx, req) => {
    let base = req
    [cat(base, b"-1"), cat(base, b"-2"), cat(base, b"-3")]
  })
  let enc = HpackEncoder::new()
  let _ = engine.feed(Settings(params=[], ack=false))
  let (c, _) = drive_request(engine, [
    req_headers(enc, b"/count.C/Up", end_stream=false),
    data_msg(b"n", end_stream=true),
  ])
  // three distinct reply messages, in order, then the closing status.
  assert_eq(c.messages.length(), 3)
  assert_eq(c.messages[0] == b"n-1", true)
  assert_eq(c.messages[1] == b"n-2", true)
  assert_eq(c.messages[2] == b"n-3", true)
  assert_eq(c.trailer_seen, true)
  assert_eq(c.grpc_status == b"0", true)
  assert_eq(engine.stream_state(1), Closed)
}

///|
test "client-streaming: many request messages aggregate into one reply" {
  let engine = H2Server::new()
  engine.register_client_streaming("/sum.S/Add", (_ctx, msgs) => {
    let buf = Buffer()
    buf.write_bytes(b"got:")
    for m in msgs {
      buf.write_byte(b'/')
      buf.write_bytes(m)
    }
    buf.to_bytes()
  })
  let enc = HpackEncoder::new()
  let _ = engine.feed(Settings(params=[], ack=false))
  let (c, _) = drive_request(engine, [
    req_headers(enc, b"/sum.S/Add", end_stream=false),
    data_msg(b"a", end_stream=false),
    data_msg(b"b", end_stream=false),
    data_msg(b"c", end_stream=true),
  ])
  assert_eq(c.messages.length(), 1)
  // All three messages were collected, in order, before the reply fired.
  assert_eq(c.messages[0] == b"got:/a/b/c", true)
  assert_eq(c.grpc_status == b"0", true)
  assert_eq(engine.stream_state(1), Closed)
}

///|
test "bidi-streaming: replies interleave with requests, each message echoed as it arrives" {
  let engine = H2Server::new()
  let seen = Ref(0)
  engine.register_bidi("/chat.C/Echo", _ctx => {
    let count = seen
    BidiHandler::{
      on_message: m => {
        count.val = count.val + 1
        [cat(b"re:", m)]
      },
      on_end: () => [b"bye"],
    }
  })
  let enc = HpackEncoder::new()
  let _ = engine.feed(Settings(params=[], ack=false))
  // feed one message at a time and watch a reply come back before the next arrives.
  let _ = engine.feed(req_headers(enc, b"/chat.C/Echo", end_stream=false))
  let dec = HpackDecoder::new()
  let after_first = collect(
    engine.feed(data_msg(b"one", end_stream=false)),
    dec,
  )
  assert_eq(after_first.messages.length(), 1)
  assert_eq(after_first.messages[0] == b"re:one", true)
  assert_eq(after_first.trailer_seen, false)
  assert_eq(seen.val, 1)
  let after_second = collect(
    engine.feed(data_msg(b"two", end_stream=false)),
    dec,
  )
  assert_eq(after_second.messages.length(), 1)
  assert_eq(after_second.messages[0] == b"re:two", true)
  assert_eq(seen.val, 2)
  // the client half-closes: on_end runs, its final message plus the trailer flow.
  let after_end = collect(engine.feed(data_msg(b"three", end_stream=true)), dec)
  assert_eq(after_end.messages.length(), 2)
  assert_eq(after_end.messages[0] == b"re:three", true)
  assert_eq(after_end.messages[1] == b"bye", true)
  assert_eq(after_end.trailer_seen, true)
  assert_eq(after_end.grpc_status == b"0", true)
  assert_eq(seen.val, 3)
  assert_eq(engine.stream_state(1), Closed)
}

///|
test "server-streaming honours flow control across the multi-message exchange" {
  let engine = H2Server::new()
  // three 10-byte messages => 3 * (5 prefix + 10) = 45 octets of response body.
  engine.register_server_streaming("/big.B/Stream", (_ctx, _req) => {
    [b"AAAAAAAAAA", b"BBBBBBBBBB", b"CCCCCCCCCC"]
  })
  let enc = HpackEncoder::new()
  // advertise a tiny 8-octet initial window before the stream opens.
  let _ = engine.feed(
    Settings(params=[(settings_initial_window_size, 8)], ack=false),
  )
  let _ = engine.feed(req_headers(enc, b"/big.B/Stream", end_stream=false))
  let data_sent = Ref(0)
  let trailer = Ref(false)
  tally(engine.feed(data_msg(b"go", end_stream=true)), data_sent, trailer)
  // only the 8 octets the window allows escaped; no trailer while body remains.
  assert_eq(data_sent.val, 8)
  assert_eq(trailer.val, false)
  // grow both windows and the remaining 37 octets plus the trailer flow.
  tally(
    engine.feed(WindowUpdate(stream_id=0, increment=1000)),
    data_sent,
    trailer,
  )
  tally(
    engine.feed(WindowUpdate(stream_id=1, increment=1000)),
    data_sent,
    trailer,
  )
  assert_eq(data_sent.val, 45)
  assert_eq(trailer.val, true)
  assert_eq(engine.stream_state(1), Closed)
}

///|
test "metadata and grpc-timeout deadline are surfaced to the handler" {
  let engine = H2Server::new()
  engine.register_unary("/meta.M/Echo", (ctx, _req) => {
    // echo the deadline (in ms) and a custom request header into the reply, and
    // set response initial and trailing metadata.
    ctx.add_header(b"x-init", b"v1")
    ctx.add_trailer(b"x-server", b"moonrpc")
    let token = match ctx.metadata_get(b"x-token") {
      Some(v) => v
      None => b"none"
    }
    let deadline = match ctx.deadline_millis {
      Some(ms) => int_to_ascii_bytes(ms)
      None => b"no-deadline"
    }
    let buf = Buffer()
    buf.write_bytes(token)
    buf.write_byte(b'|')
    buf.write_bytes(deadline)
    buf.to_bytes()
  })
  let enc = HpackEncoder::new()
  let block = enc.encode([
    { name: b":path", value: b"/meta.M/Echo", },
    { name: b"content-type", value: b"application/grpc", },
    { name: b"grpc-timeout", value: b"2S", },
    { name: b"x-token", value: b"secret", },
  ])
  let _ = engine.feed(Settings(params=[], ack=false))
  let dec = HpackDecoder::new()
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
  let c = collect(out, dec)
  assert_eq(c.messages.length(), 1)
  // reply begins with the echoed token "secret".
  let reply = c.messages[0]
  assert_eq(reply[0] == b's' && reply[5] == b't', true)
  // 2S parsed to 2000 ms: the reply ends with "2000".
  let n = reply.length()
  assert_eq(reply[n - 4] == b'2' && reply[n - 3] == b'0', true)
  assert_eq(reply[n - 2] == b'0' && reply[n - 1] == b'0', true)
  // response initial metadata x-init rode in the response HEADERS.
  let mut init_found = false
  for h in c.header_meta {
    if h.name == b"x-init" && h.value == b"v1" {
      init_found = true
    }
  }
  assert_eq(init_found, true)
  // trailing metadata x-server rode along with grpc-status.
  assert_eq(c.grpc_status == b"0", true)
  let mut found = false
  for h in c.trailer_meta {
    if h.name == b"x-server" && h.value == b"moonrpc" {
      found = true
    }
  }
  assert_eq(found, true)
}

///|
test "grpc-timeout parses every unit to milliseconds" {
  assert_eq(parse_grpc_timeout(b"1H") == Some(3600000), true)
  assert_eq(parse_grpc_timeout(b"5M") == Some(300000), true)
  assert_eq(parse_grpc_timeout(b"3S") == Some(3000), true)
  assert_eq(parse_grpc_timeout(b"250m") == Some(250), true)
  assert_eq(parse_grpc_timeout(b"5000u") == Some(5), true)
  assert_eq(parse_grpc_timeout(b"2000000n") == Some(2), true)
  assert_eq(parse_grpc_timeout(b"") == None, true)
  assert_eq(parse_grpc_timeout(b"9X") == None, true)
  assert_eq(parse_grpc_timeout(b"aS") == None, true)
  // More than 8 digits is malformed (RFC), not a giant deadline.
  assert_eq(parse_grpc_timeout(b"999999999S") == None, true)
  // An 8-digit hour value overflows a 32-bit millisecond count; it saturates at
  // Int.MAX rather than wrapping to a negative (already-past) deadline.
  assert_eq(parse_grpc_timeout(b"99999999H") == Some(2147483647), true)
}
