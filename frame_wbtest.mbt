///|
/// Encode a frame, decode it back, and assert the byte count round-trips exactly.
fn roundtrip(f : Frame) -> Frame raise {
  let bytes = f.encode()
  let (g, consumed) = decode_frame(bytes)
  assert_eq(consumed, bytes.length())
  g
}

///|
test "frame header: 9-octet layout round-trips (RFC 7540 §4.1)" {
  let h : FrameHeader = {
    length: 0x0000FF,
    ftype: 0x1,
    flags: 0x25,
    stream_id: 1,
  }
  let bytes = h.encode()
  assert_eq(bytes.length(), 9)
  assert_eq(bytes[0].to_int(), 0x00)
  assert_eq(bytes[1].to_int(), 0x00)
  assert_eq(bytes[2].to_int(), 0xFF) // length low byte
  assert_eq(bytes[3].to_int(), 0x01) // type HEADERS
  assert_eq(bytes[4].to_int(), 0x25) // flags
  assert_eq(bytes[8].to_int(), 0x01) // stream id
  let d = decode_frame_header(bytes)
  assert_eq(d == h, true)
}

///|
test "frame header: reserved top bit is masked off the stream id" {
  // stream id field with the reserved bit set on the wire -> masked to 31 bits.
  let raw = b"\x00\x00\x00\x08\x00\x80\x00\x00\x01"
  let d = decode_frame_header(raw)
  assert_eq(d.stream_id, 1)
  assert_eq(d.ftype, frame_window_update)
}

///|
test "DATA frame round-trips with END_STREAM and padding" {
  let f = Frame::Data(stream_id=3, data=b"payload", end_stream=true, padding=0)
  assert_eq(roundtrip(f) == f, true)
  let bytes = f.encode()
  assert_eq(bytes[3].to_int(), frame_data)
  assert_eq(bytes[4].to_int() & flag_end_stream, flag_end_stream)
  let padded = Frame::Data(stream_id=1, data=b"hi", end_stream=false, padding=4)
  assert_eq(roundtrip(padded) == padded, true)
  let pbytes = padded.encode()
  assert_eq(pbytes[4].to_int() & flag_padded, flag_padded)
  // payload = pad-length(1) + data(2) + padding(4) = 7
  assert_eq(be_read_u24(pbytes, 0), 7)
}

///|
test "HEADERS frame round-trips with priority, END_HEADERS and padding" {
  let plain = Frame::Headers(
    stream_id=1,
    fragment=b"\x82\x86\x84",
    end_stream=false,
    end_headers=true,
    priority=None,
    padding=0,
  )
  assert_eq(roundtrip(plain) == plain, true)
  let pri : Priority = { exclusive: true, stream_dependency: 3, weight: 200, }
  let full = Frame::Headers(
    stream_id=5,
    fragment=b"\x82",
    end_stream=true,
    end_headers=true,
    priority=Some(pri),
    padding=6,
  )
  assert_eq(roundtrip(full) == full, true)
  let bytes = full.encode()
  let flags = bytes[4].to_int()
  assert_eq(flags & flag_end_stream, flag_end_stream)
  assert_eq(flags & flag_end_headers, flag_end_headers)
  assert_eq(flags & flag_priority, flag_priority)
  assert_eq(flags & flag_padded, flag_padded)
}

///|
test "PRIORITY frame is exactly 5 octets and round-trips" {
  let f = Frame::Priority(stream_id=3, priority={
    exclusive: false,
    stream_dependency: 7,
    weight: 15,
  })
  let bytes = f.encode()
  assert_eq(be_read_u24(bytes, 0), 5)
  assert_eq(roundtrip(f) == f, true)
}

///|
test "RST_STREAM carries a 32-bit error code" {
  let f = Frame::RstStream(stream_id=3, error_code=error_cancel)
  let bytes = f.encode()
  assert_eq(be_read_u24(bytes, 0), 4)
  assert_eq(roundtrip(f) == f, true)
  match roundtrip(f) {
    RstStream(error_code~, ..) => assert_eq(error_code, error_cancel)
    _ => assert_eq(true, false)
  }
}

///|
test "SETTINGS frame round-trips parameters and the ACK form" {
  let params = [
    (settings_header_table_size, 4096),
    (settings_max_concurrent_streams, 100),
    (settings_initial_window_size, 65535),
  ]
  let f = Frame::Settings(params~, ack=false)
  let bytes = f.encode()
  assert_eq(be_read_u24(bytes, 0), 18) // 3 params * 6 octets
  assert_eq(roundtrip(f) == f, true)
  let ack = Frame::Settings(params=[], ack=true)
  let abytes = ack.encode()
  assert_eq(be_read_u24(abytes, 0), 0)
  assert_eq(abytes[4].to_int() & flag_ack, flag_ack)
  assert_eq(roundtrip(ack) == ack, true)
}

///|
test "PUSH_PROMISE round-trips the promised stream id and fragment" {
  let f = Frame::PushPromise(
    stream_id=1,
    promised_id=2,
    fragment=b"\x82\x84",
    end_headers=true,
    padding=0,
  )
  assert_eq(roundtrip(f) == f, true)
  match roundtrip(f) {
    PushPromise(promised_id~, ..) => assert_eq(promised_id, 2)
    _ => assert_eq(true, false)
  }
}

///|
test "PING frame is 8 octets and round-trips ACK" {
  let f = Frame::Ping(payload=b"\x01\x02\x03\x04\x05\x06\x07\x08", ack=false)
  let bytes = f.encode()
  assert_eq(be_read_u24(bytes, 0), 8)
  assert_eq(roundtrip(f) == f, true)
  let ack = Frame::Ping(payload=b"\x00\x00\x00\x00\x00\x00\x00\x00", ack=true)
  assert_eq(ack.encode()[4].to_int() & flag_ack, flag_ack)
  assert_eq(roundtrip(ack) == ack, true)
}

///|
test "GOAWAY carries last-stream-id, error code and debug data" {
  let f = Frame::GoAway(
    last_stream_id=7,
    error_code=error_protocol_error,
    debug=b"boom",
  )
  assert_eq(roundtrip(f) == f, true)
  let bytes = f.encode()
  assert_eq(bytes[4 + 5].to_int() & 0x7F, 0) // reserved bit clear on stream 0
}

///|
test "WINDOW_UPDATE round-trips a 31-bit increment" {
  let f = Frame::WindowUpdate(stream_id=1, increment=0x7FFFFFFF)
  let bytes = f.encode()
  assert_eq(be_read_u24(bytes, 0), 4)
  assert_eq(roundtrip(f) == f, true)
}

///|
test "CONTINUATION round-trips the header fragment with END_HEADERS" {
  let f = Frame::Continuation(
    stream_id=1,
    fragment=b"\x40\x82",
    end_headers=true,
  )
  assert_eq(roundtrip(f) == f, true)
  assert_eq(f.encode()[4].to_int() & flag_end_headers, flag_end_headers)
}

///|
test "unknown/extension frame type is preserved verbatim (RFC 7540 §4.1)" {
  let f = Frame::Unknown(
    ftype=0xEF,
    flags=0x03,
    stream_id=9,
    payload=b"extension",
  )
  assert_eq(roundtrip(f) == f, true)
  assert_eq(f.frame_type(), 0xEF)
}

///|
test "decode_frame raises Incomplete on a short buffer" {
  let full = Frame::Data(
    stream_id=1,
    data=b"abcdef",
    end_stream=false,
    padding=0,
  ).encode()
  // truncate mid-payload
  let short = full[0:full.length() - 2].to_owned()
  let raised = try {
    let _ = decode_frame(short)
    false
  } catch {
    Incomplete => true
    _ => false
  }
  assert_eq(raised, true)
}

///|
test "decode_frame reports frame-size errors for fixed-size frames" {
  // RST_STREAM claims a 5-octet payload (must be 4).
  let bad = b"\x00\x00\x05\x03\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00"
  let raised = try {
    let _ = decode_frame(bad)
    false
  } catch {
    FrameSizeError(_) => true
    _ => false
  }
  assert_eq(raised, true)
}

///|
test "two frames decode sequentially from one buffer via the consumed count" {
  let a = Frame::Ping(payload=b"\x00\x00\x00\x00\x00\x00\x00\x01", ack=false)
  let b = Frame::WindowUpdate(stream_id=1, increment=100)
  let buf = Buffer()
  buf.write_bytes(a.encode())
  buf.write_bytes(b.encode())
  let stream = buf.to_bytes()
  let (fa, na) = decode_frame(stream)
  assert_eq(fa == a, true)
  let (fb, nb) = decode_frame(stream, offset=na)
  assert_eq(fb == b, true)
  assert_eq(na + nb, stream.length())
}
