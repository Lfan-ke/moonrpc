///|
test "gRPC length-prefixed framing round-trip" {
  let payload = b"hello grpc"
  let frame = encode_message(payload)
  assert_eq(frame.length(), 15) // 5-byte prefix + 10-byte payload
  assert_eq(frame[0].to_int(), 0) // not compressed
  assert_eq(frame[4].to_int(), 10) // length low byte
  let (compressed, body) = decode_message(frame).unwrap()
  assert_eq(compressed, false)
  assert_eq(body, payload)
}

///|
test "framing: 4-byte big-endian length for a 300-byte payload" {
  let buf = Buffer()
  for i = 0; i < 300; i = i + 1 {
    buf.write_byte(b'\x00')
  }
  let payload = buf.to_bytes()
  let frame = encode_message(payload)
  assert_eq(frame.length(), 305)
  // 300 = 0x0000012C -> [0, 0, 1, 44]
  assert_eq(frame[1].to_int(), 0)
  assert_eq(frame[2].to_int(), 0)
  assert_eq(frame[3].to_int(), 1)
  assert_eq(frame[4].to_int(), 44)
  let (_, body) = decode_message(frame).unwrap()
  assert_eq(body.length(), 300)
}

///|
test "framing: compressed flag and short-frame handling" {
  let frame = encode_message(b"x", compressed=true)
  assert_eq(frame[0].to_int(), 1)
  let (compressed, _) = decode_message(frame).unwrap()
  assert_eq(compressed, true)
  // fewer than 5 bytes -> None
  let short = match decode_message(b"\x00\x00") {
    None => true
    Some(_) => false
  }
  assert_eq(short, true)
}

///|
test "framing: a high-bit-set length prefix is rejected, not sliced out of bounds" {
  // The 4-byte length 0x80000000 decodes to a negative Int; a naive `data[5:5+len]`
  // slices with end < start and aborts. It must yield None instead.
  let hostile = b"\x00\x80\x00\x00\x00\x01\x02"
  assert_eq(decode_message(hostile) is None, true)
  // A length past what the buffer holds is likewise None (a partial frame).
  assert_eq(decode_message(b"\x00\x00\x00\x00\x7f\x01\x02") is None, true)
}

///|
test "gRPC status codes" {
  assert_eq(Status::code(Ok), 0)
  assert_eq(Status::code(NotFound), 5)
  assert_eq(Status::code(Unauthenticated), 16)
  assert_eq(Status::name(NotFound), "NOT_FOUND")
  assert_eq(Status::name(Ok), "OK")
}

///|
test "method path" {
  let m : Method = { service: "greet.Greeter", name: "SayHello" }
  assert_eq(m.path(), "/greet.Greeter/SayHello")
}
