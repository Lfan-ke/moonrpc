///|
test "HPACK static table: RFC 7541 Appendix A landmarks" {
  assert_eq(static_table.length(), 61)
  assert_eq(hpack_static_entry(1).unwrap(), (":authority", ""))
  assert_eq(hpack_static_entry(2).unwrap(), (":method", "GET"))
  assert_eq(hpack_static_entry(3).unwrap(), (":method", "POST"))
  assert_eq(hpack_static_entry(4).unwrap(), (":path", "/"))
  assert_eq(hpack_static_entry(8).unwrap(), (":status", "200"))
  assert_eq(
    hpack_static_entry(16).unwrap(),
    ("accept-encoding", "gzip, deflate"),
  )
  assert_eq(hpack_static_entry(48).unwrap(), ("proxy-authenticate", ""))
  assert_eq(hpack_static_entry(49).unwrap(), ("proxy-authorization", ""))
  assert_eq(hpack_static_entry(61).unwrap(), ("www-authenticate", ""))
}

///|
test "HPACK static table: out-of-range lookups return None" {
  assert_eq(hpack_static_entry(0), None)
  assert_eq(hpack_static_entry(62), None)
  assert_eq(hpack_static_entry(-1), None)
  assert_eq(hpack_static_table().length(), 61)
}

///|
test "HPACK integer: RFC 7541 §5.1 example 1 — 10 on a 5-bit prefix" {
  let e = hpack_encode_int(10, 5)
  assert_eq(e.length(), 1)
  assert_eq(e[0].to_int(), 0x0A)
  let (v, n) = hpack_decode_int(e, 0, 5)
  assert_eq(v, 10)
  assert_eq(n, 1)
}

///|
test "HPACK integer: RFC 7541 §5.1 example 2 — 1337 on a 5-bit prefix" {
  let e = hpack_encode_int(1337, 5)
  assert_eq(e.length(), 3)
  assert_eq(e[0].to_int(), 0x1F)
  assert_eq(e[1].to_int(), 0x9A)
  assert_eq(e[2].to_int(), 0x0A)
  let (v, n) = hpack_decode_int(e, 0, 5)
  assert_eq(v, 1337)
  assert_eq(n, 3)
}

///|
test "HPACK integer: RFC 7541 §5.1 example 3 — 42 on an 8-bit prefix" {
  let e = hpack_encode_int(42, 8)
  assert_eq(e.length(), 1)
  assert_eq(e[0].to_int(), 0x2A)
  let (v, n) = hpack_decode_int(e, 0, 8)
  assert_eq(v, 42)
  assert_eq(n, 1)
}

///|
test "HPACK integer: flag bits above the prefix are ignored on decode" {
  // 0xEA = 0b111_01010: three flag bits set, 5-bit prefix value 0x0A = 10.
  let (v, n) = hpack_decode_int(b"\xEA", 0, 5)
  assert_eq(v, 10)
  assert_eq(n, 1)
}

///|
test "HPACK integer: boundary at the prefix max needs a continuation octet" {
  // 31 on a 5-bit prefix: prefix maxes out (0x1F), then a single 0x00 tail.
  let e = hpack_encode_int(31, 5)
  assert_eq(e.length(), 2)
  assert_eq(e[0].to_int(), 0x1F)
  assert_eq(e[1].to_int(), 0x00)
  let (v, n) = hpack_decode_int(e, 0, 5)
  assert_eq(v, 31)
  assert_eq(n, 2)
}

///|
test "HPACK integer: round-trip across prefixes and a decode offset" {
  for bits = 1; bits <= 8; bits = bits + 1 {
    for val in [0, 1, 100, 1337, 100000] {
      let e = hpack_encode_int(val, bits)
      let (v, n) = hpack_decode_int(e, 0, bits)
      assert_eq(v, val)
      assert_eq(n, e.length())
    }
  }
  // decode starting partway into a buffer
  let e = hpack_encode_int(1337, 5)
  let buf = Buffer()
  buf.write_byte(b'\xFF')
  buf.write_byte(b'\xFF')
  buf.write_bytes(e)
  let (v, n) = hpack_decode_int(buf.to_bytes(), 2, 5)
  assert_eq(v, 1337)
  assert_eq(n, 3)
}

///|
test "HPACK string literal: non-Huffman encode carries H=0 and the length" {
  let e = hpack_encode_string(b"custom-key")
  assert_eq(hpack_string_is_huffman(e, 0), false)
  assert_eq(e[0].to_int(), 0x0A) // H=0, length 10 on a 7-bit prefix
  assert_eq(e.length(), 11)
  let (s, n) = hpack_decode_string(e, 0)
  assert_eq(s, b"custom-key")
  assert_eq(n, 11)
}

///|
test "HPACK string literal: long value uses a multi-octet length" {
  let body = Buffer()
  for i = 0; i < 300; i = i + 1 {
    body.write_byte(b'\x7A')
  }
  let payload = body.to_bytes()
  let e = hpack_encode_string(payload)
  // 300 length on a 7-bit prefix: 0x7F then 300-127=173 -> 0xAD 0x01.
  assert_eq(e[0].to_int(), 0x7F)
  assert_eq(e[1].to_int(), 0xAD)
  assert_eq(e[2].to_int(), 0x01)
  let (s, n) = hpack_decode_string(e, 0)
  assert_eq(s.length(), 300)
  assert_eq(n, 303)
}

///|
test "HPACK string literal: empty value" {
  let e = hpack_encode_string(b"")
  assert_eq(e.length(), 1)
  assert_eq(e[0].to_int(), 0x00)
  let (s, n) = hpack_decode_string(e, 0)
  assert_eq(s, b"")
  assert_eq(n, 1)
}

///|
test "HPACK string literal: Huffman H bit is detectable" {
  // Hand-built literal with H=1, length 1 -> 0x81 then one octet.
  assert_eq(hpack_string_is_huffman(b"\x81\xFF", 0), true)
}
