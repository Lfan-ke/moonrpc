///|
test "Huffman table has 257 entries incl. EOS (RFC 7541 Appendix B)" {
  assert_eq(huffman_table.length(), 257)
  assert_eq(huffman_table[huffman_eos], (0x3fffffff, 30))
  assert_eq(huffman_table[48], (0x0, 5)) // '0'
  assert_eq(huffman_table[97], (0x3, 5)) // 'a'
  assert_eq(huffman_table[32], (0x14, 6)) // ' '
}

///|
test "Huffman encodes the RFC 7541 §C.4.1 vector www.example.com" {
  let coded = huffman_encode(b"www.example.com")
  assert_eq(coded, b"\xf1\xe3\xc2\xe5\xf2\x3a\x6b\xa0\xab\x90\xf4\xff")
  assert_eq(coded.length(), 12)
  assert_eq(huffman_decode(coded), b"www.example.com")
}

///|
test "Huffman encodes the RFC 7541 §C.4.2 vector no-cache" {
  let coded = huffman_encode(b"no-cache")
  assert_eq(coded, b"\xa8\xeb\x10\x64\x9c\xbf")
  assert_eq(huffman_decode(coded), b"no-cache")
}

///|
test "Huffman encodes the RFC 7541 §C.4.3 vectors custom-key/custom-value" {
  assert_eq(huffman_encode(b"custom-key"), b"\x25\xa8\x49\xe9\x5b\xa9\x7d\x7f")
  assert_eq(
    huffman_encode(b"custom-value"),
    b"\x25\xa8\x49\xe9\x5b\xb8\xe8\xb4\xbf",
  )
}

///|
test "Huffman round-trips every octet value 0..=255" {
  let all = Buffer()
  for i = 0; i < 256; i = i + 1 {
    all.write_byte(i.to_byte())
  }
  let payload = all.to_bytes()
  assert_eq(huffman_decode(huffman_encode(payload)), payload)
  // empty input
  assert_eq(huffman_encode(b""), b"")
  assert_eq(huffman_decode(b""), b"")
}

///|
test "Huffman encoded length matches the produced byte count" {
  for s in [b"www.example.com", b"no-cache", b"gzip", b"302", b"private"] {
    assert_eq(huffman_encoded_length(s), huffman_encode(s).length())
  }
}

///|
test "Huffman decode rejects a non-all-ones padding" {
  // 'a' is 00011 (5 bits); pad the remaining 3 bits with 000 instead of 111.
  let raised = try {
    let _ = huffman_decode(b"\x18") // 00011 000
    false
  } catch {
    HuffmanError(_) => true
    _ => false
  }
  assert_eq(raised, true)
}

///|
test "Huffman decode rejects padding longer than 7 bits" {
  // 'a'=00011 then a full 0xFF byte of padding (>7 bits of ones after a symbol).
  let raised = try {
    let _ = huffman_decode(b"\x1f\xff")
    false
  } catch {
    HuffmanError(_) => true
    _ => false
  }
  assert_eq(raised, true)
}
