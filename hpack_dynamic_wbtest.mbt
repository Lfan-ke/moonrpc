///|
fn h(name : Bytes, value : Bytes) -> Header {
  { name, value }
}

///|
/// The C.3 first-request header list.
let req1 : Array[Header] = [
  h(b":method", b"GET"),
  h(b":scheme", b"http"),
  h(b":path", b"/"),
  h(b":authority", b"www.example.com"),
]

///|
/// The C.3.2 second-request header list.
let req2 : Array[Header] = [
  h(b":method", b"GET"),
  h(b":scheme", b"http"),
  h(b":path", b"/"),
  h(b":authority", b"www.example.com"),
  h(b"cache-control", b"no-cache"),
]

///|
test "static table -> bytes mirrors static_table (61 ASCII entries)" {
  assert_eq(static_table_bytes.length(), 61)
  assert_eq(static_table_bytes[0], (b":authority", b""))
  assert_eq(static_table_bytes[1], (b":method", b"GET"))
  assert_eq(static_table_bytes[7], (b":status", b"200"))
}

///|
test "HPACK encoder reproduces the RFC 7541 §C.3.1 request byte-for-byte" {
  let enc = HpackEncoder::new(huffman=false)
  let out = enc.encode(req1)
  assert_eq(
    out, b"\x82\x86\x84\x41\x0f\x77\x77\x77\x2e\x65\x78\x61\x6d\x70\x6c\x65\x2e\x63\x6f\x6d",
  )
  // the literal added :authority to the dynamic table (size 10+15+32 = 57)
  assert_eq(enc.table.count(), 1)
  assert_eq(enc.table.current_size(), 57)
}

///|
test "HPACK encoder carries dynamic state into the §C.3.2 second request" {
  let enc = HpackEncoder::new(huffman=false)
  let _ = enc.encode(req1)
  let out = enc.encode(req2)
  // :authority now resolves to dynamic index 62 (0xbe); cache-control is name 24.
  assert_eq(out, b"\x82\x86\x84\xbe\x58\x08\x6e\x6f\x2d\x63\x61\x63\x68\x65")
}

///|
test "HPACK encoder emits Huffman literals for the §C.4.1 request" {
  let enc = HpackEncoder::new(huffman=true)
  let out = enc.encode(req1)
  assert_eq(
    out, b"\x82\x86\x84\x41\x8c\xf1\xe3\xc2\xe5\xf2\x3a\x6b\xa0\xab\x90\xf4\xff",
  )
}

///|
test "HPACK decoder parses the §C.3.1 request bytes" {
  let dec = HpackDecoder::new()
  let headers = dec.decode(
    b"\x82\x86\x84\x41\x0f\x77\x77\x77\x2e\x65\x78\x61\x6d\x70\x6c\x65\x2e\x63\x6f\x6d",
  )
  assert_eq(headers.length(), 4)
  assert_eq(headers[0], h(b":method", b"GET"))
  assert_eq(headers[3], h(b":authority", b"www.example.com"))
  assert_eq(dec.table.count(), 1)
  assert_eq(dec.table.current_size(), 57)
  // dynamic index 62 now resolves to the freshly inserted entry.
  match hpack_lookup(dec.table, 62) {
    Some((name, value)) => {
      assert_eq(name, b":authority")
      assert_eq(value, b"www.example.com")
    }
    None => assert_eq(true, false)
  }
}

///|
test "HPACK decoder resolves dynamic references across §C.3.1 then §C.3.2" {
  let dec = HpackDecoder::new()
  let _ = dec.decode(
    b"\x82\x86\x84\x41\x0f\x77\x77\x77\x2e\x65\x78\x61\x6d\x70\x6c\x65\x2e\x63\x6f\x6d",
  )
  let headers = dec.decode(
    b"\x82\x86\x84\xbe\x58\x08\x6e\x6f\x2d\x63\x61\x63\x68\x65",
  )
  assert_eq(headers.length(), 5)
  assert_eq(headers[3], h(b":authority", b"www.example.com"))
  assert_eq(headers[4], h(b"cache-control", b"no-cache"))
  assert_eq(dec.table.count(), 2)
}

///|
test "HPACK decoder handles the §C.4.1 Huffman request" {
  let dec = HpackDecoder::new()
  let headers = dec.decode(
    b"\x82\x86\x84\x41\x8c\xf1\xe3\xc2\xe5\xf2\x3a\x6b\xa0\xab\x90\xf4\xff",
  )
  assert_eq(headers[3], h(b":authority", b"www.example.com"))
}

///|
test "HPACK encoder/decoder round-trip a mixed header list (huffman on/off)" {
  let headers = [
    h(b":status", b"200"),
    h(b"content-type", b"application/grpc"),
    h(b"grpc-encoding", b"gzip"),
    h(b"custom-metadata-bin", b"\x00\x01\x02\xff"),
    h(b"grpc-status", b"0"),
  ]
  for huff in [true, false] {
    let enc = HpackEncoder::new(huffman=huff)
    let dec = HpackDecoder::new()
    let decoded = dec.decode(enc.encode(headers))
    assert_eq(decoded.length(), headers.length())
    for i = 0; i < headers.length(); i = i + 1 {
      assert_eq(decoded[i], headers[i])
    }
  }
}

///|
test "dynamic table evicts oldest entries to stay within max_size (§4.4)" {
  let t = DynamicTable::new(max_size=60)
  t.add(b":authority", b"www.example.com") // size 57
  assert_eq(t.count(), 1)
  assert_eq(t.current_size(), 57)
  t.add(b"cache-control", b"no-cache") // size 53, forces eviction of the first
  assert_eq(t.count(), 1)
  assert_eq(t.current_size(), 53)
  match hpack_lookup(t, 62) {
    Some((name, _)) => assert_eq(name, b"cache-control")
    None => assert_eq(true, false)
  }
}

///|
test "dynamic table: an entry larger than max_size empties the table (§4.4)" {
  let t = DynamicTable::new(max_size=40)
  t.add(b"x", b"y") // size 34
  assert_eq(t.count(), 1)
  t.add(b"aaaaaaaa", b"bbbbbbbb") // size 8+8+32 = 48 > 40 -> table emptied, not stored
  assert_eq(t.count(), 0)
  assert_eq(t.current_size(), 0)
}

///|
test "dynamic table size update shrinks then the decoder honours it" {
  let dec = HpackDecoder::new(max_size=4096)
  let _ = dec.decode(
    b"\x82\x86\x84\x41\x0f\x77\x77\x77\x2e\x65\x78\x61\x6d\x70\x6c\x65\x2e\x63\x6f\x6d",
  )
  assert_eq(dec.table.count(), 1)
  // a size-update to 0 evicts everything (0x20 = update, value 0 on a 5-bit prefix)
  let _ = dec.decode(b"\x20")
  assert_eq(dec.table.count(), 0)
  assert_eq(dec.table.current_size(), 0)
}

///|
test "HPACK size-update encoding round-trips through the decoder" {
  let bytes = hpack_encode_size_update(256)
  assert_eq(bytes[0].to_int() & 0xE0, 0x20) // 001 prefix
  let dec = HpackDecoder::new(max_size=4096)
  let _ = dec.decode(bytes)
  assert_eq(dec.table.max_size, 256)
}

///|
test "decoder rejects a size update above the SETTINGS limit" {
  let dec = HpackDecoder::new(max_size=100)
  let raised = try {
    let _ = dec.decode(hpack_encode_size_update(4096))
    false
  } catch {
    HpackDecodeError(_) => true
    _ => false
  }
  assert_eq(raised, true)
}

///|
test "decoder rejects an out-of-range index" {
  let dec = HpackDecoder::new()
  let raised = try {
    let _ = dec.decode(b"\xff\x00") // index 62 with an empty dynamic table
    false
  } catch {
    HpackDecodeError(_) => true
    _ => false
  }
  assert_eq(raised, true)
}
