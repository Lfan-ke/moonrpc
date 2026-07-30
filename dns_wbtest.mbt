// The DNS message codec (RFC 1035 §4), checked against hand-built wire vectors.

///|
test "dns: encode a recursion-desired A query for example.com, byte for byte" {
  let query = dns_encode_query(0x1234, "example.com", dns_type_a)
  // id 0x1234, flags 0x0100 (RD), QD=1, then 7"example"3"com"0, QTYPE=A, QCLASS=IN.
  let expect = b"\x12\x34\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x07example\x03com\x00\x00\x01\x00\x01"
  assert_eq(query == expect, true)
}

///|
test "dns: decode an A response whose answer name is a compression pointer" {
  // Header (id 0x1234, QR|RD|RA, AN=1), the question, then an answer whose NAME is the
  // pointer 0xC00C back to the question name, TYPE=A, TTL=300, RDATA=93.184.216.34.
  let msg = b"\x12\x34\x81\x80\x00\x01\x00\x01\x00\x00\x00\x00\x07example\x03com\x00\x00\x01\x00\x01\xc0\x0c\x00\x01\x00\x01\x00\x00\x01\x2c\x00\x04\x5d\xb8\xd8\x22"
  let resp = dns_decode_response(msg)
  assert_eq(resp.id, 0x1234)
  assert_eq(resp.rcode, 0)
  assert_eq(resp.answers.length(), 1)
  assert_eq(resp.answers[0].name, "example.com")
  assert_eq(resp.answers[0].rtype, dns_type_a)
  assert_eq(resp.answers[0].ttl, 300)
  assert_eq(resp.answers[0].address, "93.184.216.34")
  assert_eq(resp.addresses(), ["93.184.216.34"])
}

///|
test "dns: decode an AAAA response into an uncompressed hextet address" {
  // Same shape, TYPE=AAAA (0x001c), RDATA = 2001:db8::1 in full 16 octets.
  let msg = b"\x12\x35\x81\x80\x00\x01\x00\x01\x00\x00\x00\x00\x07example\x03com\x00\x00\x1c\x00\x01\xc0\x0c\x00\x1c\x00\x01\x00\x00\x01\x2c\x00\x10\x20\x01\x0d\xb8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01"
  let resp = dns_decode_response(msg)
  assert_eq(resp.answers.length(), 1)
  assert_eq(resp.answers[0].rtype, dns_type_aaaa)
  assert_eq(resp.answers[0].address, "2001:db8:0:0:0:0:0:1")
  assert_eq(resp.addresses(), ["2001:db8:0:0:0:0:0:1"])
}
