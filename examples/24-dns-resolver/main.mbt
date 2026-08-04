// The DNS message codec (RFC 1035) behind gRPC's `dns:///` resolver: encode a
// recursion-desired A/AAAA query, decode a response, and pull out the resolved
// addresses (§4.1.4 name-compression pointers are followed). This encodes a query for
// `grpc.example.com`, then hand-builds a response with two A records and one AAAA record
// — the answer bytes a UDP resolver would hand back — and decodes it.
//
//     moon run examples/24-dns-resolver

///|
fn be16(buf : Buffer, v : Int) -> Unit {
  buf.write_byte(((v >> 8) & 0xFF).to_byte())
  buf.write_byte((v & 0xFF).to_byte())
}

///|
fn push_all(buf : Buffer, xs : Array[Int]) -> Unit {
  for x in xs {
    buf.write_byte((x & 0xFF).to_byte())
  }
}

///|
fn hex(bs : Bytes) -> String {
  let digits = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
  ]
  let buf = StringBuilder::new()
  for b in bs {
    let v = b.to_int() & 0xFF
    buf.write_char(digits[v >> 4])
    buf.write_char(digits[v & 0xF])
  }
  buf.to_string()
}

///|
/// Build the wire response `8.8.8.8` would return for `grpc.example.com`: a header, the
/// echoed question, and three answers. Answer names use the §4.1.4 compression pointer
/// (`0xC00C`) back to the question name at offset 12.
fn build_response() -> Bytes {
  let buf = Buffer()
  // Header: id, flags (response, RD, RA, NOERROR), QD=1, AN=3, NS=0, AR=0.
  be16(buf, 0x1234)
  be16(buf, 0x8180)
  be16(buf, 1)
  be16(buf, 3)
  be16(buf, 0)
  be16(buf, 0)
  // Question: grpc.example.com, type A, class IN.
  push_all(buf, [4, 'g'.to_int(), 'r'.to_int(), 'p'.to_int(), 'c'.to_int()])
  push_all(buf, [
    7,
    'e'.to_int(),
    'x'.to_int(),
    'a'.to_int(),
    'm'.to_int(),
    'p'.to_int(),
    'l'.to_int(),
    'e'.to_int(),
  ])
  push_all(buf, [3, 'c'.to_int(), 'o'.to_int(), 'm'.to_int(), 0])
  be16(buf, 1)
  be16(buf, 1)
  // Answer 1: A 93.184.216.34.
  fn a_record(rdata : Array[Int]) -> Unit {
    push_all(buf, [0xC0, 0x0C]) // pointer to the question name
    be16(buf, 1) // type A
    be16(buf, 1) // class IN
    push_all(buf, [0, 0, 1, 0x2C]) // TTL 300
    be16(buf, rdata.length())
    push_all(buf, rdata)
  }

  a_record([93, 184, 216, 34])
  a_record([93, 184, 216, 35])
  // Answer 3: AAAA 2606:2800:220:1:248:1893:25c8:1946.
  push_all(buf, [0xC0, 0x0C])
  be16(buf, 28) // type AAAA
  be16(buf, 1)
  push_all(buf, [0, 0, 1, 0x2C])
  be16(buf, 16)
  push_all(buf, [
    0x26, 0x06, 0x28, 0x00, 0x02, 0x20, 0x00, 0x01, 0x02, 0x48, 0x18, 0x93, 0x25,
    0xc8, 0x19, 0x46,
  ])
  buf.to_bytes()
}

///|
fn run() -> Unit {
  // Encode a recursion-desired A query.
  let query = @moonrpc.dns_encode_query(
    0x1234, "grpc.example.com", @moonrpc.dns_type_a,
  )
  println("A query (\{query.length()} octets): \{hex(query)}")

  // Decode the response and read the addresses out.
  let response = @moonrpc.dns_decode_response(build_response())
  println(
    "response id=\{response.id}, rcode=\{response.rcode}, \{response.answers.length()} answers:",
  )
  for rec in response.answers {
    let kind = if rec.rtype == @moonrpc.dns_type_a {
      "A"
    } else if rec.rtype == @moonrpc.dns_type_aaaa {
      "AAAA"
    } else {
      "other"
    }
    println("  \{rec.name} \{kind} ttl=\{rec.ttl} -> \{rec.address}")
  }
  println("resolver addresses:")
  for addr in response.addresses() {
    println("  \{addr}")
  }
}

///|
#coverage.skip
fn main {
  run()
}
