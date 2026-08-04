// The exact bytes a gRPC client puts on the wire for one unary call —
// greet.Greeter/SayHello — assembled from moonrpc's pure codecs with no server
// and no socket: the HTTP/2 frame layer (RFC 7540), an HPACK header block
// (RFC 7541), the length-prefixed gRPC message, and the protobuf body. The same
// bytes are then decoded straight back, the way a reader drains them off a
// connection.
//
//     moon run examples/00-unary-on-the-wire

///|
fn ascii(s : String) -> Bytes {
  let buf = Buffer()
  for i = 0; i < s.length(); i = i + 1 {
    buf.write_byte((s[i].to_int() & 0xFF).to_byte())
  }
  buf.to_bytes()
}

///|
/// A `HelloRequest { name = 1 }` in the proto3 binary format.
fn hello_request(name : String) -> Bytes {
  let w = @moonrpc.PbWriter::new()
  w.string_(1, name)
  w.to_bytes()
}

///|
/// The HEADERS a unary caller sends: the HTTP/2 pseudo-headers, the gRPC content
/// type, and `te: trailers`.
fn request_headers(rpc : @moonrpc.Method) -> Array[@moonrpc.Header] {
  [
    { name: b":method", value: b"POST" },
    { name: b":scheme", value: b"http" },
    { name: b":path", value: ascii(rpc.path()) },
    { name: b":authority", value: b"127.0.0.1:50051" },
    { name: b"content-type", value: b"application/grpc+proto" },
    { name: b"te", value: b"trailers" },
  ]
}

///|
fn main {
  let rpc = @moonrpc.Method::{ service: "greet.Greeter", name: "SayHello" }
  let headers = request_headers(rpc)
  let block = @moonrpc.HpackEncoder::new().encode(headers)
  let msg = @moonrpc.encode_message(hello_request("heke1228"))
  let head = @moonrpc.Frame::Headers(
    stream_id=1,
    fragment=block,
    end_stream=false,
    end_headers=true,
    priority=None,
    padding=0,
  )
  let data = @moonrpc.Frame::Data(
    stream_id=1,
    data=msg,
    end_stream=true,
    padding=0,
  )
  let wire = {
    let buf = Buffer()
    buf.write_bytes(head.encode())
    buf.write_bytes(data.encode())
    buf.to_bytes()
  }
  println("POST \{rpc.path()}")
  println(
    "\{wire.length()} octets on the wire: HEADERS + DATA(END_STREAM) on stream 1",
  )
  let dec = @moonrpc.HpackDecoder::new()
  let mut off = 0
  try {
    while off < wire.length() {
      let (frame, consumed) = @moonrpc.decode_frame(wire, offset=off)
      off += consumed
      match frame {
        Headers(fragment~, ..) => {
          let back = dec.decode(fragment)
          let same = if back == headers { "ok" } else { "MISMATCH" }
          println("  \{back.length()} headers, HPACK round-trip: \{same}")
        }
        Data(data~, ..) => {
          let (compressed, payload) = @moonrpc.decode_message(data).unwrap()
          let r = @moonrpc.PbReader::new(payload)
          let (field, _) = r.read_tag()
          println(
            "  message: compressed=\{compressed}, field \{field} = \{r.read_string()}",
          )
        }
        _ => ()
      }
    }
  } catch {
    _ => println("  wire did not decode")
  }
  let ok : @moonrpc.Status = Ok
  let missing : @moonrpc.Status = NotFound
  println(
    "grpc-status \{ok.code()} = \{ok.name()}, \{missing.code()} = \{missing.name()}",
  )
}
