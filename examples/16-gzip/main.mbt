// Per-message gzip (RFC 1952 framing over RFC 1951 DEFLATE): `gunzip` inflates a gzip
// member, and the client engine transparently inflates a `grpc-encoding: gzip` reply
// whose message has the compression flag set. These vectors are real gzip members
// (fixed-Huffman, stored, and one with an FNAME field), and the last is delivered as a
// compressed reply the caller receives already decompressed.
//
//     moon run examples/16-gzip

///|
fn unhex(s : String) -> Bytes {
  fn nib(c : Char) -> Int {
    let v = c.to_int()
    if v >= 0x30 && v <= 0x39 {
      v - 0x30
    } else if v >= 0x61 && v <= 0x66 {
      v - 0x61 + 10
    } else {
      v - 0x41 + 10
    }
  }

  let chars = s.to_array()
  let buf = Buffer()
  let mut i = 0
  while i + 1 < chars.length() {
    buf.write_byte(((nib(chars[i]) << 4) | nib(chars[i + 1])).to_byte())
    i = i + 2
  }
  buf.to_bytes()
}

///|
fn ascii(bs : Bytes) -> String {
  let buf = StringBuilder()
  for b in bs {
    buf.write_char((b.to_int() & 0xFF).unsafe_to_char())
  }
  buf.to_string()
}

///|
fn run() -> Unit raise {
  // Three real gzip members, one per DEFLATE block type / optional field.
  let fixed = unhex(
    "1f8b0800cb9b6a6a02ffcb48cdc9c9d75148afca2c5028cf2fca490100eb83468211000000",
  )
  let stored = unhex(
    "1f8b0800fe9d6a6a04ff011d00e2ff73746f72656420626c6f636b2c206e6f20687566666d616e20686572652b46bf561d000000",
  )
  let named = unhex(
    "1f8b08080000000002ff68692e74787400cb4bcc4d4d51c84dcd4d4a2d522848acccc94f4c01004e5eca2a14000000",
  )
  println("gunzip fixed-Huffman: \{ascii(@moonrpc.gunzip(fixed))}")
  println("gunzip stored block:  \{ascii(@moonrpc.gunzip(stored))}")
  println("gunzip FNAME member:  \{ascii(@moonrpc.gunzip(named))}")

  // The client-side path: a peer replies `grpc-encoding: gzip` with a compression-flagged
  // message; the caller gets the plaintext, never the gzip bytes.
  let client = @moonrpc.H2Client::new()
  let (id, _req) = client.unary("/echo.Echo/Say", b"x")
  let _ = client.feed(Settings(params=[], ack=false))
  let enc = @moonrpc.HpackEncoder::new()
  let hblock = enc.encode([
    { name: b":status", value: b"200", },
    { name: b"content-type", value: b"application/grpc", },
    { name: b"grpc-encoding", value: b"gzip", },
  ])
  let _ = client.feed(
    Headers(
      stream_id=id,
      fragment=hblock,
      end_stream=false,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  let _ = client.feed(
    Data(
      stream_id=id,
      data=@moonrpc.encode_message(fixed, compressed=true),
      end_stream=false,
      padding=0,
    ),
  )
  let tblock = enc.encode([{ name: b"grpc-status", value: b"0", }])
  let _ = client.feed(
    Headers(
      stream_id=id,
      fragment=tblock,
      end_stream=true,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  let reply = client.reply(id)
  println(
    "gzip reply inflated by the client: \{ascii(reply.messages[0])} (grpc-status \{reply.grpc_status})",
  )
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
