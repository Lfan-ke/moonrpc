// Call metadata: custom request headers a client attaches, surfaced to the handler
// through `RpcContext`, and response initial- and trailing-metadata the handler sets
// back. Binary `-bin` metadata rides the wire base64-encoded and is surfaced as raw
// bytes on both sides. This drives one unary call carrying an ASCII and a binary header,
// echoed back as a response header and trailer, plus the standalone base64 helpers.
//
//     moon run examples/13-metadata

///|
fn drive(
  server : @moonrpc.H2Server,
  client : @moonrpc.H2Client,
  id : Int,
  initial : Array[@moonrpc.Frame],
) -> Unit raise {
  let to_server : Array[@moonrpc.Frame] = []
  let to_client : Array[@moonrpc.Frame] = []
  for f in client.preface() {
    to_server.push(f)
  }
  for f in server.preface() {
    to_client.push(f)
  }
  for f in initial {
    to_server.push(f)
  }
  let mut si = 0
  let mut ci = 0
  let mut steps = 0
  while !client.is_done(id) && steps < 100000 {
    steps = steps + 1
    if si < to_server.length() {
      let f = to_server[si]
      si = si + 1
      for r in server.feed(f) {
        to_client.push(r)
      }
    } else if ci < to_client.length() {
      let f = to_client[ci]
      ci = ci + 1
      for r in client.feed(f) {
        to_server.push(r)
      }
    } else {
      break
    }
  }
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
fn hex(bs : Bytes) -> String {
  let digits = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
  ]
  let buf = StringBuilder()
  for b in bs {
    let v = b.to_int() & 0xFF
    buf.write_char(digits[v >> 4])
    buf.write_char(digits[v & 0xF])
  }
  buf.to_string()
}

///|
fn header_value(hs : Array[@moonrpc.Header], name : Bytes) -> Bytes {
  for h in hs {
    if h.name == name {
      return h.value
    }
  }
  b""
}

///|
fn run() -> Unit raise {
  // The `-bin` convention and the base64 codec underneath it, on their own.
  let trace = b"\x00\x23\x33\xff\x10"
  let is_bin = @moonrpc.is_binary_metadata(b"trace-bin")
  let is_txt = @moonrpc.is_binary_metadata(b"x-user")
  println("is_binary_metadata(trace-bin): \{is_bin}")
  println("is_binary_metadata(x-user):    \{is_txt}")
  let wire = @moonrpc.metadata_value_to_wire(b"trace-bin", trace)
  println("  trace bytes \{hex(trace)} -> on the wire \{ascii(wire)}")
  let back = @moonrpc.metadata_value_from_wire(b"trace-bin", wire)
  println("  base64 round-trip: \{back == trace}")

  // The same headers on a real call.
  let server = @moonrpc.H2Server::new()
  server.register_unary("/echo.Echo/Meta", (ctx, _req) => {
    let user = match ctx.metadata_get(b"x-user") {
      Some(v) => v
      None => b"(none)"
    }
    let seen_trace = match ctx.metadata_get(b"trace-bin") {
      Some(v) => v
      None => b""
    }
    // Echo the user as initial metadata and the binary trace back as a trailer.
    ctx.add_header(b"x-echo-user", user)
    ctx.add_trailer(b"trace-bin", seen_trace)
    b"ok"
  })
  let client = @moonrpc.H2Client::new()
  let (id, initial) = client.unary("/echo.Echo/Meta", b"ping", metadata=[
    { name: b"x-user", value: b"heke1228", },
    { name: b"trace-bin", value: trace, },
  ])
  drive(server, client, id, initial)
  let reply = client.reply(id)
  let echoed_user = ascii(header_value(reply.headers, b"x-echo-user"))
  let echoed_trace = header_value(reply.trailers, b"trace-bin")
  println("call status: \{reply.grpc_status}")
  println("response header x-echo-user: \{echoed_user}")
  println(
    "response trailer trace-bin: \{hex(echoed_trace)}, matches sent: \{echoed_trace == trace}",
  )
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
