// Deadlines: a client sends its timeout as the `grpc-timeout` header, and the server
// surfaces it to the handler as `RpcContext::deadline_millis`. This drives one call with
// a 233 ms budget — the handler reads the parsed deadline back — and shows the
// `grpc-timeout` codec across several RFC units (`H`/`M`/`S`/`m`/`u`/`n`).
//
//     moon run examples/15-deadlines

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
fn int_msg(n : Int) -> Bytes {
  let w = @moonrpc.PbWriter::new()
  w.int32(1, n)
  w.to_bytes()
}

///|
fn read_int_field(msg : Bytes) -> Int {
  let r = @moonrpc.PbReader::new(msg)
  try {
    let _ = r.read_tag()
    r.read_int32()
  } catch {
    _ => -1
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
fn show_parse(v : Bytes) -> Unit {
  let out = match @moonrpc.parse_grpc_timeout(v) {
    Some(ms) => "\{ms} ms"
    None => "malformed"
  }
  println("  parse_grpc_timeout(\{ascii(v)}) = \{out}")
}

///|
fn run() -> Unit raise {
  // The `grpc-timeout` codec: encode whole millis, parse every unit back to millis.
  println(
    "encode_grpc_timeout(233) = \{ascii(@moonrpc.encode_grpc_timeout(233))}",
  )
  show_parse(b"233m") // milliseconds
  show_parse(b"5S") // seconds
  show_parse(b"2M") // minutes
  show_parse(b"1H") // hours
  show_parse(b"500u") // microseconds floor to whole millis
  show_parse(b"50000000n") // nanoseconds (8-digit cap), floor to whole millis
  show_parse(b"999999999m") // over the 8-digit field ⇒ rejected as malformed

  // The deadline on a real call: the handler reads it back off the context.
  let server = @moonrpc.H2Server::new()
  server.register_unary("/slow.Slow/Wait", (ctx, _req) => {
    match ctx.deadline_millis {
      Some(ms) => int_msg(ms)
      None => int_msg(-1)
    }
  })
  let client = @moonrpc.H2Client::new()
  let (id, initial) = client.unary(
    "/slow.Slow/Wait",
    b"work",
    timeout_millis=Some(233),
  )
  drive(server, client, id, initial)
  let reply = client.reply(id)
  println("call status: \{reply.grpc_status}")
  println("deadline the handler saw: \{read_int_field(reply.messages[0])} ms")
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
