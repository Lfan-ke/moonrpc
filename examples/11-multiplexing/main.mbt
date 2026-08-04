// Multiplexing: many concurrent calls over one connection, each on its own
// client-allocated odd stream id (RFC 7540 §5.1.1), sharing the connection's HPACK and
// flow-control state. Two calls to different methods are opened before either completes,
// their request frames interleaved on the wire, and both replies come back keyed by
// stream id. Driven in memory over the two pure engines.
//
//     moon run examples/11-multiplexing

///|
fn drive(
  server : @moonrpc.H2Server,
  client : @moonrpc.H2Client,
  ids : Array[Int],
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
  while !all_done(client, ids) && steps < 100000 {
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
fn all_done(client : @moonrpc.H2Client, ids : Array[Int]) -> Bool {
  for id in ids {
    if !client.is_done(id) {
      return false
    }
  }
  true
}

///|
fn text_msg(s : String) -> Bytes {
  let w = @moonrpc.PbWriter::new()
  w.string_(1, s)
  w.to_bytes()
}

///|
fn read_text(msg : Bytes) -> String {
  let r = @moonrpc.PbReader::new(msg)
  try {
    let _ = r.read_tag()
    r.read_string()
  } catch {
    _ => ""
  }
}

///|
fn ascii_upper(s : String) -> String {
  let buf = StringBuilder::new()
  for c in s {
    let v = c.to_int()
    buf.write_char(
      if v >= 0x61 && v <= 0x7A {
        (v - 0x20).unsafe_to_char()
      } else {
        c
      },
    )
  }
  buf.to_string()
}

///|
fn run() -> Unit raise {
  let server = @moonrpc.H2Server::new()
  server.register("/echo.Echo/Upper", req => {
    text_msg(ascii_upper(read_text(req)))
  })
  server.register("/echo.Echo/Repeat", req => {
    let s = read_text(req)
    text_msg(s + s)
  })
  let client = @moonrpc.H2Client::new()

  // Open both calls before driving either, and interleave their request frames on the
  // one connection.
  let (id1, f1) = client.unary("/echo.Echo/Upper", text_msg("heke1228"))
  let (id2, f2) = client.unary("/echo.Echo/Repeat", text_msg("qwq"))
  let initial : Array[@moonrpc.Frame] = []
  for f in f1 {
    initial.push(f)
  }
  for f in f2 {
    initial.push(f)
  }
  println("two calls open on one connection: streams \{id1} and \{id2}")
  drive(server, client, [id1, id2], initial)
  let r1 = client.reply(id1)
  let r2 = client.reply(id2)
  println(
    "  stream \{id1} Upper  -> \{read_text(r1.messages[0])} (status \{r1.grpc_status})",
  )
  println(
    "  stream \{id2} Repeat -> \{read_text(r2.messages[0])} (status \{r2.grpc_status})",
  )
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
