// In-memory drive of the client engine against the server engine: real request
// frames out of `H2Client`, through the actual frame + HPACK codecs, into
// `H2Server`, and its response frames back — no sockets, every backend. This is
// the client-side mutation-verification target: break request framing, response
// reassembly, or client flow control and these go red.

///|
/// Shuttle frames between a client and server engine until the call `id` completes:
/// deliver the client's queued frames to the server, its replies back to the
/// client, and repeat. The preface handshake is seeded first.
fn drive_pair(
  server : H2Server,
  client : H2Client,
  id : Int,
  initial : Array[Frame],
) -> Unit raise {
  let to_server : Array[Frame] = []
  let to_client : Array[Frame] = []
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
test "client+server: a unary call round-trips over the two pure engines" {
  let server = H2Server::new()
  server.register("/echo.Echo/Say", req => cat(b"echo:", req))
  let client = H2Client::new()
  let (id, frames) = client.unary("/echo.Echo/Say", b"world")
  drive_pair(server, client, id, frames)
  let reply = client.reply(id)
  assert_eq(reply.status == b"200", true)
  assert_eq(reply.grpc_status, 0)
  assert_eq(reply.messages.length(), 1)
  assert_eq(reply.messages[0] == b"echo:world", true)
}

///|
test "client+server: a server-streaming call reassembles every framed reply in order" {
  let server = H2Server::new()
  server.register_server_streaming("/count.C/Up", (_ctx, req) => {
    [cat(req, b"-1"), cat(req, b"-2"), cat(req, b"-3")]
  })
  let client = H2Client::new()
  let id = client.open("/count.C/Up")
  let frames = client.send(id, b"n", end=true)
  drive_pair(server, client, id, frames)
  let reply = client.reply(id)
  assert_eq(reply.grpc_status, 0)
  assert_eq(reply.messages.length(), 3)
  assert_eq(reply.messages[0] == b"n-1", true)
  assert_eq(reply.messages[1] == b"n-2", true)
  assert_eq(reply.messages[2] == b"n-3", true)
}

///|
test "client+server: a client-streaming call sends many messages for one reply" {
  let server = H2Server::new()
  server.register_client_streaming("/sum.S/Add", (_ctx, msgs) => {
    let buf = Buffer()
    buf.write_bytes(b"got")
    for m in msgs {
      buf.write_byte(b'/')
      buf.write_bytes(m)
    }
    buf.to_bytes()
  })
  let client = H2Client::new()
  let id = client.open("/sum.S/Add")
  let all : Array[Frame] = []
  for f in client.send(id, b"a") {
    all.push(f)
  }
  for f in client.send(id, b"b") {
    all.push(f)
  }
  for f in client.send(id, b"c") {
    all.push(f)
  }
  for f in client.close_send(id) {
    all.push(f)
  }
  drive_pair(server, client, id, all)
  let reply = client.reply(id)
  assert_eq(reply.grpc_status, 0)
  assert_eq(reply.messages.length(), 1)
  assert_eq(reply.messages[0] == b"got/a/b/c", true)
}

///|
test "client+server: request metadata reaches the handler; response metadata reaches the client" {
  let server = H2Server::new()
  server.register_unary("/meta.M/Do", (ctx, _req) => {
    let token = match ctx.metadata_get(b"x-token") {
      Some(v) => v
      None => b"none"
    }
    ctx.add_header(b"x-server", b"on")
    ctx.add_trailer(b"x-count", b"1")
    cat(b"token:", token)
  })
  let client = H2Client::new()
  let id = client.open("/meta.M/Do", metadata=[
    { name: b"x-token", value: b"abc" },
  ])
  let frames = client.send(id, b"", end=true)
  drive_pair(server, client, id, frames)
  let reply = client.reply(id)
  assert_eq(reply.messages[0] == b"token:abc", true)
  assert_eq(header_present(reply.headers, b"x-server", b"on"), true)
  assert_eq(header_present(reply.trailers, b"x-count", b"1"), true)
}

///|
/// Whether `headers` contains an exact `(name, value)` pair.
fn header_present(headers : Array[Header], name : Bytes, value : Bytes) -> Bool {
  for h in headers {
    if h.name == name && h.value == value {
      return true
    }
  }
  false
}

///|
/// Count DATA octets in a batch and note whether an END_STREAM DATA frame appears.
fn tally_data(
  frames : Array[Frame],
  octets : Ref[Int],
  fin : Ref[Bool],
) -> Unit {
  for f in frames {
    match f {
      Data(data~, end_stream~, ..) => {
        octets.val = octets.val + data.length()
        if end_stream {
          fin.val = true
        }
      }
      _ => ()
    }
  }
}

///|
test "client: a small send window splits the request and a WINDOW_UPDATE releases the rest" {
  let client = H2Client::new()
  // peer advertises a tiny 5-octet initial window before the call opens.
  let _ = client.feed(
    Settings(params=[(settings_initial_window_size, 5)], ack=false),
  )
  let id = client.open("/big.Svc/M")
  let octets = Ref(0)
  let fin = Ref(false)
  // a 20-byte body -> 25 octets framed; only 5 escape the window, no END_STREAM.
  tally_data(client.send(id, b"0123456789ABCDEFGHIJ", end=true), octets, fin)
  assert_eq(octets.val, 5)
  assert_eq(fin.val, false)
  // a zero increment releases nothing.
  tally_data(client.feed(WindowUpdate(stream_id=id, increment=0)), octets, fin)
  assert_eq(octets.val, 5)
  // grow the stream window and the remaining 20 octets plus END_STREAM flow.
  tally_data(
    client.feed(WindowUpdate(stream_id=id, increment=1000)),
    octets,
    fin,
  )
  assert_eq(octets.val, 25)
  assert_eq(fin.val, true)
}

///|
test "client: a reply split across two DATA frames is reassembled into one message" {
  let client = H2Client::new()
  let id = client.open("/x.Y/Z")
  let _ = client.send(id, b"q", end=true)
  let enc = HpackEncoder::new()
  // initial response HEADERS.
  let _ = client.feed(
    Headers(
      stream_id=id,
      fragment=enc.encode([{ name: b":status", value: b"200" }]),
      end_stream=false,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  // one length-prefixed message "hello" split mid-way across two DATA frames.
  let framed = encode_message(b"hello")
  let _ = client.feed(
    Data(stream_id=id, data=framed[0:3].to_owned(), end_stream=false, padding=0),
  )
  assert_eq(client.reply(id).messages.length(), 0)
  let _ = client.feed(
    Data(
      stream_id=id,
      data=framed[3:framed.length()].to_owned(),
      end_stream=false,
      padding=0,
    ),
  )
  // trailers close the stream.
  let _ = client.feed(
    Headers(
      stream_id=id,
      fragment=enc.encode([{ name: b"grpc-status", value: b"0" }]),
      end_stream=true,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  let reply = client.reply(id)
  assert_eq(reply.messages.length(), 1)
  assert_eq(reply.messages[0] == b"hello", true)
  assert_eq(reply.grpc_status, 0)
}

///|
test "client: an odd stream id is allocated per call" {
  let client = H2Client::new()
  assert_eq(client.open("/a/b"), 1)
  assert_eq(client.open("/a/b"), 3)
  assert_eq(client.open("/a/b"), 5)
}
