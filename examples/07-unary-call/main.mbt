// A whole unary call end to end, in memory: the pure client engine (`H2Client`) and
// the pure server engine (`H2Server`) exchanging real HTTP/2 frames — HEADERS, DATA,
// grpc-status trailers — with no socket. The server registers a `SayHello` handler;
// the client opens a stream, sends the request, and reads the reply and its status
// back off the `CallReply`.
//
//     moon run examples/07-unary-call

///|
/// Shuttle frames between the two engines until every call in `ids` is done: deliver
/// the client's queued frames to the server, its replies back to the client, repeat.
/// The preface handshake (each side's opening SETTINGS) is seeded first.
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
fn ascii(bs : Bytes) -> String {
  let buf = StringBuilder()
  for b in bs {
    buf.write_char((b.to_int() & 0xFF).unsafe_to_char())
  }
  buf.to_string()
}

///|
fn hello_request(name : String) -> Bytes {
  let w = @moonrpc.PbWriter::new()
  w.string_(1, name)
  w.to_bytes()
}

///|
/// Read the single `string` field out of a `{ string = 1 }` message.
fn read_string_field(msg : Bytes) -> String {
  let r = @moonrpc.PbReader::new(msg)
  try {
    let _ = r.read_tag()
    r.read_string()
  } catch {
    _ => ""
  }
}

///|
fn run() -> Unit raise {
  let server = @moonrpc.H2Server::new()
  server.register("/greet.Greeter/SayHello", req => {
    let w = @moonrpc.PbWriter::new()
    w.string_(1, "hello, " + read_string_field(req))
    w.to_bytes()
  })
  let client = @moonrpc.H2Client::new()
  let (id, initial) = client.unary(
    "/greet.Greeter/SayHello",
    hello_request("heke1228"),
  )
  println("call opened on stream \{id}")
  drive(server, client, [id], initial)
  let reply = client.reply(id)
  println(":status \{ascii(reply.status)}")
  println(
    "grpc-status \{reply.grpc_status} (\{reply.messages.length()} message)",
  )
  println("reply: \{read_string_field(reply.messages[0])}")
  println("stream state on server: \{server.stream_state(id)}")

  // An unregistered path gets a trailers-only UNIMPLEMENTED (12), no reply message.
  let (id2, init2) = client.unary(
    "/greet.Greeter/Missing",
    hello_request("qwq"),
  )
  drive(server, client, [id2], init2)
  let reply2 = client.reply(id2)
  println(
    "unrouted call: grpc-status \{reply2.grpc_status}, \{reply2.messages.length()} messages",
  )
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
