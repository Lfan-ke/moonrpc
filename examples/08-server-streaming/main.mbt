// Server streaming: one request, an ordered run of replies. The server registers a
// `Count/Up` handler that returns several messages; the client opens the stream, sends
// the single request with END_STREAM, and reassembles every framed reply in order off
// one `CallReply`. All in memory over the two pure engines — no socket.
//
//     moon run examples/08-server-streaming

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
/// A message `{ int32 n = 1 }`.
fn count_msg(n : Int) -> Bytes {
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
fn run() -> Unit raise {
  let server = @moonrpc.H2Server::new()
  // One request carrying a start value; the reply stream counts up from it.
  server.register_server_streaming("/count.Count/Up", (_ctx, req) => {
    let start = read_int_field(req)
    [count_msg(start), count_msg(start + 1), count_msg(start + 2)]
  })
  let client = @moonrpc.H2Client::new()
  let (id, initial) = client.unary("/count.Count/Up", count_msg(233))
  drive(server, client, [id], initial)
  // Read the reply stream incrementally: `take_messages` drains the buffered replies
  // (clearing them), the counterpart to reading them all at once off `reply`.
  println("has buffered replies: \{client.has_messages(id)}")
  let messages = client.take_messages(id)
  let status = client.reply(id).grpc_status
  println("grpc-status \{status}, \{messages.length()} replies:")
  for m in messages {
    println("  n = \{read_int_field(m)}")
  }
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
