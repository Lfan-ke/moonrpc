// Client streaming: many requests collected, one reply at half-close. The client opens
// the stream, sends several request messages, then half-closes with `close_send`; the
// server's handler receives the whole batch and returns a single reply. Driven in
// memory over the two pure engines.
//
//     moon run examples/09-client-streaming

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
    _ => 0
  }
}

///|
fn run() -> Unit raise {
  let server = @moonrpc.H2Server::new()
  server.register_client_streaming("/sum.Sum/Add", (_ctx, msgs) => {
    let mut total = 0
    for m in msgs {
      total = total + read_int_field(m)
    }
    int_msg(total)
  })
  let client = @moonrpc.H2Client::new()
  let id = client.open("/sum.Sum/Add")
  let addends = [23, 42, 66, 102]
  // Every `send` returns the frames to write now; collect them, then half-close.
  let initial : Array[@moonrpc.Frame] = []
  for x in addends {
    for f in client.send(id, int_msg(x)) {
      initial.push(f)
    }
  }
  for f in client.close_send(id) {
    initial.push(f)
  }
  drive(server, client, [id], initial)
  let reply = client.reply(id)
  let sum = read_int_field(reply.messages[0])
  println("sent \{addends.length()} addends, grpc-status \{reply.grpc_status}")
  println("server summed them to \{sum}")
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
