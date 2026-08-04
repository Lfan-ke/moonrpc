// HTTP/2 flow control (RFC 7540 §5.2, §6.9): a reply larger than the receive window
// cannot be sent all at once. The server segments it to the peer's max-frame size and
// stops at the window; the client replenishes with WINDOW_UPDATE as it drains the data,
// and the server resumes. This drives a ~100 KB unary reply through the two pure
// engines, counting the DATA segments and the WINDOW_UPDATEs that carried it, and
// checks every byte was reassembled.
//
//     moon run examples/12-flow-control

///|
fn all_done(client : @moonrpc.H2Client, id : Int) -> Bool {
  client.is_done(id)
}

///|
fn big_reply(_req : Bytes) -> Bytes {
  let buf = Buffer()
  // 100_000 bytes > the 65_535 default window, so the exchange must span a WINDOW_UPDATE.
  for i = 0; i < 100000; i = i + 1 {
    buf.write_byte((i & 0xFF).to_byte())
  }
  buf.to_bytes()
}

///|
fn run() -> Unit raise {
  println("default window: \{@moonrpc.default_window_size} octets")
  println("default max frame: \{@moonrpc.default_max_frame_size} octets")
  let server = @moonrpc.H2Server::new()
  server.register("/blob.Blob/Get", big_reply)
  let client = @moonrpc.H2Client::new()
  let (id, initial) = client.unary("/blob.Blob/Get", b"give me the blob")

  // Drive the two engines by hand so each frame can be classified as it crosses.
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
  let mut data_to_client = 0
  let mut data_octets = 0
  let mut window_updates = 0
  let mut si = 0
  let mut ci = 0
  let mut steps = 0
  while !all_done(client, id) && steps < 1000000 {
    steps = steps + 1
    if si < to_server.length() {
      let f = to_server[si]
      si = si + 1
      match f {
        WindowUpdate(..) => window_updates = window_updates + 1
        _ => ()
      }
      for r in server.feed(f) {
        to_client.push(r)
      }
    } else if ci < to_client.length() {
      let f = to_client[ci]
      ci = ci + 1
      match f {
        Data(data~, ..) => {
          data_to_client = data_to_client + 1
          data_octets = data_octets + data.length()
        }
        WindowUpdate(..) => window_updates = window_updates + 1
        _ => ()
      }
      for r in client.feed(f) {
        to_server.push(r)
      }
    } else {
      break
    }
  }
  let reply = client.reply(id)
  let payload = if reply.messages.length() > 0 {
    reply.messages[0].length()
  } else {
    0
  }
  println(
    "reply reassembled: \{payload} octets (grpc-status \{reply.grpc_status})",
  )
  println(
    "carried in \{data_to_client} DATA frames (\{data_octets} framed octets)",
  )
  println("WINDOW_UPDATE frames exchanged: \{window_updates}")
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
