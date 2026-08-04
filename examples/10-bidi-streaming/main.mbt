// Bidirectional streaming: each request is answered as it arrives, and a farewell is
// sent at half-close. The server registers a `BidiHandler` — `on_message` fires per
// request message and returns the replies to send right then, `on_end` fires after the
// client half-closes. The client sends several messages, then closes; every reply comes
// back in order. Driven in memory over the two pure engines.
//
//     moon run examples/10-bidi-streaming

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
fn run() -> Unit raise {
  let server = @moonrpc.H2Server::new()
  server.register_bidi("/chat.Chat/Echo", _ctx => @moonrpc.BidiHandler::{
    on_message: m => [text_msg("echo: " + read_text(m))],
    on_end: () => [text_msg("bye")],
  })
  let client = @moonrpc.H2Client::new()
  let id = client.open("/chat.Chat/Echo")
  let lines = ["emm", "hhh", "qwq"]
  let initial : Array[@moonrpc.Frame] = []
  for line in lines {
    for f in client.send(id, text_msg(line)) {
      initial.push(f)
    }
  }
  for f in client.close_send(id) {
    initial.push(f)
  }
  drive(server, client, [id], initial)
  let reply = client.reply(id)
  println(
    "sent \{lines.length()} messages, got \{reply.messages.length()} back (grpc-status \{reply.grpc_status}):",
  )
  for m in reply.messages {
    println("  \{read_text(m)}")
  }
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
