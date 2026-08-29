// Server interceptors: a chain wrapped around a method handler, outermost first. Each
// interceptor sees the context and request, may rewrite either, calls `next` to proceed,
// and may post-process the reply — or return without calling `next` to short-circuit.
// This shows a unary chain rewriting request and reply, a short-circuiting auth guard,
// and a server-streaming interceptor appending a message. Driven over the pure engines.
//
//     moon run examples/17-interceptors

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
fn bcat(a : Bytes, b : Bytes) -> Bytes {
  let buf = Buffer()
  buf.write_bytes(a)
  buf.write_bytes(b)
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
  // A unary chain: registration order is outer-to-inner, so the first-registered
  // interceptor wraps the rest and its reply post-processing is the outermost.
  let server = @moonrpc.H2Server::new()
  server.register("/svc.S/M", req => bcat(b"handler(", bcat(req, b")")))
  server.add_unary_interceptor((ctx, req, next) => {
    bcat(b"outer[", bcat(next(ctx, req), b"]"))
  })
  server.add_unary_interceptor((ctx, req, next) => {
    next(ctx, bcat(b"inner+", req))
  })
  // A fresh client per server: an H2Client's HPACK codec is stateful over one connection.
  let c1 = @moonrpc.H2Client::new()
  let (id, initial) = c1.unary("/svc.S/M", b"x")
  drive(server, c1, id, initial)
  println("unary chain reply: \{ascii(c1.reply(id).messages[0])}")

  // A second server whose interceptor short-circuits: it returns before `next`, so the
  // handler never runs.
  let guarded = @moonrpc.H2Server::new()
  guarded.register("/svc.S/Secret", _req => b"the-secret")
  guarded.add_unary_interceptor((_ctx, _req, _next) => b"denied: no token")
  let c2 = @moonrpc.H2Client::new()
  let (id2, init2) = c2.unary("/svc.S/Secret", b"give me the secret")
  drive(guarded, c2, id2, init2)
  println("guarded call reply: \{ascii(c2.reply(id2).messages[0])}")

  // A server-streaming interceptor appends a trailing message to the handler's run.
  let streamer = @moonrpc.H2Server::new()
  streamer.register_server_streaming("/svc.S/Feed", (_ctx, req) => {
    [bcat(req, b"-1"), bcat(req, b"-2")]
  })
  streamer.add_stream_interceptor((ctx, req, next) => {
    let out = next(ctx, req)
    out.push(b"intercepted")
    out
  })
  let c3 = @moonrpc.H2Client::new()
  let (id3, init3) = c3.unary("/svc.S/Feed", b"n")
  drive(streamer, c3, id3, init3)
  let reply3 = c3.reply(id3)
  println("stream chain (\{reply3.messages.length()} replies):")
  for m in reply3.messages {
    println("  \{ascii(m)}")
  }
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
