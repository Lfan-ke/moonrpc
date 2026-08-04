// Graceful shutdown with GOAWAY (RFC 7540 §6.8): a draining peer announces the highest
// stream id it will still process, so no new stream races the shutdown, and streams above
// that id were never seen and are safe to re-issue. This completes one call, then has the
// server send GOAWAY(last=1); the client marks the higher, unprocessed stream retryable and
// refuses to open new ones. It also feeds a GOAWAY into the server to flip its drain flag.
//
//     moon run examples/25-goaway-shutdown

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
fn run() -> Unit raise {
  let server = @moonrpc.H2Server::new()
  server.register("/echo.Echo/Say", req => req)
  let client = @moonrpc.H2Client::new()

  // A first call completes normally on stream 1.
  let (id1, init1) = client.unary("/echo.Echo/Say", b"heke1228")
  drive(server, client, id1, init1)
  println(
    "stream \{id1} completed, grpc-status \{client.reply(id1).grpc_status}",
  )

  // A second call is opened (stream 3) but the server starts draining before processing it.
  let id2 = client.open("/echo.Echo/Say")
  println("opened stream \{id2}, not yet processed")

  // The server sends GOAWAY announcing stream 1 as the last it will handle.
  for f in server.goaway(id1) {
    let _ = client.feed(f)
  }
  println("client saw GOAWAY: \{client.goaway_received()}")
  println("  last processed stream id: \{client.goaway_last_stream_id()}")
  println(
    "  stream \{id1} retryable: \{client.is_retryable(id1)} (at or below last -> maybe processed)",
  )
  println(
    "  stream \{id2} retryable: \{client.is_retryable(id2)} (above last -> never seen, safe to re-issue)",
  )

  // A draining client refuses to open new streams: `open` returns the reserved id 0.
  let refused = client.open("/echo.Echo/Say")
  println("open after GOAWAY returns reserved id: \{refused}")

  // The mirror direction: a GOAWAY from the client flips the server's drain flag.
  println("server draining before: \{server.goaway_received()}")
  let _ = server.feed(
    GoAway(last_stream_id=id1, error_code=@moonrpc.error_no_error, debug=b""),
  )
  println("server draining after client GOAWAY: \{server.goaway_received()}")
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
