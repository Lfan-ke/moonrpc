// Rich errors: a handler ends a call with a non-OK status through `RpcContext::fail`,
// and attaches structured detail with `add_error_detail`. The server sends the base
// `grpc-status` / `grpc-message` trailer and packs the code, message, and details into
// a `google.rpc.Status` in the `grpc-status-details-bin` trailer. The client surfaces
// the code and percent-decoded message; this decodes the packed status back too.
//
//     moon run examples/14-rich-errors

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
/// Look for a header by name across both metadata blocks. A failing unary call sends a
/// trailers-only response, so its packed status rides the single header block the client
/// surfaces as initial metadata rather than as separate trailers.
fn find_meta(reply : @moonrpc.CallReply, name : Bytes) -> Bytes {
  for h in reply.headers {
    if h.name == name {
      return h.value
    }
  }
  for h in reply.trailers {
    if h.name == name {
      return h.value
    }
  }
  b""
}

///|
/// A `google.protobuf.Any`-style detail: a type URL (field 1) and an opaque value
/// (field 2). Any bytes work — the transport treats details as opaque.
fn make_detail(type_url : String, value : String) -> Bytes {
  let w = @moonrpc.PbWriter::new()
  w.string_(1, type_url)
  w.string_(2, value)
  w.to_bytes()
}

///|
/// Decode the `google.rpc.Status` in a `grpc-status-details-bin` trailer: `code = 1`,
/// `message = 2`, repeated `details = 3`.
fn decode_status_details(body : Bytes) -> (Int, String, Int) raise {
  let r = @moonrpc.PbReader::new(body)
  let mut code = 0
  let mut message = ""
  let mut details = 0
  while !r.eof() {
    let (field, wire) = r.read_tag()
    match field {
      1 => code = r.read_int32()
      2 => message = r.read_string()
      3 => {
        let _ = r.read_bytes()
        details = details + 1
      }
      _ => r.skip(wire)
    }
  }
  (code, message, details)
}

///|
fn run() -> Unit raise {
  // `grpc-message` rides the wire percent-encoded; the client decodes it. The codec on
  // its own:
  let decoded = @moonrpc.percent_decode(b"user%20not%20found%3A%2042")
  println("percent_decode: \{decoded}")
  let server = @moonrpc.H2Server::new()
  server.register_unary("/users.Users/Get", (ctx, _req) => {
    ctx.add_error_detail(
      make_detail("type.googleapis.com/google.rpc.ErrorInfo", "reason=ABSENT"),
    )
    ctx.add_error_detail(
      make_detail("type.googleapis.com/google.rpc.ResourceInfo", "users/42"),
    )
    ctx.fail(@moonrpc.Status::code(NotFound), "user 42 does not exist") // discarded once the call fails
    b""
  })
  let client = @moonrpc.H2Client::new()
  let (id, initial) = client.unary("/users.Users/Get", b"id=42")
  drive(server, client, id, initial)
  let reply = client.reply(id)
  let not_found : @moonrpc.Status = NotFound
  println(
    "grpc-status \{reply.grpc_status} (\{not_found.name()}), \{reply.messages.length()} messages",
  )
  println("grpc-message: \{reply.grpc_message}")
  let packed = find_meta(reply, b"grpc-status-details-bin")
  let (code, message, details) = decode_status_details(packed)
  println(
    "google.rpc.Status: code=\{code}, message=\{message}, \{details} detail(s) attached",
  )
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
