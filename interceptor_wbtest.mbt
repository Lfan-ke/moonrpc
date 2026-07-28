// Server interceptor chains, exercised over the two pure engines: registration
// order (outermost first), request/response rewriting, short-circuit, and the
// server-streaming variant.

///|
test "interceptor: a unary chain runs outermost-first and can rewrite request and reply" {
  let server = H2Server::new()
  server.register("/svc.S/M", req => cat(b"handler(", cat(req, b")")))
  // outer prepends a tag to the reply; inner prepends to the request.
  server.add_unary_interceptor((ctx, req, next) => {
    cat(b"outer[", cat(next(ctx, req), b"]"))
  })
  server.add_unary_interceptor((ctx, req, next) => {
    next(ctx, cat(b"inner+", req))
  })
  let client = H2Client::new()
  let (id, frames) = client.unary("/svc.S/M", b"x")
  drive_pair(server, client, id, frames)
  // outer wraps the whole thing; inner mutated the request the handler saw.
  assert_eq(client.reply(id).messages[0] == b"outer[handler(inner+x)]", true)
}

///|
test "interceptor: a unary interceptor can short-circuit without calling the handler" {
  let server = H2Server::new()
  let reached = Ref(false)
  server.register("/svc.S/Guard", _req => {
    reached.val = true
    b"handler-ran"
  })
  server.add_unary_interceptor((_ctx, _req, _next) => b"denied")
  let client = H2Client::new()
  let (id, frames) = client.unary("/svc.S/Guard", b"x")
  drive_pair(server, client, id, frames)
  assert_eq(client.reply(id).messages[0] == b"denied", true)
  // the handler never ran.
  assert_eq(reached.val, false)
}

///|
test "interceptor: a server-streaming chain can post-process the reply sequence" {
  let server = H2Server::new()
  server.register_server_streaming("/svc.S/Str", (_ctx, req) => {
    [cat(req, b"-1"), cat(req, b"-2")]
  })
  // append an extra trailing message produced by the interceptor.
  server.add_stream_interceptor((ctx, req, next) => {
    let out = next(ctx, req)
    out.push(b"intercepted")
    out
  })
  let client = H2Client::new()
  let id = client.open("/svc.S/Str")
  let frames = client.send(id, b"n", end=true)
  drive_pair(server, client, id, frames)
  let reply = client.reply(id)
  assert_eq(reply.messages.length(), 3)
  assert_eq(reply.messages[0] == b"n-1", true)
  assert_eq(reply.messages[1] == b"n-2", true)
  assert_eq(reply.messages[2] == b"intercepted", true)
}
