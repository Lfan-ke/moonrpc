///|
/// A finished call must not keep its body around. The engine's call map is otherwise
/// insert-only, so a long-lived connection would hold one full request and response
/// per RPC it ever made, and every settings or window frame would walk that history.
test "client engine releases a finished call" {
  let e = H2Client::new()
  let (id, _) = e.unary("/echo.E/Say", b"hello", metadata=[])
  assert_eq(e.is_done(id), false)
  e.release(id)
  // An unknown id reads as done, which is how a driver loop terminates.
  assert_eq(e.is_done(id), true)
  // Releasing twice is a no-op rather than an error.
  e.release(id)
  assert_eq(e.is_done(id), true)
}

///|
/// The server keeps a finished stream's state — callers still ask it for that — but
/// drops the buffers behind it once the trailers have gone.
test "server engine drops a finished stream's buffers but keeps its state" {
  let e = H2Server::new()
  e.register("/echo.E/Say", req => req)
  let enc = HpackEncoder::new()
  let block = enc.encode([
    { name: b":method", value: b"POST", },
    { name: b":scheme", value: b"http", },
    { name: b":path", value: b"/echo.E/Say", },
    { name: b"content-type", value: b"application/grpc", },
    { name: b"te", value: b"trailers", },
  ])
  let _ = e.feed(Settings(params=[], ack=false))
  let _ = e.feed(
    Headers(
      stream_id=1,
      fragment=block,
      end_stream=false,
      end_headers=true,
      priority=None,
      padding=0,
    ),
  )
  let _ = e.feed(
    Data(stream_id=1, data=encode_message(b"hi"), end_stream=true, padding=0),
  )
  // The response is complete, so the stream reads as Closed and holds nothing.
  assert_eq(e.stream_state(1), StreamState::Closed)
  guard e.streams.get(1) is Some(s) else {
    fail("the finished stream was dropped")
  }
  assert_eq(s.data.length(), 0)
  assert_eq(s.req_msgs.length(), 0)
  assert_eq(s.out.length(), 0)
}
