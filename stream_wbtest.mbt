///|
test "connection preface is the exact 24-octet magic (RFC 7540 §3.5)" {
  assert_eq(connection_preface.length(), 24)
  assert_eq(connection_preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
  assert_eq(has_connection_preface(connection_preface), true)
  let buf = Buffer()
  buf.write_bytes(connection_preface)
  buf.write_bytes(b"\x00\x00\x00\x04\x00\x00\x00\x00\x00") // a SETTINGS frame after
  assert_eq(has_connection_preface(buf.to_bytes()), true)
  assert_eq(has_connection_preface(b"PRI * HTTP/1.1\r\n\r\n"), false)
  assert_eq(has_connection_preface(b"PRI"), false)
}

///|
test "stream lifecycle: idle -> open -> half-closed -> closed (RFC 7540 §5.1)" {
  let s = Stream::new(1)
  assert_eq(s.state, Idle)
  assert_eq(s.send(StreamEvent::Headers(end_stream=false)), Open)
  assert_eq(s.recv(StreamEvent::Data(end_stream=false)), Open)
  assert_eq(s.recv(StreamEvent::Data(end_stream=true)), HalfClosedRemote)
  assert_eq(s.send(StreamEvent::Data(end_stream=true)), Closed)
}

///|
test "stream lifecycle: HEADERS with END_STREAM opens then half-closes locally" {
  let s = Stream::new(3)
  assert_eq(s.send(StreamEvent::Headers(end_stream=true)), HalfClosedLocal)
  assert_eq(s.recv(StreamEvent::Headers(end_stream=true)), Closed)
}

///|
test "stream lifecycle: push reserves then opens (RFC 7540 §5.1)" {
  let promised = Stream::new(2)
  assert_eq(promised.recv(StreamEvent::Reserve), ReservedRemote)
  assert_eq(
    promised.recv(StreamEvent::Headers(end_stream=false)),
    HalfClosedLocal,
  )
  let pushed = Stream::new(4)
  assert_eq(pushed.send(StreamEvent::Reserve), ReservedLocal)
  assert_eq(
    pushed.send(StreamEvent::Headers(end_stream=false)),
    HalfClosedRemote,
  )
}

///|
test "stream lifecycle: RST_STREAM closes from any active state" {
  let s = Stream::new(1)
  let _ = s.send(StreamEvent::Headers(end_stream=false))
  assert_eq(s.send(StreamEvent::RstStream), Closed)
}

///|
test "stream lifecycle: illegal transitions raise (RFC 7540 §5.1)" {
  // DATA on an idle stream is a protocol error.
  let idle_data = try {
    let _ = StreamState::on_recv(Idle, StreamEvent::Data(end_stream=false))
    false
  } catch {
    InvalidTransition(_) => true
  }
  assert_eq(idle_data, true)
  // Receiving DATA after the remote already half-closed is STREAM_CLOSED.
  let after_close = try {
    let _ = StreamState::on_recv(
      HalfClosedRemote,
      StreamEvent::Data(end_stream=false),
    )
    false
  } catch {
    InvalidTransition(_) => true
  }
  assert_eq(after_close, true)
  // Anything on a closed stream (except nothing here) raises.
  let on_closed = try {
    let _ = StreamState::on_send(Closed, StreamEvent::Data(end_stream=false))
    false
  } catch {
    InvalidTransition(_) => true
  }
  assert_eq(on_closed, true)
}

///|
test "stream identifier parity rules (RFC 7540 §5.1.1)" {
  assert_eq(stream_is_client_initiated(1), true)
  assert_eq(stream_is_client_initiated(3), true)
  assert_eq(stream_is_client_initiated(2), false)
  assert_eq(stream_is_client_initiated(0), false)
  assert_eq(stream_is_server_initiated(2), true)
  assert_eq(stream_is_server_initiated(1), false)
  assert_eq(stream_is_server_initiated(0), false)
  assert_eq(stream_id_valid_for_initiator(1, by_client=true), true)
  assert_eq(stream_id_valid_for_initiator(2, by_client=true), false)
  assert_eq(stream_id_valid_for_initiator(2, by_client=false), true)
  assert_eq(stream_id_valid_for_initiator(0, by_client=false), false)
}
