// The HTTP/2 stream state machine (RFC 7540 §5.1) and the §5.1.1 identifier rules.
// A stream walks idle -> open -> half-closed -> closed as END_STREAM headers and
// data are sent and received; an illegal frame for the current state is rejected
// rather than silently accepted. This drives one unary call's stream through its
// whole lifecycle from the client side, then shows a rejected transition and the
// odd/even id rules that keep client- and server-initiated streams from colliding.
//
//     moon run examples/03-stream-lifecycle

///|
#coverage.skip
fn main {
  let s = @moonrpc.Stream::new(1)
  println("stream \{1}: start \{s.state}")
  try {
    // The client opens the stream with request HEADERS, streams the request body,
    // and half-closes with END_STREAM — now it can only receive.
    println("  send HEADERS            -> \{s.send(Headers(end_stream=false))}")
    println("  send DATA(END_STREAM)   -> \{s.send(Data(end_stream=true))}")
    // The server's response HEADERS, body, and grpc-status trailer arrive; the
    // END_STREAM trailer closes the stream.
    println("  recv HEADERS            -> \{s.recv(Headers(end_stream=false))}")
    println("  recv DATA               -> \{s.recv(Data(end_stream=false))}")
    println("  recv HEADERS(END_STREAM)-> \{s.recv(Headers(end_stream=true))}")
  } catch {
    e => println("  unexpected: \{e}")
  }

  // A fresh stream cannot receive DATA before any HEADERS opened it — the state
  // machine raises instead of inventing a transition.
  let idle = @moonrpc.Stream::new(3)
  let outcome = try {
    let _ = idle.recv(Data(end_stream=false))
    "accepted (unexpected)"
  } catch {
    e => "rejected -> \{e}"
  }
  println("illegal DATA in idle: \{outcome}")

  // §5.1.1: clients open odd streams, servers open even ones, 0 is the connection
  // control stream that neither may open.
  println("id 1 client-initiated: \{@moonrpc.stream_is_client_initiated(1)}")
  println("id 2 server-initiated: \{@moonrpc.stream_is_server_initiated(2)}")
  println(
    "id 2 legal for a client to open: \{@moonrpc.stream_id_valid_for_initiator(2, by_client=true)}",
  )
  println(
    "id 0 legal for anyone to open: \{@moonrpc.stream_id_valid_for_initiator(0, by_client=true)}",
  )
}
