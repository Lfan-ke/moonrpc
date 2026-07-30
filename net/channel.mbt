// The native gRPC Channel client: a long-lived, multiplexed h2c connection over a
// real `@socket.Tcp`, driven by the pure `@moonrpc.H2Client` engine. The engine
// allocates stream ids, builds request HEADERS + DATA, and reassembles replies;
// this file only pumps bytes — write the engine's request frames, read response
// frames off the socket, feed them back — and enforces the client-side deadline by
// racing the read loop against a timer.

///|
/// A connected gRPC channel. One TCP connection carries every call, each on its own
/// client-allocated (odd) stream id, so calls multiplex over the shared HPACK and
/// flow-control state the engine keeps.
pub struct Channel {
  conn : @socket.Tcp
  engine : @moonrpc.H2Client
}

///|
/// Open a channel to `host:port`: connect, send the 24-octet connection preface and
/// the client SETTINGS. The server's SETTINGS are read and acknowledged lazily on
/// the first call.
pub async fn Channel::connect(host : String, port : Int) -> Channel {
  let conn = @socket.Tcp::connect(
    @socket.Addr::parse(host + ":" + port.to_string()),
  )
  let engine = @moonrpc.H2Client::new()
  conn.write(@moonrpc.connection_preface)
  write_frames(conn, engine.preface())
  { conn, engine }
}

///|
/// Close the underlying connection. Outstanding calls are abandoned.
pub fn Channel::close(self : Channel) -> Unit {
  self.conn.close()
}

///|
/// Read response frames for call `id`, feeding each to the engine and writing back
/// whatever it produces (SETTINGS ack, PING pong, WINDOW_UPDATE), until the engine
/// marks the call complete.
async fn Channel::pump(self : Channel, id : Int) -> Unit {
  while !self.engine.is_done(id) {
    let frame = read_frame(self.conn)
    write_frames(self.conn, self.engine.feed(frame))
  }
}

///|
/// Drive a call to completion, enforcing its deadline when one is set: if the timer
/// elapses first, reset the stream (RST_STREAM CANCEL) and return a synthetic
/// `DEADLINE_EXCEEDED` reply instead of the server's.
async fn Channel::drive(
  self : Channel,
  id : Int,
  timeout_millis : Int?,
) -> @moonrpc.CallReply {
  match timeout_millis {
    None => {
      self.pump(id)
      self.engine.reply(id)
    }
    Some(ms) =>
      match (@async.with_timeout_opt(ms, () => self.pump(id)) : Unit?) {
        Some(_) => self.engine.reply(id)
        None => {
          write_frames(self.conn, [
            @moonrpc.Frame::RstStream(
              stream_id=id,
              error_code=@moonrpc.error_cancel,
            ),
          ]) catch {
            _ => ()
          }
          {
            status: b"",
            grpc_status: @moonrpc.Status::code(DeadlineExceeded),
            headers: [],
            messages: [],
            trailers: [],
          }
        }
      }
  }
}

///|
/// Issue one unary attempt on a fresh stream and drive it to its reply.
async fn Channel::unary_once(
  self : Channel,
  path : String,
  request : Bytes,
  metadata : Array[@moonrpc.Header],
  timeout_millis : Int?,
) -> @moonrpc.CallReply {
  let (id, frames) = self.engine.unary(
    path,
    request,
    metadata~,
    timeout_millis~,
  )
  write_frames(self.conn, frames)
  self.drive(id, timeout_millis)
}

///|
/// Make a unary call: one request message, one reply. `timeout_millis`, when set, is
/// sent as `grpc-timeout` and enforced locally. With a `retry` policy the call is
/// re-issued on a new stream after a backoff sleep while its status is retryable and
/// attempts remain — but only when it is still uncommitted (no reply message has
/// arrived), as gRPC requires.
pub async fn Channel::unary(
  self : Channel,
  path : String,
  request : Bytes,
  metadata? : Array[@moonrpc.Header] = [],
  timeout_millis? : Int? = None,
  retry? : @moonrpc.RetryPolicy? = None,
) -> @moonrpc.CallReply {
  match retry {
    None => self.unary_once(path, request, metadata, timeout_millis)
    Some(policy) => {
      let mut attempt = 1
      let mut reply = self.unary_once(path, request, metadata, timeout_millis)
      while reply.messages.length() == 0 &&
            policy.should_retry(reply.grpc_status, attempt) {
        @async.sleep(policy.backoff_millis(attempt))
        reply = self.unary_once(path, request, metadata, timeout_millis)
        attempt = attempt + 1
      }
      reply
    }
  }
}

///|
/// Make a server-streaming call: one request message, then read the ordered run of
/// reply messages (in `CallReply::messages`) until the server closes the stream.
pub async fn Channel::server_streaming(
  self : Channel,
  path : String,
  request : Bytes,
  metadata? : Array[@moonrpc.Header] = [],
  timeout_millis? : Int? = None,
) -> @moonrpc.CallReply {
  let id = self.engine.open(path, metadata~, timeout_millis~)
  write_frames(self.conn, self.engine.send(id, request, end=true))
  self.drive(id, timeout_millis)
}

///|
/// Make a client-streaming call: send every message in `requests`, half-close, then
/// read the single reply.
pub async fn Channel::client_streaming(
  self : Channel,
  path : String,
  requests : Array[Bytes],
  metadata? : Array[@moonrpc.Header] = [],
  timeout_millis? : Int? = None,
) -> @moonrpc.CallReply {
  let id = self.engine.open(path, metadata~, timeout_millis~)
  for m in requests {
    write_frames(self.conn, self.engine.send(id, m))
  }
  write_frames(self.conn, self.engine.close_send(id))
  self.drive(id, timeout_millis)
}

///|
/// Make a bidirectional-streaming call: send every message in `requests`, half-close,
/// then read the ordered run of reply messages the server produced (in
/// `CallReply::messages`). This is the eager form the engine's synchronous core
/// models — every request is sent before the replies are collected, which pairs with
/// the eager server `BidiHandler`. A truly interactive exchange (sending while
/// receiving) or several concurrent calls multiplexed on one channel need the shared
/// read-demux loop on the roadmap; until then a `Channel` drives one call at a time.
pub async fn Channel::bidi(
  self : Channel,
  path : String,
  requests : Array[Bytes],
  metadata? : Array[@moonrpc.Header] = [],
  timeout_millis? : Int? = None,
) -> @moonrpc.CallReply {
  let id = self.engine.open(path, metadata~, timeout_millis~)
  for m in requests {
    write_frames(self.conn, self.engine.send(id, m))
  }
  write_frames(self.conn, self.engine.close_send(id))
  self.drive(id, timeout_millis)
}

///|
/// Register the `grpc.health.v1.Health` service (Check + Watch) on this server.
pub fn GrpcServer::register_health(
  self : GrpcServer,
  health : @moonrpc.HealthService,
) -> Unit {
  for entry in health.handlers() {
    self.handlers[entry.0] = entry.1
  }
}
