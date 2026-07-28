// The pure server-side HTTP/2 protocol engine that turns a stream of decoded
// frames into a stream of frames to send back — the transport-independent core
// of a gRPC server. It drives the §5.1 stream state machine, the stateful HPACK
// encoder/decoder, and connection- and stream-level flow control (RFC 7540 §6.9),
// dispatching a completed `application/grpc` request to a registered handler and
// framing the reply as HEADERS + DATA + trailers. No sockets and no async here:
// `feed` is a total function over frames, so the whole engine is exercised
// in-memory on every backend; the socket driver in `net/` only pumps bytes.

///|
/// The HTTP/2 default flow-control window and initial `SETTINGS_INITIAL_WINDOW_SIZE`
/// (RFC 7540 §6.9.2): 65 535 octets.
pub let default_window_size : Int = 65535

///|
/// The HTTP/2 default (and minimum) `SETTINGS_MAX_FRAME_SIZE` (RFC 7540 §6.5.2).
pub let default_max_frame_size : Int = 16384

///|
/// A registered unary gRPC handler: it maps a request message (already stripped
/// of its length prefix) to a reply message. The engine adds the length prefix
/// and the `grpc-status: 0` trailer around a successful return.
pub(all) struct SrvStream {
  id : Int
  mut state : StreamState
  header_block : Buffer
  data : Buffer
  mut headers_complete : Bool
  mut end_stream_recv : Bool
  mut path : String
  mut recv_window : Int
  mut send_window : Int
  mut out : Bytes
  mut out_off : Int
  mut trailers : Array[Header]?
  mut trailers_sent : Bool
  mut dispatched : Bool
}

///|
fn SrvStream::new(id : Int, send_window : Int) -> SrvStream {
  {
    id,
    state: Idle,
    header_block: Buffer(),
    data: Buffer(),
    headers_complete: false,
    end_stream_recv: false,
    path: "",
    recv_window: default_window_size,
    send_window,
    out: b"",
    out_off: 0,
    trailers: None,
    trailers_sent: false,
    dispatched: false,
  }
}

///|
/// The server side of one HTTP/2 connection: the HPACK codec pair, the live
/// streams, the connection-level flow-control windows, and the peer's settings
/// that bound what we may send. Persistent across the whole connection because
/// HPACK and flow control are stateful.
pub struct H2Server {
  handlers : Map[String, (Bytes) -> Bytes]
  encoder : HpackEncoder
  decoder : HpackDecoder
  streams : Map[Int, SrvStream]
  mut conn_recv_window : Int
  mut conn_send_window : Int
  mut remote_initial_window : Int
  mut remote_max_frame : Int
  mut goaway_received : Bool
}

///|
/// A fresh server engine with no registered handlers. Flow-control windows start
/// at the HTTP/2 defaults until the peer's SETTINGS adjust them.
pub fn H2Server::new() -> H2Server {
  {
    handlers: Map([]),
    encoder: HpackEncoder::new(),
    decoder: HpackDecoder::new(),
    streams: Map([]),
    conn_recv_window: default_window_size,
    conn_send_window: default_window_size,
    remote_initial_window: default_window_size,
    remote_max_frame: default_max_frame_size,
    goaway_received: false,
  }
}

///|
/// Register a unary handler for a fully-qualified gRPC path (`/pkg.Service/Method`).
/// A request whose `:path` matches is dispatched to `handler`; an unmatched path
/// gets a trailers-only `grpc-status: 12` (UNIMPLEMENTED) response.
pub fn H2Server::register(
  self : H2Server,
  path : String,
  handler : (Bytes) -> Bytes,
) -> Unit {
  self.handlers[path] = handler
}

///|
/// Whether the peer has sent GOAWAY; the driver stops accepting new streams once
/// this is set.
pub fn H2Server::goaway_received(self : H2Server) -> Bool {
  self.goaway_received
}

///|
/// The server's opening frames (RFC 7540 §3.5): a SETTINGS frame disabling server
/// push. Sent immediately after the client connection preface is validated, before
/// any request frame is read.
pub fn H2Server::preface(self : H2Server) -> Array[Frame] {
  ignore(self)
  [Settings(params=[(settings_enable_push, 0)], ack=false)]
}

// -- small octet helpers ----------------------------------------------------

///|
/// Latin-1 `Bytes` → `String`, one code unit per octet. HTTP/2 header names/values
/// are byte strings; `:path` and `:method` are ASCII, so this is exact for them.
fn bytes_to_ascii(b : Bytes) -> String {
  let sb = StringBuilder::new()
  for i = 0; i < b.length(); i = i + 1 {
    sb.write_char(b[i].to_int().unsafe_to_char())
  }
  sb.to_string()
}

///|
/// A small non-negative integer as its ASCII decimal `Bytes` (for `grpc-status`).
fn int_to_ascii_bytes(n : Int) -> Bytes {
  let buf = Buffer()
  if n == 0 {
    buf.write_byte(b'0')
  } else {
    let digits : Array[Int] = []
    let mut v = n
    while v > 0 {
      digits.push(v % 10)
      v = v / 10
    }
    for i = digits.length() - 1; i >= 0; i = i - 1 {
      buf.write_byte((digits[i] + 0x30).to_byte())
    }
  }
  buf.to_bytes()
}

///|
/// The value of the first header named `name` in `headers`, or `None`.
fn header_value(headers : Array[Header], name : Bytes) -> Bytes? {
  for h in headers {
    if h.name == name {
      return Some(h.value)
    }
  }
  None
}

///|
fn min3(a : Int, b : Int, c : Int) -> Int {
  let m = if a < b { a } else { b }
  if m < c {
    m
  } else {
    c
  }
}

// -- the frame-processing core ----------------------------------------------

///|
fn H2Server::stream(self : H2Server, id : Int) -> SrvStream {
  match self.streams.get(id) {
    Some(s) => s
    None => {
      let s = SrvStream::new(id, self.remote_initial_window)
      self.streams[id] = s
      s
    }
  }
}

///|
/// Apply a peer SETTINGS parameter. A change to `INITIAL_WINDOW_SIZE` retroactively
/// shifts every open stream's send window by the delta (RFC 7540 §6.9.2).
fn H2Server::apply_setting(self : H2Server, id : Int, value : Int) -> Unit {
  if id == settings_initial_window_size {
    let delta = value - self.remote_initial_window
    self.remote_initial_window = value
    for _, s in self.streams {
      s.send_window = s.send_window + delta
    }
  } else if id == settings_max_frame_size {
    self.remote_max_frame = value
  } else if id == settings_header_table_size {
    self.encoder.table.set_max_size(value)
  }
}

///|
/// Emit connection- and stream-level WINDOW_UPDATE frames when a receive window
/// has fallen below half the default, replenishing it to the default (a simple,
/// correct auto-tuning policy; RFC 7540 §6.9).
fn H2Server::replenish(self : H2Server, s : SrvStream) -> Array[Frame] {
  let frames : Array[Frame] = []
  let threshold = default_window_size / 2
  if self.conn_recv_window < threshold {
    let inc = default_window_size - self.conn_recv_window
    self.conn_recv_window = self.conn_recv_window + inc
    frames.push(WindowUpdate(stream_id=0, increment=inc))
  }
  if s.recv_window < threshold {
    let inc = default_window_size - s.recv_window
    s.recv_window = s.recv_window + inc
    frames.push(WindowUpdate(stream_id=s.id, increment=inc))
  }
  frames
}

///|
/// Feed one decoded incoming frame to the engine, advancing all state and
/// returning the frames to write back (SETTINGS ack, PING pong, WINDOW_UPDATE,
/// and — once a request completes — the framed gRPC response). Raises on an
/// illegal stream transition or a malformed header block.
pub fn H2Server::feed(self : H2Server, frame : Frame) -> Array[Frame] raise {
  match frame {
    Settings(params~, ack~) =>
      if ack {
        []
      } else {
        for p in params {
          self.apply_setting(p.0, p.1)
        }
        [Settings(params=[], ack=true)]
      }
    Ping(payload~, ack~) => if ack { [] } else { [Ping(payload~, ack=true)] }
    WindowUpdate(stream_id~, increment~) => {
      if stream_id == 0 {
        self.conn_send_window = self.conn_send_window + increment
      } else {
        self.stream(stream_id).send_window += increment
      }
      self.pump_all()
    }
    Headers(stream_id~, fragment~, end_stream~, end_headers~, ..) => {
      let s = self.stream(stream_id)
      s.state = s.state.on_recv(Headers(end_stream~))
      s.header_block.write_bytes(fragment)
      if end_stream {
        s.end_stream_recv = true
      }
      if end_headers {
        self.complete_headers(s)
      }
      self.maybe_dispatch(s)
    }
    Continuation(stream_id~, fragment~, end_headers~) => {
      let s = self.stream(stream_id)
      s.header_block.write_bytes(fragment)
      if end_headers {
        self.complete_headers(s)
      }
      self.maybe_dispatch(s)
    }
    Data(stream_id~, data~, end_stream~, padding~) => {
      let s = self.stream(stream_id)
      s.state = s.state.on_recv(Data(end_stream~))
      let flow_len = data.length() + padding + (if padding > 0 { 1 } else { 0 })
      self.conn_recv_window = self.conn_recv_window - flow_len
      s.recv_window = s.recv_window - flow_len
      s.data.write_bytes(data)
      if end_stream {
        s.end_stream_recv = true
      }
      let frames = self.replenish(s)
      for f in self.maybe_dispatch(s) {
        frames.push(f)
      }
      frames
    }
    RstStream(stream_id~, ..) => {
      match self.streams.get(stream_id) {
        Some(s) => s.state = Closed
        None => ()
      }
      []
    }
    GoAway(..) => {
      self.goaway_received = true
      []
    }
    Priority(..) | PushPromise(..) | Unknown(..) => []
  }
}

///|
/// HPACK-decode this stream's accumulated header block and pull out `:path`.
fn H2Server::complete_headers(self : H2Server, s : SrvStream) -> Unit raise {
  let headers = self.decoder.decode(s.header_block.to_bytes())
  s.path = match header_value(headers, b":path") {
    Some(p) => bytes_to_ascii(p)
    None => ""
  }
  s.headers_complete = true
}

///|
/// Dispatch once the request is fully received (headers complete and END_STREAM
/// seen), exactly once per stream.
fn H2Server::maybe_dispatch(
  self : H2Server,
  s : SrvStream,
) -> Array[Frame] raise {
  if s.headers_complete && s.end_stream_recv && !s.dispatched {
    s.dispatched = true
    self.dispatch(s)
  } else {
    []
  }
}

///|
/// Run the registered handler (or answer UNIMPLEMENTED) and produce the response
/// frames: a HEADERS with `:status 200` + `content-type application/grpc`, the
/// length-prefixed reply as flow-controlled DATA, then a trailer HEADERS carrying
/// `grpc-status`.
fn H2Server::dispatch(self : H2Server, s : SrvStream) -> Array[Frame] raise {
  let message = match decode_message(s.data.to_bytes()) {
    Some((_, payload)) => payload
    None => b""
  }
  match self.handlers.get(s.path) {
    Some(handler) => {
      let reply = handler(message)
      let block = self.encoder.encode([
        { name: b":status", value: b"200" },
        { name: b"content-type", value: b"application/grpc" },
        { name: b"grpc-encoding", value: b"identity" },
      ])
      let frames : Array[Frame] = [
        Headers(
          stream_id=s.id,
          fragment=block,
          end_stream=false,
          end_headers=true,
          priority=None,
          padding=0,
        ),
      ]
      s.state = s.state.on_send(Headers(end_stream=false))
      s.out = encode_message(reply)
      s.out_off = 0
      s.trailers = Some([{ name: b"grpc-status", value: b"0" }])
      for f in self.pump_stream(s) {
        frames.push(f)
      }
      frames
    }
    None => {
      let block = self.encoder.encode([
        { name: b":status", value: b"200" },
        { name: b"content-type", value: b"application/grpc" },
        {
          name: b"grpc-status",
          value: int_to_ascii_bytes(Status::code(Unimplemented)),
        },
        { name: b"grpc-message", value: b"method not found" },
      ])
      s.state = s.state.on_send(Headers(end_stream=true))
      [
        Headers(
          stream_id=s.id,
          fragment=block,
          end_stream=true,
          end_headers=true,
          priority=None,
          padding=0,
        ),
      ]
    }
  }
}

///|
/// Emit as much of a stream's pending response DATA as the connection and stream
/// send windows allow, in `remote_max_frame`-sized frames, then the trailer
/// HEADERS once the body is fully drained. Re-invoked when a WINDOW_UPDATE grows a
/// window (RFC 7540 §6.9.1): back-pressure is honoured, never violated.
fn H2Server::pump_stream(self : H2Server, s : SrvStream) -> Array[Frame] raise {
  let frames : Array[Frame] = []
  while s.out_off < s.out.length() {
    let remaining = s.out.length() - s.out_off
    let budget = min3(
      self.conn_send_window,
      s.send_window,
      self.remote_max_frame,
    )
    if budget <= 0 {
      break
    }
    let n = if remaining < budget { remaining } else { budget }
    let chunk = s.out[s.out_off:s.out_off + n].to_owned()
    s.out_off = s.out_off + n
    self.conn_send_window = self.conn_send_window - n
    s.send_window = s.send_window - n
    frames.push(Data(stream_id=s.id, data=chunk, end_stream=false, padding=0))
  }
  if s.out_off >= s.out.length() && !s.trailers_sent {
    match s.trailers {
      Some(tr) => {
        let block = self.encoder.encode(tr)
        frames.push(
          Headers(
            stream_id=s.id,
            fragment=block,
            end_stream=true,
            end_headers=true,
            priority=None,
            padding=0,
          ),
        )
        s.state = s.state.on_send(Headers(end_stream=true))
        s.trailers_sent = true
      }
      None => ()
    }
  }
  frames
}

///|
/// Pump every stream that still has buffered response data (after a connection-level
/// WINDOW_UPDATE lifts back-pressure on all of them at once).
fn H2Server::pump_all(self : H2Server) -> Array[Frame] raise {
  let frames : Array[Frame] = []
  for _, s in self.streams {
    if s.dispatched && !s.trailers_sent {
      for f in self.pump_stream(s) {
        frames.push(f)
      }
    }
  }
  frames
}

///|
/// The lifecycle state of stream `id`, or `Idle` if the engine has never seen it.
pub fn H2Server::stream_state(self : H2Server, id : Int) -> StreamState {
  match self.streams.get(id) {
    Some(s) => s.state
    None => Idle
  }
}
