// The pure client-side HTTP/2 protocol engine — the transport-independent core of
// a gRPC Channel, symmetric to `H2Server`. It allocates client stream ids, builds
// request HEADERS (HPACK-encoded) and length-prefixed DATA honouring the send
// windows, and turns the response frames back into `(:status, grpc-status, initial
// metadata, reply messages, trailers)`. No sockets and no async: `feed` is a total
// function over frames, so the whole client path runs in-memory on every backend;
// the `Channel` driver in `net/` only pumps bytes.

///|
/// One client-side call: its stream id, the request side (the length-prefixed
/// request body still to send, the send window, and whether the request has been
/// half-closed), and the response side (the accumulating DATA with a cursor over
/// the messages already pulled out, the recv window, the captured `:status` and
/// `grpc-status`, and the response initial and trailing metadata).
pub(all) struct ClientCall {
  id : Int
  path : Bytes
  authority : Bytes
  metadata : Array[Header]
  timeout : Bytes?
  mut out : Bytes
  mut out_off : Int
  mut req_ended : Bool
  mut req_headers_sent : Bool
  mut req_fin_sent : Bool
  mut send_window : Int
  data : Buffer
  mut data_off : Int
  messages : Array[Bytes]
  mut recv_window : Int
  mut status : Bytes
  mut grpc_status : Int?
  // The response's `grpc-encoding` (empty = identity). A reply message with the
  // compression flag set under `gzip` is inflated before it reaches the caller.
  mut resp_encoding : Bytes
  resp_headers : Array[Header]
  resp_trailers : Array[Header]
  mut headers_seen : Bool
  mut done : Bool
}

///|
fn ClientCall::new(
  id : Int,
  path : Bytes,
  authority : Bytes,
  metadata : Array[Header],
  timeout : Bytes?,
  send_window : Int,
) -> ClientCall {
  {
    id,
    path,
    authority,
    metadata,
    timeout,
    out: b"",
    out_off: 0,
    req_ended: false,
    req_headers_sent: false,
    req_fin_sent: false,
    send_window,
    data: Buffer(),
    data_off: 0,
    messages: [],
    recv_window: default_window_size,
    status: b"",
    grpc_status: None,
    resp_encoding: b"",
    resp_headers: [],
    resp_trailers: [],
    headers_seen: false,
    done: false,
  }
}

///|
/// The completed result of a call: the HTTP `:status`, the numeric `grpc-status`
/// (`-1` if the peer never sent one), the response initial metadata, the reply
/// messages in order, and the trailing metadata.
pub(all) struct CallReply {
  status : Bytes
  grpc_status : Int
  headers : Array[Header]
  messages : Array[Bytes]
  trailers : Array[Header]
}

///|
/// The client side of one HTTP/2 connection: the HPACK codec pair (stateful across
/// every call on the connection), the live calls keyed by stream id, the next
/// odd stream id to allocate (§5.1.1), and the connection-level flow-control state
/// bounded by the peer's SETTINGS.
pub struct H2Client {
  encoder : HpackEncoder
  decoder : HpackDecoder
  calls : Map[Int, ClientCall]
  mut next_id : Int
  mut conn_send_window : Int
  mut conn_recv_window : Int
  mut remote_initial_window : Int
  mut remote_max_frame : Int
}

///|
/// A fresh client engine with no live calls. The first call takes stream id 1.
pub fn H2Client::new() -> H2Client {
  {
    encoder: HpackEncoder::new(),
    decoder: HpackDecoder::new(),
    calls: Map([]),
    next_id: 1,
    conn_send_window: default_window_size,
    conn_recv_window: default_window_size,
    remote_initial_window: default_window_size,
    remote_max_frame: default_max_frame_size,
  }
}

///|
/// The client's opening frames (RFC 7540 §3.5): a SETTINGS frame disabling server
/// push. Written right after the 24-octet connection preface bytes, before any
/// request.
pub fn H2Client::preface(self : H2Client) -> Array[Frame] {
  ignore(self)
  [Settings(params=[(settings_enable_push, 0)], ack=false)]
}

///|
/// Open a new call on this connection for `path` (`/pkg.Service/Method`), returning
/// its freshly allocated stream id. `metadata` is sent as custom request HEADERS;
/// `timeout_millis`, when set, becomes the `grpc-timeout` header. The request body
/// is added with `send` and half-closed with `close_send`.
pub fn H2Client::open(
  self : H2Client,
  path : String,
  metadata? : Array[Header] = [],
  authority? : String = "127.0.0.1",
  timeout_millis? : Int? = None,
) -> Int {
  let id = self.next_id
  self.next_id = self.next_id + 2
  let timeout = match timeout_millis {
    Some(ms) => Some(encode_grpc_timeout(ms))
    None => None
  }
  let call = ClientCall::new(
    id,
    ascii_to_bytes(path),
    ascii_to_bytes(authority),
    metadata,
    timeout,
    self.remote_initial_window,
  )
  self.calls[id] = call
  id
}

///|
/// Append one request `message` to a call and return the frames to write now (the
/// request HEADERS the first time, then as much length-prefixed DATA as the send
/// windows allow). Set `end` on the last message to half-close the request.
pub fn H2Client::send(
  self : H2Client,
  id : Int,
  message : Bytes,
  end? : Bool = false,
) -> Array[Frame] {
  match self.calls.get(id) {
    Some(c) => {
      c.out = cat(c.out, encode_message(message))
      if end {
        c.req_ended = true
      }
      self.produce_request(c)
    }
    None => []
  }
}

///|
/// Half-close the request side of a call (no more request messages) and return any
/// frames that completes — the trailing END_STREAM.
pub fn H2Client::close_send(self : H2Client, id : Int) -> Array[Frame] {
  match self.calls.get(id) {
    Some(c) => {
      c.req_ended = true
      self.produce_request(c)
    }
    None => []
  }
}

///|
/// Open a unary call and return `(stream_id, frames_to_write)` in one step: the
/// request HEADERS and the single length-prefixed request message with END_STREAM.
pub fn H2Client::unary(
  self : H2Client,
  path : String,
  request : Bytes,
  metadata? : Array[Header] = [],
  authority? : String = "127.0.0.1",
  timeout_millis? : Int? = None,
) -> (Int, Array[Frame]) {
  let id = self.open(path, metadata~, authority~, timeout_millis~)
  (id, self.send(id, request, end=true))
}

///|
/// Emit the request frames a call can send right now: the HEADERS once, then as
/// much buffered DATA as the connection and stream send windows and the peer's
/// max-frame size allow, the last frame carrying END_STREAM once the request is
/// half-closed. A half-close with an already-drained body emits an empty
/// END_STREAM DATA.
fn H2Client::produce_request(self : H2Client, c : ClientCall) -> Array[Frame] {
  let frames : Array[Frame] = []
  if !c.req_headers_sent {
    let headers : Array[Header] = [
      { name: b":method", value: b"POST" },
      { name: b":scheme", value: b"http" },
      { name: b":path", value: c.path },
      { name: b":authority", value: c.authority },
      { name: b"te", value: b"trailers" },
      { name: b"content-type", value: b"application/grpc" },
    ]
    match c.timeout {
      Some(t) => headers.push({ name: b"grpc-timeout", value: t })
      None => ()
    }
    for h in c.metadata {
      // A `-bin` metadata value goes on the wire base64-encoded.
      headers.push({
        name: h.name,
        value: metadata_value_to_wire(h.name, h.value),
      })
    }
    let block = self.encoder.encode(headers)
    let end = c.req_ended && c.out.length() == 0
    frames.push(
      Headers(
        stream_id=c.id,
        fragment=block,
        end_stream=end,
        end_headers=true,
        priority=None,
        padding=0,
      ),
    )
    c.req_headers_sent = true
    if end {
      c.req_fin_sent = true
    }
  }
  while c.out_off < c.out.length() {
    let remaining = c.out.length() - c.out_off
    let budget = min3(
      self.conn_send_window,
      c.send_window,
      self.remote_max_frame,
    )
    if budget <= 0 {
      break
    }
    let n = if remaining < budget { remaining } else { budget }
    let chunk = c.out[c.out_off:c.out_off + n].to_owned()
    c.out_off = c.out_off + n
    self.conn_send_window = self.conn_send_window - n
    c.send_window = c.send_window - n
    let last = c.out_off >= c.out.length() && c.req_ended
    if last {
      c.req_fin_sent = true
    }
    frames.push(Data(stream_id=c.id, data=chunk, end_stream=last, padding=0))
  }
  if c.req_ended && !c.req_fin_sent && c.out_off >= c.out.length() {
    c.req_fin_sent = true
    frames.push(Data(stream_id=c.id, data=b"", end_stream=true, padding=0))
  }
  frames
}

///|
/// Feed one decoded response frame to the engine, advancing all state and returning
/// the frames to write back (SETTINGS ack, PING pong, WINDOW_UPDATE replenishing a
/// receive window, and — once a WINDOW_UPDATE lifts back-pressure — any remaining
/// request DATA). Captures `:status`, `grpc-status`, response metadata, and the
/// reassembled reply messages.
pub fn H2Client::feed(self : H2Client, frame : Frame) -> Array[Frame] raise {
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
        match self.calls.get(stream_id) {
          Some(c) => c.send_window = c.send_window + increment
          None => ()
        }
      }
      self.pump_requests()
    }
    Headers(stream_id~, fragment~, end_stream~, ..) => {
      match self.calls.get(stream_id) {
        Some(c) => {
          let headers = self.decoder.decode(fragment)
          for h in headers {
            if h.name == b":status" {
              c.status = h.value
            } else if h.name == b"grpc-status" {
              c.grpc_status = Some(ascii_bytes_to_int(h.value))
            } else if h.name == b"grpc-message" {
              ()
            } else if h.name == b"grpc-encoding" {
              c.resp_encoding = h.value
            } else if !c.headers_seen {
              if !is_reserved_header(h.name) {
                // A `-bin` metadata value arrives base64-encoded; surface raw bytes.
                c.resp_headers.push({
                  name: h.name,
                  value: metadata_value_from_wire(h.name, h.value),
                })
              }
            } else {
              c.resp_trailers.push({
                name: h.name,
                value: metadata_value_from_wire(h.name, h.value),
              })
            }
          }
          c.headers_seen = true
          if end_stream {
            c.done = true
          }
        }
        None => ()
      }
      []
    }
    Data(stream_id~, data~, end_stream~, padding~) =>
      match self.calls.get(stream_id) {
        Some(c) => {
          let flow = data.length() + padding + (if padding > 0 { 1 } else { 0 })
          self.conn_recv_window = self.conn_recv_window - flow
          c.recv_window = c.recv_window - flow
          c.data.write_bytes(data)
          drain_call_messages(c)
          if end_stream {
            c.done = true
          }
          self.replenish(c)
        }
        None => []
      }
    RstStream(stream_id~, ..) => {
      match self.calls.get(stream_id) {
        Some(c) => c.done = true
        None => ()
      }
      []
    }
    GoAway(..)
    | Priority(..)
    | PushPromise(..)
    | Continuation(..)
    | Unknown(..) => []
  }
}

///|
/// Apply a peer SETTINGS parameter, mirroring the delta of a changed
/// `INITIAL_WINDOW_SIZE` onto every open call's send window (RFC 7540 §6.9.2).
fn H2Client::apply_setting(self : H2Client, id : Int, value : Int) -> Unit {
  if id == settings_initial_window_size {
    // Ignore an out-of-range window (RFC 7540 §6.5.2 caps it at 2^31-1) rather than
    // letting a negative value corrupt every call's send-window math.
    if value < 0 {
      return
    }
    let delta = value - self.remote_initial_window
    self.remote_initial_window = value
    for _, c in self.calls {
      c.send_window = c.send_window + delta
    }
  } else if id == settings_max_frame_size {
    // Clamp to the RFC 7540 §6.5.2 range [2^14, 2^24-1].
    self.remote_max_frame = if value < default_max_frame_size {
      default_max_frame_size
    } else if value > 0xFFFFFF {
      0xFFFFFF
    } else {
      value
    }
  } else if id == settings_header_table_size {
    if value >= 0 {
      self.encoder.table.set_max_size(value)
    }
  }
}

///|
/// Emit connection- and stream-level WINDOW_UPDATE frames when a receive window has
/// fallen below half the default, replenishing it to the default (RFC 7540 §6.9).
fn H2Client::replenish(self : H2Client, c : ClientCall) -> Array[Frame] {
  let frames : Array[Frame] = []
  let threshold = default_window_size / 2
  if self.conn_recv_window < threshold {
    let inc = default_window_size - self.conn_recv_window
    self.conn_recv_window = self.conn_recv_window + inc
    frames.push(WindowUpdate(stream_id=0, increment=inc))
  }
  if c.recv_window < threshold {
    let inc = default_window_size - c.recv_window
    c.recv_window = c.recv_window + inc
    frames.push(WindowUpdate(stream_id=c.id, increment=inc))
  }
  frames
}

///|
/// Continue any call whose request body is not fully sent after a WINDOW_UPDATE
/// lifted back-pressure.
fn H2Client::pump_requests(self : H2Client) -> Array[Frame] {
  let frames : Array[Frame] = []
  for _, c in self.calls {
    if c.req_headers_sent && !c.req_fin_sent {
      for f in self.produce_request(c) {
        frames.push(f)
      }
    }
  }
  frames
}

///|
/// Whether a call has fully completed (its response ended). An unknown id counts as
/// done so a driver loop terminates.
pub fn H2Client::is_done(self : H2Client, id : Int) -> Bool {
  match self.calls.get(id) {
    Some(c) => c.done
    None => true
  }
}

///|
/// The completed result of a call. Meaningful once `is_done` is true.
pub fn H2Client::reply(self : H2Client, id : Int) -> CallReply {
  match self.calls.get(id) {
    Some(c) => {
      let grpc = match c.grpc_status {
        Some(v) => v
        None => -1
      }
      {
        status: c.status,
        grpc_status: grpc,
        headers: c.resp_headers,
        messages: c.messages,
        trailers: c.resp_trailers,
      }
    }
    None =>
      { status: b"", grpc_status: -1, headers: [], messages: [], trailers: [] }
  }
}

///|
/// Pull every complete length-prefixed reply message now buffered on a call into
/// `messages`, advancing the read cursor; partial trailing bytes stay buffered.
fn drain_call_messages(c : ClientCall) -> Unit {
  let all = c.data.to_bytes()
  let n = all.length()
  let mut off = c.data_off
  while n - off >= 5 {
    let compressed = all[off].to_int() != 0
    let len = (all[off + 1].to_int() << 24) |
      (all[off + 2].to_int() << 16) |
      (all[off + 3].to_int() << 8) |
      all[off + 4].to_int()
    // A high-bit-set prefix decodes negative; reject that or a length past the cap
    // (surfacing RESOURCE_EXHAUSTED) instead of slicing the buffer out of bounds.
    if len < 0 || len > max_message_size {
      if c.grpc_status is None {
        c.grpc_status = Some(Status::code(ResourceExhausted))
      }
      break
    }
    if n - off < 5 + len {
      break
    }
    let body = all[off + 5:off + 5 + len].to_owned()
    // A reply message with the compression flag set is inflated when the response
    // encoding is gzip; a decode failure (never expected from a well-behaved server)
    // falls back to the raw body rather than dropping the message.
    if compressed && c.resp_encoding == b"gzip" {
      c.messages.push(gunzip(body) catch { _ => body })
    } else {
      c.messages.push(body)
    }
    off = off + 5 + len
  }
  c.data_off = off
}

///|
/// Parse ASCII decimal `Bytes` (a `grpc-status` value) to an `Int`; non-digits stop
/// the scan.
fn ascii_bytes_to_int(b : Bytes) -> Int {
  let mut n = 0
  for i = 0; i < b.length(); i = i + 1 {
    let c = b[i].to_int()
    if c < 0x30 || c > 0x39 {
      break
    }
    n = n * 10 + (c - 0x30)
  }
  n
}

///|
/// Encode whole `millis` as a `grpc-timeout` header value (RFC gRPC HTTP/2 mapping):
/// the `m` (millisecond) unit when the count fits the 8-digit field, else seconds
/// with the `S` unit.
pub fn encode_grpc_timeout(millis : Int) -> Bytes {
  if millis <= 99999999 {
    cat(int_to_ascii_bytes(millis), b"m")
  } else {
    cat(int_to_ascii_bytes(millis / 1000), b"S")
  }
}
