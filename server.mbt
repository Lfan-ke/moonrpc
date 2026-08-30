// The pure server-side HTTP/2 protocol engine that turns a stream of decoded
// frames into a stream of frames to send back — the transport-independent core
// of a gRPC server. It drives the §5.1 stream state machine, the stateful HPACK
// encoder/decoder, and connection- and stream-level flow control (RFC 7540 §6.9),
// routing a completed `application/grpc` request to one of the four call kinds
// (unary / server- / client- / bidi-streaming) and framing each produced message
// as its own length-prefixed DATA, closed by `grpc-status` trailers. No sockets
// and no async here: `feed` is a total function over frames, so the whole engine
// runs in-memory on every backend; the socket driver in `net/` only pumps bytes.

///|
/// The HTTP/2 default flow-control window and initial `SETTINGS_INITIAL_WINDOW_SIZE`
/// (RFC 7540 §6.9.2): 65 535 octets.
pub let default_window_size : Int = 65535

///|
/// The HTTP/2 default (and minimum) `SETTINGS_MAX_FRAME_SIZE` (RFC 7540 §6.5.2).
pub let default_max_frame_size : Int = 16384

///|
/// The `SETTINGS_MAX_CONCURRENT_STREAMS` the server advertises and enforces unless a
/// caller says otherwise: 100, the value gRPC and nghttp2 both settle on. RFC 9113
/// §5.1.2 leaves the setting unset by default, which means unbounded — and every live
/// stream holds a request buffer, so a peer that opens streams and never finishes them
/// pins memory for as long as the connection lasts.
pub let default_max_streams : Int = 100

///|
/// Add a flow-control increment to a window, saturating at the ±2^31-1 range RFC 7540
/// §6.9.1 allows instead of wrapping. A wrap to a large negative window would stall
/// every send on that stream/connection (a WINDOW_UPDATE-driven deadlock), and a wrap
/// to a bogus positive one would over-send; the sum is taken in 64 bits and clamped.
fn add_window(cur : Int, inc : Int) -> Int {
  let sum = cur.to_int64() + inc.to_int64()
  if sum > 0x7FFFFFFFL {
    0x7FFFFFFF
  } else if sum < -0x7FFFFFFFL {
    -0x7FFFFFFF
  } else {
    sum.to_int()
  }
}

///|
/// Raise once an accumulated field block has grown past `max_header_list_size` — the
/// backstop against a CONTINUATION flood pinning unbounded memory. RFC 9113 §4.2 makes
/// a block over a limit the endpoint will hold a FRAME_SIZE_ERROR, and it is fatal to
/// the connection: the block never reaches the decoder, so the HPACK context stops
/// matching the peer's.
fn guard_header_size(buf : Buffer) -> Unit raise H2Fault {
  if buf.length() > max_header_list_size {
    raise ConnFault(
      code=error_frame_size_error,
      why="field block over " + max_header_list_size.to_string() + " octets",
    )
  }
}

///|
/// One server-side stream: its lifecycle state, the accumulating request header
/// block and DATA (with a cursor over the length-prefixed messages already pulled
/// out of it), the per-stream flow-control windows, and the response side — the
/// bytes still to send, whether the initial HEADERS and the trailers have gone
/// out, and any live bidi call state.
pub(all) struct SrvStream {
  id : Int
  mut state : StreamState
  header_block : Buffer
  data : Buffer
  mut req_off : Int
  req_msgs : Array[Bytes]
  mut bidi_processed : Int
  mut headers_complete : Bool
  // A field block is open: the last HEADERS/CONTINUATION left END_HEADERS clear, so a
  // CONTINUATION is expected next and any other frame — or a CONTINUATION without this
  // set — is a §6.10 violation.
  mut in_headers : Bool
  mut end_stream_recv : Bool
  mut path : String
  mut ctx : RpcContext
  // When the call's `grpc-timeout` runs out, on the clock the driver installed. `None`
  // when the request carried no timeout.
  mut deadline_at : Int64?
  mut recv_window : Int
  mut send_window : Int
  mut out : Bytes
  mut out_off : Int
  mut started : Bool
  mut started_response : Bool
  mut response_ended : Bool
  mut finalized : Bool
  mut trailers_only : Bool
  // The `:status` of the response. A gRPC call is always 200 and carries its outcome in
  // `grpc-status`; a request that is not a gRPC call at all is answered with a real HTTP
  // status instead, so a plain HTTP client cannot read the refusal as a success.
  mut http_status : Int
  mut status_code : Int
  mut status_message : String
  // The call's `grpc-encoding` (empty when absent, i.e. identity). A compressed
  // message under `gzip` is inflated in-place; under any other encoding the call is
  // answered with UNIMPLEMENTED rather than misreading the compressed bytes.
  mut req_encoding : Bytes
  // A compressed request arrived under an encoding we cannot decode (or a gzip member
  // that failed to inflate). Answered with UNIMPLEMENTED, since we could not recover
  // the message the handler was meant to see.
  mut compressed_unsupported : Bool
  // A request message declared a length past `max_message_size` (or a negative length
  // from a high-bit-set prefix). Answered with RESOURCE_EXHAUSTED instead of trusting
  // the length to slice the buffer.
  mut oversize : Bool
  mut headers_sent : Bool
  mut trailers_sent : Bool
  mut bidi : BidiHandler?
}

///|
fn SrvStream::new(id : Int, send_window : Int) -> SrvStream {
  {
    id,
    state: Idle,
    header_block: Buffer(),
    data: Buffer(),
    req_off: 0,
    req_msgs: [],
    bidi_processed: 0,
    headers_complete: false,
    in_headers: false,
    end_stream_recv: false,
    path: "",
    ctx: RpcContext::empty(),
    deadline_at: None,
    recv_window: default_window_size,
    send_window,
    out: b"",
    out_off: 0,
    started: false,
    started_response: false,
    response_ended: false,
    finalized: false,
    trailers_only: false,
    http_status: 200,
    status_code: 0,
    status_message: "",
    req_encoding: b"",
    compressed_unsupported: false,
    oversize: false,
    headers_sent: false,
    trailers_sent: false,
    bidi: None,
  }
}

///|
/// End a stream a RST_STREAM has crossed in either direction. §5.1 closes it the
/// moment the reset is sent or received and §5.4.2 forbids any further frame on it,
/// so leaving the state alone would let `advance` go on serving a stream the peer has
/// been told is dead. The map entry survives — callers still ask a finished stream for
/// its state — but its buffers do not, on the same reasoning as the trailers path.
fn SrvStream::retire(self : SrvStream) -> Unit {
  self.state = Closed
  self.in_headers = false
  self.header_block.reset()
  self.data.reset()
  self.req_msgs.clear()
  self.out = b""
  self.out_off = 0
}

///|
/// The server side of one HTTP/2 connection: the HPACK codec pair, the live
/// streams, the connection-level flow-control windows, and the peer's settings
/// that bound what we may send. Persistent across the whole connection because
/// HPACK and flow control are stateful.
pub struct H2Server {
  handlers : Map[String, Handler]
  encoder : HpackEncoder
  decoder : HpackDecoder
  streams : Map[Int, SrvStream]
  unary_interceptors : Array[UnaryInterceptor]
  stream_interceptors : Array[StreamInterceptor]
  mut conn_recv_window : Int
  mut conn_send_window : Int
  mut remote_initial_window : Int
  mut remote_max_frame : Int
  // How many streams this connection runs at once: what `preface` advertises as
  // SETTINGS_MAX_CONCURRENT_STREAMS and what a new HEADERS is measured against.
  mut max_streams : Int
  // What the engine reads to tell whether a deadline has passed. Pure by default —
  // there is no clock a wasm build can read — so a driver installs a real one.
  mut clock : () -> Int64
  // The stream whose field block is still open, i.e. the one a CONTINUATION must
  // continue. §6.10 makes a field block a contiguous run of frames, so while this is
  // set nothing else may arrive on any stream.
  mut continuing : Int?
  mut goaway_received : Bool
  // The highest client stream id opened so far. A new HEADERS must use an odd id
  // strictly greater than this (RFC 7540 §5.1.1); anything else is a PROTOCOL_ERROR.
  mut last_client_stream : Int
  // A connection error has been reported. The GOAWAY that says so was returned by the
  // `feed` that found it; nothing after it is processed (RFC 9113 §5.4.1).
  mut closing : Bool
  // The last stream id our own GOAWAY announced, once one has gone out. A HEADERS
  // opening a stream above it is declined with REFUSED_STREAM rather than served,
  // because we already told the peer we would not get to it (RFC 9113 §6.8).
  mut drained : Int?
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
    unary_interceptors: [],
    stream_interceptors: [],
    conn_recv_window: default_window_size,
    conn_send_window: default_window_size,
    remote_initial_window: default_window_size,
    remote_max_frame: default_max_frame_size,
    max_streams: default_max_streams,
    clock: () => 0L,
    continuing: None,
    goaway_received: false,
    last_client_stream: 0,
    closing: false,
    drained: None,
  }
}

///|
/// Register a unary handler for a fully-qualified gRPC path (`/pkg.Service/Method`):
/// one request message in, one reply message out. An unmatched path gets a
/// trailers-only `grpc-status: 12` (UNIMPLEMENTED) response.
pub fn H2Server::register(
  self : H2Server,
  path : String,
  handler : (Bytes) -> Bytes,
) -> Unit {
  self.handlers[path] = Unary((_ctx, req) => handler(req))
}

///|
/// Drop everything the engine still holds for a finished stream. The map is only ever
/// inserted into otherwise, so a long-lived connection would keep one full request and
/// response per RPC it has ever served. An unknown id is a no-op.
pub fn H2Server::release(self : H2Server, id : Int) -> Unit {
  self.streams.remove(id)
}

///|
/// Register a handler of any of the four gRPC call kinds.
pub fn H2Server::register_handler(
  self : H2Server,
  path : String,
  handler : Handler,
) -> Unit {
  self.handlers[path] = handler
}

///|
/// Register a unary handler that also sees the call context (metadata, deadline,
/// and the response metadata slots).
pub fn H2Server::register_unary(
  self : H2Server,
  path : String,
  handler : (RpcContext, Bytes) -> Bytes,
) -> Unit {
  self.handlers[path] = Unary(handler)
}

///|
/// Register a server-streaming handler: one request message, an ordered sequence
/// of reply messages, each framed as its own gRPC message.
pub fn H2Server::register_server_streaming(
  self : H2Server,
  path : String,
  handler : (RpcContext, Bytes) -> Array[Bytes],
) -> Unit {
  self.handlers[path] = ServerStreaming(handler)
}

///|
/// Register a client-streaming handler: every request message the client sends is
/// collected, and after the client half-closes the handler returns one reply.
pub fn H2Server::register_client_streaming(
  self : H2Server,
  path : String,
  handler : (RpcContext, Array[Bytes]) -> Bytes,
) -> Unit {
  self.handlers[path] = ClientStreaming(handler)
}

///|
/// Register a bidirectional-streaming handler. The factory runs once per call and
/// returns a `BidiHandler` whose `on_message` fires per request message (its
/// replies stream out immediately) and whose `on_end` fires at half-close.
pub fn H2Server::register_bidi(
  self : H2Server,
  path : String,
  factory : (RpcContext) -> BidiHandler,
) -> Unit {
  self.handlers[path] = Bidi(factory)
}

///|
/// Whether the peer has sent GOAWAY; the driver stops accepting new streams once
/// this is set.
pub fn H2Server::goaway_received(self : H2Server) -> Bool {
  self.goaway_received
}

///|
/// The server's opening frames (RFC 7540 §3.5): a SETTINGS frame disabling server push
/// and naming how many streams this connection will run at once. Sent immediately after
/// the client connection preface is validated, before any request frame is read.
pub fn H2Server::preface(self : H2Server) -> Array[Frame] {
  [
    Settings(
      params=[
        (settings_enable_push, 0),
        (settings_max_concurrent_streams, self.max_streams),
      ],
      ack=false,
    ),
  ]
}

///|
/// Set how many streams this connection may run at once. The value is both advertised
/// as `SETTINGS_MAX_CONCURRENT_STREAMS` and enforced on arrival, so set it before
/// `preface` and the peer is told exactly what it will be held to.
pub fn H2Server::set_max_streams(self : H2Server, n : Int) -> Unit {
  self.max_streams = n
}

///|
/// Install the clock the engine measures `grpc-timeout` deadlines against: a function
/// returning the current time in milliseconds on any monotonic scale. The engine is a
/// pure state machine with no clock of its own, so until one is installed every
/// deadline is inert; the native driver installs `@async.now`.
pub fn H2Server::set_clock(self : H2Server, clock : () -> Int64) -> Unit {
  self.clock = clock
}

///|
/// Frames for a graceful shutdown: a GOAWAY announcing `last_stream_id` — the highest
/// client stream the server will still process (RFC 7540 §6.8) — with NO_ERROR, so
/// the peer opens no new streams while in-flight ones finish. The driver writes these
/// before closing the connection. A stream opened above `last_stream_id` afterwards is
/// declined with RST_STREAM(REFUSED_STREAM), which tells the client it was never
/// processed and can be re-issued elsewhere.
pub fn H2Server::goaway(self : H2Server, last_stream_id : Int) -> Array[Frame] {
  self.drained = Some(last_stream_id)
  [GoAway(last_stream_id~, error_code=error_no_error, debug=b"")]
}

///|
/// Whether a connection error has been reported. The GOAWAY carrying it came back from
/// the `feed` that found it, so the driver writes that and then closes; the engine
/// processes nothing more (RFC 9113 §5.4.1).
pub fn H2Server::closing(self : H2Server) -> Bool {
  self.closing
}

// -- small octet helpers ----------------------------------------------------

///|
/// Latin-1 `Bytes` → `String`, one code unit per octet. HTTP/2 header names/values
/// are byte strings; `:path` and `:method` are ASCII, so this is exact for them.
fn bytes_to_ascii(b : Bytes) -> String {
  let sb = StringBuilder()
  for i = 0; i < b.length(); i = i + 1 {
    sb.write_char(b[i].to_int().unsafe_to_char())
  }
  sb.to_string()
}

///|
/// Frame an HPACK header block as HEADERS plus, when it exceeds the peer's
/// `SETTINGS_MAX_FRAME_SIZE`, one or more CONTINUATION frames (RFC 7540 §4.3 / §6.10):
/// the block is split into `max_frame`-sized fragments, the first a HEADERS with
/// `end_headers=false`, the rest CONTINUATION, the last carrying `end_headers=true`.
fn emit_header_frames(
  stream_id : Int,
  block : Bytes,
  end_stream : Bool,
  max_frame_in : Int,
) -> Array[Frame] {
  // Never fragment at zero: a bad peer MAX_FRAME_SIZE is clamped on apply, but floor
  // here too so the split loop always advances rather than looping on empty chunks.
  let max_frame = if max_frame_in < 1 {
    default_max_frame_size
  } else {
    max_frame_in
  }
  let frames : Array[Frame] = []
  if block.length() <= max_frame {
    frames.push(
      Headers(
        stream_id~,
        fragment=block,
        end_stream~,
        end_headers=true,
        priority=None,
        padding=0,
      ),
    )
    return frames
  }
  frames.push(
    Headers(
      stream_id~,
      fragment=block[0:max_frame].to_owned(),
      end_stream~,
      end_headers=false,
      priority=None,
      padding=0,
    ),
  )
  let mut off = max_frame
  while off < block.length() {
    let n = if block.length() - off < max_frame {
      block.length() - off
    } else {
      max_frame
    }
    let chunk = block[off:off + n].to_owned()
    off = off + n
    frames.push(
      Continuation(
        stream_id~,
        fragment=chunk,
        end_headers=off >= block.length(),
      ),
    )
  }
  frames
}

///|
/// Serialize a `google.rpc.Status` (`code` = 1, `message` = 2, repeated `details` =
/// 3, each an already-serialized `google.protobuf.Any`) to its protobuf wire bytes —
/// the payload of the `grpc-status-details-bin` trailer that carries rich errors.
fn encode_status_details(
  code : Int,
  message : String,
  details : Array[Bytes],
) -> Bytes {
  let w = PbWriter::new()
  w.int32(1, code)
  w.string_(2, message)
  for d in details {
    w.message_(3, d)
  }
  w.to_bytes()
}

///|
/// Percent-encode a `grpc-message` value (gRPC spec §"Responses"): a UTF-8 string
/// whose bytes outside printable ASCII `%x20-%x7E`, and `%` itself, are written as
/// `%XX` with uppercase hex. Lets a status message carry spaces, punctuation, and
/// non-ASCII text on the header line safely.
fn percent_encode(msg : String) -> Bytes {
  let src = @utf8.encode(msg)
  let buf = Buffer()
  for i = 0; i < src.length(); i = i + 1 {
    let c = src[i].to_int()
    if c >= 0x20 && c <= 0x7E && c != 0x25 {
      buf.write_byte(src[i])
    } else {
      let hex = "0123456789ABCDEF"
      buf.write_byte(b'%')
      buf.write_byte(hex[(c >> 4) & 0xf].to_int().to_byte())
      buf.write_byte(hex[c & 0xf].to_int().to_byte())
    }
  }
  buf.to_bytes()
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
/// Concatenate two byte strings.
fn cat(a : Bytes, b : Bytes) -> Bytes {
  let buf = Buffer()
  buf.write_bytes(a)
  buf.write_bytes(b)
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
/// Whether `b` begins with `prefix`.
fn starts_with(b : Bytes, prefix : Bytes) -> Bool {
  if b.length() < prefix.length() {
    return false
  }
  for i = 0; i < prefix.length(); i = i + 1 {
    if b[i] != prefix[i] {
      return false
    }
  }
  true
}

///|
/// How to refuse a request that is not a gRPC call — the HTTP `:status` to answer with,
/// the `grpc-status` to pair with it, and why — or `None` when it is one. gRPC
/// PROTOCOL-HTTP2 fixes the request line: `:method` is POST, `te` carries `trailers`,
/// and `content-type` begins `application/grpc`. The content-type check is the one the
/// spec gives a status of its own (415), and it is the load-bearing one: without it a
/// browser form post or a health-checking GET is dispatched as a call and answered
/// 200 + `grpc-status`, which every non-gRPC client reads as success.
fn request_reject(headers : Array[Header]) -> (Int, Int, String)? {
  match header_value(headers, b"content-type") {
    Some(ct) if starts_with(ct, b"application/grpc") => ()
    _ =>
      return Some(
        (
          415,
          Status::code(InvalidArgument),
          "content-type is not application/grpc",
        ),
      )
  }
  match header_value(headers, b":method") {
    Some(m) if m == b"POST" => ()
    _ => return Some((405, Status::code(Internal), ":method is not POST"))
  }
  match header_value(headers, b"te") {
    Some(te) if te == b"trailers" => ()
    _ => return Some((400, Status::code(Internal), "te: trailers is missing"))
  }
  None
}

///|
/// Whether a request header is a pseudo-header (`:`-prefixed) or a reserved gRPC/
/// HTTP header, and so is *not* surfaced as call metadata (RFC 7540 §8.1.2.1 plus
/// the gRPC HTTP/2 mapping).
fn is_reserved_header(name : Bytes) -> Bool {
  (name.length() > 0 && name[0] == b':') ||
  name == b"content-type" ||
  name == b"te" ||
  name == b"grpc-timeout" ||
  name == b"grpc-encoding" ||
  name == b"grpc-accept-encoding" ||
  name == b"user-agent"
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

///|
/// Pull every complete length-prefixed message now buffered on this stream into
/// `req_msgs`, advancing the read cursor. Partial trailing bytes stay buffered for
/// a later DATA frame to complete.
fn drain_messages(s : SrvStream) -> Unit {
  let all = s.data.to_bytes()
  let n = all.length()
  let mut off = s.req_off
  while n - off >= 5 {
    let compressed = all[off].to_int() != 0
    let len = (all[off + 1].to_int() << 24) |
      (all[off + 2].to_int() << 16) |
      (all[off + 3].to_int() << 8) |
      all[off + 4].to_int()
    // A high-bit-set prefix decodes negative; either that or a length past the cap is
    // rejected before it can drive an out-of-bounds slice or pin unbounded buffer.
    if len < 0 || len > max_message_size {
      s.oversize = true
      break
    }
    if n - off < 5 + len {
      break
    }
    // The message-encoding flag (byte 0): a set flag means the body is compressed
    // with the call's grpc-encoding. We decode `gzip` in place; any other encoding
    // (or a gzip member that fails to inflate) is flagged rather than handed to the
    // handler as if the compressed bytes were the message.
    if compressed {
      let body = all[off + 5:off + 5 + len].to_owned()
      if s.req_encoding == b"gzip" {
        let plain = Some(gunzip(body)) catch { _ => None }
        match plain {
          Some(m) => s.req_msgs.push(m)
          None => s.compressed_unsupported = true
        }
      } else {
        s.compressed_unsupported = true
      }
    } else {
      s.req_msgs.push(all[off + 5:off + 5 + len].to_owned())
    }
    off = off + 5 + len
  }
  s.req_off = off
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
    // Ignore an out-of-range window (RFC 7540 §6.5.2 caps it at 2^31-1; a value with
    // the high bit set decodes negative here) rather than corrupting send-window math.
    if value < 0 {
      return
    }
    let delta = value - self.remote_initial_window
    self.remote_initial_window = value
    for _, s in self.streams {
      s.send_window = add_window(s.send_window, delta)
    }
  } else if id == settings_max_frame_size {
    // Clamp to the RFC 7540 §6.5.2 range [2^14, 2^24-1]; a smaller value (e.g. 0)
    // would stall header emission in an endless empty-CONTINUATION loop.
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
/// Emit a connection-level WINDOW_UPDATE when the connection receive window has fallen
/// below half the default, replenishing it to the default (a simple, correct
/// auto-tuning policy; RFC 7540 §6.9).
fn H2Server::credit_conn(self : H2Server) -> Array[Frame] {
  if self.conn_recv_window < default_window_size / 2 {
    let inc = default_window_size - self.conn_recv_window
    self.conn_recv_window = self.conn_recv_window + inc
    return [WindowUpdate(stream_id=0, increment=inc)]
  }
  []
}

///|
/// Emit connection- and stream-level WINDOW_UPDATE frames when a receive window
/// has fallen below half the default, replenishing it to the default.
fn H2Server::replenish(self : H2Server, s : SrvStream) -> Array[Frame] {
  let frames = self.credit_conn()
  let threshold = default_window_size / 2
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
/// and — as the request stream progresses — the framed gRPC response messages and
/// trailers). A protocol fault comes back as the frames that report it rather than
/// as a raise the driver can only discard: RST_STREAM for a stream error, and for a
/// connection error a GOAWAY, after which `closing` is set and every later frame is
/// ignored (RFC 9113 §5.4).
pub fn H2Server::feed(self : H2Server, frame : Frame) -> Array[Frame] {
  if self.closing {
    return []
  }
  self.step(frame) catch {
    f => self.report(f)
  }
}

///|
/// Turn a fault into the frames that report it and leave the engine in the state that
/// report implies. A stream error also hands back the connection-window credit the dead
/// stream's octets were counted against: the peer spent them on a window it shares with
/// every other stream, and a connection that never returns them stalls a window's worth
/// at a time.
fn H2Server::report(self : H2Server, f : H2Fault) -> Array[Frame] {
  let frames : Array[Frame] = []
  match f {
    ConnFault(..) => self.closing = true
    // The RST_STREAM about to go out closes the stream, so the response the handler
    // may already have queued on it must not follow it onto the wire.
    StreamFault(id~, ..) => {
      if self.streams.get(id) is Some(s) {
        s.retire()
      }
      for g in self.credit_conn() {
        frames.push(g)
      }
    }
  }
  for g in fault_frames(f, self.last_client_stream) {
    frames.push(g)
  }
  frames
}

///|
/// Expire every call whose deadline has passed, returning the frames that end them.
/// The driver calls this on whatever schedule it keeps time on; the engine also checks
/// the deadline whenever a stream advances, so a call that is making progress never
/// needs a tick to be cut off.
pub fn H2Server::tick(self : H2Server) -> Array[Frame] {
  if self.closing {
    return []
  }
  self.sweep() catch {
    f => self.report(f)
  }
}

///|
/// Whether this stream's deadline has passed on the installed clock.
fn H2Server::past_deadline(self : H2Server, s : SrvStream) -> Bool {
  match s.deadline_at {
    Some(t) => (self.clock)() >= t
    None => false
  }
}

///|
/// End a call whose deadline has passed with DEADLINE_EXCEEDED — the status gRPC's
/// table has both ends generate. Anything the handler produced after the deadline goes
/// no further: the client has stopped waiting for it. A client still sending gets a
/// RST_STREAM too, since trailers alone leave the stream half-open and the peer free to
/// go on filling buffers for a call that is over.
fn H2Server::expire(
  self : H2Server,
  s : SrvStream,
) -> Array[Frame] raise H2Fault {
  s.out = b""
  s.out_off = 0
  s.status_code = Status::code(DeadlineExceeded)
  s.status_message = "deadline exceeded"
  if !s.headers_sent {
    s.trailers_only = true
  }
  s.started = true
  s.finalized = true
  s.response_ended = true
  let frames = self.produce(s)
  if !s.end_stream_recv {
    frames.push(RstStream(stream_id=s.id, error_code=error_no_error))
    s.retire()
  }
  frames
}

///|
/// Close out every stream whose deadline has passed. A stream that has already sent its
/// status is finished and out of the deadline's reach, whatever its state says.
fn H2Server::sweep(self : H2Server) -> Array[Frame] raise H2Fault {
  let frames : Array[Frame] = []
  for _, s in self.streams {
    if !(s.state is Closed) && !s.trailers_sent && self.past_deadline(s) {
      for f in self.expire(s) {
        frames.push(f)
      }
    }
  }
  frames
}

///|
/// Emit what a stream can send right now — or, once its deadline has passed, the frames
/// that end it instead. Every path that pushes a stream forward from outside `advance`
/// goes through here, so a WINDOW_UPDATE cannot nudge a call along after it is over.
fn H2Server::flush(
  self : H2Server,
  s : SrvStream,
) -> Array[Frame] raise H2Fault {
  if !s.trailers_sent && self.past_deadline(s) {
    return self.expire(s)
  }
  self.produce(s)
}

///|
/// Advance a stream by a received event, classifying a refused §5.1 transition.
fn recv_event(s : SrvStream, ev : StreamEvent) -> StreamState raise H2Fault {
  s.state.on_recv(ev) catch {
    InvalidTransition(m) => raise transition_fault(s.id, s.state, m)
  }
}

///|
/// Advance a stream by an event we are sending. A refusal here is our own bug, not the
/// peer's, so it ends the connection with INTERNAL_ERROR rather than blaming a stream.
fn send_event(s : SrvStream, ev : StreamEvent) -> StreamState raise H2Fault {
  s.state.on_send(ev) catch {
    InvalidTransition(m) => raise ConnFault(code=error_internal_error, why=m)
  }
}

///|
/// The frame-processing core. Everything that can go wrong is classified into an
/// `H2Fault`, which `feed` turns into the frames that report it.
fn H2Server::step(self : H2Server, frame : Frame) -> Array[Frame] raise H2Fault {
  // §6.10: a field block is a contiguous run of HEADERS then CONTINUATION. Nothing may
  // be interleaved into it — not a frame on another stream, not one on this one — so
  // while a block is open the only frame that can be processed is its continuation.
  if self.continuing is Some(id) {
    let continues = match frame {
      Continuation(stream_id~, ..) => stream_id == id
      _ => false
    }
    if !continues {
      raise ConnFault(
        code=error_protocol_error,
        why="frame interleaved into an open field block",
      )
    }
  }
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
      // §6.9: a zero increment credits nothing and is an error — on the connection
      // window the whole connection's, on a stream only that stream's.
      if increment == 0 {
        let why = "WINDOW_UPDATE with a zero increment"
        if stream_id == 0 {
          raise ConnFault(code=error_protocol_error, why~)
        }
        raise StreamFault(id=stream_id, code=error_protocol_error, why~)
      }
      if stream_id == 0 {
        // §6.9.1: a window may not exceed 2^31-1, and the endpoint whose window would
        // overflow terminates rather than quietly clamping.
        if window_overflows(self.conn_send_window, increment) {
          raise ConnFault(
            code=error_flow_control_error,
            why="connection window over 2^31-1",
          )
        }
        self.conn_send_window = add_window(self.conn_send_window, increment)
      } else {
        let s = self.stream(stream_id)
        if window_overflows(s.send_window, increment) {
          raise StreamFault(
            id=stream_id,
            code=error_flow_control_error,
            why="stream window over 2^31-1",
          )
        }
        s.send_window = add_window(s.send_window, increment)
      }
      self.pump_all()
    }
    Headers(stream_id~, fragment~, end_stream~, end_headers~, ..) => {
      // §5.1.1: stream 0 is the connection control stream and carries no request.
      if stream_id == 0 {
        raise ConnFault(code=error_protocol_error, why="HEADERS on stream 0")
      }
      // A HEADERS opening a new stream must carry an odd id strictly greater than any
      // client stream seen (RFC 7540 §5.1.1); an even/zero/re-used id would otherwise
      // silently open — or reopen — a stream on the wrong state.
      if self.streams.get(stream_id) is None {
        if !stream_is_client_initiated(stream_id) ||
          stream_id <= self.last_client_stream {
          raise ConnFault(
            code=error_protocol_error,
            why="invalid client stream id " + stream_id.to_string(),
          )
        }
        // §6.8: we already named the last stream we would handle, so this one is
        // declined outright — REFUSED_STREAM says it was never processed.
        match self.drained {
          Some(last) if stream_id > last =>
            raise StreamFault(
              id=stream_id,
              code=error_refused_stream,
              why="server is draining",
            )
          _ => ()
        }
        // §5.1.2: the peer was told how many streams it may run at once, and one over
        // that is refused rather than served — REFUSED_STREAM says it was never
        // processed, so the client is free to re-issue it on another connection.
        if self.active_streams() >= self.max_streams {
          raise StreamFault(
            id=stream_id,
            code=error_refused_stream,
            why="over SETTINGS_MAX_CONCURRENT_STREAMS",
          )
        }
        self.last_client_stream = stream_id
      }
      let s = self.stream(stream_id)
      s.state = recv_event(s, Headers(end_stream~))
      s.header_block.write_bytes(fragment)
      guard_header_size(s.header_block)
      if end_stream {
        s.end_stream_recv = true
      }
      if end_headers {
        self.complete_headers(s)
      } else {
        s.in_headers = true
        self.continuing = Some(stream_id)
      }
      self.advance(s)
    }
    Continuation(stream_id~, fragment~, end_headers~) => {
      if stream_id == 0 {
        raise ConnFault(
          code=error_protocol_error,
          why="CONTINUATION on stream 0",
        )
      }
      // §6.10: a CONTINUATION must follow a HEADERS or CONTINUATION that left
      // END_HEADERS clear. With no block open there is nothing to continue, and the
      // fragment cannot be handed to the decoder either, so the connection ends.
      let s = match self.streams.get(stream_id) {
        Some(s) if s.in_headers => s
        _ =>
          raise ConnFault(
            code=error_protocol_error,
            why="CONTINUATION with no field block in progress",
          )
      }
      s.header_block.write_bytes(fragment)
      guard_header_size(s.header_block)
      if end_headers {
        self.continuing = None
        self.complete_headers(s)
      }
      self.advance(s)
    }
    Data(stream_id~, data~, end_stream~, padding~) => {
      // §6.1: DATA belongs to a stream, never to the connection.
      if stream_id == 0 {
        raise ConnFault(code=error_protocol_error, why="DATA on stream 0")
      }
      // §6.9.1: the octets count against the connection window whatever becomes of the
      // stream, so this is charged before anything can refuse the frame — the peer has
      // already spent them, and a frame we go on to reject is credited back rather than
      // never counted at all. Overrunning the window we advertised is the one thing
      // that cannot be absorbed: it means the peer is not doing flow control.
      let flow_len = data.length() + padding + (if padding > 0 { 1 } else { 0 })
      self.conn_recv_window = self.conn_recv_window - flow_len
      if self.conn_recv_window < 0 {
        raise ConnFault(
          code=error_flow_control_error,
          why="DATA past the connection receive window",
        )
      }
      let s = self.stream(stream_id)
      s.state = recv_event(s, Data(end_stream~))
      s.recv_window = s.recv_window - flow_len
      if s.recv_window < 0 {
        raise StreamFault(
          id=stream_id,
          code=error_flow_control_error,
          why="DATA past the stream receive window",
        )
      }
      s.data.write_bytes(data)
      if end_stream {
        s.end_stream_recv = true
      }
      let frames = self.replenish(s)
      for f in self.advance(s) {
        frames.push(f)
      }
      frames
    }
    RstStream(stream_id~, ..) => {
      // §6.4: RST_STREAM names the stream it resets; stream 0 names none.
      if stream_id == 0 {
        raise ConnFault(code=error_protocol_error, why="RST_STREAM on stream 0")
      }
      match self.streams.get(stream_id) {
        Some(s) => s.retire()
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
/// HPACK-decode this stream's accumulated header block, pull out `:path`, and build
/// the call context (surfaced metadata + parsed `grpc-timeout` deadline).
fn H2Server::complete_headers(
  self : H2Server,
  s : SrvStream,
) -> Unit raise H2Fault {
  // §4.3: a field block that will not decode leaves the HPACK context unusable for
  // every later block on the connection, so it is fatal to the connection, not to
  // this stream.
  let headers = self.decoder.decode(s.header_block.to_bytes()) catch {
    e => raise ConnFault(code=error_compression_error, why=Show::to_string(e))
  }
  s.path = match header_value(headers, b":path") {
    Some(p) => bytes_to_ascii(p)
    None => ""
  }
  s.headers_complete = true
  s.in_headers = false
  // Not a gRPC request at all: it is answered on the spot with an HTTP status, and no
  // handler is ever resolved for it.
  if request_reject(headers) is Some((http, code, why)) {
    s.http_status = http
    s.status_code = code
    s.status_message = why
    s.trailers_only = true
    s.started = true
    s.response_ended = true
    return
  }
  let metadata : Array[Header] = []
  for h in headers {
    if !is_reserved_header(h.name) {
      // A `-bin` metadata value arrives base64-encoded; surface the raw bytes.
      metadata.push({
        name: h.name,
        value: metadata_value_from_wire(h.name, h.value),
      })
    }
  }
  let deadline = match header_value(headers, b"grpc-timeout") {
    Some(v) => parse_grpc_timeout(v)
    None => None
  }
  s.deadline_at = match deadline {
    Some(d) => Some((self.clock)() + d.to_int64())
    None => None
  }
  s.req_encoding = match header_value(headers, b"grpc-encoding") {
    Some(v) => v
    None => b""
  }
  s.ctx = {
    path: s.path,
    metadata,
    deadline_millis: deadline,
    resp_headers: [],
    resp_trailers: [],
    status_fail: None,
    error_details: [],
  }
}

///|
/// Advance a stream after new frames: resolve its handler, route any newly
/// completed request messages to it (streaming replies flow out as they are
/// produced), finalize once the client half-closes, and emit whatever the send
/// windows now permit. Idempotent — safe to call after every frame.
fn H2Server::advance(
  self : H2Server,
  s : SrvStream,
) -> Array[Frame] raise H2Fault {
  if !s.headers_complete {
    return []
  }
  if !s.started {
    s.started = true
    match self.handlers.get(s.path) {
      Some(Bidi(factory)) => s.bidi = Some(factory(s.ctx))
      Some(_) => ()
      None => {
        s.trailers_only = true
        s.status_code = Status::code(Unimplemented)
        s.status_message = "method not found"
        s.response_ended = true
      }
    }
  }
  // The call ran out of time before this frame arrived, so whatever it carries is for a
  // call that is already over.
  if !s.trailers_sent && self.past_deadline(s) {
    return self.expire(s)
  }
  if !s.trailers_only {
    drain_messages(s)
    // A compressed request under an encoding we do not decode ends the call with
    // UNIMPLEMENTED (gRPC's answer for an unsupported message-encoding), rather than
    // running the handler over misread bytes.
    if s.compressed_unsupported && !s.response_ended {
      s.trailers_only = true
      s.status_code = Status::code(Unimplemented)
      s.status_message = "grpc-encoding not supported; server accepts identity, gzip"
      s.response_ended = true
      return self.produce(s)
    }
    // A request message longer than the receive cap ends the call rather than trusting
    // the declared length (gRPC's RESOURCE_EXHAUSTED for an over-size message).
    if s.oversize && !s.response_ended {
      s.trailers_only = true
      s.status_code = Status::code(ResourceExhausted)
      s.status_message = "received message larger than max"
      s.response_ended = true
      return self.produce(s)
    }
    match self.handlers.get(s.path) {
      Some(Bidi(_)) =>
        match s.bidi {
          Some(bh) =>
            while s.bidi_processed < s.req_msgs.length() {
              let m = s.req_msgs[s.bidi_processed]
              s.bidi_processed = s.bidi_processed + 1
              for r in (bh.on_message)(m) {
                self.enqueue(s, r)
              }
            }
          None => ()
        }
      _ => ()
    }
    if s.end_stream_recv && !s.finalized {
      s.finalized = true
      self.finalize(s)
      // The handler had the whole call to itself and may have spent more of it than the
      // deadline allowed; its reply is no more sendable than one produced after a tick.
      if self.past_deadline(s) {
        return self.expire(s)
      }
    }
  }
  self.produce(s)
}

///|
/// Append a reply message to the stream's outbound buffer, length-prefixed, and
/// mark that a normal response has begun (so its initial HEADERS get sent).
fn H2Server::enqueue(self : H2Server, s : SrvStream, msg : Bytes) -> Unit {
  ignore(self)
  s.out = cat(s.out, encode_message(msg))
  s.started_response = true
}

///|
/// Run the resolved handler at half-close, enqueue its reply message(s), and mark
/// the response body complete so the trailers follow once the body drains.
fn H2Server::finalize(self : H2Server, s : SrvStream) -> Unit {
  match self.handlers.get(s.path) {
    Some(Unary(h)) => {
      let msg = if s.req_msgs.length() > 0 { s.req_msgs[0] } else { b"" }
      let effective = compose_unary(self.unary_interceptors, h)
      self.deliver(s, [effective(s.ctx, msg)])
    }
    Some(ServerStreaming(h)) => {
      let msg = if s.req_msgs.length() > 0 { s.req_msgs[0] } else { b"" }
      let effective = compose_stream(self.stream_interceptors, h)
      self.deliver(s, effective(s.ctx, msg))
    }
    Some(ClientStreaming(h)) => self.deliver(s, [h(s.ctx, s.req_msgs)])
    Some(Bidi(_)) =>
      match s.bidi {
        Some(bh) => self.deliver(s, (bh.on_end)())
        None => ()
      }
    None => ()
  }
  s.started_response = true
  s.response_ended = true
}

///|
/// Enqueue a handler's reply messages, unless the handler ended the call with a
/// non-OK status via `ctx.fail` — then set that status and drop the replies (a
/// non-OK gRPC response carries no message body).
fn H2Server::deliver(
  self : H2Server,
  s : SrvStream,
  replies : Array[Bytes],
) -> Unit {
  match s.ctx.status_fail {
    Some((code, message)) => {
      s.status_code = code
      s.status_message = message
      // Nothing has been written yet ⇒ trailers-only; otherwise the status rides
      // the closing trailers after whatever was already sent.
      if !s.started_response {
        s.trailers_only = true
      }
    }
    None =>
      for r in replies {
        self.enqueue(s, r)
      }
  }
}

///|
/// Emit the frames a stream can send right now: the response HEADERS once a normal
/// response has begun, as much buffered DATA as the connection and stream send
/// windows and `remote_max_frame` allow, then the trailer HEADERS carrying
/// `grpc-status` once the body is fully drained. The trailers-only UNIMPLEMENTED
/// path is a single END_STREAM HEADERS with no DATA.
fn H2Server::produce(
  self : H2Server,
  s : SrvStream,
) -> Array[Frame] raise H2Fault {
  let frames : Array[Frame] = []
  if s.trailers_only {
    if !s.trailers_sent {
      let tr : Array[Header] = [
        { name: b":status", value: int_to_ascii_bytes(s.http_status), },
        { name: b"content-type", value: b"application/grpc", },
        { name: b"grpc-accept-encoding", value: b"identity,gzip", },
        { name: b"grpc-status", value: int_to_ascii_bytes(s.status_code), },
        { name: b"grpc-message", value: percent_encode(s.status_message), },
      ]
      if s.status_code != 0 {
        tr.push({
          name: b"grpc-status-details-bin",
          value: base64_encode(
            encode_status_details(
              s.status_code,
              s.status_message,
              s.ctx.error_details,
            ),
          ),
        })
      }
      let block = self.encoder.encode(tr)
      for f in emit_header_frames(s.id, block, true, self.remote_max_frame) {
        frames.push(f)
      }
      s.state = send_event(s, Headers(end_stream=true))
      s.trailers_sent = true
    }
    return frames
  }
  if s.started_response && !s.headers_sent {
    let headers : Array[Header] = [
      { name: b":status", value: b"200", },
      { name: b"content-type", value: b"application/grpc", },
      { name: b"grpc-encoding", value: b"identity", },
      { name: b"grpc-accept-encoding", value: b"identity,gzip", },
    ]
    for h in s.ctx.resp_headers {
      headers.push({
        name: h.name,
        value: metadata_value_to_wire(h.name, h.value),
      })
    }
    let block = self.encoder.encode(headers)
    for f in emit_header_frames(s.id, block, false, self.remote_max_frame) {
      frames.push(f)
    }
    s.state = send_event(s, Headers(end_stream=false))
    s.headers_sent = true
  }
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
  if s.response_ended &&
    s.out_off >= s.out.length() &&
    s.headers_sent &&
    !s.trailers_sent {
    let trailers : Array[Header] = [
      { name: b"grpc-status", value: int_to_ascii_bytes(s.status_code), },
    ]
    // A non-OK status that surfaced after the body already started rides the closing
    // trailers together with its message.
    if s.status_code != 0 && s.status_message != "" {
      trailers.push({
        name: b"grpc-message",
        value: percent_encode(s.status_message),
      })
    }
    if s.status_code != 0 {
      trailers.push({
        name: b"grpc-status-details-bin",
        value: base64_encode(
          encode_status_details(
            s.status_code,
            s.status_message,
            s.ctx.error_details,
          ),
        ),
      })
    }
    for h in s.ctx.resp_trailers {
      trailers.push({
        name: h.name,
        value: metadata_value_to_wire(h.name, h.value),
      })
    }
    let block = self.encoder.encode(trailers)
    // Split like every other header block: a block over the peer's MAX_FRAME_SIZE is
    // a FRAME_SIZE_ERROR at the far end, and rich error details make trailers the
    // block most likely to get there.
    for f in emit_header_frames(s.id, block, true, self.remote_max_frame) {
      frames.push(f)
    }
    s.state = send_event(s, Headers(end_stream=true))
    s.trailers_sent = true
    // Trailers close the stream, so its buffers will not be read again. The entry
    // itself stays — callers still ask a finished stream for its state — but a
    // long-lived connection would otherwise hold one full request and response body
    // per RPC it has ever served.
    s.header_block.reset()
    s.data.reset()
    s.req_msgs.clear()
    s.out = b""
    s.out_off = 0
  }
  frames
}

///|
/// Pump every stream that has started but not finished sending (after a
/// connection-level WINDOW_UPDATE lifts back-pressure on all of them at once).
fn H2Server::pump_all(self : H2Server) -> Array[Frame] raise H2Fault {
  let frames : Array[Frame] = []
  for _, s in self.streams {
    // A stream we reset, or the peer did, sends nothing more — asking it to would be
    // a send from `Closed`, i.e. our own state machine refusing us.
    if s.started && !s.trailers_sent && !(s.state is Closed) {
      for f in self.flush(s) {
        frames.push(f)
      }
    }
  }
  frames
}

///|
/// The highest client stream id this connection has opened, which is what a GOAWAY
/// has to name so the peer knows which of its streams were accepted.
pub fn H2Server::highest_stream(self : H2Server) -> Int {
  self.last_client_stream
}

///|
/// How many streams are still running: the ones RFC 9113 §5.1.2 counts against
/// `SETTINGS_MAX_CONCURRENT_STREAMS`, which is `open` and both `half-closed` states. A
/// graceful shutdown waits for this to reach zero before closing the connection.
/// Finished streams stay in the map with their state but are not counted, and neither
/// is one the engine only knows of because a WINDOW_UPDATE named it — that stream was
/// never opened, so it holds nothing and occupies no slot.
pub fn H2Server::active_streams(self : H2Server) -> Int {
  let mut n = 0
  for _id, s in self.streams {
    match s.state {
      Open | HalfClosedLocal | HalfClosedRemote => n = n + 1
      _ => ()
    }
  }
  n
}

///|
/// The lifecycle state of stream `id`, or `Idle` if the engine has never seen it.
pub fn H2Server::stream_state(self : H2Server, id : Int) -> StreamState {
  match self.streams.get(id) {
    Some(s) => s.state
    None => Idle
  }
}
