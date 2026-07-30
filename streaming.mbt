// The four gRPC call kinds and the per-call context. A handler is registered as
// one of `Unary` / `ServerStreaming` / `ClientStreaming` / `Bidi`; the server
// engine routes a decoded request stream to it and frames whatever messages it
// produces. Everything here is pure and all-backend — the streaming logic runs
// in-memory on every target, and only the socket driver in `net/` is native.

///|
/// The context surfaced to a handler for one RPC call: the invoked `:path`, the
/// request metadata (custom HEADERS, minus the pseudo- and reserved gRPC headers),
/// the deadline parsed from `grpc-timeout` (in milliseconds, `None` when absent),
/// and mutable slots for response initial metadata and trailing metadata the
/// handler can set before it returns.
pub(all) struct RpcContext {
  path : String
  metadata : Array[Header]
  deadline_millis : Int?
  resp_headers : Array[Header]
  resp_trailers : Array[Header]
  // Set by a handler that wants to end the call with a non-OK gRPC status
  // (`(code, message)`); the server emits it as the `grpc-status` / `grpc-message`
  // trailer and discards any reply the handler also returned.
  mut status_fail : (Int, String)?
  // Serialized `google.protobuf.Any` details attached to a non-OK status; the server
  // packs them into a `google.rpc.Status` and emits it as `grpc-status-details-bin`.
  error_details : Array[Bytes]
}

///|
/// A context with no path, metadata, or deadline — the placeholder a stream holds
/// until its request HEADERS are decoded.
pub fn RpcContext::empty() -> RpcContext {
  {
    path: "",
    metadata: [],
    deadline_millis: None,
    resp_headers: [],
    resp_trailers: [],
    status_fail: None,
    error_details: [],
  }
}

///|
/// End the call with a non-OK gRPC status. A handler calls this (e.g.
/// `ctx.fail(Status::code(NotFound), "user 42 does not exist")`) instead of
/// returning a normal reply; the server sends the `grpc-status` / `grpc-message`
/// trailer and drops the reply message. The five common codes have named helpers on
/// [`Status`]; any code is accepted.
pub fn RpcContext::fail(
  self : RpcContext,
  code : Int,
  message : String,
) -> Unit {
  self.status_fail = Some((code, message))
}

///|
/// Attach a rich-error detail (a serialized `google.protobuf.Any`) to a failing
/// call. The server packs the status code, message, and every detail into a
/// `google.rpc.Status` and sends it base64-encoded in the `grpc-status-details-bin`
/// trailer — gRPC's rich-error channel. Combine with `fail`, which sets the base
/// `grpc-status` / `grpc-message`.
pub fn RpcContext::add_error_detail(self : RpcContext, detail : Bytes) -> Unit {
  self.error_details.push(detail)
}

///|
/// The value of request metadata `name`, or `None`. Names are matched byte-for-byte
/// (gRPC lowercases header names on the wire), so binary `-bin` metadata works too.
pub fn RpcContext::metadata_get(self : RpcContext, name : Bytes) -> Bytes? {
  for h in self.metadata {
    if h.name == name {
      return Some(h.value)
    }
  }
  None
}

///|
/// Add an initial-metadata header to the response. Only takes effect if called
/// before the response HEADERS are flushed (any point inside a unary / server- /
/// client-streaming handler, or inside a bidi factory before the first message).
pub fn RpcContext::add_header(
  self : RpcContext,
  name : Bytes,
  value : Bytes,
) -> Unit {
  self.resp_headers.push({ name, value })
}

///|
/// Add a trailing-metadata header, sent in the trailer HEADERS alongside
/// `grpc-status`.
pub fn RpcContext::add_trailer(
  self : RpcContext,
  name : Bytes,
  value : Bytes,
) -> Unit {
  self.resp_trailers.push({ name, value })
}

///|
/// A live bidirectional call. `on_message` is invoked once per fully-received
/// request message and returns the reply messages to send right then; `on_end`
/// runs after the client half-closes and returns the final replies. Both feed the
/// same flow-controlled response stream, so responses interleave with requests.
pub(all) struct BidiHandler {
  on_message : (Bytes) -> Array[Bytes]
  on_end : () -> Array[Bytes]
}

///|
/// A registered method, in one of gRPC's four cardinalities. The reply shape
/// mirrors the request shape: streaming handlers produce an ordered `Array[Bytes]`
/// of messages, each framed as its own length-prefixed gRPC message on the wire.
pub(all) enum Handler {
  Unary((RpcContext, Bytes) -> Bytes)
  ServerStreaming((RpcContext, Bytes) -> Array[Bytes])
  ClientStreaming((RpcContext, Array[Bytes]) -> Bytes)
  Bidi((RpcContext) -> BidiHandler)
}

///|
/// Parse a `grpc-timeout` value (RFC: up to 8 ASCII digits then a unit —
/// `H`/`M`/`S`/`m`/`u`/`n`) to whole milliseconds, flooring sub-millisecond units.
/// `None` for a malformed value.
pub fn parse_grpc_timeout(v : Bytes) -> Int? {
  if v.length() < 2 {
    return None
  }
  let mut n = 0
  for i = 0; i < v.length() - 1; i = i + 1 {
    let c = v[i].to_int()
    if c < 0x30 || c > 0x39 {
      return None
    }
    n = n * 10 + (c - 0x30)
  }
  match v[v.length() - 1] {
    b'H' => Some(n * 3600 * 1000)
    b'M' => Some(n * 60 * 1000)
    b'S' => Some(n * 1000)
    b'm' => Some(n)
    b'u' => Some(n / 1000)
    b'n' => Some(n / 1000000)
    _ => None
  }
}
