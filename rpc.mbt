///|
/// The default cap on a single received gRPC message (4 MiB, matching gRPC's default
/// `MaxRecvMsgSize`). A length prefix above this — including one whose 4 bytes decode
/// to a negative `Int` because the high bit is set — is rejected rather than trusted,
/// so a hostile prefix can neither slice out of bounds nor pin unbounded buffer.
pub let max_message_size : Int = 4 * 1024 * 1024

///|
/// The cap on one accumulated header block (HEADERS plus its CONTINUATION frames).
/// Without it a peer could stream endless non-final CONTINUATION frames and grow the
/// buffer without bound (a CONTINUATION flood); 128 KiB is far above any real gRPC
/// request's headers.
pub let max_header_list_size : Int = 128 * 1024

///|
/// Encode a payload as a gRPC *Length-Prefixed-Message*: a 1-byte compression
/// flag, a 4-byte big-endian length, then the payload. This is the framing every
/// gRPC transport shares (gRPC-Web over HTTP/1.1 and real gRPC over HTTP/2 alike).
pub fn encode_message(payload : Bytes, compressed? : Bool = false) -> Bytes {
  let n = payload.length()
  let buf = Buffer()
  buf.write_byte((if compressed { 1 } else { 0 }).to_byte())
  buf.write_byte((n >> 24).to_byte())
  buf.write_byte((n >> 16).to_byte())
  buf.write_byte((n >> 8).to_byte())
  buf.write_byte(n.to_byte())
  buf.write_bytes(payload)
  buf.to_bytes()
}

///|
/// Decode one gRPC length-prefixed message from the front of `data`, returning
/// `(compressed, payload)`, or `None` if fewer than a full frame is present.
pub fn decode_message(data : Bytes) -> (Bool, Bytes)? {
  if data.length() < 5 {
    return None
  }
  let flag = data[0].to_int()
  let len = (data[1].to_int() << 24) |
    (data[2].to_int() << 16) |
    (data[3].to_int() << 8) |
    data[4].to_int()
  // A high-bit-set prefix decodes to a negative `Int`; compare without recomputing
  // `5 + len` (which would wrap) so a hostile length can't slice past the buffer.
  if len < 0 || len > data.length() - 5 {
    return None
  }
  Some((flag != 0, data[5:5 + len].to_owned()))
}

///|
/// The 17 canonical gRPC status codes (`grpc-status`).
pub(all) enum Status {
  Ok
  Cancelled
  Unknown
  InvalidArgument
  DeadlineExceeded
  NotFound
  AlreadyExists
  PermissionDenied
  ResourceExhausted
  FailedPrecondition
  Aborted
  OutOfRange
  Unimplemented
  Internal
  Unavailable
  DataLoss
  Unauthenticated
} derive(Eq)

///|
/// The numeric `grpc-status` code.
pub fn Status::code(self : Status) -> Int {
  match self {
    Ok => 0
    Cancelled => 1
    Unknown => 2
    InvalidArgument => 3
    DeadlineExceeded => 4
    NotFound => 5
    AlreadyExists => 6
    PermissionDenied => 7
    ResourceExhausted => 8
    FailedPrecondition => 9
    Aborted => 10
    OutOfRange => 11
    Unimplemented => 12
    Internal => 13
    Unavailable => 14
    DataLoss => 15
    Unauthenticated => 16
  }
}

///|
/// The canonical uppercase status name.
pub fn Status::name(self : Status) -> String {
  match self {
    Ok => "OK"
    Cancelled => "CANCELLED"
    Unknown => "UNKNOWN"
    InvalidArgument => "INVALID_ARGUMENT"
    DeadlineExceeded => "DEADLINE_EXCEEDED"
    NotFound => "NOT_FOUND"
    AlreadyExists => "ALREADY_EXISTS"
    PermissionDenied => "PERMISSION_DENIED"
    ResourceExhausted => "RESOURCE_EXHAUSTED"
    FailedPrecondition => "FAILED_PRECONDITION"
    Aborted => "ABORTED"
    OutOfRange => "OUT_OF_RANGE"
    Unimplemented => "UNIMPLEMENTED"
    Internal => "INTERNAL"
    Unavailable => "UNAVAILABLE"
    DataLoss => "DATA_LOSS"
    Unauthenticated => "UNAUTHENTICATED"
  }
}

///|
/// A fully-qualified RPC method: `package.Service` and the method name.
pub(all) struct Method {
  service : String
  name : String
}

///|
/// The gRPC HTTP/2 `:path`, i.e. `/package.Service/Method`.
pub fn Method::path(self : Method) -> String {
  "/" + self.service + "/" + self.name
}
