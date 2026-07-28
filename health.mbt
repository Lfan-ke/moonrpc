// The standard `grpc.health.v1.Health` service (Check + Watch). The two messages
// are small enough to hand-code against the protobuf wire format (a `string`
// field and an `enum` field) without pulling in a full message runtime: a health
// probe is `HealthCheckRequest { string service = 1 }` and the reply is
// `HealthCheckResponse { ServingStatus status = 1 }`.

///|
/// The `grpc.health.v1.HealthCheckResponse.ServingStatus` enum.
pub(all) enum ServingStatus {
  StatusUnknown
  Serving
  NotServing
  ServiceUnknown
} derive(Eq)

///|
/// The wire value of a serving status (the protobuf enum number).
pub fn ServingStatus::code(self : ServingStatus) -> Int {
  match self {
    StatusUnknown => 0
    Serving => 1
    NotServing => 2
    ServiceUnknown => 3
  }
}

///|
/// The serving status for a protobuf enum number; out-of-range numbers read as
/// `StatusUnknown`.
pub fn ServingStatus::from_code(n : Int) -> ServingStatus {
  match n {
    1 => Serving
    2 => NotServing
    3 => ServiceUnknown
    _ => StatusUnknown
  }
}

// -- minimal protobuf wire helpers ------------------------------------------

///|
/// Append a base-128 varint (protobuf §"Base 128 Varints") to `buf`.
fn pb_write_varint(buf : Buffer, value : Int) -> Unit {
  let mut v = value
  while v >= 0x80 {
    buf.write_byte(((v & 0x7F) | 0x80).to_byte())
    v = v >> 7
  }
  buf.write_byte(v.to_byte())
}

///|
/// Read a base-128 varint at `offset`, returning `(value, bytes_consumed)`.
fn pb_read_varint(data : Bytes, offset : Int) -> (Int, Int) {
  let mut value = 0
  let mut shift = 0
  let mut consumed = 0
  let mut more = true
  while more {
    let octet = data[offset + consumed].to_int()
    consumed = consumed + 1
    value = value | ((octet & 0x7F) << shift)
    shift = shift + 7
    if (octet & 0x80) == 0 {
      more = false
    }
  }
  (value, consumed)
}

///|
/// Encode a `HealthCheckRequest`: field 1 (`service`, a length-delimited string).
/// An empty service name encodes to the empty message, the wire form of the
/// overall-server check.
pub fn encode_health_request(service : Bytes) -> Bytes {
  let buf = Buffer()
  if service.length() > 0 {
    buf.write_byte(b'\x0a') // field 1, wire type 2 (length-delimited)
    pb_write_varint(buf, service.length())
    buf.write_bytes(service)
  }
  buf.to_bytes()
}

///|
/// Decode the `service` field of a `HealthCheckRequest`, or empty bytes when the
/// field is absent (the overall-server check). Unknown fields are skipped.
pub fn decode_health_request(msg : Bytes) -> Bytes {
  let mut off = 0
  let mut service = b""
  while off < msg.length() {
    let (tag, tlen) = pb_read_varint(msg, off)
    off = off + tlen
    let field = tag >> 3
    let wire = tag & 0x7
    if wire == 2 {
      let (len, llen) = pb_read_varint(msg, off)
      off = off + llen
      if field == 1 {
        service = msg[off:off + len].to_owned()
      }
      off = off + len
    } else if wire == 0 {
      let (_, vlen) = pb_read_varint(msg, off)
      off = off + vlen
    } else {
      break
    }
  }
  service
}

///|
/// Encode a `HealthCheckResponse`: field 1 (`status`, a varint enum). The `SERVING`
/// default `0` still encodes to the empty message per protobuf default-omission.
pub fn encode_health_response(status : ServingStatus) -> Bytes {
  let buf = Buffer()
  if status.code() != 0 {
    buf.write_byte(b'\x08') // field 1, wire type 0 (varint)
    pb_write_varint(buf, status.code())
  }
  buf.to_bytes()
}

///|
/// Decode the `status` field of a `HealthCheckResponse`; an absent field reads as
/// the `0` default (`StatusUnknown`).
pub fn decode_health_response(msg : Bytes) -> ServingStatus {
  let mut off = 0
  let mut status = 0
  while off < msg.length() {
    let (tag, tlen) = pb_read_varint(msg, off)
    off = off + tlen
    let field = tag >> 3
    let wire = tag & 0x7
    if wire == 0 {
      let (v, vlen) = pb_read_varint(msg, off)
      off = off + vlen
      if field == 1 {
        status = v
      }
    } else if wire == 2 {
      let (len, llen) = pb_read_varint(msg, off)
      off = off + llen + len
    } else {
      break
    }
  }
  ServingStatus::from_code(status)
}

// -- the service ------------------------------------------------------------

///|
/// The gRPC HTTP/2 path of the `Check` method.
pub let health_check_path : String = "/grpc.health.v1.Health/Check"

///|
/// The gRPC HTTP/2 path of the `Watch` method.
pub let health_watch_path : String = "/grpc.health.v1.Health/Watch"

///|
/// A `grpc.health.v1.Health` service backed by a per-service status table. The
/// empty key `""` is the overall-server status; a fresh service reports the whole
/// server `SERVING`.
pub struct HealthService {
  statuses : Map[String, ServingStatus]
}

///|
/// A health service reporting the overall server as `SERVING`.
pub fn HealthService::new() -> HealthService {
  { statuses: Map([("", Serving)]) }
}

///|
/// Set the serving status of a named service (or the overall server with `""`).
pub fn HealthService::set_status(
  self : HealthService,
  service : String,
  status : ServingStatus,
) -> Unit {
  self.statuses[service] = status
}

///|
/// The serving status of a named service: its set status, or `ServiceUnknown` when
/// the service was never registered.
pub fn HealthService::check(
  self : HealthService,
  service : String,
) -> ServingStatus {
  match self.statuses.get(service) {
    Some(s) => s
    None => ServiceUnknown
  }
}

///|
/// The `(path, handler)` pairs implementing the service: `Check` as a unary method
/// and `Watch` as a server-streaming method that emits the current status. A live
/// `Watch` that also pushes on every later change needs a streaming source the
/// pure engine's eager `ServerStreaming` shape does not model, so this emits the
/// status at subscribe time — the first message a real `Watch` always sends.
pub fn HealthService::handlers(
  self : HealthService,
) -> Array[(String, Handler)] {
  let check : Handler = Unary((_ctx, req) => {
    let service = bytes_to_ascii(decode_health_request(req))
    encode_health_response(self.check(service))
  })
  let watch : Handler = ServerStreaming((_ctx, req) => {
    let service = bytes_to_ascii(decode_health_request(req))
    [encode_health_response(self.check(service))]
  })
  [(health_check_path, check), (health_watch_path, watch)]
}

///|
/// Register `Check` and `Watch` on a pure server engine.
pub fn HealthService::install(self : HealthService, server : H2Server) -> Unit {
  for entry in self.handlers() {
    server.register_handler(entry.0, entry.1)
  }
}
