// The self-built HTTP/2 frame layer (RFC 7540 §4, §6). Byte-level, pure, and
// exhaustively round-trippable: every frame type encodes to and decodes from the
// exact wire bytes, so the transport can be layered on top without touching the
// codec. No sockets here — that is the multiplexer's job, sequenced after.

///|
/// HTTP/2 frame type codes (RFC 7540 §6).
pub let frame_data : Int = 0x0

///|
pub let frame_headers : Int = 0x1

///|
pub let frame_priority : Int = 0x2

///|
pub let frame_rst_stream : Int = 0x3

///|
pub let frame_settings : Int = 0x4

///|
pub let frame_push_promise : Int = 0x5

///|
pub let frame_ping : Int = 0x6

///|
pub let frame_goaway : Int = 0x7

///|
pub let frame_window_update : Int = 0x8

///|
pub let frame_continuation : Int = 0x9

///|
/// HTTP/2 frame flags (RFC 7540 §6). Flags are type-specific; the same bit
/// carries different meaning per frame type, hence the shared numeric values.
pub let flag_end_stream : Int = 0x1

///|
/// `ACK` on SETTINGS and PING shares bit 0x1 with `END_STREAM`.
pub let flag_ack : Int = 0x1

///|
pub let flag_end_headers : Int = 0x4

///|
pub let flag_padded : Int = 0x8

///|
pub let flag_priority : Int = 0x20

///|
/// SETTINGS parameter identifiers (RFC 7540 §6.5.2).
pub let settings_header_table_size : Int = 0x1

///|
pub let settings_enable_push : Int = 0x2

///|
pub let settings_max_concurrent_streams : Int = 0x3

///|
pub let settings_initial_window_size : Int = 0x4

///|
pub let settings_max_frame_size : Int = 0x5

///|
pub let settings_max_header_list_size : Int = 0x6

///|
/// HTTP/2 error codes (RFC 7540 §7), carried by RST_STREAM and GOAWAY.
pub let error_no_error : Int = 0x0

///|
pub let error_protocol_error : Int = 0x1

///|
pub let error_internal_error : Int = 0x2

///|
pub let error_flow_control_error : Int = 0x3

///|
pub let error_settings_timeout : Int = 0x4

///|
pub let error_stream_closed : Int = 0x5

///|
pub let error_frame_size_error : Int = 0x6

///|
pub let error_refused_stream : Int = 0x7

///|
pub let error_cancel : Int = 0x8

///|
pub let error_compression_error : Int = 0x9

///|
pub let error_connect_error : Int = 0xA

///|
pub let error_enhance_your_calm : Int = 0xB

///|
pub let error_inadequate_security : Int = 0xC

///|
pub let error_http_1_1_required : Int = 0xD

///|
/// The stream-priority block shared by PRIORITY frames and the optional priority
/// section of HEADERS (RFC 7540 §5.3.2). `weight` is the raw wire byte (`0..255`);
/// the effective priority weight is `weight + 1` (§6.3).
pub(all) struct Priority {
  exclusive : Bool
  stream_dependency : Int
  weight : Int
} derive(Eq)

///|
/// A raised decode failure. `Incomplete` is the normal "need more bytes" signal a
/// streaming reader catches to wait for the rest; the others are protocol errors.
pub suberror FrameError {
  Incomplete
  FrameSizeError(String)
  ProtocolError(String)
} derive(Eq)

///|
pub impl Show for FrameError with fn output(self, logger) {
  match self {
    Incomplete => logger.write_string("Incomplete")
    FrameSizeError(m) => logger.write_string("FrameSizeError(" + m + ")")
    ProtocolError(m) => logger.write_string("ProtocolError(" + m + ")")
  }
}

///|
/// A decoded HTTP/2 frame (RFC 7540 §6). Each variant carries the semantic
/// payload with padding already stripped; `padding` is the number of padding
/// bytes to (re)emit. `Unknown` preserves extension/unrecognised frames verbatim
/// so a reader can forward or ignore them (RFC 7540 §4.1).
pub(all) enum Frame {
  Data(stream_id~ : Int, data~ : Bytes, end_stream~ : Bool, padding~ : Int)
  Headers(
    stream_id~ : Int,
    fragment~ : Bytes,
    end_stream~ : Bool,
    end_headers~ : Bool,
    priority~ : Priority?,
    padding~ : Int
  )
  Priority(stream_id~ : Int, priority~ : Priority)
  RstStream(stream_id~ : Int, error_code~ : Int)
  Settings(params~ : Array[(Int, Int)], ack~ : Bool)
  PushPromise(
    stream_id~ : Int,
    promised_id~ : Int,
    fragment~ : Bytes,
    end_headers~ : Bool,
    padding~ : Int
  )
  Ping(payload~ : Bytes, ack~ : Bool)
  GoAway(last_stream_id~ : Int, error_code~ : Int, debug~ : Bytes)
  WindowUpdate(stream_id~ : Int, increment~ : Int)
  Continuation(stream_id~ : Int, fragment~ : Bytes, end_headers~ : Bool)
  Unknown(ftype~ : Int, flags~ : Int, stream_id~ : Int, payload~ : Bytes)
} derive(Eq)

// -- big-endian primitives --------------------------------------------------

///|
fn be_write_u16(buf : Buffer, n : Int) -> Unit {
  buf.write_byte((n >> 8).to_byte())
  buf.write_byte(n.to_byte())
}

///|
fn be_write_u24(buf : Buffer, n : Int) -> Unit {
  buf.write_byte((n >> 16).to_byte())
  buf.write_byte((n >> 8).to_byte())
  buf.write_byte(n.to_byte())
}

///|
fn be_write_u32(buf : Buffer, n : Int) -> Unit {
  buf.write_byte((n >> 24).to_byte())
  buf.write_byte((n >> 16).to_byte())
  buf.write_byte((n >> 8).to_byte())
  buf.write_byte(n.to_byte())
}

///|
fn be_read_u16(data : Bytes, off : Int) -> Int {
  (data[off].to_int() << 8) | data[off + 1].to_int()
}

///|
fn be_read_u24(data : Bytes, off : Int) -> Int {
  (data[off].to_int() << 16) |
  (data[off + 1].to_int() << 8) |
  data[off + 2].to_int()
}

///|
fn be_read_u32(data : Bytes, off : Int) -> Int {
  (data[off].to_int() << 24) |
  (data[off + 1].to_int() << 16) |
  (data[off + 2].to_int() << 8) |
  data[off + 3].to_int()
}

///|
/// Write a 31-bit stream identifier with the reserved top bit optionally set (the
/// `E` exclusive bit in a priority block; always clear elsewhere per §4.1).
fn be_write_stream_id(buf : Buffer, id : Int, top_bit : Bool) -> Unit {
  let hi = (id >> 24) & 0x7F
  buf.write_byte((if top_bit { hi | 0x80 } else { hi }).to_byte())
  buf.write_byte((id >> 16).to_byte())
  buf.write_byte((id >> 8).to_byte())
  buf.write_byte(id.to_byte())
}

///|
fn be_write_priority(buf : Buffer, p : Priority) -> Unit {
  be_write_stream_id(buf, p.stream_dependency, p.exclusive)
  buf.write_byte(p.weight.to_byte())
}

///|
fn parse_priority(payload : Bytes, off : Int) -> Priority {
  let raw = be_read_u32(payload, off)
  {
    exclusive: (payload[off].to_int() & 0x80) != 0,
    stream_dependency: raw & 0x7FFFFFFF,
    weight: payload[off + 4].to_int(),
  }
}

// -- frame header -----------------------------------------------------------

///|
/// The fixed 9-octet frame header (RFC 7540 §4.1): a 24-bit payload length, an
/// 8-bit type, 8-bit flags, a reserved bit, and a 31-bit stream identifier.
pub(all) struct FrameHeader {
  length : Int
  ftype : Int
  flags : Int
  stream_id : Int
} derive(Eq)

///|
/// Encode a `FrameHeader` to its 9 wire octets.
pub fn FrameHeader::encode(self : FrameHeader) -> Bytes {
  let buf = Buffer()
  be_write_u24(buf, self.length)
  buf.write_byte(self.ftype.to_byte())
  buf.write_byte(self.flags.to_byte())
  be_write_stream_id(buf, self.stream_id, false)
  buf.to_bytes()
}

///|
/// Decode the 9-octet frame header at `offset`. Raises `Incomplete` when fewer
/// than 9 octets are available. The reserved bit is masked off the stream id.
pub fn decode_frame_header(
  data : Bytes,
  offset? : Int = 0,
) -> FrameHeader raise FrameError {
  if offset + 9 > data.length() {
    raise Incomplete
  }
  {
    length: be_read_u24(data, offset),
    ftype: data[offset + 3].to_int(),
    flags: data[offset + 4].to_int(),
    stream_id: be_read_u32(data, offset + 5) & 0x7FFFFFFF,
  }
}

// -- frame encoding ---------------------------------------------------------

///|
/// The numeric frame-type code of this frame (RFC 7540 §6).
pub fn Frame::frame_type(self : Frame) -> Int {
  match self {
    Data(..) => frame_data
    Headers(..) => frame_headers
    Priority(..) => frame_priority
    RstStream(..) => frame_rst_stream
    Settings(..) => frame_settings
    PushPromise(..) => frame_push_promise
    Ping(..) => frame_ping
    GoAway(..) => frame_goaway
    WindowUpdate(..) => frame_window_update
    Continuation(..) => frame_continuation
    Unknown(ftype~, ..) => ftype
  }
}

///|
/// Assemble a full frame from its header fields and an already-built payload.
fn emit(ftype : Int, flags : Int, stream_id : Int, payload : Bytes) -> Bytes {
  let buf = Buffer()
  be_write_u24(buf, payload.length())
  buf.write_byte(ftype.to_byte())
  buf.write_byte(flags.to_byte())
  be_write_stream_id(buf, stream_id, false)
  buf.write_bytes(payload)
  buf.to_bytes()
}

///|
/// Write `n` zero padding octets.
fn write_padding(buf : Buffer, n : Int) -> Unit {
  for i = 0; i < n; i = i + 1 {
    buf.write_byte(b'\x00')
  }
}

///|
/// Encode this frame to its complete wire representation (9-octet header +
/// payload), the exact inverse of `decode_frame`.
pub fn Frame::encode(self : Frame) -> Bytes {
  match self {
    Data(stream_id~, data~, end_stream~, padding~) => {
      let mut flags = 0
      if end_stream {
        flags = flags | flag_end_stream
      }
      let body = Buffer()
      if padding > 0 {
        flags = flags | flag_padded
        body.write_byte(padding.to_byte())
      }
      body.write_bytes(data)
      write_padding(body, padding)
      emit(frame_data, flags, stream_id, body.to_bytes())
    }
    Headers(
      stream_id~,
      fragment~,
      end_stream~,
      end_headers~,
      priority~,
      padding~
    ) => {
      let mut flags = 0
      if end_stream {
        flags = flags | flag_end_stream
      }
      if end_headers {
        flags = flags | flag_end_headers
      }
      let body = Buffer()
      if padding > 0 {
        flags = flags | flag_padded
        body.write_byte(padding.to_byte())
      }
      match priority {
        Some(p) => {
          flags = flags | flag_priority
          be_write_priority(body, p)
        }
        None => ()
      }
      body.write_bytes(fragment)
      write_padding(body, padding)
      emit(frame_headers, flags, stream_id, body.to_bytes())
    }
    Priority(stream_id~, priority~) => {
      let body = Buffer()
      be_write_priority(body, priority)
      emit(frame_priority, 0, stream_id, body.to_bytes())
    }
    RstStream(stream_id~, error_code~) => {
      let body = Buffer()
      be_write_u32(body, error_code)
      emit(frame_rst_stream, 0, stream_id, body.to_bytes())
    }
    Settings(params~, ack~) => {
      let body = Buffer()
      if !ack {
        for pair in params {
          be_write_u16(body, pair.0)
          be_write_u32(body, pair.1)
        }
      }
      emit(frame_settings, if ack { flag_ack } else { 0 }, 0, body.to_bytes())
    }
    PushPromise(stream_id~, promised_id~, fragment~, end_headers~, padding~) => {
      let mut flags = 0
      if end_headers {
        flags = flags | flag_end_headers
      }
      let body = Buffer()
      if padding > 0 {
        flags = flags | flag_padded
        body.write_byte(padding.to_byte())
      }
      be_write_stream_id(body, promised_id, false)
      body.write_bytes(fragment)
      write_padding(body, padding)
      emit(frame_push_promise, flags, stream_id, body.to_bytes())
    }
    Ping(payload~, ack~) =>
      emit(frame_ping, if ack { flag_ack } else { 0 }, 0, payload)
    GoAway(last_stream_id~, error_code~, debug~) => {
      let body = Buffer()
      be_write_stream_id(body, last_stream_id, false)
      be_write_u32(body, error_code)
      body.write_bytes(debug)
      emit(frame_goaway, 0, 0, body.to_bytes())
    }
    WindowUpdate(stream_id~, increment~) => {
      let body = Buffer()
      be_write_stream_id(body, increment, false)
      emit(frame_window_update, 0, stream_id, body.to_bytes())
    }
    Continuation(stream_id~, fragment~, end_headers~) =>
      emit(
        frame_continuation,
        if end_headers {
          flag_end_headers
        } else {
          0
        },
        stream_id,
        fragment,
      )
    Unknown(ftype~, flags~, stream_id~, payload~) =>
      emit(ftype, flags, stream_id, payload)
  }
}

// -- frame decoding ---------------------------------------------------------

///|
/// Strip the padding of a PADDED frame body, returning `(content, pad_len)` where
/// `content` excludes the leading pad-length octet and the trailing pad octets.
fn strip_padding(payload : Bytes) -> (Bytes, Int) raise FrameError {
  if payload.length() < 1 {
    raise ProtocolError("padded frame missing pad-length octet")
  }
  let pad_len = payload[0].to_int()
  if 1 + pad_len > payload.length() {
    raise ProtocolError("pad length exceeds payload")
  }
  (payload[1:payload.length() - pad_len].to_owned(), pad_len)
}

///|
/// Decode exactly one frame at `offset`, returning `(frame, bytes_consumed)`
/// where `bytes_consumed` is `9 + payload_length`. Raises `Incomplete` when the
/// buffer does not yet hold the whole frame, or a protocol error when the payload
/// is malformed for its type. The inverse of `Frame::encode`.
pub fn decode_frame(
  data : Bytes,
  offset? : Int = 0,
) -> (Frame, Int) raise FrameError {
  let hdr = decode_frame_header(data, offset~)
  let start = offset + 9
  if start + hdr.length > data.length() {
    raise Incomplete
  }
  let payload = data[start:start + hdr.length].to_owned()
  let padded = (hdr.flags & flag_padded) != 0
  let frame = if hdr.ftype == frame_data {
    let (body, pad) = if padded { strip_padding(payload) } else { (payload, 0) }
    Frame::Data(
      stream_id=hdr.stream_id,
      data=body,
      end_stream=(hdr.flags & flag_end_stream) != 0,
      padding=pad,
    )
  } else if hdr.ftype == frame_headers {
    let (region, pad) = if padded {
      strip_padding(payload)
    } else {
      (payload, 0)
    }
    let has_priority = (hdr.flags & flag_priority) != 0
    let priority = if has_priority {
      if region.length() < 5 {
        raise ProtocolError("HEADERS priority block truncated")
      }
      Some(parse_priority(region, 0))
    } else {
      None
    }
    let frag_start = if has_priority { 5 } else { 0 }
    Frame::Headers(
      stream_id=hdr.stream_id,
      fragment=region[frag_start:].to_owned(),
      end_stream=(hdr.flags & flag_end_stream) != 0,
      end_headers=(hdr.flags & flag_end_headers) != 0,
      priority~,
      padding=pad,
    )
  } else if hdr.ftype == frame_priority {
    if payload.length() != 5 {
      raise FrameSizeError("PRIORITY frame must be 5 octets")
    }
    Priority(stream_id=hdr.stream_id, priority=parse_priority(payload, 0))
  } else if hdr.ftype == frame_rst_stream {
    if payload.length() != 4 {
      raise FrameSizeError("RST_STREAM frame must be 4 octets")
    }
    Frame::RstStream(
      stream_id=hdr.stream_id,
      error_code=be_read_u32(payload, 0),
    )
  } else if hdr.ftype == frame_settings {
    let ack = (hdr.flags & flag_ack) != 0
    if ack && payload.length() != 0 {
      raise FrameSizeError("SETTINGS ACK must carry no payload")
    }
    if payload.length() % 6 != 0 {
      raise FrameSizeError("SETTINGS payload must be a multiple of 6 octets")
    }
    let params = []
    for i = 0; i < payload.length(); i = i + 6 {
      params.push((be_read_u16(payload, i), be_read_u32(payload, i + 2)))
    }
    Settings(params~, ack~)
  } else if hdr.ftype == frame_push_promise {
    let (region, pad) = if padded {
      strip_padding(payload)
    } else {
      (payload, 0)
    }
    if region.length() < 4 {
      raise ProtocolError("PUSH_PROMISE missing promised stream id")
    }
    PushPromise(
      stream_id=hdr.stream_id,
      promised_id=be_read_u32(region, 0) & 0x7FFFFFFF,
      fragment=region[4:].to_owned(),
      end_headers=(hdr.flags & flag_end_headers) != 0,
      padding=pad,
    )
  } else if hdr.ftype == frame_ping {
    if payload.length() != 8 {
      raise FrameSizeError("PING frame must be 8 octets")
    }
    Ping(payload~, ack=(hdr.flags & flag_ack) != 0)
  } else if hdr.ftype == frame_goaway {
    if payload.length() < 8 {
      raise FrameSizeError("GOAWAY frame must be at least 8 octets")
    }
    GoAway(
      last_stream_id=be_read_u32(payload, 0) & 0x7FFFFFFF,
      error_code=be_read_u32(payload, 4),
      debug=payload[8:].to_owned(),
    )
  } else if hdr.ftype == frame_window_update {
    if payload.length() != 4 {
      raise FrameSizeError("WINDOW_UPDATE frame must be 4 octets")
    }
    WindowUpdate(
      stream_id=hdr.stream_id,
      increment=be_read_u32(payload, 0) & 0x7FFFFFFF,
    )
  } else if hdr.ftype == frame_continuation {
    Continuation(
      stream_id=hdr.stream_id,
      fragment=payload,
      end_headers=(hdr.flags & flag_end_headers) != 0,
    )
  } else {
    Unknown(ftype=hdr.ftype, flags=hdr.flags, stream_id=hdr.stream_id, payload~)
  }
  (frame, 9 + hdr.length)
}
