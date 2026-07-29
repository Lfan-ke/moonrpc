// A pure, all-backend protobuf binary wire-format runtime (the encoding described
// at protobuf.dev "Encoding"). It carries the four wire types a proto3 message
// needs — varint (0), fixed64 (1), length-delimited (2), fixed32 (5) — plus the
// zigzag transform for the `sint*` types. Group wire types (3/4) are gone from
// proto3 and rejected on decode. No sockets and no async: `PbWriter`/`PbReader`
// are total over bytes, so every message struct that serialises through them runs
// on every backend. `descriptor.mbt` and the health/reflection services build on
// this instead of hand-rolling varints per message.

///|
/// The four protobuf wire types carried by a field tag's low three bits. The two
/// group types (`3` start-group, `4` end-group) are deprecated and unsupported, so
/// `from_code` rejects them.
pub(all) enum WireType {
  Varint
  Fixed64
  LengthDelim
  Fixed32
} derive(Eq)

///|
pub impl Show for WireType with fn output(self, logger) {
  logger.write_string(
    match self {
      Varint => "Varint"
      Fixed64 => "Fixed64"
      LengthDelim => "LengthDelim"
      Fixed32 => "Fixed32"
    },
  )
}

///|
/// The wire-type number (the tag's low three bits).
pub fn WireType::code(self : WireType) -> Int {
  match self {
    Varint => 0
    Fixed64 => 1
    LengthDelim => 2
    Fixed32 => 5
  }
}

///|
/// The wire type for a tag's low three bits, or `None` for the deprecated group
/// types (`3`/`4`) and any out-of-range value.
pub fn WireType::from_code(n : Int) -> WireType? {
  match n {
    0 => Some(Varint)
    1 => Some(Fixed64)
    2 => Some(LengthDelim)
    5 => Some(Fixed32)
    _ => None
  }
}

///|
/// A raised protobuf decode failure: `Truncated` when the buffer ends inside a
/// field, `BadWireType` for a group or unknown wire type, `Overflow` for a varint
/// longer than ten octets, and `BadUtf8` for an invalid string field.
pub suberror PbError {
  Truncated
  BadWireType(Int)
  Overflow
  BadUtf8
} derive(Eq)

///|
pub impl Show for PbError with fn output(self, logger) {
  match self {
    Truncated => logger.write_string("Truncated")
    BadWireType(n) => logger.write_string("BadWireType(" + n.to_string() + ")")
    Overflow => logger.write_string("Overflow")
    BadUtf8 => logger.write_string("BadUtf8")
  }
}

// -- writer -----------------------------------------------------------------

///|
/// An append-only protobuf message encoder. Field writers append a tag and the
/// field body; `to_bytes` yields the finished message. Fields are written in the
/// caller's order — protobuf places no ordering requirement on distinct fields, and
/// a message struct's encoder writes them by ascending number by convention.
pub struct PbWriter {
  buf : Buffer
}

///|
/// A fresh, empty message encoder.
pub fn PbWriter::new() -> PbWriter {
  { buf: Buffer() }
}

///|
/// The bytes written so far.
pub fn PbWriter::to_bytes(self : PbWriter) -> Bytes {
  self.buf.to_bytes()
}

///|
/// Append a base-128 varint (protobuf "Base 128 Varints"): seven bits per octet,
/// little-endian groups, the high bit marking continuation.
pub fn PbWriter::write_varint(self : PbWriter, value : UInt64) -> Unit {
  let mut v = value
  while v >= 0x80 {
    self.buf.write_byte(((v & 0x7F) | 0x80).to_byte())
    v = v >> 7
  }
  self.buf.write_byte(v.to_byte())
}

///|
/// Append a field tag: `(field_number << 3) | wire_type`, itself a varint.
pub fn PbWriter::write_tag(
  self : PbWriter,
  field : Int,
  wire : WireType,
) -> Unit {
  self.write_varint(((field << 3) | wire.code()).to_uint64())
}

///|
/// Append a little-endian 32-bit fixed value (wire type 5, no tag).
pub fn PbWriter::write_fixed32(self : PbWriter, v : UInt) -> Unit {
  self.buf.write_byte((v & 0xFF).to_byte())
  self.buf.write_byte(((v >> 8) & 0xFF).to_byte())
  self.buf.write_byte(((v >> 16) & 0xFF).to_byte())
  self.buf.write_byte(((v >> 24) & 0xFF).to_byte())
}

///|
/// Append a little-endian 64-bit fixed value (wire type 1, no tag).
pub fn PbWriter::write_fixed64(self : PbWriter, v : UInt64) -> Unit {
  for i = 0; i < 8; i = i + 1 {
    self.buf.write_byte(((v >> (i * 8)) & 0xFF).to_byte())
  }
}

///|
/// Append a length-delimited body: a varint length then the raw bytes (wire type
/// 2, no tag).
pub fn PbWriter::write_len_delim(self : PbWriter, body : Bytes) -> Unit {
  self.write_varint(body.length().to_uint64())
  self.buf.write_bytes(body)
}

// field writers -- each is `tag` + body.

///|
/// Write an `int32` field. Negative values sign-extend to a full ten-octet varint,
/// exactly as the reference implementation encodes them.
pub fn PbWriter::int32(self : PbWriter, field : Int, v : Int) -> Unit {
  self.write_tag(field, Varint)
  self.write_varint(v.to_int64().reinterpret_as_uint64())
}

///|
/// Write an `int64` field.
pub fn PbWriter::int64(self : PbWriter, field : Int, v : Int64) -> Unit {
  self.write_tag(field, Varint)
  self.write_varint(v.reinterpret_as_uint64())
}

///|
/// Write a `uint32` field.
pub fn PbWriter::uint32(self : PbWriter, field : Int, v : UInt) -> Unit {
  self.write_tag(field, Varint)
  self.write_varint(v.to_uint64())
}

///|
/// Write a `uint64` field.
pub fn PbWriter::uint64(self : PbWriter, field : Int, v : UInt64) -> Unit {
  self.write_tag(field, Varint)
  self.write_varint(v)
}

///|
/// Write a `sint32` field (zigzag-encoded so small-magnitude negatives stay short).
pub fn PbWriter::sint32(self : PbWriter, field : Int, v : Int) -> Unit {
  self.write_tag(field, Varint)
  self.write_varint(((v << 1) ^ (v >> 31)).reinterpret_as_uint().to_uint64())
}

///|
/// Write a `sint64` field (zigzag-encoded).
pub fn PbWriter::sint64(self : PbWriter, field : Int, v : Int64) -> Unit {
  self.write_tag(field, Varint)
  self.write_varint(((v << 1) ^ (v >> 63)).reinterpret_as_uint64())
}

///|
/// Write a `bool` field.
pub fn PbWriter::bool_(self : PbWriter, field : Int, v : Bool) -> Unit {
  self.write_tag(field, Varint)
  self.write_varint(if v { 1 } else { 0 })
}

///|
/// Write an enum field (its integer value, as a varint).
pub fn PbWriter::enum_(self : PbWriter, field : Int, v : Int) -> Unit {
  self.write_tag(field, Varint)
  self.write_varint(v.to_int64().reinterpret_as_uint64())
}

///|
/// Write a `fixed32`/`sfixed32`/`float` field.
pub fn PbWriter::fixed32(self : PbWriter, field : Int, v : UInt) -> Unit {
  self.write_tag(field, Fixed32)
  self.write_fixed32(v)
}

///|
/// Write a `fixed64`/`sfixed64`/`double` field.
pub fn PbWriter::fixed64(self : PbWriter, field : Int, v : UInt64) -> Unit {
  self.write_tag(field, Fixed64)
  self.write_fixed64(v)
}

///|
/// Write a `bytes` field.
pub fn PbWriter::bytes_(self : PbWriter, field : Int, v : Bytes) -> Unit {
  self.write_tag(field, LengthDelim)
  self.write_len_delim(v)
}

///|
/// Write a `string` field (UTF-8 encoded).
pub fn PbWriter::string_(self : PbWriter, field : Int, v : String) -> Unit {
  self.write_tag(field, LengthDelim)
  self.write_len_delim(@utf8.encode(v))
}

///|
/// Write an embedded-message field: the pre-encoded sub-message as a
/// length-delimited body.
pub fn PbWriter::message_(self : PbWriter, field : Int, v : Bytes) -> Unit {
  self.write_tag(field, LengthDelim)
  self.write_len_delim(v)
}

// -- reader -----------------------------------------------------------------

///|
/// A forward cursor over an encoded protobuf message. `read_tag` pulls the next
/// field's number and wire type; the typed readers then consume its body. `skip`
/// discards an unknown field's body so a decoder tolerates fields it does not know
/// (protobuf forward compatibility).
pub struct PbReader {
  data : Bytes
  mut pos : Int
}

///|
/// A reader positioned at the start of `data`.
pub fn PbReader::new(data : Bytes) -> PbReader {
  { data, pos: 0 }
}

///|
/// Whether the whole message has been consumed.
pub fn PbReader::eof(self : PbReader) -> Bool {
  self.pos >= self.data.length()
}

///|
fn PbReader::byte(self : PbReader) -> Int raise PbError {
  if self.pos >= self.data.length() {
    raise Truncated
  }
  let b = self.data[self.pos].to_int()
  self.pos = self.pos + 1
  b
}

///|
/// Read a base-128 varint. Raises `Overflow` past ten octets and `Truncated` if the
/// buffer ends mid-varint.
pub fn PbReader::read_varint(self : PbReader) -> UInt64 raise PbError {
  let mut value : UInt64 = 0
  let mut shift = 0
  while true {
    if shift >= 64 {
      raise Overflow
    }
    let octet = self.byte()
    value = value | ((octet & 0x7F).to_uint64() << shift)
    if (octet & 0x80) == 0 {
      break
    }
    shift = shift + 7
  }
  value
}

///|
/// Read a field tag, returning `(field_number, wire_type)`. Raises `BadWireType`
/// for a group or unknown wire type.
pub fn PbReader::read_tag(self : PbReader) -> (Int, WireType) raise PbError {
  let tag = self.read_varint().to_int()
  let field = tag >> 3
  match WireType::from_code(tag & 0x7) {
    Some(w) => (field, w)
    None => raise BadWireType(tag & 0x7)
  }
}

///|
/// Read a little-endian 32-bit fixed value.
pub fn PbReader::read_fixed32(self : PbReader) -> UInt raise PbError {
  let b0 = self.byte()
  let b1 = self.byte()
  let b2 = self.byte()
  let b3 = self.byte()
  (b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)).reinterpret_as_uint()
}

///|
/// Read a little-endian 64-bit fixed value.
pub fn PbReader::read_fixed64(self : PbReader) -> UInt64 raise PbError {
  let mut v : UInt64 = 0
  for i = 0; i < 8; i = i + 1 {
    v = v | (self.byte().to_uint64() << (i * 8))
  }
  v
}

///|
/// Read a length-delimited body's raw bytes.
pub fn PbReader::read_len_delim(self : PbReader) -> Bytes raise PbError {
  let len = self.read_varint().to_int()
  if len < 0 || self.pos + len > self.data.length() {
    raise Truncated
  }
  let out = self.data[self.pos:self.pos + len].to_owned()
  self.pos = self.pos + len
  out
}

///|
/// Read an `int32` field body (the low 32 bits of the varint).
pub fn PbReader::read_int32(self : PbReader) -> Int raise PbError {
  self.read_varint().to_int()
}

///|
/// Read an `int64` field body.
pub fn PbReader::read_int64(self : PbReader) -> Int64 raise PbError {
  self.read_varint().reinterpret_as_int64()
}

///|
/// Read a `uint32` field body.
pub fn PbReader::read_uint32(self : PbReader) -> UInt raise PbError {
  self.read_varint().to_uint()
}

///|
/// Read a `uint64` field body.
pub fn PbReader::read_uint64(self : PbReader) -> UInt64 raise PbError {
  self.read_varint()
}

///|
/// Read a `sint32` field body (zigzag-decoded).
pub fn PbReader::read_sint32(self : PbReader) -> Int raise PbError {
  let u = self.read_varint()
  ((u >> 1).reinterpret_as_int64() ^ -(u & 1).reinterpret_as_int64()).to_int()
}

///|
/// Read a `sint64` field body (zigzag-decoded).
pub fn PbReader::read_sint64(self : PbReader) -> Int64 raise PbError {
  let u = self.read_varint()
  (u >> 1).reinterpret_as_int64() ^ -(u & 1).reinterpret_as_int64()
}

///|
/// Read a `bool` field body.
pub fn PbReader::read_bool(self : PbReader) -> Bool raise PbError {
  self.read_varint() != 0
}

///|
/// Read a `bytes` field body.
pub fn PbReader::read_bytes(self : PbReader) -> Bytes raise PbError {
  self.read_len_delim()
}

///|
/// Read a `string` field body, decoding UTF-8. Raises `BadUtf8` on invalid bytes.
pub fn PbReader::read_string(self : PbReader) -> String raise PbError {
  let body = self.read_len_delim()
  @utf8.decode(body) catch {
    _ => raise BadUtf8
  }
}

///|
/// Discard the body of a field whose number the decoder does not recognise, given
/// its wire type — the mechanism behind protobuf's forward compatibility.
pub fn PbReader::skip(self : PbReader, wire : WireType) -> Unit raise PbError {
  match wire {
    Varint => {
      let _ = self.read_varint()
    }
    Fixed64 => {
      let _ = self.read_fixed64()
    }
    Fixed32 => {
      let _ = self.read_fixed32()
    }
    LengthDelim => {
      let _ = self.read_len_delim()
    }
  }
}
