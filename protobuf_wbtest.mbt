// Exhaustive tests for the protobuf wire runtime: each wire type round-trips, the
// known-answer vectors from the protobuf "Encoding" page hold byte-for-byte, and
// the decoder rejects truncated, overlong, and group-wire-typed input.

///|
test "varint: known-answer vectors (protobuf Encoding page)" {
  let one = PbWriter::new()
  one.write_varint(1)
  assert_eq(one.to_bytes() == b"\x01", true)
  let zero = PbWriter::new()
  zero.write_varint(0)
  assert_eq(zero.to_bytes() == b"\x00", true)
  // 300 -> 0xAC 0x02 (the page's worked example).
  let w = PbWriter::new()
  w.write_varint(300)
  assert_eq(w.to_bytes() == b"\xac\x02", true)
  // 150 -> 0x96 0x01.
  let w150 = PbWriter::new()
  w150.write_varint(150)
  assert_eq(w150.to_bytes() == b"\x96\x01", true)
}

///|
test "varint: round-trips across boundary magnitudes" {
  let cases : Array[UInt64] = [
    0, 1, 127, 128, 300, 16383, 16384, 0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF,
  ]
  for v in cases {
    let w = PbWriter::new()
    w.write_varint(v)
    let r = PbReader::new(w.to_bytes())
    assert_eq(r.read_varint() == v, true)
    assert_eq(r.eof(), true)
  }
  // the all-ones 64-bit value takes exactly ten octets.
  let w = PbWriter::new()
  w.write_varint(0xFFFFFFFFFFFFFFFF)
  assert_eq(w.to_bytes().length(), 10)
}

///|
test "tag: field number and wire type pack and unpack" {
  // field 1, length-delimited -> 0x0A.
  let w = PbWriter::new()
  w.write_tag(1, LengthDelim)
  assert_eq(w.to_bytes() == b"\x0a", true)
  // field 1, varint -> 0x08.
  let v = PbWriter::new()
  v.write_tag(1, Varint)
  assert_eq(v.to_bytes() == b"\x08", true)
  // field 16 needs a two-octet tag: (16<<3)|2 = 130 -> 0x82 0x01.
  let big = PbWriter::new()
  big.write_tag(16, LengthDelim)
  let r = PbReader::new(big.to_bytes())
  let (field, wire) = r.read_tag()
  assert_eq(field, 16)
  assert_eq(wire == LengthDelim, true)
}

///|
test "string field: the protobuf 'testing' vector" {
  // message { string b = 2 } = "testing" -> 0x12 0x07 t e s t i n g.
  let w = PbWriter::new()
  w.string_(2, "testing")
  let expect = b"\x12\x07\x74\x65\x73\x74\x69\x6e\x67"
  assert_eq(w.to_bytes() == expect, true)
  let r = PbReader::new(w.to_bytes())
  let (field, wire) = r.read_tag()
  assert_eq(field, 2)
  assert_eq(wire == LengthDelim, true)
  assert_eq(r.read_string() == "testing", true)
}

///|
test "int32: a negative value sign-extends to ten octets" {
  // -1 as int32 encodes to a full 64-bit varint of all ones.
  let w = PbWriter::new()
  w.int32(1, -1)
  let body = w.to_bytes()
  assert_eq(body[0].to_int(), 0x08) // tag: field 1, varint
  assert_eq(body.length(), 11) // 1 tag octet + 10 varint octets
  let r = PbReader::new(body)
  let _ = r.read_tag()
  assert_eq(r.read_int32(), -1)
}

///|
test "int64 / uint64: full-width round-trip" {
  let w = PbWriter::new()
  w.int64(1, -9223372036854775808L)
  w.uint64(2, 0xFFFFFFFFFFFFFFFF)
  let r = PbReader::new(w.to_bytes())
  let _ = r.read_tag()
  assert_eq(r.read_int64() == -9223372036854775808L, true)
  let _ = r.read_tag()
  assert_eq(r.read_uint64() == 0xFFFFFFFFFFFFFFFF, true)
}

///|
test "sint32 / sint64: zigzag keeps small negatives short" {
  // sint32 -1 zigzags to 1 -> a single varint octet after the tag.
  let w = PbWriter::new()
  w.sint32(1, -1)
  assert_eq(w.to_bytes() == b"\x08\x01", true)
  // exhaustive round-trip of the zigzag inverse.
  let cases32 : Array[Int] = [0, -1, 1, -2, 2, 2147483647, -2147483648]
  for v in cases32 {
    let e = PbWriter::new()
    e.sint32(3, v)
    let r = PbReader::new(e.to_bytes())
    let _ = r.read_tag()
    assert_eq(r.read_sint32(), v)
  }
  let cases64 : Array[Int64] = [
    0, -1, 1, -2, 2, 9223372036854775807L, -9223372036854775808L,
  ]
  for v in cases64 {
    let e = PbWriter::new()
    e.sint64(4, v)
    let r = PbReader::new(e.to_bytes())
    let _ = r.read_tag()
    assert_eq(r.read_sint64() == v, true)
  }
}

///|
test "fixed32 / fixed64: little-endian byte order" {
  let w = PbWriter::new()
  w.fixed32(1, 0x0A0B0C0D)
  // tag 0x0D (field 1, wire 5) then LE bytes 0D 0C 0B 0A.
  assert_eq(w.to_bytes() == b"\x0d\x0d\x0c\x0b\x0a", true)
  let r = PbReader::new(w.to_bytes())
  let (_, wire) = r.read_tag()
  assert_eq(wire == Fixed32, true)
  assert_eq(r.read_fixed32() == 0x0A0B0C0D, true)
  let w64 = PbWriter::new()
  w64.fixed64(1, 0x0102030405060708)
  let r64 = PbReader::new(w64.to_bytes())
  let (_, w64t) = r64.read_tag()
  assert_eq(w64t == Fixed64, true)
  assert_eq(r64.read_fixed64() == 0x0102030405060708, true)
}

///|
test "bool / enum / bytes round-trip" {
  let w = PbWriter::new()
  w.bool_(1, true)
  w.enum_(2, 5)
  w.bytes_(3, b"\x00\xff\x10")
  let r = PbReader::new(w.to_bytes())
  let _ = r.read_tag()
  assert_eq(r.read_bool(), true)
  let _ = r.read_tag()
  assert_eq(r.read_int32(), 5)
  let _ = r.read_tag()
  assert_eq(r.read_bytes() == b"\x00\xff\x10", true)
}

///|
test "skip: an unknown field of every wire type is discarded" {
  // a message with fields the reader does not know, of all four wire types, then
  // a field 9 string it does — skipping must land exactly on field 9.
  let w = PbWriter::new()
  w.uint64(1, 300) // varint
  w.fixed64(2, 42) // fixed64
  w.fixed32(3, 7) // fixed32
  w.bytes_(4, b"junk") // length-delimited
  w.string_(9, "kept") // the field we read
  let r = PbReader::new(w.to_bytes())
  let mut kept = ""
  while !r.eof() {
    match r.read_tag() {
      (9, LengthDelim) => kept = r.read_string()
      (_, wire) => r.skip(wire)
    }
  }
  assert_eq(kept == "kept", true)
}

///|
test "decode rejects a truncated varint" {
  // 0xFF has the continuation bit set but no successor octet.
  let raised = try {
    let r = PbReader::new(b"\xff\xff")
    let _ = r.read_varint()
    false
  } catch {
    Truncated => true
    _ => false
  }
  assert_eq(raised, true)
}

///|
test "decode rejects a varint longer than ten octets (Overflow)" {
  // eleven octets all with the continuation bit set.
  let bad = b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01"
  let raised = try {
    let r = PbReader::new(bad)
    let _ = r.read_varint()
    false
  } catch {
    Overflow => true
    _ => false
  }
  assert_eq(raised, true)
}

///|
test "decode rejects a group wire type (BadWireType)" {
  // tag for field 1, wire type 3 (start-group) = 0x0B.
  let raised = try {
    let r = PbReader::new(b"\x0b")
    let _ = r.read_tag()
    false
  } catch {
    BadWireType(3) => true
    _ => false
  }
  assert_eq(raised, true)
}

///|
test "decode rejects a length-delimited field running past the buffer" {
  // field 1, length 10, but only two payload octets present.
  let raised = try {
    let r = PbReader::new(b"\x0a\x0a\x01\x02")
    let _ = r.read_tag()
    let _ = r.read_bytes()
    false
  } catch {
    Truncated => true
    _ => false
  }
  assert_eq(raised, true)
}

///|
test "WireType code/from_code and group rejection" {
  assert_eq(Varint.code(), 0)
  assert_eq(Fixed64.code(), 1)
  assert_eq(LengthDelim.code(), 2)
  assert_eq(Fixed32.code(), 5)
  assert_eq(WireType::from_code(0) == Some(Varint), true)
  assert_eq(WireType::from_code(2) == Some(LengthDelim), true)
  assert_eq(WireType::from_code(3) == None, true) // start-group
  assert_eq(WireType::from_code(4) == None, true) // end-group
  assert_eq(WireType::from_code(6) == None, true)
}

///|
test "protobuf: map<string,string> entries and packed repeated int32 round-trip" {
  let w = PbWriter::new()
  w.map_string_string(1, "a", "1")
  w.map_string_string(1, "b", "2")
  w.packed_int32(2, [1, -1, 300, 0])
  let r = PbReader::new(w.to_bytes())
  let entries : Array[(String, String)] = []
  let mut packed : Array[Int] = []
  while !r.eof() {
    let (field, wire) = r.read_tag()
    match field {
      1 => entries.push(r.read_map_string_string())
      2 => packed = r.read_packed_int32()
      _ => r.skip(wire)
    }
  }
  assert_eq(entries.length(), 2)
  assert_eq(entries[0].0 == "a" && entries[0].1 == "1", true)
  assert_eq(entries[1].0 == "b" && entries[1].1 == "2", true)
  assert_eq(packed == [1, -1, 300, 0], true)
}

///|
test "protobuf: a length-delimited field claiming a huge length raises, not aborts" {
  // A varint length of 0x7FFFFFFF at a non-zero offset makes `pos + len` wrap negative;
  // the guard must compare against the bytes remaining instead and raise Truncated.
  let r = PbReader::new(b"\x0a\xff\xff\xff\xff\x07\x01\x02")
  let raised = try {
    let (_, _) = r.read_tag()
    let _ = r.read_len_delim()
    false
  } catch {
    _ => true
  }
  assert_eq(raised, true)
}
