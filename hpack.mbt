///|
/// The 61-entry HPACK static header table (RFC 7541, Appendix A). Entry `i`
/// (0-based here) carries HPACK index `i + 1`; use `hpack_static_entry` for the
/// 1-based RFC lookup. Values are empty for name-only entries.
let static_table : Array[(String, String)] = [
  (":authority", ""),
  (":method", "GET"),
  (":method", "POST"),
  (":path", "/"),
  (":path", "/index.html"),
  (":scheme", "http"),
  (":scheme", "https"),
  (":status", "200"),
  (":status", "204"),
  (":status", "206"),
  (":status", "304"),
  (":status", "400"),
  (":status", "404"),
  (":status", "500"),
  ("accept-charset", ""),
  ("accept-encoding", "gzip, deflate"),
  ("accept-language", ""),
  ("accept-ranges", ""),
  ("accept", ""),
  ("access-control-allow-origin", ""),
  ("age", ""),
  ("allow", ""),
  ("authorization", ""),
  ("cache-control", ""),
  ("content-disposition", ""),
  ("content-encoding", ""),
  ("content-language", ""),
  ("content-length", ""),
  ("content-location", ""),
  ("content-range", ""),
  ("content-type", ""),
  ("cookie", ""),
  ("date", ""),
  ("etag", ""),
  ("expect", ""),
  ("expires", ""),
  ("from", ""),
  ("host", ""),
  ("if-match", ""),
  ("if-modified-since", ""),
  ("if-none-match", ""),
  ("if-range", ""),
  ("if-unmodified-since", ""),
  ("last-modified", ""),
  ("link", ""),
  ("location", ""),
  ("max-forwards", ""),
  ("proxy-authenticate", ""),
  ("proxy-authorization", ""),
  ("range", ""),
  ("referer", ""),
  ("refresh", ""),
  ("retry-after", ""),
  ("server", ""),
  ("set-cookie", ""),
  ("strict-transport-security", ""),
  ("transfer-encoding", ""),
  ("user-agent", ""),
  ("vary", ""),
  ("via", ""),
  ("www-authenticate", ""),
]

///|
/// The 61-entry HPACK static header table (RFC 7541, Appendix A) as `(name,
/// value)` pairs in RFC index order, i.e. `result[0]` is index 1 (`:authority`)
/// and `result[60]` is index 61 (`www-authenticate`).
pub fn hpack_static_table() -> Array[(String, String)] {
  static_table
}

///|
/// Look up an HPACK static-table entry by its 1-based RFC index (`1..=61`),
/// returning `(name, value)` or `None` when the index is out of range.
pub fn hpack_static_entry(index : Int) -> (String, String)? {
  if index < 1 || index > static_table.length() {
    None
  } else {
    Some(static_table[index - 1])
  }
}

///|
/// Encode `value` as an HPACK integer with an `prefix_bits`-bit prefix (RFC 7541
/// §5.1). The high `8 - prefix_bits` bits of the first octet are left zero for
/// the caller to OR in any flag bits. Examples: `10` on a 5-bit prefix is
/// `[0x0A]`; `1337` on a 5-bit prefix is `[0x1F, 0x9A, 0x0A]`.
pub fn hpack_encode_int(value : Int, prefix_bits : Int) -> Bytes {
  let buf = Buffer()
  let max_prefix = (1 << prefix_bits) - 1
  if value < max_prefix {
    buf.write_byte(value.to_byte())
  } else {
    buf.write_byte(max_prefix.to_byte())
    let mut rest = value - max_prefix
    while rest >= 128 {
      buf.write_byte((rest % 128 + 128).to_byte())
      rest = rest / 128
    }
    buf.write_byte(rest.to_byte())
  }
  buf.to_bytes()
}

///|
/// Decode an HPACK integer with an `prefix_bits`-bit prefix from `data` starting
/// at `offset` (RFC 7541 §5.1), returning `(value, bytes_consumed)`. Any flag
/// bits above the prefix in the first octet are masked off and ignored.
pub fn hpack_decode_int(
  data : Bytes,
  offset : Int,
  prefix_bits : Int,
) -> (Int, Int) {
  let max_prefix = (1 << prefix_bits) - 1
  let first = data[offset].to_int() & max_prefix
  if first < max_prefix {
    (first, 1)
  } else {
    let mut value = max_prefix
    let mut shift = 0
    let mut consumed = 1
    let mut more = true
    while more {
      let octet = data[offset + consumed].to_int()
      consumed = consumed + 1
      value = value + (octet & 127) * (1 << shift)
      shift = shift + 7
      if (octet & 128) == 0 {
        more = false
      }
    }
    (value, consumed)
  }
}

///|
/// Whether the string literal at `offset` is Huffman-coded, i.e. the `H` bit
/// (the top bit of the length octet) is set (RFC 7541 §5.2).
pub fn hpack_string_is_huffman(data : Bytes, offset : Int) -> Bool {
  (data[offset].to_int() & 0x80) != 0
}

///|
/// Encode `octets` as a non-Huffman HPACK string literal (RFC 7541 §5.2): the
/// length as a 7-bit-prefix integer with the `H` bit clear, followed by the raw
/// octets.
pub fn hpack_encode_string(octets : Bytes) -> Bytes {
  let buf = Buffer()
  buf.write_bytes(hpack_encode_int(octets.length(), 7))
  buf.write_bytes(octets)
  buf.to_bytes()
}

///|
/// Decode an HPACK string literal from `data` at `offset`, returning `(octets,
/// bytes_consumed)`. The length is read as a 7-bit-prefix integer (the `H` bit
/// is masked off); this is the inverse of `hpack_encode_string` for `H = 0`.
/// Use `hpack_string_is_huffman` first if the literal may be Huffman-coded, as
/// Huffman decoding is not applied here.
pub fn hpack_decode_string(data : Bytes, offset : Int) -> (Bytes, Int) {
  let (len, int_len) = hpack_decode_int(data, offset, 7)
  let start = offset + int_len
  (data[start:start + len].to_owned(), int_len + len)
}
