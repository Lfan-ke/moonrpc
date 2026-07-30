// Base64 (RFC 4648) for `-bin` metadata. gRPC carries binary metadata under keys
// suffixed `-bin`, base64-encoded on the wire; a call surfaces the decoded bytes and
// re-encodes on the way out. Pure and all-backend, like the rest of the engine.

///|
let b64_alphabet : Bytes = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

///|
/// Standard base64 encode with `=` padding.
pub fn base64_encode(data : Bytes) -> Bytes {
  let out = @buffer.Buffer()
  let n = data.length()
  let mut i = 0
  while i + 3 <= n {
    let x = (data[i].to_int() << 16) |
      (data[i + 1].to_int() << 8) |
      data[i + 2].to_int()
    out.write_byte(b64_alphabet[(x >> 18) & 0x3f])
    out.write_byte(b64_alphabet[(x >> 12) & 0x3f])
    out.write_byte(b64_alphabet[(x >> 6) & 0x3f])
    out.write_byte(b64_alphabet[x & 0x3f])
    i = i + 3
  }
  let rem = n - i
  if rem == 1 {
    let x = data[i].to_int() << 16
    out.write_byte(b64_alphabet[(x >> 18) & 0x3f])
    out.write_byte(b64_alphabet[(x >> 12) & 0x3f])
    out.write_byte(b'=')
    out.write_byte(b'=')
  } else if rem == 2 {
    let x = (data[i].to_int() << 16) | (data[i + 1].to_int() << 8)
    out.write_byte(b64_alphabet[(x >> 18) & 0x3f])
    out.write_byte(b64_alphabet[(x >> 12) & 0x3f])
    out.write_byte(b64_alphabet[(x >> 6) & 0x3f])
    out.write_byte(b'=')
  }
  out.to_bytes()
}

///|
fn b64_val(c : Int) -> Int {
  if c >= 65 && c <= 90 {
    c - 65
  } else if c >= 97 && c <= 122 {
    c - 97 + 26
  } else if c >= 48 && c <= 57 {
    c - 48 + 52
  } else if c == 43 {
    62
  } else if c == 47 {
    63
  } else {
    -1
  }
}

///|
/// Standard base64 decode. Non-alphabet bytes (padding, whitespace) are skipped, so
/// both padded and unpadded input decode.
pub fn base64_decode(data : Bytes) -> Bytes {
  let vals : Array[Int] = []
  for i = 0; i < data.length(); i = i + 1 {
    let v = b64_val(data[i].to_int())
    if v >= 0 {
      vals.push(v)
    }
  }
  let out = @buffer.Buffer()
  let mut i = 0
  while i + 2 <= vals.length() {
    out.write_byte(((vals[i] << 2) | (vals[i + 1] >> 4)).to_byte())
    if i + 3 <= vals.length() {
      out.write_byte(
        (((vals[i + 1] & 0xf) << 4) | (vals[i + 2] >> 2)).to_byte(),
      )
      if i + 4 <= vals.length() {
        out.write_byte((((vals[i + 2] & 0x3) << 6) | vals[i + 3]).to_byte())
      }
    }
    i = i + 4
  }
  out.to_bytes()
}

///|
/// Whether a metadata key names binary content: it ends in `-bin` (gRPC's convention
/// for base64-on-the-wire values).
pub fn is_binary_metadata(name : Bytes) -> Bool {
  let n = name.length()
  n >= 4 &&
  name[n - 4] == b'-' &&
  name[n - 3] == b'b' &&
  name[n - 2] == b'i' &&
  name[n - 1] == b'n'
}

///|
/// Decode a metadata value coming off the wire: base64-decoded for a `-bin` key,
/// verbatim otherwise.
pub fn metadata_value_from_wire(name : Bytes, value : Bytes) -> Bytes {
  if is_binary_metadata(name) {
    base64_decode(value)
  } else {
    value
  }
}

///|
/// Encode a metadata value for the wire: base64-encoded for a `-bin` key, verbatim
/// otherwise.
pub fn metadata_value_to_wire(name : Bytes, value : Bytes) -> Bytes {
  if is_binary_metadata(name) {
    base64_encode(value)
  } else {
    value
  }
}
