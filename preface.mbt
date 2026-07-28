// The HTTP/2 connection preface (RFC 7540 §3.5).

///|
/// The HTTP/2 client connection preface (RFC 7540 §3.5): the fixed, case-sensitive
/// 24-octet sequence `PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n` a client sends before its
/// first frame, which must be a SETTINGS frame. Its bytes deliberately form a
/// malformed HTTP/1.1 request so an HTTP/1.1-only server rejects it cleanly.
pub let connection_preface : Bytes = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

///|
/// Whether `data` begins with the exact 24-octet HTTP/2 connection preface.
pub fn has_connection_preface(data : Bytes) -> Bool {
  let magic = connection_preface
  if data.length() < magic.length() {
    return false
  }
  for i = 0; i < magic.length(); i = i + 1 {
    if data[i] != magic[i] {
      return false
    }
  }
  true
}
