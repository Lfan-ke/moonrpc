// The `grpc-message` percent codec (gRPC HTTP/2 spec §"Responses"): the client's `percent_decode`
// is the inverse of the server's `percent_encode`.

///|
test "grpc-message: percent_decode inverts percent_encode and handles escapes and literals" {
  // A status message with a space, quotes, a literal percent, and non-ASCII text round-trips.
  let msg = "not found: user \"a\" (50% off) — 李雷"
  assert_eq(percent_decode(percent_encode(msg)), msg)
  // A raw escape decodes to the byte its two hex digits name.
  assert_eq(percent_decode(b"a%20b"), "a b")
  assert_eq(percent_decode(b"%41%42"), "AB")
  // A malformed or truncated escape is left literal.
  assert_eq(percent_decode(b"100%"), "100%")
  assert_eq(percent_decode(b"x%zzy"), "x%zzy")
  // A plain ASCII message passes through unchanged.
  assert_eq(percent_decode(b"already fine"), "already fine")
}
