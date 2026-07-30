// Copyright 2026 Leo Cheng
// SPDX-License-Identifier: Apache-2.0

///|
test "base64 encodes and decodes the RFC 4648 test vectors" {
  assert_eq(@moonrpc.base64_encode(b"") == b"", true)
  assert_eq(@moonrpc.base64_encode(b"f") == b"Zg==", true)
  assert_eq(@moonrpc.base64_encode(b"fo") == b"Zm8=", true)
  assert_eq(@moonrpc.base64_encode(b"foo") == b"Zm9v", true)
  assert_eq(@moonrpc.base64_encode(b"foob") == b"Zm9vYg==", true)
  assert_eq(@moonrpc.base64_encode(b"fooba") == b"Zm9vYmE=", true)
  assert_eq(@moonrpc.base64_encode(b"foobar") == b"Zm9vYmFy", true)
  assert_eq(@moonrpc.base64_decode(b"Zm9vYmFy") == b"foobar", true)
  assert_eq(@moonrpc.base64_decode(b"Zg==") == b"f", true)
  // A binary round-trip over all byte values in a small window.
  let raw = b"\x00\x01\x02\xfe\xff\x80\x7f"
  assert_eq(@moonrpc.base64_decode(@moonrpc.base64_encode(raw)) == raw, true)
}

///|
test "the -bin metadata convention base64s only binary keys" {
  assert_eq(@moonrpc.is_binary_metadata(b"trace-bin"), true)
  assert_eq(@moonrpc.is_binary_metadata(b"trace-id"), false)
  assert_eq(@moonrpc.is_binary_metadata(b"bin"), false)
  // a -bin value goes on the wire base64-encoded and comes back raw.
  let raw = b"\x00\xff\x10"
  let wire = @moonrpc.metadata_value_to_wire(b"x-bin", raw)
  assert_eq(wire == @moonrpc.base64_encode(raw), true)
  assert_eq(@moonrpc.metadata_value_from_wire(b"x-bin", wire) == raw, true)
  // a plain key passes through untouched.
  assert_eq(@moonrpc.metadata_value_to_wire(b"x-id", b"abc") == b"abc", true)
  assert_eq(@moonrpc.metadata_value_from_wire(b"x-id", b"abc") == b"abc", true)
}
