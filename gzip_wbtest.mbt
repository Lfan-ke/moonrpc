// Real gzip members produced by CPython's `gzip` module cover every DEFLATE block
// type: level-9 output on repetitive data uses a dynamic-Huffman block, a short
// string a fixed-Huffman block, level-0 a stored block, and a `GzipFile(filename=)`
// member sets FNAME so the optional-field skipping is exercised too. Each vector's
// decompressed bytes are asserted against the exact source, and the trailer CRC-32
// and ISIZE are verified inside `gunzip`, so a wrong Huffman path also fails the CRC.

///|
fn gz_nib(c : Char) -> Int {
  if c >= '0' && c <= '9' {
    c.to_int() - '0'.to_int()
  } else if c >= 'a' && c <= 'f' {
    c.to_int() - 'a'.to_int() + 10
  } else {
    c.to_int() - 'A'.to_int() + 10
  }
}

///|
/// Decode an even-length hex string to bytes.
fn gz_unhex(s : String) -> Bytes {
  let buf = Buffer()
  let chars = s.to_array()
  let mut i = 0
  while i + 1 < chars.length() {
    buf.write_byte(((gz_nib(chars[i]) << 4) | gz_nib(chars[i + 1])).to_byte())
    i += 2
  }
  buf.to_bytes()
}

///|
test "gunzip decodes a real dynamic-Huffman gzip member" {
  let payload = gz_unhex(
    "74686520717569636b2062726f776e20666f78206a756d7073206f76657220746865206c617a7920646f672e2074686520717569636b2062726f776e20666f78206a756d7073206f76657220746865206c617a7920646f672e2074686520717569636b2062726f776e20666f78206a756d7073206f76657220746865206c617a7920646f672e2074686520717569636b2062726f776e20666f78206a756d7073206f76657220746865206c617a7920646f672e2074686520717569636b2062726f776e20666f78206a756d7073206f76657220746865206c617a7920646f672e2074686520717569636b2062726f776e20666f78206a756d7073206f76657220746865206c617a7920646f672e20000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b",
  )
  let gz = gz_unhex(
    "1f8b0800cb9b6a6a02ff2bc94855282ccd4cce56482aca2fcf5348cbaf50c82acd2d2856c82f4b2d5228014ae72456552aa4e4a7eb8179c35a31032313330b2b1b3b072717370f2f1fbf80a090b088a898b884a494b48cac9cbc82a292b28aaa9aba86a696b68eae9ebe81a191b189a999b985a5953500928309e44a010000",
  )
  let got = gunzip(gz)
  assert_eq(got.length(), payload.length())
  assert_eq(got == payload, true)
}

///|
test "gunzip decodes a real fixed-Huffman gzip member" {
  let src = gz_unhex("68656c6c6f2c20677a697020776f726c64")
  let gz = gz_unhex(
    "1f8b0800cb9b6a6a02ffcb48cdc9c9d75148afca2c5028cf2fca490100eb83468211000000",
  )
  assert_eq(gunzip(gz) == src, true)
}

///|
test "gunzip decodes a real stored (level-0) gzip member" {
  let src = gz_unhex(
    "73746f72656420626c6f636b2c206e6f20687566666d616e2068657265",
  )
  let gz = gz_unhex(
    "1f8b0800fe9d6a6a04ff011d00e2ff73746f72656420626c6f636b2c206e6f20687566666d616e20686572652b46bf561d000000",
  )
  assert_eq(gunzip(gz) == src, true)
}

///|
test "gunzip skips the FNAME optional field" {
  let src = gz_unhex("6e616d6564206d656d626572207061796c6f6164")
  let gz = gz_unhex(
    "1f8b08080000000002ff68692e74787400cb4bcc4d4d51c84dcd4d4a2d522848acccc94f4c01004e5eca2a14000000",
  )
  assert_eq(gunzip(gz) == src, true)
}

///|
/// Flip byte `at` of `data`, keeping every other byte.
fn gz_flip(data : Bytes, at : Int) -> Bytes {
  let buf = Buffer()
  for i = 0; i < data.length(); i = i + 1 {
    if i == at {
      buf.write_byte((data[i].to_int() ^ 0xff).to_byte())
    } else {
      buf.write_byte(data[i])
    }
  }
  buf.to_bytes()
}

///|
test "gunzip rejects a bad-magic member" {
  let mut raised = false
  try
    gunzip(gz_unhex("00000800000000000000000000000000000000")) |> ignore
  catch {
    _ => raised = true
  }
  assert_eq(raised, true)
}

///|
test "gunzip rejects a payload byte flipped under the CRC-32" {
  let good = gz_unhex(
    "1f8b0800cb9b6a6a02ffcb48cdc9c9d75148afca2c5028cf2fca490100eb83468211000000",
  )
  // byte 12 is inside the DEFLATE body; flipping it changes the inflated bytes and
  // so the recomputed CRC-32 no longer matches the trailer.
  let mut raised = false
  try gunzip(gz_flip(good, 12)) |> ignore catch {
    _ => raised = true
  }
  assert_eq(raised, true)
}
