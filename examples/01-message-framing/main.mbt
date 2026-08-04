// The gRPC Length-Prefixed-Message framing (the codec shared by gRPC-Web over
// HTTP/1.1 and real gRPC over HTTP/2): one flag octet, a 4-byte big-endian length,
// then the payload. `encode_message` lays it down and `decode_message` reads it
// back, reporting whether the compression flag was set and handing back the body.
// Only whole frames decode — a short buffer is `None`, the signal a reader uses to
// wait for more bytes.
//
//     moon run examples/01-message-framing

///|
fn hex(bs : Bytes) -> String {
  let digits = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
  ]
  let buf = StringBuilder::new()
  for b in bs {
    let v = b.to_int() & 0xFF
    buf.write_char(digits[v >> 4])
    buf.write_char(digits[v & 0xF])
  }
  buf.to_string()
}

///|
fn show(label : String, frame : Bytes) -> Unit {
  match @moonrpc.decode_message(frame) {
    Some((compressed, payload)) =>
      println(
        "\{label}: \{hex(frame)}  ->  compressed=\{compressed}, \{payload.length()} body octets",
      )
    None => println("\{label}: \{hex(frame)}  ->  incomplete")
  }
}

///|
#coverage.skip
fn main {
  // The flag octet is 0 for identity and 1 for a compressed body; the length is the
  // body length, not counting the 5-octet prefix.
  let plain = @moonrpc.encode_message(b"heke1228")
  show("identity ", plain)
  let compressed = @moonrpc.encode_message(b"heke1228", compressed=true)
  show("flagged  ", compressed)

  // An empty payload still frames: the 5-octet prefix with a zero length.
  show("empty    ", @moonrpc.encode_message(b""))

  // A reader that has only part of the prefix (or part of the body) gets `None` and
  // waits — the frame is not yet whole.
  match @moonrpc.decode_message(b"\x00\x00\x00") {
    Some(_) => println("partial  : decoded (unexpected)")
    None => println("partial  : 3 octets -> incomplete, wait for more")
  }
  let short = plain[0:plain.length() - 2].to_owned()
  match @moonrpc.decode_message(short) {
    Some(_) => println("truncated: decoded (unexpected)")
    None =>
      println(
        "truncated: length says 8 body octets, only 6 present -> incomplete",
      )
  }

  // The prefix caps the body at `max_message_size`; a decoder rejects a length
  // beyond it rather than allocating on a hostile prefix.
  println("max message size: \{@moonrpc.max_message_size} octets (4 MiB)")
}
