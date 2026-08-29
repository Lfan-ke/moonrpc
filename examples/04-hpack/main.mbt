// Complete HPACK (RFC 7541): the header-compression stack a gRPC connection runs on.
// The 61-entry static table, prefix integers (§5.1), string literals with Huffman
// coding (§5.2, Appendix B), and the size-bounded dynamic table with eviction (§4).
// This drives each primitive on its own, then the stateful encoder/decoder pair over
// a header list, printing the compressed bytes and the exact round-trip.
//
//     moon run examples/04-hpack

///|
fn hex(bs : Bytes) -> String {
  let digits = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
  ]
  let buf = StringBuilder()
  for b in bs {
    let v = b.to_int() & 0xFF
    buf.write_char(digits[v >> 4])
    buf.write_char(digits[v & 0xF])
  }
  buf.to_string()
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}

///|
fn run() -> Unit raise {
  // The static table indexes the header fields every request repeats. Index 2 is the
  // `:method GET` entry; index 8 is `:status 200`.
  let table = @moonrpc.hpack_static_table()
  println("static table: \{table.length()} entries")
  match @moonrpc.hpack_static_entry(2) {
    Some((n, v)) => println("  [2] = \{n}: \{v}")
    None => ()
  }
  match @moonrpc.hpack_static_entry(8) {
    Some((n, v)) => println("  [8] = \{n}: \{v}")
    None => ()
  }

  // Prefix integers (§5.1): a value below the prefix max fits one octet; 1337 with a
  // 5-bit prefix spills into continuation octets. Encode then decode straight back.
  for value in [10, 31, 1337, 66666] {
    let enc = @moonrpc.hpack_encode_int(value, 5)
    let status = try {
      let (back, consumed) = @moonrpc.hpack_decode_int(enc, 0, 5)
      if back == value && consumed == enc.length() {
        "ok"
      } else {
        "MISMATCH"
      }
    } catch {
      e => "raised \{e}"
    }
    println("  int \{value} (5-bit prefix) = \{hex(enc)} (\{status})")
  }

  // String literals (§5.2): the raw form and the Huffman form of the same octets. The
  // `auto` encoder keeps whichever is shorter, setting the `H` bit when it Huffman-codes.
  let host = b"www.example.com"
  let raw = @moonrpc.hpack_encode_string(host)
  let huff = @moonrpc.hpack_encode_string_huffman(host)
  let auto = @moonrpc.hpack_encode_string_auto(host)
  println("string \"www.example.com\":")
  println(
    "  raw     \{raw.length()} octets, H=\{@moonrpc.hpack_string_is_huffman(raw, 0)}",
  )
  println(
    "  huffman \{huff.length()} octets, H=\{@moonrpc.hpack_string_is_huffman(huff, 0)}  (§C.4.1: \{hex(@moonrpc.huffman_encode(host))})",
  )
  println("  auto picks the huffman form: \{auto == huff}")
  let (read_back, _) = @moonrpc.hpack_read_string(auto, 0)
  println("  decoded back: \{read_back == host}")

  // The dynamic table (§4): a FIFO of recently seen fields, each costing
  // `name + value + 32` octets, bounded by its max size. Filling past the bound evicts
  // the oldest entry; shrinking the bound evicts to fit.
  let tbl = @moonrpc.DynamicTable::new(max_size=100)
  tbl.add(b"custom-key", b"heke1228")
  tbl.add(b"x-trace-id", b"23333")
  println(
    "dynamic table: \{tbl.count()} entries, \{tbl.current_size()}/100 octets",
  )
  tbl.add(b"another-header", b"a fairly long value that forces eviction")
  println(
    "  after a large insert: \{tbl.count()} entries, \{tbl.current_size()}/100 octets",
  )
  tbl.set_max_size(40)
  println(
    "  after shrinking to 40: \{tbl.count()} entries, \{tbl.current_size()}/40 octets",
  )
  println("  size-update opcode: \{hex(@moonrpc.hpack_encode_size_update(40))}")

  // The stateful encoder/decoder pair over a real header list — the block a client puts
  // in a HEADERS frame, decoded back to the same fields.
  let enc = @moonrpc.HpackEncoder::new(huffman=true)
  let headers : Array[@moonrpc.Header] = [
    { name: b":method", value: b"POST", },
    { name: b":path", value: b"/greet.Greeter/SayHello", },
    { name: b":authority", value: b"www.example.com", },
    { name: b"content-type", value: b"application/grpc+proto", },
  ]
  let block = enc.encode(headers)
  let dec = @moonrpc.HpackDecoder::new()
  let decoded = dec.decode(block)
  println(
    "header block: \{headers.length()} fields -> \{block.length()} octets -> \{decoded.length()} fields, round-trip \{decoded == headers}",
  )
}
