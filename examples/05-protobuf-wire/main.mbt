// The pure protobuf wire runtime (proto3 binary format) every gRPC message
// serialises through: a `PbWriter` / `PbReader` pair over varints, zigzag-coded
// signed ints, `fixed32` / `fixed64`, length-delimited fields, packed repeated
// fields, and `map<string,string>` entries. This writes one message touching every
// scalar kind, prints the tag stream, and reads each field straight back.
//
//     moon run examples/05-protobuf-wire

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
fn join_ints(xs : Array[Int]) -> String {
  let buf = StringBuilder()
  for i, x in xs {
    if i > 0 {
      buf.write_string(", ")
    }
    buf.write_string(x.to_string())
  }
  buf.to_string()
}

///|
fn run() -> Unit raise {
  let w = @moonrpc.PbWriter::new()
  w.string_(1, "heke1228") // length-delimited
  w.int32(2, -1) // a negative int32 varint spends the full ten octets
  w.sint32(3, -233) // zigzag keeps a small magnitude short
  w.uint64(4, 23333)
  w.bool_(5, true)
  w.enum_(6, 2)
  w.fixed32(7, 66666)
  w.fixed64(8, 23333)
  w.bytes_(9, b"qwq")
  w.packed_int32(10, [23, 42, 233]) // one length-delimited run of varints
  w.map_string_string(11, "emm", "hhh") // a map<string,string> entry
  let body = w.to_bytes()
  println("\{body.length()} octets: \{hex(body)}")

  // Read the fields back in order, dispatching on the field number and wire type the
  // tag carries. `WireType` is the low three bits of each tag.
  let r = @moonrpc.PbReader::new(body)
  while !r.eof() {
    let (field, wire) = r.read_tag()
    let shown = match field {
      1 => r.read_string()
      2 => r.read_int32().to_string()
      3 => r.read_sint32().to_string()
      4 => r.read_uint64().to_string()
      5 => r.read_bool().to_string()
      6 => r.read_int32().to_string()
      7 => r.read_fixed32().to_string()
      8 => r.read_fixed64().to_string()
      9 => "\{r.read_bytes().length()} bytes"
      10 => "[" + join_ints(r.read_packed_int32()) + "]"
      11 => {
        let (k, v) = r.read_map_string_string()
        "\{k} => \{v}"
      }
      _ => {
        r.skip(wire)
        "(skipped)"
      }
    }
    println("  field \{field} (\{wire}) = \{shown}")
  }
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
