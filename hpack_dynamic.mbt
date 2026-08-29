// HPACK header compression: the dynamic table with size-bounded eviction
// (RFC 7541 §2.3.2, §4), the six header-field representations (§6.1–6.3), and a
// stateful encoder/decoder pair that share the static (§2.3.1) and dynamic index
// address spaces. Byte-oriented throughout so binary metadata (`-bin`) survives.

///|
/// A decoded header field: `name` and `value` as raw octet strings (HTTP/2 header
/// names and values are byte sequences, and gRPC `-bin` metadata is binary).
pub(all) struct Header {
  name : Bytes
  value : Bytes
} derive(Eq, Debug)

///|
/// ASCII/latin-1 `String` → `Bytes`, one octet per code unit. Used only for the
/// static table, whose entries are all ASCII (RFC 7541 Appendix A).
fn ascii_to_bytes(s : String) -> Bytes {
  let buf = Buffer()
  for i = 0; i < s.length(); i = i + 1 {
    buf.write_byte((s[i].to_int() & 0xFF).to_byte())
  }
  buf.to_bytes()
}

///|
/// The static table as `(name, value)` octet pairs, in RFC index order (index 1
/// at position 0). Derived once from `static_table`.
let static_table_bytes : Array[(Bytes, Bytes)] = {
  let out : Array[(Bytes, Bytes)] = []
  for e in static_table {
    out.push((ascii_to_bytes(e.0), ascii_to_bytes(e.1)))
  }
  out
}

// -- string literals with Huffman (RFC 7541 §5.2) ---------------------------

///|
/// Encode `octets` as a Huffman-coded HPACK string literal (RFC 7541 §5.2): the
/// `H` bit set, the Huffman length as a 7-bit-prefix integer, then the code.
pub fn hpack_encode_string_huffman(octets : Bytes) -> Bytes {
  let coded = huffman_encode(octets)
  let lp = hpack_encode_int(coded.length(), 7)
  let buf = Buffer()
  buf.write_byte((lp[0].to_int() | 0x80).to_byte())
  buf.write_bytes(lp[1:].to_owned())
  buf.write_bytes(coded)
  buf.to_bytes()
}

///|
/// Encode `octets` as an HPACK string literal, choosing the shorter of the raw
/// (`H = 0`) and Huffman (`H = 1`) forms — the standard encoder heuristic.
pub fn hpack_encode_string_auto(octets : Bytes) -> Bytes {
  if huffman_encoded_length(octets) < octets.length() {
    hpack_encode_string_huffman(octets)
  } else {
    hpack_encode_string(octets)
  }
}

///|
/// Read an HPACK string literal at `offset`, resolving Huffman coding when the
/// `H` bit is set, returning `(octets, bytes_consumed)`. Unlike
/// `hpack_decode_string`, this applies Huffman decoding. Raises on bad Huffman.
pub fn hpack_read_string(
  data : Bytes,
  offset : Int,
) -> (Bytes, Int) raise HpackError {
  let huff = hpack_string_is_huffman(data, offset)
  let (raw, consumed) = hpack_decode_string(data, offset)
  if huff {
    (huffman_decode(raw), consumed)
  } else {
    (raw, consumed)
  }
}

// -- dynamic table (RFC 7541 §2.3.2, §4) ------------------------------------

///|
/// The HPACK dynamic table: a FIFO of recently seen `(name, value)` entries,
/// newest first (`entries[0]`), bounded by `max_size` octets where each entry
/// costs `name.len + value.len + 32` (RFC 7541 §4.1). Adding evicts the oldest
/// entries until the newcomer fits; an entry larger than `max_size` empties the
/// table and is not stored (§4.4).
pub(all) struct DynamicTable {
  mut entries : Array[(Bytes, Bytes)]
  mut size : Int
  mut max_size : Int
}

///|
/// A new empty dynamic table bounded by `max_size` octets (default 4096, the
/// HTTP/2 initial `SETTINGS_HEADER_TABLE_SIZE`).
pub fn DynamicTable::new(max_size? : Int = 4096) -> DynamicTable {
  { entries: [], size: 0, max_size, }
}

///|
fn entry_size(name : Bytes, value : Bytes) -> Int {
  name.length() + value.length() + 32
}

///|
/// Evict oldest entries until the table is within `max_size`.
fn DynamicTable::evict_to_fit(self : DynamicTable) -> Unit {
  while self.size > self.max_size && self.entries.length() > 0 {
    let old = self.entries.remove(self.entries.length() - 1)
    self.size = self.size - entry_size(old.0, old.1)
  }
}

///|
/// Resize the table (a dynamic table size update, RFC 7541 §4.2), evicting to fit.
pub fn DynamicTable::set_max_size(self : DynamicTable, new_max : Int) -> Unit {
  self.max_size = new_max
  self.evict_to_fit()
}

///|
/// Insert `(name, value)` at the front, evicting oldest entries to make room. If
/// the entry alone exceeds `max_size` the table ends up empty (RFC 7541 §4.4).
pub fn DynamicTable::add(
  self : DynamicTable,
  name : Bytes,
  value : Bytes,
) -> Unit {
  let sz = entry_size(name, value)
  while self.size + sz > self.max_size && self.entries.length() > 0 {
    let old = self.entries.remove(self.entries.length() - 1)
    self.size = self.size - entry_size(old.0, old.1)
  }
  if sz <= self.max_size {
    self.entries.insert(0, (name, value))
    self.size = self.size + sz
  }
}

///|
/// The number of entries currently in the dynamic table.
pub fn DynamicTable::count(self : DynamicTable) -> Int {
  self.entries.length()
}

///|
/// The current total size of the dynamic table in octets (§4.1 accounting).
pub fn DynamicTable::current_size(self : DynamicTable) -> Int {
  self.size
}

///|
/// Resolve an HPACK index against the combined static + dynamic address space
/// (RFC 7541 §2.3.3): `1..=61` index the static table; `62..` index the dynamic
/// table newest-first. Returns `None` when out of range.
fn hpack_lookup(dt : DynamicTable, index : Int) -> (Bytes, Bytes)? {
  let statics = static_table_bytes.length()
  if index >= 1 && index <= statics {
    Some(static_table_bytes[index - 1])
  } else {
    let d = index - statics - 1
    if d >= 0 && d < dt.entries.length() {
      Some(dt.entries[d])
    } else {
      None
    }
  }
}

///|
/// Encode a dynamic table size update (RFC 7541 §6.3): `001` prefix with the new
/// maximum size as a 5-bit-prefix integer.
pub fn hpack_encode_size_update(new_max : Int) -> Bytes {
  let e = hpack_encode_int(new_max, 5)
  let buf = Buffer()
  buf.write_byte((e[0].to_int() | 0x20).to_byte())
  buf.write_bytes(e[1:].to_owned())
  buf.to_bytes()
}

// -- decoder (RFC 7541 §6) --------------------------------------------------

///|
/// A stateful HPACK decoder: it owns a dynamic table that persists across the
/// header blocks of a connection. `limit` is the peer-agreed hard cap
/// (`SETTINGS_HEADER_TABLE_SIZE`) a size update may not exceed.
pub(all) struct HpackDecoder {
  table : DynamicTable
  limit : Int
}

///|
/// A new decoder whose dynamic table is bounded by `max_size` octets (also the
/// hard cap enforced on dynamic table size updates).
pub fn HpackDecoder::new(max_size? : Int = 4096) -> HpackDecoder {
  { table: DynamicTable::new(max_size~), limit: max_size, }
}

///|
/// Read a header-field name that is either indexed (`index != 0`) or a following
/// string literal (`index == 0`).
fn HpackDecoder::read_name(
  self : HpackDecoder,
  block : Bytes,
  index : Int,
  off : Int,
) -> (Bytes, Int) raise HpackError {
  if index == 0 {
    hpack_read_string(block, off)
  } else {
    match hpack_lookup(self.table, index) {
      Some((name, _)) => (name, 0)
      None =>
        raise HpackDecodeError("name index out of range: " + index.to_string())
    }
  }
}

///|
/// Decode one complete header block into its header list (RFC 7541 §6), mutating
/// the dynamic table for incrementally indexed fields and size updates. Raises
/// `HpackDecodeError`/`HuffmanError` on any malformed representation.
pub fn HpackDecoder::decode(
  self : HpackDecoder,
  block : Bytes,
) -> Array[Header] raise HpackError {
  let out : Array[Header] = []
  let mut off = 0
  let n = block.length()
  while off < n {
    let b = block[off].to_int()
    if (b & 0x80) != 0 {
      // §6.1 Indexed Header Field.
      let (index, consumed) = hpack_decode_int(block, off, 7)
      off = off + consumed
      if index == 0 {
        raise HpackDecodeError("indexed header field with index 0")
      }
      match hpack_lookup(self.table, index) {
        Some((name, value)) => out.push({ name, value, })
        None =>
          raise HpackDecodeError("index out of range: " + index.to_string())
      }
    } else if (b & 0x40) != 0 {
      // §6.2.1 Literal Header Field with Incremental Indexing.
      let (index, consumed) = hpack_decode_int(block, off, 6)
      off = off + consumed
      let (name, name_consumed) = self.read_name(block, index, off)
      off = off + name_consumed
      let (value, value_consumed) = hpack_read_string(block, off)
      off = off + value_consumed
      self.table.add(name, value)
      out.push({ name, value, })
    } else if (b & 0x20) != 0 {
      // §6.3 Dynamic Table Size Update.
      let (new_max, consumed) = hpack_decode_int(block, off, 5)
      off = off + consumed
      if new_max > self.limit {
        raise HpackDecodeError(
          "dynamic table size update exceeds SETTINGS limit",
        )
      }
      self.table.set_max_size(new_max)
    } else {
      // §6.2.2 without indexing (0x00) / §6.2.3 never indexed (0x10); 4-bit prefix.
      let (index, consumed) = hpack_decode_int(block, off, 4)
      off = off + consumed
      let (name, name_consumed) = self.read_name(block, index, off)
      off = off + name_consumed
      let (value, value_consumed) = hpack_read_string(block, off)
      off = off + value_consumed
      out.push({ name, value, })
    }
  }
  out
}

// -- encoder (RFC 7541 §6) --------------------------------------------------

///|
/// A stateful HPACK encoder: it owns a dynamic table mirroring the decoder's, and
/// prefers indexed representations. `huffman` selects Huffman string literals when
/// they are shorter.
pub(all) struct HpackEncoder {
  table : DynamicTable
  mut huffman : Bool
}

///|
/// A new encoder bounded by `max_size` octets; `huffman` (default `true`) enables
/// the shorter-of-two string-literal heuristic.
pub fn HpackEncoder::new(
  max_size? : Int = 4096,
  huffman? : Bool = true,
) -> HpackEncoder {
  { table: DynamicTable::new(max_size~), huffman, }
}

///|
/// Find the best index for `(name, value)`: an exact `(idx, true)` match, else a
/// name-only `(idx, false)` match, else `(0, false)` for no match at all.
fn HpackEncoder::find(
  self : HpackEncoder,
  name : Bytes,
  value : Bytes,
) -> (Int, Bool) {
  let statics = static_table_bytes.length()
  let mut name_idx = 0
  for i = 0; i < statics; i = i + 1 {
    let (n, v) = static_table_bytes[i]
    if n == name {
      if v == value {
        return (i + 1, true)
      }
      if name_idx == 0 {
        name_idx = i + 1
      }
    }
  }
  for d = 0; d < self.table.entries.length(); d = d + 1 {
    let (n, v) = self.table.entries[d]
    if n == name {
      let idx = statics + 1 + d
      if v == value {
        return (idx, true)
      }
      if name_idx == 0 {
        name_idx = idx
      }
    }
  }
  (name_idx, false)
}

///|
fn HpackEncoder::encode_string(self : HpackEncoder, octets : Bytes) -> Bytes {
  if self.huffman {
    hpack_encode_string_auto(octets)
  } else {
    hpack_encode_string(octets)
  }
}

///|
/// Write an integer with a flag OR'd into the high bits of its first octet.
fn write_prefixed(
  buf : Buffer,
  value : Int,
  prefix_bits : Int,
  flag : Int,
) -> Unit {
  let e = hpack_encode_int(value, prefix_bits)
  buf.write_byte((e[0].to_int() | flag).to_byte())
  buf.write_bytes(e[1:].to_owned())
}

///|
/// Encode a header list into a header block (RFC 7541 §6), using indexed fields
/// where possible and literal-with-incremental-indexing otherwise (mutating the
/// dynamic table to mirror what the peer decoder will build). The output decodes
/// back to the same header list via `HpackDecoder`.
pub fn HpackEncoder::encode(
  self : HpackEncoder,
  headers : Array[Header],
) -> Bytes {
  let buf = Buffer()
  for h in headers {
    let (idx, exact) = self.find(h.name, h.value)
    if exact {
      write_prefixed(buf, idx, 7, 0x80)
    } else {
      if idx != 0 {
        write_prefixed(buf, idx, 6, 0x40)
      } else {
        buf.write_byte(b'\x40')
        buf.write_bytes(self.encode_string(h.name))
      }
      buf.write_bytes(self.encode_string(h.value))
      self.table.add(h.name, h.value)
    }
  }
  buf.to_bytes()
}
