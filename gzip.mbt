// A self-contained DEFLATE (RFC 1951) inflater and gzip (RFC 1952) reader — pure
// MoonBit, all-backend, no vendored C. gRPC's `grpc-encoding: gzip` frames one gzip
// member per message; this decodes them so a compressed request reaches the handler
// as its real bytes, upgrading the server from "identity only" to accepting gzip.
// Encoding is out of scope: a server need only *accept* a compressed request.

///|
/// A gzip / DEFLATE decode failure: a malformed member, an unsupported feature, or
/// a checksum mismatch.
pub suberror GzError {
  GzError(String)
}

///|
/// DEFLATE length codes 257..285: the base length each encodes (index = sym-257).
let gz_length_base : Array[Int] = [
  3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83,
  99, 115, 131, 163, 195, 227, 258,
]

///|
/// Extra bits read after each length code 257..285.
let gz_length_extra : Array[Int] = [
  0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5,
  5, 0,
]

///|
/// DEFLATE distance codes 0..29: the base distance each encodes.
let gz_dist_base : Array[Int] = [
  1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769,
  1025, 1537, 2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577,
]

///|
/// Extra bits read after each distance code 0..29.
let gz_dist_extra : Array[Int] = [
  0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11,
  12, 12, 13, 13,
]

///|
/// The 19 code-length codes are transmitted in this permuted order (RFC 1951 §3.2.7).
let gz_cl_order : Array[Int] = [
  16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
]

///|
/// A DEFLATE bit source: bits are consumed LSB-first out of the byte stream, and a
/// Huffman code is built MSB-first by shifting each new bit into the low end.
priv struct GzBits {
  data : Bytes
  mut pos : Int
  mut acc : Int
  mut nbits : Int
}

///|
fn GzBits::new(data : Bytes, start : Int) -> GzBits {
  { data, pos: start, acc: 0, nbits: 0, }
}

///|
/// The next bit (LSB of the current byte first).
fn GzBits::bit(self : GzBits) -> Int raise GzError {
  if self.nbits == 0 {
    if self.pos >= self.data.length() {
      raise GzError("truncated DEFLATE stream")
    }
    self.acc = self.data[self.pos].to_int()
    self.pos += 1
    self.nbits = 8
  }
  let b = self.acc & 1
  self.acc = self.acc >> 1
  self.nbits -= 1
  b
}

///|
/// The next `count` bits as an integer, first bit read into the LSB.
fn GzBits::bits(self : GzBits, count : Int) -> Int raise GzError {
  let mut v = 0
  for i = 0; i < count; i = i + 1 {
    v = v | (self.bit() << i)
  }
  v
}

///|
/// Discard the current partial byte, aligning to the next byte boundary (a stored
/// block's LEN starts there).
fn GzBits::align(self : GzBits) -> Unit {
  self.acc = 0
  self.nbits = 0
}

///|
/// A canonical Huffman decode table (RFC 1951 §3.2.2): `counts[len]` symbols share
/// code length `len`, and `symbols` lists every coded symbol ordered by
/// (length, symbol value). Mirrors the tables in zlib's puff.c.
priv struct GzHuff {
  counts : Array[Int]
  symbols : Array[Int]
}

///|
/// Build a canonical Huffman table from `lengths` (one code length per symbol, 0 =
/// symbol absent) over the first `n` symbols.
fn GzHuff::build(lengths : Array[Int], n : Int) -> GzHuff raise GzError {
  let max_bits = 15
  let counts = Array::make(max_bits + 1, 0)
  for i = 0; i < n; i = i + 1 {
    counts[lengths[i]] += 1
  }
  // Reject an over-subscribed code — more codes assigned at some length than the
  // remaining code space allows (zlib puff.c's `left < 0`) — so a malformed dynamic
  // table raises rather than building a table that misdecodes.
  let mut left = 1
  for len = 1; len <= max_bits; len = len + 1 {
    left = (left << 1) - counts[len]
    if left < 0 {
      raise GzError("over-subscribed Huffman code")
    }
  }
  let offsets = Array::make(max_bits + 2, 0)
  for len = 1; len <= max_bits; len = len + 1 {
    offsets[len + 1] = offsets[len] + counts[len]
  }
  let symbols = Array::make(n, 0)
  for sym = 0; sym < n; sym = sym + 1 {
    if lengths[sym] != 0 {
      symbols[offsets[lengths[sym]]] = sym
      offsets[lengths[sym]] += 1
    }
  }
  { counts, symbols, }
}

///|
/// Decode one symbol, reading bits and widening the candidate code one length at a
/// time until it falls inside a length's assigned range (puff.c's `decode`).
fn GzHuff::decode(self : GzHuff, br : GzBits) -> Int raise GzError {
  let mut code = 0
  let mut first = 0
  let mut index = 0
  for len = 1; len <= 15; len = len + 1 {
    code = code | br.bit()
    let count = self.counts[len]
    if code - first < count {
      return self.symbols[index + (code - first)]
    }
    index = index + count
    first = (first + count) << 1
    code = code << 1
  }
  raise GzError("invalid Huffman code")
}

///|
/// Inflate one block body given its literal/length and distance tables, appending
/// decoded bytes to `out`; returns at the end-of-block symbol (256). Back-references
/// copy byte-by-byte so overlapping (run-length) matches inflate correctly.
fn gz_inflate_block(
  br : GzBits,
  lit : GzHuff,
  dist : GzHuff,
  out : Array[Byte],
) -> Unit raise GzError {
  for ;; {
    let sym = lit.decode(br)
    if sym == 256 {
      break
    } else if sym < 256 {
      out.push(sym.to_byte())
    } else {
      let idx = sym - 257
      if idx >= gz_length_base.length() {
        raise GzError("invalid length symbol")
      }
      let len = gz_length_base[idx] + br.bits(gz_length_extra[idx])
      let dcode = dist.decode(br)
      if dcode >= gz_dist_base.length() {
        raise GzError("invalid distance symbol")
      }
      let d = gz_dist_base[dcode] + br.bits(gz_dist_extra[dcode])
      if d > out.length() {
        raise GzError("distance runs before the start of output")
      }
      let start = out.length() - d
      for k = 0; k < len; k = k + 1 {
        out.push(out[start + k])
      }
    }
  }
}

///|
/// The fixed literal/length tree (RFC 1951 §3.2.6): 0..143 → 8 bits, 144..255 → 9,
/// 256..279 → 7, 280..287 → 8.
fn gz_fixed_lit() -> GzHuff raise GzError {
  let lengths = Array::make(288, 0)
  for i = 0; i < 144; i = i + 1 {
    lengths[i] = 8
  }
  for i = 144; i < 256; i = i + 1 {
    lengths[i] = 9
  }
  for i = 256; i < 280; i = i + 1 {
    lengths[i] = 7
  }
  for i = 280; i < 288; i = i + 1 {
    lengths[i] = 8
  }
  GzHuff::build(lengths, 288)
}

///|
/// The fixed distance tree: all 30 codes are 5 bits.
fn gz_fixed_dist() -> GzHuff raise GzError {
  GzHuff::build(Array::make(30, 5), 30)
}

///|
/// Read a dynamic block's Huffman tables (RFC 1951 §3.2.7): the code-length code
/// lengths (permuted), then the run-length-coded literal/length and distance code
/// lengths, and return the two decode tables.
fn gz_dynamic_tables(br : GzBits) -> (GzHuff, GzHuff) raise GzError {
  let hlit = br.bits(5) + 257
  let hdist = br.bits(5) + 1
  let hclen = br.bits(4) + 4
  let cl_lengths = Array::make(19, 0)
  for i = 0; i < hclen; i = i + 1 {
    cl_lengths[gz_cl_order[i]] = br.bits(3)
  }
  let cl = GzHuff::build(cl_lengths, 19)
  let total = hlit + hdist
  let lengths = Array::make(total, 0)
  let mut i = 0
  while i < total {
    let sym = cl.decode(br)
    if sym < 16 {
      lengths[i] = sym
      i += 1
    } else if sym == 16 {
      if i == 0 {
        raise GzError("code-length repeat with no previous code")
      }
      let rep = 3 + br.bits(2)
      let prev = lengths[i - 1]
      for _r = 0; _r < rep; _r = _r + 1 {
        if i >= total {
          raise GzError("code-length repeat overruns")
        }
        lengths[i] = prev
        i += 1
      }
    } else if sym == 17 {
      let rep = 3 + br.bits(3)
      for _r = 0; _r < rep; _r = _r + 1 {
        if i >= total {
          raise GzError("code-length repeat overruns")
        }
        lengths[i] = 0
        i += 1
      }
    } else if sym == 18 {
      let rep = 11 + br.bits(7)
      for _r = 0; _r < rep; _r = _r + 1 {
        if i >= total {
          raise GzError("code-length repeat overruns")
        }
        lengths[i] = 0
        i += 1
      }
    } else {
      raise GzError("invalid code-length symbol")
    }
  }
  let lit_lengths = Array::make(hlit, 0)
  for j = 0; j < hlit; j = j + 1 {
    lit_lengths[j] = lengths[j]
  }
  let dist_lengths = Array::make(hdist, 0)
  for j = 0; j < hdist; j = j + 1 {
    dist_lengths[j] = lengths[hlit + j]
  }
  (GzHuff::build(lit_lengths, hlit), GzHuff::build(dist_lengths, hdist))
}

///|
/// Inflate a raw DEFLATE stream beginning at bit offset `start` in `data`: stored
/// (type 0), fixed-Huffman (type 1), and dynamic-Huffman (type 2) blocks, until the
/// final block.
fn gz_inflate_raw(data : Bytes, start : Int) -> Bytes raise GzError {
  let br = GzBits::new(data, start)
  let out : Array[Byte] = []
  let mut last = false
  while !last {
    if br.bit() == 1 {
      last = true
    }
    let btype = br.bits(2)
    if btype == 0 {
      br.align()
      if br.pos + 4 > data.length() {
        raise GzError("truncated stored block header")
      }
      let len = data[br.pos].to_int() | (data[br.pos + 1].to_int() << 8)
      let nlen = data[br.pos + 2].to_int() | (data[br.pos + 3].to_int() << 8)
      // LEN and its one's-complement NLEN must agree (RFC 1951 §3.2.4).
      if (len ^ 0xFFFF) != nlen {
        raise GzError("stored block LEN/NLEN mismatch")
      }
      br.pos = br.pos + 4
      if br.pos + len > data.length() {
        raise GzError("truncated stored block")
      }
      for _k = 0; _k < len; _k = _k + 1 {
        out.push(data[br.pos])
        br.pos += 1
      }
    } else if btype == 1 {
      gz_inflate_block(br, gz_fixed_lit(), gz_fixed_dist(), out)
    } else if btype == 2 {
      let (lit, dist) = gz_dynamic_tables(br)
      gz_inflate_block(br, lit, dist, out)
    } else {
      raise GzError("reserved DEFLATE block type")
    }
  }
  let buf = Buffer()
  for b in out {
    buf.write_byte(b)
  }
  buf.to_bytes()
}

///|
/// CRC-32 (IEEE 802.3: reflected input/output, polynomial 0xEDB88320) over `data` —
/// the checksum a gzip trailer carries. Computed bitwise, no lookup table.
fn gz_crc32(data : Bytes) -> UInt {
  let mut crc = 0xFFFFFFFF_U
  for i = 0; i < data.length(); i = i + 1 {
    crc = crc ^ data[i].to_uint()
    for _b = 0; _b < 8; _b = _b + 1 {
      if (crc & 1_U) != 0_U {
        crc = (crc >> 1) ^ 0xEDB88320_U
      } else {
        crc = crc >> 1
      }
    }
  }
  crc ^ 0xFFFFFFFF_U
}

///|
/// Decode a single gzip member (RFC 1952): validate the 10-byte header, skip the
/// optional EXTRA / NAME / COMMENT / HCRC fields, inflate the DEFLATE body, and
/// verify the trailing CRC-32 and ISIZE. Raises [`GzError`] on any malformation.
pub fn gunzip(data : Bytes) -> Bytes raise GzError {
  let n = data.length()
  if n < 18 {
    raise GzError("gzip member too short")
  }
  if data[0] != 0x1f || data[1] != 0x8b {
    raise GzError("bad gzip magic")
  }
  if data[2] != 0x08 {
    raise GzError("unsupported gzip compression method")
  }
  let flg = data[3].to_int()
  let mut pos = 10
  if (flg & 0x04) != 0 { // FEXTRA
    if pos + 2 > n {
      raise GzError("truncated FEXTRA field")
    }
    let xlen = data[pos].to_int() | (data[pos + 1].to_int() << 8)
    pos = pos + 2 + xlen
  }
  if (flg & 0x08) != 0 { // FNAME
    while pos < n && data[pos] != 0 {
      pos += 1
    }
    pos += 1
  }
  if (flg & 0x10) != 0 { // FCOMMENT
    while pos < n && data[pos] != 0 {
      pos += 1
    }
    pos += 1
  }
  if (flg & 0x02) != 0 { // FHCRC
    pos += 2
  }
  if pos + 8 > n {
    raise GzError("truncated gzip body")
  }
  let out = gz_inflate_raw(data, pos)
  // Trailer: CRC-32 then ISIZE, both little-endian, occupying the last 8 bytes.
  let want_crc = data[n - 8].to_uint() |
    (data[n - 7].to_uint() << 8) |
    (data[n - 6].to_uint() << 16) |
    (data[n - 5].to_uint() << 24)
  let want_isize = data[n - 4].to_uint() |
    (data[n - 3].to_uint() << 8) |
    (data[n - 2].to_uint() << 16) |
    (data[n - 1].to_uint() << 24)
  if gz_crc32(out) != want_crc {
    raise GzError("gzip CRC-32 mismatch")
  }
  if out.length().reinterpret_as_uint() != want_isize {
    raise GzError("gzip ISIZE mismatch")
  }
  out
}
