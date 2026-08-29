// HPACK Huffman coding (RFC 7541 §5.2, Appendix B). The canonical 257-entry code
// table (256 symbols + EOS), an encoder that pads the final octet with the
// most-significant bits of EOS, and a prefix-trie decoder that rejects an EOS in
// the input and any padding that is not a strict `<8`-bit run of ones.

///|
/// A raised HPACK failure (Huffman decoding or header-block decoding).
pub suberror HpackError {
  HuffmanError(String)
  HpackDecodeError(String)
} derive(Eq)

///|
/// Prints the header block and the offset it gave up at.
pub impl Show for HpackError with fn output(self, logger) {
  match self {
    HuffmanError(m) => logger.write_string("HuffmanError(" + m + ")")
    HpackDecodeError(m) => logger.write_string("HpackDecodeError(" + m + ")")
  }
}

///|
/// The RFC 7541 Appendix B Huffman code table as `(code, bit_length)` indexed by
/// symbol `0..=256`; index 256 is the EOS marker. `code` is right-aligned in an
/// `Int` (every code is at most 30 bits, so it stays non-negative).
let huffman_table : Array[(Int, Int)] = [
  (0x1ff8, 13),
  (0x7fffd8, 23),
  (0xfffffe2, 28),
  (0xfffffe3, 28),
  (0xfffffe4, 28),
  (0xfffffe5, 28),
  (0xfffffe6, 28),
  (0xfffffe7, 28),
  (0xfffffe8, 28),
  (0xffffea, 24),
  (0x3ffffffc, 30),
  (0xfffffe9, 28),
  (0xfffffea, 28),
  (0x3ffffffd, 30),
  (0xfffffeb, 28),
  (0xfffffec, 28),
  (0xfffffed, 28),
  (0xfffffee, 28),
  (0xfffffef, 28),
  (0xffffff0, 28),
  (0xffffff1, 28),
  (0xffffff2, 28),
  (0x3ffffffe, 30),
  (0xffffff3, 28),
  (0xffffff4, 28),
  (0xffffff5, 28),
  (0xffffff6, 28),
  (0xffffff7, 28),
  (0xffffff8, 28),
  (0xffffff9, 28),
  (0xffffffa, 28),
  (0xffffffb, 28),
  (0x14, 6),
  (0x3f8, 10),
  (0x3f9, 10),
  (0xffa, 12),
  (0x1ff9, 13),
  (0x15, 6),
  (0xf8, 8),
  (0x7fa, 11),
  (0x3fa, 10),
  (0x3fb, 10),
  (0xf9, 8),
  (0x7fb, 11),
  (0xfa, 8),
  (0x16, 6),
  (0x17, 6),
  (0x18, 6),
  (0x0, 5),
  (0x1, 5),
  (0x2, 5),
  (0x19, 6),
  (0x1a, 6),
  (0x1b, 6),
  (0x1c, 6),
  (0x1d, 6),
  (0x1e, 6),
  (0x1f, 6),
  (0x5c, 7),
  (0xfb, 8),
  (0x7ffc, 15),
  (0x20, 6),
  (0xffb, 12),
  (0x3fc, 10),
  (0x1ffa, 13),
  (0x21, 6),
  (0x5d, 7),
  (0x5e, 7),
  (0x5f, 7),
  (0x60, 7),
  (0x61, 7),
  (0x62, 7),
  (0x63, 7),
  (0x64, 7),
  (0x65, 7),
  (0x66, 7),
  (0x67, 7),
  (0x68, 7),
  (0x69, 7),
  (0x6a, 7),
  (0x6b, 7),
  (0x6c, 7),
  (0x6d, 7),
  (0x6e, 7),
  (0x6f, 7),
  (0x70, 7),
  (0x71, 7),
  (0x72, 7),
  (0xfc, 8),
  (0x73, 7),
  (0xfd, 8),
  (0x1ffb, 13),
  (0x7fff0, 19),
  (0x1ffc, 13),
  (0x3ffc, 14),
  (0x22, 6),
  (0x7ffd, 15),
  (0x3, 5),
  (0x23, 6),
  (0x4, 5),
  (0x24, 6),
  (0x5, 5),
  (0x25, 6),
  (0x26, 6),
  (0x27, 6),
  (0x6, 5),
  (0x74, 7),
  (0x75, 7),
  (0x28, 6),
  (0x29, 6),
  (0x2a, 6),
  (0x7, 5),
  (0x2b, 6),
  (0x76, 7),
  (0x2c, 6),
  (0x8, 5),
  (0x9, 5),
  (0x2d, 6),
  (0x77, 7),
  (0x78, 7),
  (0x79, 7),
  (0x7a, 7),
  (0x7b, 7),
  (0x7ffe, 15),
  (0x7fc, 11),
  (0x3ffd, 14),
  (0x1ffd, 13),
  (0xffffffc, 28),
  (0xfffe6, 20),
  (0x3fffd2, 22),
  (0xfffe7, 20),
  (0xfffe8, 20),
  (0x3fffd3, 22),
  (0x3fffd4, 22),
  (0x3fffd5, 22),
  (0x7fffd9, 23),
  (0x3fffd6, 22),
  (0x7fffda, 23),
  (0x7fffdb, 23),
  (0x7fffdc, 23),
  (0x7fffdd, 23),
  (0x7fffde, 23),
  (0xffffeb, 24),
  (0x7fffdf, 23),
  (0xffffec, 24),
  (0xffffed, 24),
  (0x3fffd7, 22),
  (0x7fffe0, 23),
  (0xffffee, 24),
  (0x7fffe1, 23),
  (0x7fffe2, 23),
  (0x7fffe3, 23),
  (0x7fffe4, 23),
  (0x1fffdc, 21),
  (0x3fffd8, 22),
  (0x7fffe5, 23),
  (0x3fffd9, 22),
  (0x7fffe6, 23),
  (0x7fffe7, 23),
  (0xffffef, 24),
  (0x3fffda, 22),
  (0x1fffdd, 21),
  (0xfffe9, 20),
  (0x3fffdb, 22),
  (0x3fffdc, 22),
  (0x7fffe8, 23),
  (0x7fffe9, 23),
  (0x1fffde, 21),
  (0x7fffea, 23),
  (0x3fffdd, 22),
  (0x3fffde, 22),
  (0xfffff0, 24),
  (0x1fffdf, 21),
  (0x3fffdf, 22),
  (0x7fffeb, 23),
  (0x7fffec, 23),
  (0x1fffe0, 21),
  (0x1fffe1, 21),
  (0x3fffe0, 22),
  (0x1fffe2, 21),
  (0x7fffed, 23),
  (0x3fffe1, 22),
  (0x7fffee, 23),
  (0x7fffef, 23),
  (0xfffea, 20),
  (0x3fffe2, 22),
  (0x3fffe3, 22),
  (0x3fffe4, 22),
  (0x7ffff0, 23),
  (0x3fffe5, 22),
  (0x3fffe6, 22),
  (0x7ffff1, 23),
  (0x3ffffe0, 26),
  (0x3ffffe1, 26),
  (0xfffeb, 20),
  (0x7fff1, 19),
  (0x3fffe7, 22),
  (0x7ffff2, 23),
  (0x3fffe8, 22),
  (0x1ffffec, 25),
  (0x3ffffe2, 26),
  (0x3ffffe3, 26),
  (0x3ffffe4, 26),
  (0x7ffffde, 27),
  (0x7ffffdf, 27),
  (0x3ffffe5, 26),
  (0xfffff1, 24),
  (0x1ffffed, 25),
  (0x7fff2, 19),
  (0x1fffe3, 21),
  (0x3ffffe6, 26),
  (0x7ffffe0, 27),
  (0x7ffffe1, 27),
  (0x3ffffe7, 26),
  (0x7ffffe2, 27),
  (0xfffff2, 24),
  (0x1fffe4, 21),
  (0x1fffe5, 21),
  (0x3ffffe8, 26),
  (0x3ffffe9, 26),
  (0xffffffd, 28),
  (0x7ffffe3, 27),
  (0x7ffffe4, 27),
  (0x7ffffe5, 27),
  (0xfffec, 20),
  (0xfffff3, 24),
  (0xfffed, 20),
  (0x1fffe6, 21),
  (0x3fffea, 22),
  (0x1fffe7, 21),
  (0x1fffe8, 21),
  (0x7ffff3, 23),
  (0x3fffeb, 22),
  (0x3fffe9, 22),
  (0x1ffffee, 25),
  (0x1ffffef, 25),
  (0xfffff4, 24),
  (0xfffff5, 24),
  (0x3ffffea, 26),
  (0x7ffff4, 23),
  (0x3ffffeb, 26),
  (0x7ffffe6, 27),
  (0x3ffffec, 26),
  (0x3ffffed, 26),
  (0x7ffffe7, 27),
  (0x7ffffe8, 27),
  (0x7ffffe9, 27),
  (0x7ffffea, 27),
  (0x7ffffeb, 27),
  (0xffffffe, 28),
  (0x7ffffec, 27),
  (0x7ffffed, 27),
  (0x7ffffee, 27),
  (0x7ffffef, 27),
  (0x7fffff0, 27),
  (0x3ffffee, 26),
  (0x3fffffff, 30),
]

///|
/// The EOS symbol index in the Huffman table (RFC 7541 §5.2).
let huffman_eos : Int = 256

///|
/// A node of the Huffman decoding trie: children reached by consuming a `0` or
/// `1` bit (`-1` when absent), and the terminal symbol (`-1` for internal nodes).
priv struct HNode {
  mut zero : Int
  mut one : Int
  mut sym : Int
}

///|
/// Build the prefix-code decoding trie once from `huffman_table`.
fn build_huffman_trie() -> Array[HNode] {
  let nodes : Array[HNode] = [{ zero: -1, one: -1, sym: -1, }]
  for sym = 0; sym < huffman_table.length(); sym = sym + 1 {
    let (code, bits) = huffman_table[sym]
    let mut cur = 0
    for i = bits - 1; i >= 0; i = i - 1 {
      let bit = (code >> i) & 1
      let next = if bit == 0 { nodes[cur].zero } else { nodes[cur].one }
      let child = if next == -1 {
        nodes.push({ zero: -1, one: -1, sym: -1, })
        let idx = nodes.length() - 1
        if bit == 0 {
          nodes[cur].zero = idx
        } else {
          nodes[cur].one = idx
        }
        idx
      } else {
        next
      }
      cur = child
    }
    nodes[cur].sym = sym
  }
  nodes
}

///|
let huffman_trie : Array[HNode] = build_huffman_trie()

///|
/// Huffman-encode `input` (RFC 7541 §5.2): each octet becomes its code, and the
/// final partial octet is padded with the most-significant bits of the EOS code
/// (all ones). The inverse of `huffman_decode` for valid inputs.
pub fn huffman_encode(input : Bytes) -> Bytes {
  let out = Buffer()
  let mut acc : UInt64 = 0
  let mut nbits = 0
  for i = 0; i < input.length(); i = i + 1 {
    let (code, bits) = huffman_table[input[i].to_int()]
    acc = (acc << bits) | code.to_uint64()
    nbits = nbits + bits
    while nbits >= 8 {
      nbits = nbits - 8
      out.write_byte(((acc >> nbits) & 0xFF).to_int().to_byte())
    }
  }
  if nbits > 0 {
    let pad = 8 - nbits
    acc = (acc << pad) | ((1 << pad) - 1).to_uint64()
    out.write_byte((acc & 0xFF).to_int().to_byte())
  }
  out.to_bytes()
}

///|
/// The number of octets `input` occupies when Huffman-encoded, without building
/// the output — used to choose the shorter of raw vs. Huffman string literals.
pub fn huffman_encoded_length(input : Bytes) -> Int {
  let mut bits = 0
  for i = 0; i < input.length(); i = i + 1 {
    bits = bits + huffman_table[input[i].to_int()].1
  }
  (bits + 7) / 8
}

///|
/// Huffman-decode `input` (RFC 7541 §5.2). Raises `HuffmanError` if the input
/// contains the EOS symbol, if the trailing padding is not a run of fewer than 8
/// one-bits, or if the bit stream leaves the code space. The inverse of
/// `huffman_encode`.
pub fn huffman_decode(input : Bytes) -> Bytes raise HpackError {
  let out = Buffer()
  let mut node = 0
  let mut since = 0
  let mut all_ones = true
  for i = 0; i < input.length(); i = i + 1 {
    let octet = input[i].to_int()
    for b = 7; b >= 0; b = b - 1 {
      let bit = (octet >> b) & 1
      since = since + 1
      if bit == 0 {
        all_ones = false
      }
      node = if bit == 0 {
        huffman_trie[node].zero
      } else {
        huffman_trie[node].one
      }
      if node == -1 {
        raise HuffmanError("Huffman input leaves the code space")
      }
      let sym = huffman_trie[node].sym
      if sym >= 0 {
        if sym == huffman_eos {
          raise HuffmanError("EOS symbol must not appear in Huffman input")
        }
        out.write_byte(sym.to_byte())
        node = 0
        since = 0
        all_ones = true
      }
    }
  }
  if node != 0 {
    if since >= 8 {
      raise HuffmanError("Huffman padding exceeds 7 bits")
    }
    if !all_ones {
      raise HuffmanError("Huffman padding is not all ones")
    }
  }
  out.to_bytes()
}
