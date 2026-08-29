// A DNS message codec (RFC 1035 §4) — the wire primitive gRPC's `dns:///` name
// resolver needs: encode a recursion-desired A/AAAA query, decode the response, and
// pull out the resolved addresses. Names are label-length-prefixed and may use the
// §4.1.4 compression pointer (`0xC0`), which the reader follows. This is pure logic —
// all-backend and total; the async resolver in `net/` sends the query over UDP and
// hands the reply here.

///|
/// The DNS record type for an IPv4 address.
pub let dns_type_a : Int = 1

///|
/// The DNS record type for an IPv6 address.
pub let dns_type_aaaa : Int = 28

///|
/// One answer resource record, carrying the textual address for A/AAAA records (empty
/// for other types).
pub(all) struct DnsRecord {
  name : String
  rtype : Int
  ttl : Int
  address : String
} derive(Eq, Debug)

///|
/// A decoded DNS response: the transaction id echoed back, the response code (0 =
/// NOERROR), and the answer records.
pub(all) struct DnsResponse {
  id : Int
  rcode : Int
  answers : Array[DnsRecord]
} derive(Eq, Debug)

///|
/// Write a domain name as a sequence of length-prefixed labels terminated by a zero
/// byte (§3.1). `example.com` becomes `7"example"3"com"0`; empty labels are dropped so
/// a trailing dot is harmless.
fn dns_encode_name(buf : Buffer, name : String) -> Unit {
  let raw = @utf8.encode(name)
  let n = raw.length()
  let mut start = 0
  for i = 0; i <= n; i = i + 1 {
    if i == n || raw[i] == b'.' {
      let len = i - start
      if len > 0 {
        buf.write_byte(len.to_byte())
        for k = start; k < i; k = k + 1 {
          buf.write_byte(raw[k])
        }
      }
      start = i + 1
    }
  }
  buf.write_byte(b'\x00')
}

///|
/// Read a domain name starting at `start`, following any §4.1.4 compression pointer,
/// and return the dotted name plus the offset of the byte just past the name *in the
/// original stream* (a pointer terminates the name at the two-octet pointer itself).
fn dns_read_name(msg : Bytes, start : Int) -> (String, Int) {
  let out = Buffer()
  let mut off = start
  let mut next = -1
  let mut first = true
  // Bound the walk so a malformed pointer loop can't spin forever.
  let mut steps = 0
  while steps < 256 {
    steps = steps + 1
    let len = msg[off].to_int()
    if len == 0 {
      off = off + 1
      if next < 0 {
        next = off
      }
      break
    }
    if (len & 0xC0) == 0xC0 {
      let ptr = ((len & 0x3F) << 8) | msg[off + 1].to_int()
      if next < 0 {
        next = off + 2
      }
      off = ptr
      continue
    }
    if !first {
      out.write_byte(b'.')
    }
    first = false
    for k = 0; k < len; k = k + 1 {
      out.write_byte(msg[off + 1 + k])
    }
    off = off + 1 + len
  }
  let name = @utf8.decode(out.to_bytes()) catch { _ => "" }
  (name, if next < 0 { off } else { next })
}

///|
/// Format a 4-octet A record as dotted-decimal.
fn dns_format_a(msg : Bytes, off : Int) -> String {
  msg[off].to_int().to_string() +
  "." +
  msg[off + 1].to_int().to_string() +
  "." +
  msg[off + 2].to_int().to_string() +
  "." +
  msg[off + 3].to_int().to_string()
}

///|
let dns_hex_digits : Bytes = b"0123456789abcdef"

///|
/// A 16-bit group as lowercase hex with no leading zeros (but at least one digit).
fn dns_hex_group(n : Int) -> String {
  if n == 0 {
    return "0"
  }
  let buf = Buffer()
  let mut started = false
  for shift = 12; shift >= 0; shift = shift - 4 {
    let d = (n >> shift) & 0xF
    if d != 0 || started {
      started = true
      buf.write_byte(dns_hex_digits[d])
    }
  }
  @utf8.decode(buf.to_bytes()) catch {
    _ => "0"
  }
}

///|
/// Format a 16-octet AAAA record as eight colon-separated hextets (RFC 4291 §2.2 form
/// 1 — the full, uncompressed representation, which is unambiguous and connectable).
fn dns_format_aaaa(msg : Bytes, off : Int) -> String {
  let out = Buffer()
  for g = 0; g < 8; g = g + 1 {
    if g > 0 {
      out.write_byte(b':')
    }
    let group = (msg[off + g * 2].to_int() << 8) | msg[off + g * 2 + 1].to_int()
    let hex = @utf8.encode(dns_hex_group(group))
    out.write_bytes(hex)
  }
  @utf8.decode(out.to_bytes()) catch {
    _ => ""
  }
}

///|
/// Encode a standard recursion-desired query for `name` of type `qtype` (`dns_type_a`
/// or `dns_type_aaaa`) in class IN, with transaction id `id`.
pub fn dns_encode_query(id : Int, name : String, qtype : Int) -> Bytes {
  let buf = Buffer()
  be_write_u16(buf, id)
  be_write_u16(buf, 0x0100) // QR=0, opcode QUERY, RD=1 (recursion desired)
  be_write_u16(buf, 1) // QDCOUNT
  be_write_u16(buf, 0) // ANCOUNT
  be_write_u16(buf, 0) // NSCOUNT
  be_write_u16(buf, 0) // ARCOUNT
  dns_encode_name(buf, name)
  be_write_u16(buf, qtype) // QTYPE
  be_write_u16(buf, 1) // QCLASS = IN
  buf.to_bytes()
}

///|
/// Decode a DNS response: echoed id, response code, and every answer record (A/AAAA
/// records carry their textual address). Questions are skipped; authority and
/// additional sections are ignored.
pub fn dns_decode_response(msg : Bytes) -> DnsResponse {
  let id = be_read_u16(msg, 0)
  let flags = be_read_u16(msg, 2)
  let rcode = flags & 0x000F
  let qd = be_read_u16(msg, 4)
  let an = be_read_u16(msg, 6)
  let mut off = 12
  for _q = 0; _q < qd; _q = _q + 1 {
    let (_, after) = dns_read_name(msg, off)
    off = after + 4 // QTYPE(2) + QCLASS(2)
  }
  let answers : Array[DnsRecord] = []
  for _a = 0; _a < an; _a = _a + 1 {
    let (name, after) = dns_read_name(msg, off)
    let rtype = be_read_u16(msg, after)
    let ttl = be_read_u32(msg, after + 4)
    let rdlen = be_read_u16(msg, after + 8)
    let rdata = after + 10
    let address = if rtype == dns_type_a && rdlen == 4 {
      dns_format_a(msg, rdata)
    } else if rtype == dns_type_aaaa && rdlen == 16 {
      dns_format_aaaa(msg, rdata)
    } else {
      ""
    }
    answers.push({ name, rtype, ttl, address, })
    off = rdata + rdlen
  }
  { id, rcode, answers, }
}

///|
/// Every resolved A/AAAA address in the response, in record order.
pub fn DnsResponse::addresses(self : DnsResponse) -> Array[String] {
  let out : Array[String] = []
  for r in self.answers {
    if r.address != "" {
      out.push(r.address)
    }
  }
  out
}
