// The native DNS resolver: send an RFC 1035 A query over UDP and return the resolved
// IPv4 addresses. This is the runtime half of gRPC's `dns:///` name resolution — it
// pairs the pure `@moonrpc` DNS codec (query build + response parse) with a real UDP
// round-trip, so a `ManagedChannel` can turn a hostname into the backend addresses its
// pool dials.

///|
/// Resolve `name`'s A records by querying the DNS server at `server:server_port` over
/// UDP, returning the dotted-decimal addresses (empty on a mismatched-id reply or an
/// answer with no A records). `id` is the query's transaction id, echoed in the reply.
pub async fn resolve_a(
  server : String,
  server_port : Int,
  name : String,
  id? : Int = 0x2a2a,
) -> Array[String] {
  let sock = @socket.UdpClient(
    @socket.Addr::parse(server + ":" + server_port.to_string()),
  )
  defer sock.close()
  sock.send(@moonrpc.dns_encode_query(id, name, @moonrpc.dns_type_a))
  // A classic DNS/UDP message fits in 512 octets; truncation would set the TC bit and
  // ask for TCP retry, which a stub resolver against a normal recursor rarely needs.
  let buf = FixedArray::make(512, b'\x00')
  let n = sock.recv(buf)
  guard n > 0 else { return [] }
  let resp = @moonrpc.dns_decode_response(
    buf.unsafe_reinterpret_as_bytes()[0:n].to_owned(),
  )
  guard resp.id == id else { return [] }
  resp.addresses()
}
