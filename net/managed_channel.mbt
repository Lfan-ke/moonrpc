// The load-balanced gRPC client: a `ManagedChannel` dials one real `Channel` per resolved
// address, tracks each address's connectivity in the pure `@moonrpc.ConnectionPool`, and
// routes every call to a READY sub-connection its LB policy picks. This is where the pure
// decision cores (connectivity state machine + pool + picker) meet real sockets — the pool
// says *which* address to use, this drives the actual bytes over that address's connection.

///|
/// A load-balanced channel over several backends. The pool holds one sub-connection per
/// address (its connectivity state); `channels` holds the live `Channel` for each address
/// that dialed successfully.
pub struct ManagedChannel {
  pool : @moonrpc.ConnectionPool
  channels : Map[String, Channel]
}

///|
/// The `grpc-status` an RPC gets when no backend is reachable.
fn unavailable_reply() -> @moonrpc.CallReply {
  {
    status: b"",
    grpc_status: @moonrpc.Status::code(Unavailable),
    headers: [],
    messages: [],
    trailers: [],
  }
}

///|
/// Dial every backend in `targets` (each a `host, port`), marking each sub-connection READY
/// on a successful connect and TRANSIENT_FAILURE on a dial error, then load-balance calls
/// across the ones that came up. Addresses keep their given order, which is the order
/// `RoundRobin` cycles and `PickFirst` prefers.
pub async fn ManagedChannel::connect(
  targets : Array[(String, Int)],
  policy? : @moonrpc.LbPolicy = PickFirst,
) -> ManagedChannel {
  let keys = targets.map(t => t.0 + ":" + t.1.to_string())
  let pool = @moonrpc.ConnectionPool::new(keys, policy~)
  let channels : Map[String, Channel] = Map([])
  for t in targets {
    let key = t.0 + ":" + t.1.to_string()
    guard pool.subconn(key) is Some(sc) else { continue }
    sc.connect()
    try {
      let ch = Channel::connect(t.0, t.1)
      channels[key] = ch
      sc.on_connected()
    } catch {
      _ => sc.on_failure()
    }
  }
  { pool, channels }
}

///|
/// Resolve `name`'s A records via the DNS server at `dns_server:dns_port`, then dial each
/// resolved address at `target_port` and load-balance across them — gRPC's `dns:///`
/// target end to end (name resolution feeding the connection pool).
pub async fn ManagedChannel::dns_connect(
  dns_server : String,
  dns_port : Int,
  name : String,
  target_port : Int,
  policy? : @moonrpc.LbPolicy = PickFirst,
  id? : Int = 0x2a2a,
) -> ManagedChannel {
  let addrs = resolve_a(dns_server, dns_port, name, id~)
  ManagedChannel::connect(addrs.map(a => (a, target_port)), policy~)
}

///|
/// The live `Channel` for the next call, chosen by the pool's picker over the READY
/// sub-connections. `None` when nothing is reachable.
fn ManagedChannel::pick(self : ManagedChannel) -> Channel? {
  guard self.pool.pick() is Some(address) else { return None }
  self.channels.get(address)
}

///|
/// The addresses of every backend currently carrying calls, in resolver order — the READY
/// subset the picker draws from.
pub fn ManagedChannel::ready_addresses(self : ManagedChannel) -> Array[String] {
  self.pool.ready_addresses()
}

///|
/// Make a unary call on a picked backend. Returns an `UNAVAILABLE` reply when no
/// sub-connection is READY.
pub async fn ManagedChannel::unary(
  self : ManagedChannel,
  path : String,
  request : Bytes,
  metadata? : Array[@moonrpc.Header] = [],
  timeout_millis? : Int? = None,
  retry? : @moonrpc.RetryPolicy? = None,
) -> @moonrpc.CallReply {
  guard self.pick() is Some(ch) else { return unavailable_reply() }
  ch.unary(path, request, metadata~, timeout_millis~, retry~)
}

///|
/// Make a server-streaming call on a picked backend.
pub async fn ManagedChannel::server_streaming(
  self : ManagedChannel,
  path : String,
  request : Bytes,
  metadata? : Array[@moonrpc.Header] = [],
  timeout_millis? : Int? = None,
) -> @moonrpc.CallReply {
  guard self.pick() is Some(ch) else { return unavailable_reply() }
  ch.server_streaming(path, request, metadata~, timeout_millis~)
}

///|
/// Make a client-streaming call on a picked backend.
pub async fn ManagedChannel::client_streaming(
  self : ManagedChannel,
  path : String,
  requests : Array[Bytes],
  metadata? : Array[@moonrpc.Header] = [],
  timeout_millis? : Int? = None,
) -> @moonrpc.CallReply {
  guard self.pick() is Some(ch) else { return unavailable_reply() }
  ch.client_streaming(path, requests, metadata~, timeout_millis~)
}

///|
/// Make a bidirectional-streaming call on a picked backend.
pub async fn ManagedChannel::bidi(
  self : ManagedChannel,
  path : String,
  requests : Array[Bytes],
  metadata? : Array[@moonrpc.Header] = [],
  timeout_millis? : Int? = None,
) -> @moonrpc.CallReply {
  guard self.pick() is Some(ch) else { return unavailable_reply() }
  ch.bidi(path, requests, metadata~, timeout_millis~)
}

///|
/// Close every live backend connection.
pub fn ManagedChannel::close(self : ManagedChannel) -> Unit {
  for _addr, ch in self.channels {
    ch.close()
  }
}
