// The gRPC client connection pool + picker (← gRPC's subchannel pool and LB picker): the Channel
// holds one `SubConn` per resolved address and, for each call, picks a READY one according to the
// load-balancing policy. This composes the connectivity state machine (which sub-connections are
// usable) with the LB policy (which usable one to use). It is the pure decision core — the Channel
// dials the sub-connections over real sockets and calls their transition methods as events happen.

///|
/// A pool of sub-connections, one per resolved address (in resolver order), that picks a READY one
/// per an `LbPolicy`.
pub struct ConnectionPool {
  order : Array[String]
  subconns : Map[String, SubConn]
  policy : LbPolicy
  mut cursor : Int
}

///|
/// A pool over `addresses` (each a `host:port`), each sub-connection starting IDLE, choosing among
/// the READY ones with `policy` (default `PickFirst`).
pub fn ConnectionPool::new(
  addresses : Array[String],
  policy? : LbPolicy = PickFirst,
) -> ConnectionPool {
  let order : Array[String] = []
  let subconns : Map[String, SubConn] = Map([])
  for address in addresses {
    order.push(address)
    subconns[address] = SubConn::new(address)
  }
  { order, subconns, policy, cursor: 0 }
}

///|
/// The sub-connection for `address`, if the pool has one — the handle the Channel drives.
pub fn ConnectionPool::subconn(
  self : ConnectionPool,
  address : String,
) -> SubConn? {
  self.subconns.get(address)
}

///|
/// The addresses of every READY sub-connection, in resolver order.
pub fn ConnectionPool::ready_addresses(self : ConnectionPool) -> Array[String] {
  let out : Array[String] = []
  for address in self.order {
    if self.subconns.get(address) is Some(sc) && sc.is_ready() {
      out.push(address)
    }
  }
  out
}

///|
/// Pick a READY sub-connection's address for the next call, applying the LB policy over just the
/// ready ones (← gRPC picker). `None` when no sub-connection is ready.
pub fn ConnectionPool::pick(self : ConnectionPool) -> String? {
  let ready = self.ready_addresses()
  let n = ready.length()
  guard n > 0 else { return None }
  match self.policy {
    PickFirst => Some(ready[0])
    RoundRobin => {
      let address = ready[self.cursor % n]
      self.cursor = (self.cursor + 1) % n
      Some(address)
    }
  }
}
