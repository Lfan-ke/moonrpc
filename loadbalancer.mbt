// The gRPC client load-balancing policy (← gRPC's `pick_first` / `round_robin`): given the set of
// addresses a resolver produced for a target, decide which one the next call uses. This is the pure
// decision core of the Channel client's LB layer; the Channel drives it to spread calls across the
// connections it dials. `pick_first` always returns the first address (a single connection);
// `round_robin` cycles through them so load spreads evenly.

///|
/// A client-side load-balancing policy (← gRPC `loadBalancingConfig`).
pub(all) enum LbPolicy {
  PickFirst
  RoundRobin
} derive(Eq, Debug)

///|
/// A load balancer over a resolver's addresses, applying an `LbPolicy` to choose the next one.
pub struct LoadBalancer {
  addresses : Array[String]
  policy : LbPolicy
  mut cursor : Int
}

///|
/// A balancer over `addresses` (each a `host:port`) using `policy` (default `PickFirst`).
pub fn LoadBalancer::new(
  addresses : Array[String],
  policy? : LbPolicy = PickFirst,
) -> LoadBalancer {
  { addresses, policy, cursor: 0 }
}

///|
/// The address the next call should use, or `None` when the resolver produced none. `PickFirst`
/// always returns the first address; `RoundRobin` returns each in turn, wrapping around.
pub fn LoadBalancer::pick(self : LoadBalancer) -> String? {
  let n = self.addresses.length()
  guard n > 0 else { return None }
  match self.policy {
    PickFirst => Some(self.addresses[0])
    RoundRobin => {
      let address = self.addresses[self.cursor % n]
      self.cursor = (self.cursor + 1) % n
      Some(address)
    }
  }
}

///|
/// The number of addresses the balancer is choosing among.
pub fn LoadBalancer::size(self : LoadBalancer) -> Int {
  self.addresses.length()
}
