// The gRPC client load-balancing policy (← `pick_first` / `round_robin`).

///|
test "load balancer: pick_first always returns the first address" {
  let lb = LoadBalancer::new(["a:1", "b:2", "c:3"])
  assert_eq(lb.size(), 3)
  assert_eq(lb.pick(), Some("a:1"))
  assert_eq(lb.pick(), Some("a:1"))
  assert_eq(lb.pick(), Some("a:1"))
}

///|
test "load balancer: round_robin cycles through the addresses and wraps" {
  let lb = LoadBalancer::new(["a:1", "b:2", "c:3"], policy=RoundRobin)
  assert_eq(lb.pick(), Some("a:1"))
  assert_eq(lb.pick(), Some("b:2"))
  assert_eq(lb.pick(), Some("c:3"))
  // wraps back to the first
  assert_eq(lb.pick(), Some("a:1"))
  assert_eq(lb.pick(), Some("b:2"))
}

///|
test "load balancer: no addresses yields None for either policy" {
  assert_eq(LoadBalancer::new([]).pick(), None)
  assert_eq(LoadBalancer::new([], policy=RoundRobin).pick(), None)
  // a single address round-robins to itself
  let one = LoadBalancer::new(["only:1"], policy=RoundRobin)
  assert_eq(one.pick(), Some("only:1"))
  assert_eq(one.pick(), Some("only:1"))
}
