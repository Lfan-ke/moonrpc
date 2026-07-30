// The gRPC client connection pool + picker (← subchannel pool / LB picker).

///|
test "pool: picks only READY sub-connections, round-robin over the ready set in resolver order" {
  let pool = ConnectionPool::new(["a:1", "b:2", "c:3"], policy=RoundRobin)
  // Nothing is ready yet.
  assert_eq(pool.pick(), None)
  assert_eq(pool.ready_addresses(), [])
  // Bring a and c up (b stays connecting).
  guard pool.subconn("a:1") is Some(a) else { fail("no a") }
  guard pool.subconn("b:2") is Some(b) else { fail("no b") }
  guard pool.subconn("c:3") is Some(c) else { fail("no c") }
  a.connect()
  a.on_connected()
  c.connect()
  c.on_connected()
  b.connect()
  // Only a and c are ready, in resolver order.
  assert_eq(pool.ready_addresses(), ["a:1", "c:3"])
  // Round-robin cycles over just the ready set.
  assert_eq(pool.pick(), Some("a:1"))
  assert_eq(pool.pick(), Some("c:3"))
  assert_eq(pool.pick(), Some("a:1"))
  // a fails → only c is ready.
  a.on_failure()
  assert_eq(pool.ready_addresses(), ["c:3"])
}

///|
test "pool: pick_first returns the first ready address in resolver order" {
  let pool = ConnectionPool::new(["a:1", "b:2", "c:3"])
  guard pool.subconn("b:2") is Some(b) else { fail("no b") }
  guard pool.subconn("c:3") is Some(c) else { fail("no c") }
  // a is not ready; b and c are.
  b.connect()
  b.on_connected()
  c.connect()
  c.on_connected()
  // pick_first takes the first READY one in order — b, not a.
  assert_eq(pool.pick(), Some("b:2"))
  assert_eq(pool.pick(), Some("b:2"))
}
