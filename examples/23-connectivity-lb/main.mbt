// Connectivity + load balancing, the pure decision core the Channel drives. Each dialed
// address is a `SubConn` moving IDLE -> CONNECTING -> READY (dropping to TRANSIENT_FAILURE
// on error); a `ConnectionPool` holds them and picks a READY one per call under an
// `LbPolicy`. This walks a pool of three backends through connect/fail/idle events and
// shows `pick_first` vs `round_robin` choosing among the ready ones.
//
//     moon run examples/23-connectivity-lb

///|
fn show_pick(label : String, pick : String?) -> Unit {
  match pick {
    Some(addr) => println("\{label}: \{addr}")
    None => println("\{label}: (none ready)")
  }
}

///|
fn run() -> Unit {
  let backends = ["10.0.0.1:50051", "10.0.0.2:50051", "10.0.0.3:50051"]

  // A round-robin pool. Every sub-connection starts IDLE, so nothing is pickable yet.
  let pool = @moonrpc.ConnectionPool::new(backends, policy=RoundRobin)
  show_pick("pick before any connect", pool.pick())

  // Dial the first two to READY; the third fails and lands in TRANSIENT_FAILURE.
  for addr in [backends[0], backends[1]] {
    match pool.subconn(addr) {
      Some(sc) => {
        sc.connect() // IDLE -> CONNECTING
        sc.on_connected() // CONNECTING -> READY
      }
      None => ()
    }
  }
  match pool.subconn(backends[2]) {
    Some(sc) => {
      sc.connect()
      sc.on_failure() // CONNECTING -> TRANSIENT_FAILURE
    }
    None => ()
  }
  let ready = pool.ready_addresses()
  println("ready backends: \{ready.length()} of \{backends.length()}")
  for addr in ready {
    println("  \{addr}")
  }

  // round_robin cycles through just the READY ones.
  show_pick("round_robin pick #1", pool.pick())
  show_pick("round_robin pick #2", pool.pick())
  show_pick("round_robin pick #3", pool.pick())

  // A READY connection going idle drops out of the ready set.
  match pool.subconn(backends[0]) {
    Some(sc) => {
      sc.on_idle() // READY -> IDLE
      println("backend[0] went idle; is_ready = \{sc.is_ready()}")
    }
    None => ()
  }
  show_pick("round_robin after idle", pool.pick())

  // A standalone LoadBalancer over a fixed address list: pick_first always the head,
  // round_robin cycling.
  let first = @moonrpc.LoadBalancer::new(backends)
  let rr = @moonrpc.LoadBalancer::new(backends, policy=RoundRobin)
  println("load balancer over \{first.size()} addresses:")
  show_pick("  pick_first #1", first.pick())
  show_pick("  pick_first #2", first.pick())
  show_pick("  round_robin #1", rr.pick())
  show_pick("  round_robin #2", rr.pick())
  show_pick("  round_robin #3", rr.pick())
  show_pick("  round_robin #4", rr.pick())
}

///|
#coverage.skip
fn main {
  run()
}
