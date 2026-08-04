// Client keepalive as a pure state machine over a clock: send a PING once the connection
// has been idle for `keepalive_time`, and declare it dead if the ACK does not return within
// `keepalive_timeout`. This walks a millisecond timeline through a healthy ping/ack cycle
// and then a missed ACK that times the connection out — the decisions the Channel drives
// (it sends the real PING frame and closes the socket).
//
//     moon run examples/22-keepalive

///|
fn run() -> Unit {
  // Ping after 1000 ms idle; give the ACK 500 ms.
  let ka = @moonrpc.Keepalive::new(
    keepalive_time=1000L,
    keepalive_timeout=500L,
    now=0L,
  )
  println("keepalive: ping after 1000ms idle, ACK deadline 500ms")
  println("  t=500  should_ping: \{ka.should_ping(500L)} (only 500ms idle)")
  println(
    "  t=1000 should_ping: \{ka.should_ping(1000L)} (idle threshold reached)",
  )

  // Send the PING, get the ACK in time — the connection is healthy.
  ka.on_ping_sent(1000L)
  println("  t=1000 PING sent")
  println("  t=1200 timed_out: \{ka.is_timed_out(1200L)} (ACK still has time)")
  ka.on_ping_ack(1200L)
  println("  t=1200 ACK received -> alive, idle timer reset")
  println(
    "  t=1500 should_ping: \{ka.should_ping(1500L)} (idle timer restarted at ACK)",
  )

  // Real traffic keeps it idle-free; then it goes quiet again and a PING is due.
  ka.on_activity(1800L)
  println("  t=1800 activity recorded")
  println(
    "  t=2800 should_ping: \{ka.should_ping(2800L)} (idle 1000ms since activity)",
  )
  ka.on_ping_sent(2800L)
  println("  t=2800 PING sent, no ACK this time")
  println(
    "  t=3200 timed_out: \{ka.is_timed_out(3200L)} (400ms < 500ms deadline)",
  )
  println(
    "  t=3300 timed_out: \{ka.is_timed_out(3300L)} (500ms deadline hit -> dead)",
  )
}

///|
#coverage.skip
fn main {
  run()
}
