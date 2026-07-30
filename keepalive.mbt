// The gRPC client keepalive state machine (← gRPC `keepalive_time` / `keepalive_timeout`): keep a
// long-lived connection healthy by sending a PING when it has been idle, and declaring it dead if
// the PING's ACK does not come back in time. This is the pure decision core — it takes the current
// time and answers "ping now?" / "dead now?"; the Channel drives it, sending the actual HTTP/2 PING
// frame and closing the connection. Time is in the caller's unit (the same clock passed throughout).

///|
/// Keepalive state for one connection: the idle threshold that triggers a PING, the ACK deadline
/// that declares the connection dead, and the timers tracking the last activity and any in-flight
/// PING.
pub struct Keepalive {
  keepalive_time : Int64
  keepalive_timeout : Int64
  mut last_activity : Int64
  mut ping_outstanding : Bool
  mut ping_sent_at : Int64
}

///|
/// A keepalive that pings after `keepalive_time` of idleness and gives a PING `keepalive_timeout`
/// to be acknowledged. `now` seeds the activity clock.
pub fn Keepalive::new(
  keepalive_time~ : Int64,
  keepalive_timeout~ : Int64,
  now? : Int64 = 0L,
) -> Keepalive {
  {
    keepalive_time,
    keepalive_timeout,
    last_activity: now,
    ping_outstanding: false,
    ping_sent_at: 0L,
  }
}

///|
/// Record connection activity (a frame sent or received) at `now`, resetting the idle timer.
pub fn Keepalive::on_activity(self : Keepalive, now : Int64) -> Unit {
  self.last_activity = now
}

///|
/// Whether a keepalive PING should be sent at `now`: the connection has been idle at least
/// `keepalive_time` and no PING is already awaiting its ACK.
pub fn Keepalive::should_ping(self : Keepalive, now : Int64) -> Bool {
  !self.ping_outstanding && now - self.last_activity >= self.keepalive_time
}

///|
/// Record that a keepalive PING was sent at `now`.
pub fn Keepalive::on_ping_sent(self : Keepalive, now : Int64) -> Unit {
  self.ping_outstanding = true
  self.ping_sent_at = now
}

///|
/// Record that the PING's ACK arrived at `now` — the connection is alive, and this counts as
/// activity.
pub fn Keepalive::on_ping_ack(self : Keepalive, now : Int64) -> Unit {
  self.ping_outstanding = false
  self.last_activity = now
}

///|
/// Whether the connection is dead at `now`: a PING is outstanding and its ACK has not arrived within
/// `keepalive_timeout` of sending it.
pub fn Keepalive::is_timed_out(self : Keepalive, now : Int64) -> Bool {
  self.ping_outstanding && now - self.ping_sent_at >= self.keepalive_timeout
}
