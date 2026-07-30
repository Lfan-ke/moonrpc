// The gRPC sub-connection connectivity state machine (← gRPC `connectivity.State`): each address the
// Channel dials is a `SubConn` that moves through IDLE → CONNECTING → READY, drops to
// TRANSIENT_FAILURE on error (then retries via CONNECTING after backoff), and ends at SHUTDOWN. The
// load balancer picks among the sub-connections that are READY; the connection pool holds them. This
// is the pure state core — the Channel calls the transition methods as real socket events happen.

///|
/// A sub-connection's connectivity state (← gRPC `connectivity.State`).
pub(all) enum ConnectivityState {
  Idle
  Connecting
  Ready
  TransientFailure
  Shutdown
} derive(Eq, Debug)

///|
/// One address the Channel dials, tracking its connectivity state.
pub struct SubConn {
  address : String
  mut state : ConnectivityState
}

///|
/// A fresh sub-connection to `address`, starting IDLE.
pub fn SubConn::new(address : String) -> SubConn {
  { address, state: Idle }
}

///|
/// Begin connecting (IDLE or TRANSIENT_FAILURE → CONNECTING). A no-op once SHUTDOWN or already
/// connecting/ready.
pub fn SubConn::connect(self : SubConn) -> Unit {
  match self.state {
    Idle | TransientFailure => self.state = Connecting
    _ => ()
  }
}

///|
/// The connection attempt succeeded (CONNECTING → READY).
pub fn SubConn::on_connected(self : SubConn) -> Unit {
  if self.state is Connecting {
    self.state = Ready
  }
}

///|
/// A connection attempt or an established connection failed (→ TRANSIENT_FAILURE), unless shut down.
pub fn SubConn::on_failure(self : SubConn) -> Unit {
  if self.state != Shutdown {
    self.state = TransientFailure
  }
}

///|
/// A READY connection went idle (READY → IDLE).
pub fn SubConn::on_idle(self : SubConn) -> Unit {
  if self.state is Ready {
    self.state = Idle
  }
}

///|
/// Shut the sub-connection down permanently (→ SHUTDOWN, a terminal state).
pub fn SubConn::shutdown(self : SubConn) -> Unit {
  self.state = Shutdown
}

///|
/// Whether this sub-connection can carry RPCs right now.
pub fn SubConn::is_ready(self : SubConn) -> Bool {
  self.state is Ready
}
