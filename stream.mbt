// The HTTP/2 stream lifecycle state machine (RFC 7540 §5.1) and the stream-id
// parity rules (§5.1.1). Pure and total: every legal transition is enumerated,
// and any illegal one raises rather than silently mis-stepping.

///|
/// The lifecycle state of a single HTTP/2 stream (RFC 7540 §5.1).
pub(all) enum StreamState {
  Idle
  ReservedLocal
  ReservedRemote
  Open
  HalfClosedLocal
  HalfClosedRemote
  Closed
} derive(Eq, Debug)

///|
/// Prints the RFC 7540 §5.1 state name.
pub impl Show for StreamState with fn output(self, logger) {
  logger.write_string(
    match self {
      Idle => "idle"
      ReservedLocal => "reserved(local)"
      ReservedRemote => "reserved(remote)"
      Open => "open"
      HalfClosedLocal => "half-closed(local)"
      HalfClosedRemote => "half-closed(remote)"
      Closed => "closed"
    },
  )
}

///|
/// A state-changing stream event: the arrival or departure of the frames that
/// drive §5.1 transitions. `Headers`/`Data` carry the `END_STREAM` flag; `Reserve`
/// is a PUSH_PROMISE reserving this (promised) stream. Frames that never change
/// stream state (PRIORITY, WINDOW_UPDATE, SETTINGS, PING) are intentionally absent.
pub(all) enum StreamEvent {
  Headers(end_stream~ : Bool)
  Data(end_stream~ : Bool)
  Reserve
  RstStream
} derive(Eq)

///|
/// An illegal stream transition (RFC 7540 §5.1): a frame not permitted in the
/// current state (typically a `STREAM_CLOSED` or `PROTOCOL_ERROR` condition).
pub suberror StreamError {
  InvalidTransition(String)
} derive(Eq)

///|
/// Prints which transition was refused.
pub impl Show for StreamError with fn output(self, logger) {
  match self {
    InvalidTransition(m) => logger.write_string("InvalidTransition(" + m + ")")
  }
}

///|
fn bad(from : StreamState, dir : String, ev : StreamEvent) -> StreamError {
  let name = match ev {
    Headers(..) => "HEADERS"
    Data(..) => "DATA"
    Reserve => "PUSH_PROMISE"
    RstStream => "RST_STREAM"
  }
  InvalidTransition(
    dir + " " + name + " not allowed in state " + Show::to_string(from),
  )
}

///|
/// The next state after *sending* `ev` from this state (RFC 7540 §5.1, local
/// side). Raises `InvalidTransition` for a frame illegal in the current state.
pub fn StreamState::on_send(
  self : StreamState,
  ev : StreamEvent,
) -> StreamState raise StreamError {
  match (self, ev) {
    (Idle, Headers(end_stream~)) =>
      if end_stream {
        HalfClosedLocal
      } else {
        Open
      }
    (Idle, Reserve) => ReservedLocal
    (ReservedLocal, Headers(end_stream~)) =>
      if end_stream {
        Closed
      } else {
        HalfClosedRemote
      }
    (ReservedLocal, RstStream) => Closed
    (ReservedRemote, RstStream) => Closed
    (Open, Headers(end_stream~)) | (Open, Data(end_stream~)) =>
      if end_stream {
        HalfClosedLocal
      } else {
        Open
      }
    (Open, RstStream) => Closed
    (HalfClosedRemote, Headers(end_stream~))
    | (HalfClosedRemote, Data(end_stream~)) =>
      if end_stream {
        Closed
      } else {
        HalfClosedRemote
      }
    (HalfClosedRemote, RstStream) => Closed
    (HalfClosedLocal, RstStream) => Closed
    _ => raise bad(self, "sending", ev)
  }
}

///|
/// The next state after *receiving* `ev` in this state (RFC 7540 §5.1, remote
/// side — the mirror of `on_send`). Raises `InvalidTransition` on an illegal frame.
pub fn StreamState::on_recv(
  self : StreamState,
  ev : StreamEvent,
) -> StreamState raise StreamError {
  match (self, ev) {
    (Idle, Headers(end_stream~)) =>
      if end_stream {
        HalfClosedRemote
      } else {
        Open
      }
    (Idle, Reserve) => ReservedRemote
    (ReservedRemote, Headers(end_stream~)) =>
      if end_stream {
        Closed
      } else {
        HalfClosedLocal
      }
    (ReservedRemote, RstStream) => Closed
    (ReservedLocal, RstStream) => Closed
    (Open, Headers(end_stream~)) | (Open, Data(end_stream~)) =>
      if end_stream {
        HalfClosedRemote
      } else {
        Open
      }
    (Open, RstStream) => Closed
    (HalfClosedLocal, Headers(end_stream~))
    | (HalfClosedLocal, Data(end_stream~)) =>
      if end_stream {
        Closed
      } else {
        HalfClosedLocal
      }
    (HalfClosedLocal, RstStream) => Closed
    (HalfClosedRemote, RstStream) => Closed
    _ => raise bad(self, "receiving", ev)
  }
}

///|
/// A mutable stream: its identifier and current lifecycle state. `send`/`recv`
/// advance the state in place, raising on an illegal transition.
pub(all) struct Stream {
  id : Int
  mut state : StreamState
}

///|
/// A fresh idle stream with the given identifier.
pub fn Stream::new(id : Int) -> Stream {
  { id, state: Idle, }
}

///|
/// Advance this stream by sending `ev`, returning the new state.
pub fn Stream::send(
  self : Stream,
  ev : StreamEvent,
) -> StreamState raise StreamError {
  self.state = self.state.on_send(ev)
  self.state
}

///|
/// Advance this stream by receiving `ev`, returning the new state.
pub fn Stream::recv(
  self : Stream,
  ev : StreamEvent,
) -> StreamState raise StreamError {
  self.state = self.state.on_recv(ev)
  self.state
}

// -- stream identifier rules (RFC 7540 §5.1.1) ------------------------------

///|
/// Whether `id` is a client-initiated stream: a non-zero odd identifier.
pub fn stream_is_client_initiated(id : Int) -> Bool {
  id > 0 && id % 2 == 1
}

///|
/// Whether `id` is a server-initiated (pushed) stream: a non-zero even identifier.
pub fn stream_is_server_initiated(id : Int) -> Bool {
  id > 0 && id % 2 == 0
}

///|
/// Whether a peer that is a client (`by_client = true`) or server may legally
/// *open* stream `id`: clients use odd ids, servers use even ids, and `0` is the
/// connection control stream, openable by neither (RFC 7540 §5.1.1).
pub fn stream_id_valid_for_initiator(id : Int, by_client~ : Bool) -> Bool {
  if by_client {
    stream_is_client_initiated(id)
  } else {
    stream_is_server_initiated(id)
  }
}
