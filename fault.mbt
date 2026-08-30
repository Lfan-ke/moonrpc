// RFC 9113 §5.4 error handling. A protocol fault has a scope: a stream error kills one
// stream with RST_STREAM and leaves the connection running (§5.4.2), a connection error
// ends the connection after a GOAWAY that names the last stream the peer can count on
// (§5.4.1). The engine classifies a fault where it detects it and hands the driver the
// frames that say so, instead of raising into a driver whose only move is to drop the
// socket and leave the peer guessing.

///|
/// A protocol fault and the scope RFC 9113 §5.4 gives it. `code` is the §7 error code
/// that goes on the wire; `why` is the human half, carried in the GOAWAY debug data.
priv suberror H2Fault {
  StreamFault(id~ : Int, code~ : Int, why~ : String)
  ConnFault(code~ : Int, why~ : String)
}

///|
/// The frames that report a fault to the peer: RST_STREAM on the offending stream, or
/// a GOAWAY naming `last_stream` — the highest stream the sender actually processed —
/// with the reason as debug data.
fn fault_frames(fault : H2Fault, last_stream : Int) -> Array[Frame] {
  match fault {
    StreamFault(id~, code~, ..) => [RstStream(stream_id=id, error_code=code)]
    ConnFault(code~, why~) =>
      [
        GoAway(
          last_stream_id=last_stream,
          error_code=code,
          debug=ascii_to_bytes(why),
        ),
      ]
  }
}

///|
/// The fault a refused §5.1 transition amounts to. A frame arriving on a stream still
/// `Idle` is one that cannot open a stream at all, which §5.1 makes a connection error;
/// on a stream that has already finished receiving it is a stream error of type
/// STREAM_CLOSED and the rest of the connection is unaffected.
fn transition_fault(id : Int, state : StreamState, why : String) -> H2Fault {
  match state {
    Idle => ConnFault(code=error_protocol_error, why~)
    _ => StreamFault(id~, code=error_stream_closed, why~)
  }
}

///|
/// Whether adding `inc` would push a flow-control window past the 2^31-1 ceiling of
/// RFC 9113 §6.9.1. `add_window` saturates there so our own arithmetic stays sane, but
/// a peer that sends us over it has made an error we have to report, not absorb.
fn window_overflows(cur : Int, inc : Int) -> Bool {
  cur.to_int64() + inc.to_int64() > 0x7FFFFFFFL
}
