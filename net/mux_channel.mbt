// The multiplexed gRPC channel: one h2c connection carrying many calls at once, driven
// by a single background read pump. Where `Channel` handles one call at a time, a
// `MuxChannel` runs a shared read loop that reads every frame off the socket, feeds the
// `@moonrpc.H2Client` engine, and signals the owning call's inbox when its stream
// completes — so concurrent calls, interactive streaming, and keepalive PINGs share the
// connection safely. Socket writes are serialized through a mutex so frames from
// different calls never interleave.

///|
/// A multiplexed channel over one connection: the engine keeps every call's stream state,
/// `inboxes` wakes a waiting call when its stream is done, and `write_lock` serializes
/// frame writes across concurrent callers.
pub struct MuxChannel {
  conn : @socket.Tcp
  engine : @moonrpc.H2Client
  write_lock : @async.Mutex
  inboxes : Map[Int, @aqueue.Queue[Unit]]
}

///|
/// Open a multiplexed channel to `host:port`: connect and send the preface and client
/// SETTINGS. Spawn `read_pump` into a task group to drive it before making calls.
pub async fn MuxChannel::connect(host : String, port : Int) -> MuxChannel {
  let conn = @socket.Tcp::connect(
    @socket.Addr::parse(host + ":" + port.to_string()),
  )
  let engine = @moonrpc.H2Client::new()
  conn.write(@moonrpc.connection_preface)
  let ch = { conn, engine, write_lock: @async.Mutex::Mutex(), inboxes: Map([]) }
  ch.write_locked(engine.preface())
  ch
}

///|
/// Write frames to the socket under the write lock, so concurrent calls never interleave
/// their bytes.
async fn MuxChannel::write_locked(
  self : MuxChannel,
  frames : Array[@moonrpc.Frame],
) -> Unit {
  self.write_lock.acquire()
  defer self.write_lock.release()
  write_frames(self.conn, frames)
}

///|
/// The shared read loop: read a frame, feed it to the engine, write back any control
/// response (SETTINGS ack, PING pong, WINDOW_UPDATE), and wake every call whose stream has
/// completed. Returns when the connection closes. Spawn this once per channel.
pub async fn MuxChannel::read_pump(self : MuxChannel) -> Unit {
  for ;; {
    let frame = read_frame(self.conn) catch { _ => return }
    let out = self.engine.feed(frame) catch { _ => [] }
    if out.length() > 0 {
      self.write_locked(out)
    }
    let done : Array[Int] = []
    for sid, _ in self.inboxes {
      if self.engine.is_done(sid) {
        done.push(sid)
      }
    }
    for sid in done {
      if self.inboxes.get(sid) is Some(q) {
        q.put(())
      }
    }
  }
}

///|
/// Make a unary call, concurrent with any other calls on this channel: register an inbox,
/// send the request under the write lock, and wait for the pump to signal the stream
/// done, then take the reply.
pub async fn MuxChannel::unary(
  self : MuxChannel,
  path : String,
  request : Bytes,
  metadata? : Array[@moonrpc.Header] = [],
  timeout_millis? : Int? = None,
) -> @moonrpc.CallReply {
  let (id, frames) = self.engine.unary(
    path,
    request,
    metadata~,
    timeout_millis~,
  )
  let inbox : @aqueue.Queue[Unit] = @aqueue.Queue::Queue(kind=Unbounded)
  self.inboxes[id] = inbox
  self.write_locked(frames)
  inbox.get()
  self.inboxes.remove(id)
  self.engine.reply(id)
}

///|
/// Close the connection. Outstanding calls are abandoned; the read pump exits.
pub fn MuxChannel::close(self : MuxChannel) -> Unit {
  self.conn.close()
}
