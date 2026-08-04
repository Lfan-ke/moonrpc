// The standard `grpc.health.v1.Health` service: `Check` returns a service's serving
// status. This registers a health service reporting several services, installs it on
// the pure server engine, and runs a `Check` call end to end — encoding the
// `HealthCheckRequest`, driving it over the engines, decoding the `HealthCheckResponse`.
// It also shows the hand-coded message codec and the unknown-service answer.
//
//     moon run examples/18-health

///|
fn drive(
  server : @moonrpc.H2Server,
  client : @moonrpc.H2Client,
  id : Int,
  initial : Array[@moonrpc.Frame],
) -> Unit raise {
  let to_server : Array[@moonrpc.Frame] = []
  let to_client : Array[@moonrpc.Frame] = []
  for f in client.preface() {
    to_server.push(f)
  }
  for f in server.preface() {
    to_client.push(f)
  }
  for f in initial {
    to_server.push(f)
  }
  let mut si = 0
  let mut ci = 0
  let mut steps = 0
  while !client.is_done(id) && steps < 100000 {
    steps = steps + 1
    if si < to_server.length() {
      let f = to_server[si]
      si = si + 1
      for r in server.feed(f) {
        to_client.push(r)
      }
    } else if ci < to_client.length() {
      let f = to_client[ci]
      ci = ci + 1
      for r in client.feed(f) {
        to_server.push(r)
      }
    } else {
      break
    }
  }
}

///|
fn status_name(s : @moonrpc.ServingStatus) -> String {
  match s {
    Serving => "SERVING"
    NotServing => "NOT_SERVING"
    ServiceUnknown => "SERVICE_UNKNOWN"
    StatusUnknown => "UNKNOWN"
  }
}

///|
fn ascii(s : String) -> Bytes {
  let buf = Buffer()
  for i = 0; i < s.length(); i = i + 1 {
    buf.write_byte((s[i].to_int() & 0xFF).to_byte())
  }
  buf.to_bytes()
}

///|
fn run() -> Unit raise {
  let health = @moonrpc.HealthService::new()
  health.set_status("greet.Greeter", Serving)
  health.set_status("cache.Cache", NotServing)

  // The status enum maps to and from its protobuf number.
  let serving : @moonrpc.ServingStatus = Serving
  println("SERVING code = \{serving.code()}")
  let round = @moonrpc.ServingStatus::from_code(2)
  println("from_code(2) = \{status_name(round)}")

  // Direct checks against the service map.
  let greeter = status_name(health.check("greet.Greeter"))
  let cache = status_name(health.check("cache.Cache"))
  let unknown = status_name(health.check("who.Knows"))
  println("check greet.Greeter: \{greeter}")
  println("check cache.Cache:   \{cache}")
  println("check who.Knows:     \{unknown}")

  // The same probe over the wire: install on the engine and call `Check`.
  let server = @moonrpc.H2Server::new()
  health.install(server)
  let client = @moonrpc.H2Client::new()
  let req = @moonrpc.encode_health_request(ascii("greet.Greeter"))
  let (id, initial) = client.unary(@moonrpc.health_check_path, req)
  drive(server, client, id, initial)
  let reply = client.reply(id)
  let status = @moonrpc.decode_health_response(reply.messages[0])
  println(
    "Check(greet.Greeter) over the wire: \{status_name(status)} (grpc-status \{reply.grpc_status})",
  )
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
