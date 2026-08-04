// Server Reflection (`grpc.reflection.v1.ServerReflection`): a reflection client opens
// the bidi stream and sends a `ServerReflectionRequest` per query — `ListServices` to
// enumerate, `FileContainingSymbol` to fetch the descriptor for a symbol. This registers
// a real file descriptor, installs the service on the engine, and drives both queries,
// decoding each `ServerReflectionResponse`.
//
//     moon run examples/19-reflection

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
/// Drive one reflection request as its own bidi stream and decode the response.
fn reflect(
  server : @moonrpc.H2Server,
  client : @moonrpc.H2Client,
  request : @moonrpc.ReflectionRequest,
) -> @moonrpc.ReflectionResponse raise {
  let id = client.open(@moonrpc.reflection_v1_path)
  let initial : Array[@moonrpc.Frame] = []
  for
    f in client.send(id, @moonrpc.encode_reflection_request(request), end=true) {
    initial.push(f)
  }
  drive(server, client, id, initial)
  @moonrpc.decode_reflection_response(client.reply(id).messages[0])
}

///|
fn run() -> Unit raise {
  // A file descriptor for one service, indexed by the reflection database.
  let greeter : @moonrpc.ServiceDescriptor = {
    name: "Greeter",
    methods: [
      {
        name: "SayHello",
        input_type: ".greet.HelloRequest",
        output_type: ".greet.HelloReply",
        client_streaming: false,
        server_streaming: false,
      },
    ],
  }
  let refl = @moonrpc.ReflectionService::new()
  refl.add_file(
    @moonrpc.FileDescriptor::new("greet.proto", "greet", services=[greeter]),
  )
  let server = @moonrpc.H2Server::new()
  refl.install(server)
  let client = @moonrpc.H2Client::new()

  // ListServices enumerates every registered service.
  match reflect(server, client, ListServices("")) {
    ServiceList(names) => {
      println("ListServices -> \{names.length()} service(s):")
      for n in names {
        println("  \{n}")
      }
    }
    _ => println("unexpected response shape")
  }

  // FileContainingSymbol returns the descriptor bytes; decode them back to a file.
  match reflect(server, client, FileContainingSymbol("greet.Greeter")) {
    FileDescriptors(protos) => {
      let file = @moonrpc.FileDescriptor::decode(protos[0])
      println(
        "FileContainingSymbol(greet.Greeter) -> \{file.name} (package \{file.package_})",
      )
      for s in file.service_names() {
        println("  defines service \{s}")
      }
    }
    _ => println("unexpected response shape")
  }

  // A missing symbol comes back as an ErrorResponse, not a raise.
  match reflect(server, client, FileContainingSymbol("nope.Nope")) {
    ReflectionError(code, message) =>
      println("FileContainingSymbol(nope.Nope) -> error \{code}: \{message}")
    _ => println("unexpected response shape")
  }
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
