// A runnable gRPC server (helloworld-style greeter) over moonrpc's self-built h2c
// stack, used to prove interoperability with a real gRPC client — `grpcurl` lists,
// describes, and calls it through Server Reflection. If moonrpc's frames / HPACK /
// trailers / descriptor bytes diverge from the spec, a foreign client fails here.

///|
/// Unary `greet.Greeter/SayHello`: decode `HelloRequest { name = 1 }`, reply with
/// `HelloReply { message = 1 }`. An empty name ends the call with `INVALID_ARGUMENT`
/// via `ctx.fail`, exercising the non-OK status path against a real client.
fn say_hello(ctx : @moonrpc.RpcContext, req : Bytes) -> Bytes {
  let name = try {
    let r = @moonrpc.PbReader::new(req)
    let mut n = ""
    while !r.eof() {
      let (field, wire) = r.read_tag()
      if field == 1 {
        n = r.read_string()
      } else {
        r.skip(wire)
      }
    }
    n
  } catch {
    _ => ""
  }
  if name == "" {
    ctx.fail(@moonrpc.Status::code(InvalidArgument), "name is required")
    return b""
  }
  let w = @moonrpc.PbWriter::new()
  w.string_(1, "Hello, " + name)
  w.to_bytes()
}

///|
/// The `greet.proto` descriptor served over reflection so grpcurl can discover and
/// call the service without a local `.proto`.
fn greet_file() -> @moonrpc.FileDescriptor {
  let request = @moonrpc.MessageDescriptor::{
    name: "HelloRequest",
    fields: [@moonrpc.FieldDescriptor::scalar("name", 1, @moonrpc.TypeString)],
  }
  let reply = @moonrpc.MessageDescriptor::{
    name: "HelloReply",
    fields: [
      @moonrpc.FieldDescriptor::scalar("message", 1, @moonrpc.TypeString),
    ],
  }
  let greeter = @moonrpc.ServiceDescriptor::{
    name: "Greeter",
    methods: [
      @moonrpc.MethodDescriptor::{
        name: "SayHello",
        input_type: ".greet.HelloRequest",
        output_type: ".greet.HelloReply",
        client_streaming: false,
        server_streaming: false,
      },
    ],
  }
  @moonrpc.FileDescriptor::new(
    "greet.proto",
    "greet",
    messages=[request, reply],
    services=[greeter],
  )
}

///|
fn main {
  let port = match @env.args() {
    [_, p, ..] => @string.parse_int(p[:]) catch { _ => 50051 }
    _ => 50051
  }
  @async.run_async_main(() => {
    let server = @net.GrpcServer::new()
    server.register_unary("/greet.Greeter/SayHello", say_hello)
    let refl = @moonrpc.ReflectionService::new()
    refl.add_file(greet_file())
    server.register_reflection(refl)
    server.serve(host="127.0.0.1", port~)
  })
}
