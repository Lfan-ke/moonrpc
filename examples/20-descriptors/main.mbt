// The descriptor model behind reflection: `FieldDescriptor` / `MessageDescriptor` /
// `MethodDescriptor` / `ServiceDescriptor` / `FileDescriptor`, each encoding to and
// decoding from its `descriptor.proto` wire form. This builds a whole `greet.proto`,
// round-trips it through the bytes, lists the symbols it defines, and packs it into a
// `FileDescriptorSet` — the form `protoc --descriptor_set_out` emits.
//
//     moon run examples/20-descriptors

///|
fn run() -> Unit raise {
  // Field-type / label enums map to and from their descriptor.proto numbers.
  let string_ty : @moonrpc.FieldType = TypeString
  println("TypeString code = \{string_ty.code()}")
  let repeated : @moonrpc.FieldLabel = LabelRepeated
  println("LabelRepeated code = \{repeated.code()}")
  let back = @moonrpc.FieldType::from_code(11)
  println("FieldType::from_code(11) is TypeMessage: \{back == TypeMessage}")

  // A request message with a scalar field and a repeated field.
  let hello_request : @moonrpc.MessageDescriptor = {
    name: "HelloRequest",
    fields: [
      @moonrpc.FieldDescriptor::scalar("name", 1, TypeString),
      @moonrpc.FieldDescriptor::scalar(
        "tags",
        2,
        TypeString,
        label=LabelRepeated,
      ),
    ],
  }
  let hello_reply : @moonrpc.MessageDescriptor = {
    name: "HelloReply",
    fields: [@moonrpc.FieldDescriptor::scalar("message", 1, TypeString)],
  }
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
  let file = @moonrpc.FileDescriptor::new(
    "greet.proto",
    "greet",
    messages=[hello_request, hello_reply],
    services=[greeter],
  )

  // Encode the whole file, decode it straight back, and confirm the round-trip.
  let bytes = file.encode()
  let decoded = @moonrpc.FileDescriptor::decode(bytes)
  println(
    "\{file.name}: \{bytes.length()} octets, round-trip \{decoded == file}",
  )
  println("symbols this file defines:")
  for sym in file.symbols() {
    println("  \{sym}")
  }
  println("services:")
  for svc in file.service_names() {
    println("  \{svc}")
  }

  // A FileDescriptorSet bundles several files (here, just one).
  let set = @moonrpc.encode_file_descriptor_set([file])
  let files = @moonrpc.decode_file_descriptor_set(set)
  println(
    "FileDescriptorSet: \{set.length()} octets, \{files.length()} file(s), first is \{files[0].name}",
  )
}

///|
#coverage.skip
fn main {
  run() catch {
    e => println("error: \{e}")
  }
}
