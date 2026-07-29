// Tests for the descriptor model: a hand-built service descriptor round-trips
// through the `FileDescriptorProto` / `FileDescriptorSet` wire format, the derived
// symbol and service-name sets are right, and corrupting the encoded bytes is
// caught on decode.

///|
/// The descriptor of a `greet` service with one unary method and its two messages,
/// reused across the descriptor and reflection tests.
fn greet_file() -> FileDescriptor {
  let request = MessageDescriptor::{
    name: "HelloRequest",
    fields: [FieldDescriptor::scalar("name", 1, TypeString)],
  }
  let reply = MessageDescriptor::{
    name: "HelloReply",
    fields: [FieldDescriptor::scalar("message", 1, TypeString)],
  }
  let greeter = ServiceDescriptor::{
    name: "Greeter",
    methods: [
      MethodDescriptor::{
        name: "SayHello",
        input_type: ".greet.HelloRequest",
        output_type: ".greet.HelloReply",
        client_streaming: false,
        server_streaming: false,
      },
    ],
  }
  FileDescriptor::new("greet.proto", "greet", messages=[request, reply], services=[
    greeter,
  ])
}

///|
test "FieldType / FieldLabel codes match descriptor.proto" {
  assert_eq(TypeDouble.code(), 1)
  assert_eq(TypeInt32.code(), 5)
  assert_eq(TypeString.code(), 9)
  assert_eq(TypeMessage.code(), 11)
  assert_eq(TypeBytes.code(), 12)
  assert_eq(TypeSint64.code(), 18)
  assert_eq(FieldType::from_code(9) == TypeString, true)
  assert_eq(FieldType::from_code(11) == TypeMessage, true)
  // the removed group type 10 (and any unknown) falls back to TypeMessage.
  assert_eq(FieldType::from_code(10) == TypeMessage, true)
  assert_eq(LabelRepeated.code(), 3)
  assert_eq(FieldLabel::from_code(3) == LabelRepeated, true)
  assert_eq(FieldLabel::from_code(1) == LabelOptional, true)
}

///|
test "FieldDescriptor encodes and decodes, type_name only when composite" {
  let scalar = FieldDescriptor::scalar("name", 1, TypeString)
  assert_eq(FieldDescriptor::decode(scalar.encode()) == scalar, true)
  let msg = FieldDescriptor::{
    name: "author",
    number: 2,
    label: LabelOptional,
    type_: TypeMessage,
    type_name: ".greet.Person",
  }
  let back = FieldDescriptor::decode(msg.encode())
  assert_eq(back == msg, true)
  assert_eq(back.type_name == ".greet.Person", true)
  // a scalar carries no type_name field on the wire.
  assert_eq(scalar.type_name == "", true)
}

///|
test "MethodDescriptor: streaming flags survive the round-trip" {
  let bidi = MethodDescriptor::{
    name: "Chat",
    input_type: ".chat.Msg",
    output_type: ".chat.Msg",
    client_streaming: true,
    server_streaming: true,
  }
  assert_eq(MethodDescriptor::decode(bidi.encode()) == bidi, true)
}

///|
test "FileDescriptor: a full service round-trips deeply" {
  let file = greet_file()
  let back = FileDescriptor::decode(file.encode())
  assert_eq(back == file, true)
  assert_eq(back.syntax == "proto3", true)
  assert_eq(back.services[0].methods[0].name == "SayHello", true)
  assert_eq(back.messages.length(), 2)
}

///|
test "FileDescriptor: symbols and service names" {
  let file = greet_file()
  let syms = file.symbols()
  // package-qualified: one service + two messages.
  assert_eq(syms.contains("greet.Greeter"), true)
  assert_eq(syms.contains("greet.HelloRequest"), true)
  assert_eq(syms.contains("greet.HelloReply"), true)
  let names = file.service_names()
  assert_eq(names.length(), 1)
  assert_eq(names[0] == "greet.Greeter", true)
}

///|
test "FileDescriptorSet: multiple files round-trip" {
  let a = greet_file()
  let b = FileDescriptor::new("health.proto", "grpc.health.v1", services=[
    ServiceDescriptor::{ name: "Health", methods: [] },
  ])
  let set = encode_file_descriptor_set([a, b])
  let back = decode_file_descriptor_set(set)
  assert_eq(back.length(), 2)
  assert_eq(back[0] == a, true)
  assert_eq(back[1] == b, true)
}

///|
test "MUTATION: corrupting a length prefix in an encoded file is caught on decode" {
  let file = greet_file()
  let good = file.encode()
  // the file's first field is `name` (tag 0x0A at index 0, length at index 1).
  // inflate that length past the buffer -> the decoder must reject it.
  assert_eq(good[0].to_int(), 0x0A)
  let corrupt = good.to_array()
  corrupt[1] = (0x7F).to_byte()
  let bad = Bytes::from_array(corrupt)
  let raised = try {
    let _ = FileDescriptor::decode(bad)
    false
  } catch {
    _ => true
  }
  assert_eq(raised, true)
  // with the prefix restored, the same bytes decode cleanly again.
  assert_eq(FileDescriptor::decode(good) == file, true)
}
