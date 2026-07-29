// Tests for the ServerReflection service against its descriptor database, at the
// message level: ListServices enumerates registered services, FileContainingSymbol
// and FileByFilename return the real FileDescriptorProto bytes (which decode back to
// the registered file), and the miss / unsupported / malformed paths return the
// right ErrorResponse codes.

///|
/// A reflection service holding the greet file descriptor.
fn greet_reflection() -> ReflectionService {
  let refl = ReflectionService::new()
  refl.add_file(greet_file())
  refl
}

///|
test "ServerReflectionRequest: oneof arms round-trip" {
  let cases : Array[ReflectionRequest] = [
    ListServices("*"),
    FileByFilename("greet.proto"),
    FileContainingSymbol("greet.Greeter"),
  ]
  for req in cases {
    let back = decode_reflection_request(encode_reflection_request(req))
    assert_eq(back == req, true)
  }
}

///|
test "ListServices returns every registered service" {
  let refl = greet_reflection()
  let resp = refl.handle(encode_reflection_request(ListServices("*")))
  match decode_reflection_response(resp) {
    ServiceList(names) => {
      assert_eq(names.length(), 1)
      assert_eq(names[0] == "greet.Greeter", true)
    }
    _ => abort("expected a ServiceList response")
  }
}

///|
test "FileContainingSymbol returns the descriptor that decodes back to the file" {
  let refl = greet_reflection()
  let resp = refl.handle(
    encode_reflection_request(FileContainingSymbol("greet.Greeter")),
  )
  match decode_reflection_response(resp) {
    FileDescriptors(protos) => {
      assert_eq(protos.length(), 1)
      let file = FileDescriptor::decode(protos[0])
      assert_eq(file == greet_file(), true)
      assert_eq(file.services[0].name == "Greeter", true)
    }
    _ => abort("expected a FileDescriptors response")
  }
}

///|
test "FileContainingSymbol resolves a message symbol too" {
  let refl = greet_reflection()
  let resp = refl.handle(
    encode_reflection_request(FileContainingSymbol("greet.HelloRequest")),
  )
  match decode_reflection_response(resp) {
    FileDescriptors(protos) =>
      assert_eq(FileDescriptor::decode(protos[0]).name == "greet.proto", true)
    _ => abort("expected a FileDescriptors response")
  }
}

///|
test "FileByFilename returns the named file" {
  let refl = greet_reflection()
  let resp = refl.handle(
    encode_reflection_request(FileByFilename("greet.proto")),
  )
  match decode_reflection_response(resp) {
    FileDescriptors(protos) =>
      assert_eq(FileDescriptor::decode(protos[0]) == greet_file(), true)
    _ => abort("expected a FileDescriptors response")
  }
}

///|
test "an unknown symbol yields NOT_FOUND (5)" {
  let refl = greet_reflection()
  let resp = refl.handle(
    encode_reflection_request(FileContainingSymbol("nope.Missing")),
  )
  match decode_reflection_response(resp) {
    ReflectionError(code, _) => assert_eq(code, 5)
    _ => abort("expected an error response")
  }
}

///|
test "an unknown filename yields NOT_FOUND (5)" {
  let refl = greet_reflection()
  let resp = refl.handle(
    encode_reflection_request(FileByFilename("nope.proto")),
  )
  match decode_reflection_response(resp) {
    ReflectionError(code, _) => assert_eq(code, 5)
    _ => abort("expected an error response")
  }
}

///|
test "an unsupported request arm yields UNIMPLEMENTED (12)" {
  let refl = greet_reflection()
  // an empty request has no oneof arm set -> Unsupported.
  let resp = refl.handle(b"")
  match decode_reflection_response(resp) {
    ReflectionError(code, _) => assert_eq(code, 12)
    _ => abort("expected an error response")
  }
}

///|
test "a malformed request is answered, not raised" {
  let refl = greet_reflection()
  // 0x0A declares a length-delimited field 1 of length 0x7F but no payload.
  let resp = refl.handle(b"\x0a\x7f")
  match decode_reflection_response(resp) {
    ReflectionError(code, _) => assert_eq(code, 3)
    _ => abort("expected an error response")
  }
}

///|
test "MUTATION: a corrupted descriptor length breaks the client-side decode" {
  let refl = greet_reflection()
  let resp = refl.handle(
    encode_reflection_request(FileContainingSymbol("greet.Greeter")),
  )
  // pull the descriptor proto out, corrupt its leading length prefix, and confirm
  // decoding the file now fails where the intact bytes succeed.
  let protos = match decode_reflection_response(resp) {
    FileDescriptors(p) => p
    _ => abort("expected a FileDescriptors response")
  }
  let good = protos[0]
  assert_eq(FileDescriptor::decode(good).services[0].name == "Greeter", true)
  let arr = good.to_array()
  arr[1] = (0x7F).to_byte() // inflate the first field's length past the buffer
  let raised = try {
    let _ = FileDescriptor::decode(Bytes::from_array(arr))
    false
  } catch {
    _ => true
  }
  assert_eq(raised, true)
}
