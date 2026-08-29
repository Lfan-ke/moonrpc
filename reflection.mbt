// The `grpc.reflection.v1.ServerReflection` service (and its `v1alpha` alias),
// implemented on the pure server engine as a bidirectional-streaming method. A
// reflection client — `grpcurl` being the canonical one — opens the stream and
// sends a `ServerReflectionRequest` per query: `ListServices` to enumerate the
// server's services, or `FileContainingSymbol` / `FileByFilename` to fetch the
// `FileDescriptorProto` bytes that describe a symbol. The service answers each with
// a `ServerReflectionResponse`. Requests and responses ride the protobuf runtime
// (`protobuf.mbt`) and carry real descriptors (`descriptor.mbt`); the field numbers
// below are those of `grpc/reflection/v1/reflection.proto`.

///|
/// The gRPC HTTP/2 path of the v1 reflection stream.
pub let reflection_v1_path : String = "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"

///|
/// The gRPC HTTP/2 path of the legacy v1alpha reflection stream. `grpcurl` tries v1
/// first and falls back to this, so a server registers both.
pub let reflection_v1alpha_path : String = "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"

///|
/// A decoded `ServerReflectionRequest`, reduced to the one `message_request` oneof
/// arm that was set. Extension-number queries are surfaced as `Unsupported`, which
/// the service answers with an `UNIMPLEMENTED` error response.
pub(all) enum ReflectionRequest {
  ListServices(String)
  FileByFilename(String)
  FileContainingSymbol(String)
  Unsupported
} derive(Eq)

///|
/// Decode a `ServerReflectionRequest` down to its `message_request` oneof arm.
/// `host` (field 1) is accepted and ignored; an unset oneof reads as `Unsupported`.
pub fn decode_reflection_request(
  body : Bytes,
) -> ReflectionRequest raise PbError {
  let r = PbReader::new(body)
  let mut out = Unsupported
  while !r.eof() {
    match r.read_tag() {
      (3, LengthDelim) => out = FileByFilename(r.read_string())
      (4, LengthDelim) => out = FileContainingSymbol(r.read_string())
      (7, LengthDelim) => out = ListServices(r.read_string())
      (_, w) => r.skip(w)
    }
  }
  out
}

///|
/// Encode a `ServerReflectionRequest` carrying a single oneof arm. Used by an
/// in-process reflection client (and by the round-trip tests).
pub fn encode_reflection_request(req : ReflectionRequest) -> Bytes {
  let w = PbWriter::new()
  match req {
    ListServices(v) => w.string_(7, v)
    FileByFilename(v) => w.string_(3, v)
    FileContainingSymbol(v) => w.string_(4, v)
    Unsupported => ()
  }
  w.to_bytes()
}

///|
/// A decoded `ServerReflectionResponse`, reduced to the one `message_response` arm.
pub(all) enum ReflectionResponse {
  ServiceList(Array[String])
  FileDescriptors(Array[Bytes])
  ReflectionError(Int, String)
} derive(Eq)

// -- response builders ------------------------------------------------------

///|
/// A `ServiceResponse { string name = 1 }`.
fn encode_service_response(name : String) -> Bytes {
  let w = PbWriter::new()
  w.string_(1, name)
  w.to_bytes()
}

///|
/// A `ServerReflectionResponse` whose `message_response` is a
/// `ListServiceResponse` naming every service, echoing the raw `original_request`.
fn encode_list_services_response(
  services : Array[String],
  original : Bytes,
) -> Bytes {
  let list = PbWriter::new()
  for name in services {
    list.message_(1, encode_service_response(name))
  }
  let w = PbWriter::new()
  w.message_(2, original)
  w.message_(6, list.to_bytes())
  w.to_bytes()
}

///|
/// A `ServerReflectionResponse` whose `message_response` is a
/// `FileDescriptorResponse` carrying the serialised `FileDescriptorProto`s.
fn encode_file_descriptor_response(
  protos : Array[Bytes],
  original : Bytes,
) -> Bytes {
  let fdr = PbWriter::new()
  for p in protos {
    fdr.bytes_(1, p)
  }
  let w = PbWriter::new()
  w.message_(2, original)
  w.message_(4, fdr.to_bytes())
  w.to_bytes()
}

///|
/// A `ServerReflectionResponse` whose `message_response` is an `ErrorResponse`.
fn encode_error_response(
  code : Int,
  message : String,
  original : Bytes,
) -> Bytes {
  let err = PbWriter::new()
  err.int32(1, code)
  err.string_(2, message)
  let w = PbWriter::new()
  w.message_(2, original)
  w.message_(7, err.to_bytes())
  w.to_bytes()
}

///|
/// Decode a `ServerReflectionResponse` down to its `message_response` arm — the
/// half an in-process reflection client (and the tests) needs to read an answer.
pub fn decode_reflection_response(
  body : Bytes,
) -> ReflectionResponse raise PbError {
  let r = PbReader::new(body)
  let mut out = ReflectionError(2, "empty response")
  while !r.eof() {
    match r.read_tag() {
      (4, LengthDelim) => {
        let inner = PbReader::new(r.read_bytes())
        let protos : Array[Bytes] = []
        while !inner.eof() {
          match inner.read_tag() {
            (1, LengthDelim) => protos.push(inner.read_bytes())
            (_, w) => inner.skip(w)
          }
        }
        out = FileDescriptors(protos)
      }
      (6, LengthDelim) => {
        let inner = PbReader::new(r.read_bytes())
        let names : Array[String] = []
        while !inner.eof() {
          match inner.read_tag() {
            (1, LengthDelim) => {
              let svc = PbReader::new(inner.read_bytes())
              let mut name = ""
              while !svc.eof() {
                match svc.read_tag() {
                  (1, LengthDelim) => name = svc.read_string()
                  (_, w) => svc.skip(w)
                }
              }
              names.push(name)
            }
            (_, w) => inner.skip(w)
          }
        }
        out = ServiceList(names)
      }
      (7, LengthDelim) => {
        let inner = PbReader::new(r.read_bytes())
        let mut code = 2
        let mut message = ""
        while !inner.eof() {
          match inner.read_tag() {
            (1, Varint) => code = inner.read_int32()
            (2, LengthDelim) => message = inner.read_string()
            (_, w) => inner.skip(w)
          }
        }
        out = ReflectionError(code, message)
      }
      (_, w) => r.skip(w)
    }
  }
  out
}

// -- the service ------------------------------------------------------------

///|
/// A `grpc.reflection.v1.ServerReflection` service backed by an in-memory
/// descriptor database. Each added `FileDescriptor` is indexed by filename and by
/// every symbol (`package.Service` / `package.Message`) it defines, so a
/// `FileContainingSymbol` or `FileByFilename` query resolves to the right file, and
/// `ListServices` enumerates every registered service.
pub struct ReflectionService {
  files : Array[FileDescriptor]
  by_symbol : Map[String, Int]
  by_filename : Map[String, Int]
  services : Array[String]
}

///|
/// An empty reflection service. Add the descriptors of the services you serve with
/// `add_file`.
pub fn ReflectionService::new() -> ReflectionService {
  { files: [], by_symbol: Map([]), by_filename: Map([]), services: [], }
}

///|
/// Register a file descriptor: index it by filename and by each symbol it defines,
/// and add its services to the `ListServices` set.
pub fn ReflectionService::add_file(
  self : ReflectionService,
  file : FileDescriptor,
) -> Unit {
  let idx = self.files.length()
  self.files.push(file)
  self.by_filename[file.name] = idx
  for sym in file.symbols() {
    self.by_symbol[sym] = idx
  }
  for name in file.service_names() {
    self.services.push(name)
  }
}

///|
/// Answer one `ServerReflectionRequest` (raw bytes) with the encoded
/// `ServerReflectionResponse`. A decode failure or an unknown symbol/filename yields
/// an `ErrorResponse` rather than raising, since it rides a non-raising stream
/// handler.
pub fn ReflectionService::handle(
  self : ReflectionService,
  request : Bytes,
) -> Bytes {
  let parsed = decode_reflection_request(request) catch {
    _ => return encode_error_response(3, "malformed request", request)
  }
  match parsed {
    ListServices(_) => encode_list_services_response(self.services, request)
    FileContainingSymbol(sym) =>
      match self.by_symbol.get(sym) {
        Some(i) =>
          encode_file_descriptor_response([self.files[i].encode()], request)
        None => encode_error_response(5, "symbol not found: " + sym, request)
      }
    FileByFilename(name) =>
      match self.by_filename.get(name) {
        Some(i) =>
          encode_file_descriptor_response([self.files[i].encode()], request)
        None => encode_error_response(5, "file not found: " + name, request)
      }
    Unsupported =>
      encode_error_response(12, "unsupported reflection request", request)
  }
}

///|
/// The bidi handler backing `ServerReflectionInfo`: one response per request, none
/// at half-close.
pub fn ReflectionService::handler(self : ReflectionService) -> Handler {
  Bidi(_ctx => BidiHandler::{
    on_message: m => [self.handle(m)],
    on_end: () => [],
  })
}

///|
/// Register `ServerReflectionInfo` at both the v1 and v1alpha paths on a pure
/// server engine.
pub fn ReflectionService::install(
  self : ReflectionService,
  server : H2Server,
) -> Unit {
  server.register_handler(reflection_v1_path, self.handler())
  server.register_handler(reflection_v1alpha_path, self.handler())
}
