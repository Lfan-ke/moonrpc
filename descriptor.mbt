// A descriptor model for gRPC services and their messages, and the codec that
// turns it into the `FileDescriptorProto` / `FileDescriptorSet` wire bytes of
// `google/protobuf/descriptor.proto`. This is what Server Reflection hands a
// client so `grpcurl` can `list` and `describe` a service: reflection returns the
// serialised `FileDescriptorProto` of the file that defines a symbol. The model is
// built programmatically (a service registers its descriptor in code) and both
// encodes to and decodes from the wire. It carries the fields Server Reflection uses
// — package, message names and fields, services and methods — enough to describe a
// self-contained proto; enum, nested-type, import, and option fields are skipped on
// decode (and so not re-emitted), so a `protoc` set that uses them re-encodes to that
// modelled subset. Field numbers below are the ones fixed by descriptor.proto.

///|
/// The `FieldDescriptorProto.Type` enum (proto3's scalar and composite field
/// types). `TypeGroup` is intentionally absent — groups are removed from proto3.
pub(all) enum FieldType {
  TypeDouble
  TypeFloat
  TypeInt64
  TypeUint64
  TypeInt32
  TypeFixed64
  TypeFixed32
  TypeBool
  TypeString
  TypeMessage
  TypeBytes
  TypeUint32
  TypeEnum
  TypeSfixed32
  TypeSfixed64
  TypeSint32
  TypeSint64
} derive(Eq)

///|
/// The descriptor.proto enum number of a field type.
pub fn FieldType::code(self : FieldType) -> Int {
  match self {
    TypeDouble => 1
    TypeFloat => 2
    TypeInt64 => 3
    TypeUint64 => 4
    TypeInt32 => 5
    TypeFixed64 => 6
    TypeFixed32 => 7
    TypeBool => 8
    TypeString => 9
    TypeMessage => 11
    TypeBytes => 12
    TypeUint32 => 13
    TypeEnum => 14
    TypeSfixed32 => 15
    TypeSfixed64 => 16
    TypeSint32 => 17
    TypeSint64 => 18
  }
}

///|
/// The field type for a descriptor.proto enum number; an unrecognised number
/// (including the removed group type `10`) reads as `TypeMessage`.
pub fn FieldType::from_code(n : Int) -> FieldType {
  match n {
    1 => TypeDouble
    2 => TypeFloat
    3 => TypeInt64
    4 => TypeUint64
    5 => TypeInt32
    6 => TypeFixed64
    7 => TypeFixed32
    8 => TypeBool
    9 => TypeString
    12 => TypeBytes
    13 => TypeUint32
    14 => TypeEnum
    15 => TypeSfixed32
    16 => TypeSfixed64
    17 => TypeSint32
    18 => TypeSint64
    _ => TypeMessage
  }
}

///|
/// The `FieldDescriptorProto.Label`: proto3 fields are `LabelOptional` unless
/// `repeated`.
pub(all) enum FieldLabel {
  LabelOptional
  LabelRequired
  LabelRepeated
} derive(Eq)

///|
/// The descriptor.proto enum number of a label.
pub fn FieldLabel::code(self : FieldLabel) -> Int {
  match self {
    LabelOptional => 1
    LabelRequired => 2
    LabelRepeated => 3
  }
}

///|
/// The label for a descriptor.proto enum number; anything else reads as
/// `LabelOptional`.
pub fn FieldLabel::from_code(n : Int) -> FieldLabel {
  match n {
    2 => LabelRequired
    3 => LabelRepeated
    _ => LabelOptional
  }
}

///|
/// One field of a message (`FieldDescriptorProto`). `type_name` is the
/// fully-qualified name of the referenced message or enum for `TypeMessage` /
/// `TypeEnum`, and empty for scalars.
pub(all) struct FieldDescriptor {
  name : String
  number : Int
  label : FieldLabel
  type_ : FieldType
  type_name : String
} derive(Eq)

///|
/// A field with a scalar type and no composite `type_name`.
pub fn FieldDescriptor::scalar(
  name : String,
  number : Int,
  type_ : FieldType,
  label? : FieldLabel = LabelOptional,
) -> FieldDescriptor {
  { name, number, label, type_, type_name: "" }
}

///|
/// Encode a `FieldDescriptorProto`.
pub fn FieldDescriptor::encode(self : FieldDescriptor) -> Bytes {
  let w = PbWriter::new()
  w.string_(1, self.name)
  w.int32(3, self.number)
  w.enum_(4, self.label.code())
  w.enum_(5, self.type_.code())
  if self.type_name.length() > 0 {
    w.string_(6, self.type_name)
  }
  w.to_bytes()
}

///|
/// Decode a `FieldDescriptorProto`.
pub fn FieldDescriptor::decode(body : Bytes) -> FieldDescriptor raise PbError {
  let r = PbReader::new(body)
  let mut name = ""
  let mut number = 0
  let mut label = LabelOptional
  let mut type_ = TypeMessage
  let mut type_name = ""
  while !r.eof() {
    match r.read_tag() {
      (1, LengthDelim) => name = r.read_string()
      (3, Varint) => number = r.read_int32()
      (4, Varint) => label = FieldLabel::from_code(r.read_int32())
      (5, Varint) => type_ = FieldType::from_code(r.read_int32())
      (6, LengthDelim) => type_name = r.read_string()
      (_, w) => r.skip(w)
    }
  }
  { name, number, label, type_, type_name }
}

///|
/// A message type (`DescriptorProto`): its (simple) name and its fields.
pub(all) struct MessageDescriptor {
  name : String
  fields : Array[FieldDescriptor]
} derive(Eq)

///|
/// Encode a `DescriptorProto`.
pub fn MessageDescriptor::encode(self : MessageDescriptor) -> Bytes {
  let w = PbWriter::new()
  w.string_(1, self.name)
  for f in self.fields {
    w.message_(2, f.encode())
  }
  w.to_bytes()
}

///|
/// Decode a `DescriptorProto`.
pub fn MessageDescriptor::decode(
  body : Bytes,
) -> MessageDescriptor raise PbError {
  let r = PbReader::new(body)
  let mut name = ""
  let fields : Array[FieldDescriptor] = []
  while !r.eof() {
    match r.read_tag() {
      (1, LengthDelim) => name = r.read_string()
      (2, LengthDelim) => fields.push(FieldDescriptor::decode(r.read_bytes()))
      (_, w) => r.skip(w)
    }
  }
  { name, fields }
}

///|
/// One RPC method (`MethodDescriptorProto`): its name, the fully-qualified request
/// and response message names, and the two streaming flags that together pick the
/// call cardinality.
pub(all) struct MethodDescriptor {
  name : String
  input_type : String
  output_type : String
  client_streaming : Bool
  server_streaming : Bool
} derive(Eq)

///|
/// Encode a `MethodDescriptorProto`.
pub fn MethodDescriptor::encode(self : MethodDescriptor) -> Bytes {
  let w = PbWriter::new()
  w.string_(1, self.name)
  w.string_(2, self.input_type)
  w.string_(3, self.output_type)
  if self.client_streaming {
    w.bool_(5, true)
  }
  if self.server_streaming {
    w.bool_(6, true)
  }
  w.to_bytes()
}

///|
/// Decode a `MethodDescriptorProto`.
pub fn MethodDescriptor::decode(body : Bytes) -> MethodDescriptor raise PbError {
  let r = PbReader::new(body)
  let mut name = ""
  let mut input_type = ""
  let mut output_type = ""
  let mut client_streaming = false
  let mut server_streaming = false
  while !r.eof() {
    match r.read_tag() {
      (1, LengthDelim) => name = r.read_string()
      (2, LengthDelim) => input_type = r.read_string()
      (3, LengthDelim) => output_type = r.read_string()
      (5, Varint) => client_streaming = r.read_bool()
      (6, Varint) => server_streaming = r.read_bool()
      (_, w) => r.skip(w)
    }
  }
  { name, input_type, output_type, client_streaming, server_streaming }
}

///|
/// A service (`ServiceDescriptorProto`): its (simple) name and its methods.
pub(all) struct ServiceDescriptor {
  name : String
  methods : Array[MethodDescriptor]
} derive(Eq)

///|
/// Encode a `ServiceDescriptorProto`.
pub fn ServiceDescriptor::encode(self : ServiceDescriptor) -> Bytes {
  let w = PbWriter::new()
  w.string_(1, self.name)
  for m in self.methods {
    w.message_(2, m.encode())
  }
  w.to_bytes()
}

///|
/// Decode a `ServiceDescriptorProto`.
pub fn ServiceDescriptor::decode(
  body : Bytes,
) -> ServiceDescriptor raise PbError {
  let r = PbReader::new(body)
  let mut name = ""
  let methods : Array[MethodDescriptor] = []
  while !r.eof() {
    match r.read_tag() {
      (1, LengthDelim) => name = r.read_string()
      (2, LengthDelim) => methods.push(MethodDescriptor::decode(r.read_bytes()))
      (_, w) => r.skip(w)
    }
  }
  { name, methods }
}

///|
/// A single `.proto` file (`FileDescriptorProto`): its filename, package, the
/// message and service types it defines, and the syntax level. This is the unit
/// Server Reflection returns.
pub(all) struct FileDescriptor {
  name : String
  package_ : String
  messages : Array[MessageDescriptor]
  services : Array[ServiceDescriptor]
  syntax : String
} derive(Eq)

///|
/// A proto3 file with the given filename and package.
pub fn FileDescriptor::new(
  name : String,
  package_ : String,
  messages? : Array[MessageDescriptor] = [],
  services? : Array[ServiceDescriptor] = [],
) -> FileDescriptor {
  { name, package_, messages, services, syntax: "proto3" }
}

///|
/// Encode a `FileDescriptorProto`.
pub fn FileDescriptor::encode(self : FileDescriptor) -> Bytes {
  let w = PbWriter::new()
  w.string_(1, self.name)
  if self.package_.length() > 0 {
    w.string_(2, self.package_)
  }
  for m in self.messages {
    w.message_(4, m.encode())
  }
  for s in self.services {
    w.message_(6, s.encode())
  }
  if self.syntax.length() > 0 {
    w.string_(12, self.syntax)
  }
  w.to_bytes()
}

///|
/// Decode a `FileDescriptorProto`.
pub fn FileDescriptor::decode(body : Bytes) -> FileDescriptor raise PbError {
  let r = PbReader::new(body)
  let mut name = ""
  let mut package_ = ""
  let mut syntax = ""
  let messages : Array[MessageDescriptor] = []
  let services : Array[ServiceDescriptor] = []
  while !r.eof() {
    match r.read_tag() {
      (1, LengthDelim) => name = r.read_string()
      (2, LengthDelim) => package_ = r.read_string()
      (4, LengthDelim) =>
        messages.push(MessageDescriptor::decode(r.read_bytes()))
      (6, LengthDelim) =>
        services.push(ServiceDescriptor::decode(r.read_bytes()))
      (12, LengthDelim) => syntax = r.read_string()
      (_, w) => r.skip(w)
    }
  }
  { name, package_, messages, services, syntax }
}

///|
/// The fully-qualified names this file defines: `package.Service` for each service
/// and `package.Message` for each message. These are the symbols a
/// `FileContainingSymbol` reflection request can resolve to this file.
pub fn FileDescriptor::symbols(self : FileDescriptor) -> Array[String] {
  let out : Array[String] = []
  let prefix = if self.package_.length() > 0 { self.package_ + "." } else { "" }
  for s in self.services {
    out.push(prefix + s.name)
  }
  for m in self.messages {
    out.push(prefix + m.name)
  }
  out
}

///|
/// The fully-qualified names of the services this file defines (`package.Service`).
pub fn FileDescriptor::service_names(self : FileDescriptor) -> Array[String] {
  let out : Array[String] = []
  let prefix = if self.package_.length() > 0 { self.package_ + "." } else { "" }
  for s in self.services {
    out.push(prefix + s.name)
  }
  out
}

///|
/// Encode a `FileDescriptorSet` (`protoc --descriptor_set_out`): the concatenation
/// of `FileDescriptorProto`s under repeated field 1.
pub fn encode_file_descriptor_set(files : Array[FileDescriptor]) -> Bytes {
  let w = PbWriter::new()
  for f in files {
    w.message_(1, f.encode())
  }
  w.to_bytes()
}

///|
/// Decode a `FileDescriptorSet` into its files.
pub fn decode_file_descriptor_set(
  body : Bytes,
) -> Array[FileDescriptor] raise PbError {
  let r = PbReader::new(body)
  let files : Array[FileDescriptor] = []
  while !r.eof() {
    match r.read_tag() {
      (1, LengthDelim) => files.push(FileDescriptor::decode(r.read_bytes()))
      (_, w) => r.skip(w)
    }
  }
  files
}
