name = "Lfan-ke/moonrpc"

version = "0.6.1"

readme = "README.md"

repository = "https://github.com/Lfan-ke/moonrpc"

license = "Apache-2.0"

keywords = [ "grpc", "rpc", "protobuf", "http2", "moonbit" ]

description = "moonrpc — a real gRPC implementation for MoonBit (← gRPC). v0.6.1 self-builds a pure protobuf wire runtime (PbWriter/PbReader for varint, zigzag, fixed32/64, and length-delimited fields, with tag packing, unknown-field skipping, and truncation/overflow/group-type rejection) and Server Reflection: a descriptor model that builds and parses FileDescriptorProto/FileDescriptorSet bytes, and the grpc.reflection.v1.ServerReflection service (with its v1alpha alias) answering ListServices, FileContainingSymbol, and FileByFilename as a bidi stream. An in-process reflection client lists and describes a registered service over a real socket. v0.6 added the client half — a real Channel, a long-lived multiplexed h2c connection over @socket.Tcp driven by a pure H2Client engine symmetric to the server, performing unary and streaming calls against a real GrpcServer — plus the grpc.health.v1.Health service, server interceptors, and client-side grpc-timeout enforcement. The server engine (H2Server) serves all four call kinds over the self-built HTTP/2 (h2c) transport, driving the RFC 7540 frame layer, the stream state machine, complete HPACK (RFC 7541, with Huffman + dynamic table), and connection- and stream-level flow control. Includes the shared length-prefixed framing and the 17-code gRPC status model."

import {
  "moonbitlang/async@0.20.3",
}
