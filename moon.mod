name = "Lfan-ke/moonrpc"

version = "0.4.0"

readme = "README.md"

repository = "https://github.com/Lfan-ke/moonrpc"

license = "Apache-2.0"

keywords = [ "grpc", "rpc", "protobuf", "http2", "moonbit" ]

description = "moonrpc — a real gRPC implementation for MoonBit (← gRPC). v0.4 serves a real unary gRPC call over a self-built HTTP/2 (h2c) transport: a pure, all-backend server engine (H2Server) drives the RFC 7540 frame layer, the stream state machine, complete HPACK (RFC 7541, with Huffman + dynamic table), and connection- and stream-level flow control, dispatching application/grpc requests to registered handlers and framing HEADERS + DATA + grpc-status trailers; a native moonbitlang/async socket driver (GrpcServer) pumps the bytes. Includes the shared length-prefixed framing and the 17-code gRPC status model."

import {
  "moonbitlang/async@0.20.3",
}
