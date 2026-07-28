name = "Lfan-ke/moonrpc"

version = "0.5.0"

readme = "README.md"

repository = "https://github.com/Lfan-ke/moonrpc"

license = "Apache-2.0"

keywords = [ "grpc", "rpc", "protobuf", "http2", "moonbit" ]

description = "moonrpc — a real gRPC implementation for MoonBit (← gRPC). v0.5 serves all four gRPC call kinds — unary, server-, client-, and bidirectional-streaming — over a self-built HTTP/2 (h2c) transport. A pure, all-backend server engine (H2Server) drives the RFC 7540 frame layer, the stream state machine, complete HPACK (RFC 7541, with Huffman + dynamic table), and connection- and stream-level flow control honoured across the multi-message exchange, routing application/grpc requests to a handler of the matching cardinality and framing each produced message as its own length-prefixed DATA closed by grpc-status trailers. Request metadata and the grpc-timeout deadline are surfaced to the handler; a native moonbitlang/async socket driver (GrpcServer) pumps the bytes. Includes the shared length-prefixed framing and the 17-code gRPC status model."

import {
  "moonbitlang/async@0.20.3",
}
