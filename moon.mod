name = "Lfan-ke/moonrpc"

version = "0.6.0"

readme = "README.md"

repository = "https://github.com/Lfan-ke/moonrpc"

license = "Apache-2.0"

keywords = [ "grpc", "rpc", "protobuf", "http2", "moonbit" ]

description = "moonrpc — a real gRPC implementation for MoonBit (← gRPC). v0.6 adds the client half and the cross-cutting services on top of the v0.5 server. A real Channel — a long-lived, multiplexed h2c connection over @socket.Tcp driven by a pure H2Client engine symmetric to the server — opens client streams, HPACK-encodes request HEADERS, frames request DATA under the send windows, and reassembles the reply, performing unary and streaming calls against a real GrpcServer over an actual socket. The grpc.health.v1.Health service (Check + Watch) ships with a hand-coded protobuf codec; server unary and stream interceptors wrap handlers in an outermost-first chain; the grpc-timeout deadline is enforced client-side by racing the read loop against the timer. The server engine (H2Server) still serves all four call kinds over the self-built HTTP/2 (h2c) transport, driving the RFC 7540 frame layer, the stream state machine, complete HPACK (RFC 7541, with Huffman + dynamic table), and connection- and stream-level flow control. Includes the shared length-prefixed framing and the 17-code gRPC status model."

import {
  "moonbitlang/async@0.20.3",
}
