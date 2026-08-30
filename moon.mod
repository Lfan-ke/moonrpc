name = "Lfan-ke/moonrpc"

version = "0.10.0"

readme = "README.md"

repository = "https://github.com/Lfan-ke/moonrpc"

license = "Apache-2.0"

keywords = [ "grpc", "rpc", "protobuf", "http2", "moonbit" ]

description = "moonrpc — a real gRPC implementation for MoonBit (← gRPC). v0.8.1 adds Channel::stream_take, which drives a streaming call inline — no background reader — and returns once it has collected a bounded number of reply messages, so a caller can consume the first N of an unbounded subscription (etcd's Watch, a long server-streaming feed) and close, keeping the whole exchange in one task. v0.8.0 completes the client channel stack. Load balancing picks a READY sub-connection per call (pick_first or round_robin) from a connection pool that tracks each address through the gRPC connectivity states (IDLE/CONNECTING/READY/TRANSIENT_FAILURE/SHUTDOWN); a ManagedChannel dials several backends over real sockets and routes calls through the picker. Name resolution self-builds a DNS message codec (RFC 1035, A/AAAA with name compression) and a UDP resolver, so dns_connect turns a hostname into the backends it dials. Retry and hedging policies drive re-issue and parallel attempts — hedging fires a fresh attempt every hedging delay and commits on the first fatal status, cancelling the rest. Client keepalive pings an idle connection and closes it on a missed ACK. A multiplexed channel (MuxChannel) carries many concurrent calls over one connection through a shared read pump with per-stream inboxes and serialized writes, drives keepalive, and supports interactive bidirectional streaming — send while receiving, replies read message by message. v0.7.0 adds per-message gzip: a self-built DEFLATE (RFC 1951, stored/fixed/dynamic-Huffman) and gzip (RFC 1952, CRC-32 + ISIZE) reader that decompresses gzip requests on the server and gzip responses on the client, and client-side retry with exponential backoff bounded by the call deadline. It also hardens every decoder against malformed input: the gRPC length prefix, HPACK integer/string, protobuf, and health decoders now reject an out-of-range or overflowing length instead of aborting; flow-control windows saturate rather than wrap; peer SETTINGS are validated; the client reassembles a response header block across CONTINUATION; and header blocks and message sizes are capped. v0.6.1 self-builds a pure protobuf wire runtime (PbWriter/PbReader for varint, zigzag, fixed32/64, and length-delimited fields) and Server Reflection: a descriptor model and the grpc.reflection.v1.ServerReflection service (with its v1alpha alias) answering ListServices, FileContainingSymbol, and FileByFilename. v0.6 added the client half — a real Channel, a long-lived multiplexed h2c connection over @socket.Tcp driven by a pure H2Client engine symmetric to the server — plus the grpc.health.v1.Health service, server interceptors, -bin metadata, google.rpc.Status rich errors, and client-side grpc-timeout enforcement. The server engine (H2Server) serves all four call kinds over the self-built HTTP/2 (h2c) transport, driving the RFC 7540 frame layer, the stream state machine, complete HPACK (RFC 7541, with Huffman + dynamic table), and connection- and stream-level flow control. Includes the shared length-prefixed framing and the 17-code gRPC status model."

import {
  "moonbitlang/async@0.20.3",
}
