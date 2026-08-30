# Examples

A runnable tour of moonrpc — every feature the library exposes, exercised end to end
with no server and no socket. The transport codecs assemble and take apart the bytes a
gRPC call is made of; the call examples drive the pure client engine (`H2Client`) against
the pure server engine (`H2Server`) in memory, so every backend runs the whole path.

```bash
moon run examples/07-unary-call
```

| # | Example | What it shows |
| --- | --- | --- |
| 00 | [`unary-on-the-wire`](00-unary-on-the-wire/) | The exact HEADERS + DATA bytes of one unary `SayHello`, assembled from the frame, HPACK, framing, and protobuf codecs, then decoded back |
| 01 | [`message-framing`](01-message-framing/) | The length-prefixed gRPC message framing: flag octet + 4-byte length + payload, with the incomplete-buffer and max-size guards |
| 02 | [`http2-frames`](02-http2-frames/) | All ten HTTP/2 frame types (RFC 7540) round-tripping through `encode` / `decode_frame`, plus the bare `FrameHeader` and the connection preface |
| 03 | [`stream-lifecycle`](03-stream-lifecycle/) | The stream state machine (§5.1) walking idle → open → half-closed → closed, a rejected illegal transition, and the §5.1.1 id rules |
| 04 | [`hpack`](04-hpack/) | Complete HPACK (RFC 7541): static table, prefix integers, Huffman string literals, the dynamic table with eviction, and the stateful encoder/decoder |
| 05 | [`protobuf-wire`](05-protobuf-wire/) | The protobuf wire runtime: `PbWriter` / `PbReader` over varints, zigzag, fixed32/64, length-delimited, packed repeated, and `map<string,string>` |
| 06 | [`status-codes`](06-status-codes/) | The 17 canonical `grpc-status` codes with their numbers and names, and the `Method::path` routing key |
| 07 | [`unary-call`](07-unary-call/) | One unary call end to end over the two pure engines: request frames, reply, `grpc-status`, and the UNIMPLEMENTED fallback for an unrouted path |
| 08 | [`server-streaming`](08-server-streaming/) | Server streaming: one request, an ordered run of replies reassembled off one `CallReply` |
| 09 | [`client-streaming`](09-client-streaming/) | Client streaming: many request messages sent then half-closed, one reply the handler folds from the batch |
| 10 | [`bidi-streaming`](10-bidi-streaming/) | Bidirectional streaming: a `BidiHandler` answering each message as it arrives and a farewell at half-close |
| 11 | [`multiplexing`](11-multiplexing/) | Two concurrent calls over one connection on their own odd stream ids, sharing HPACK and flow-control state |
| 12 | [`flow-control`](12-flow-control/) | A ~100 KB reply segmented to the max-frame size and carried across the receive window with WINDOW_UPDATE replenishment |
| 13 | [`metadata`](13-metadata/) | Request metadata surfaced through `RpcContext`, response initial/trailing metadata, and the base64 `-bin` round-trip |
| 14 | [`rich-errors`](14-rich-errors/) | `RpcContext::fail` + `add_error_detail`, the percent-decoded `grpc-message`, and the `google.rpc.Status` packed in `grpc-status-details-bin` |
| 15 | [`deadlines`](15-deadlines/) | The `grpc-timeout` codec across every RFC unit and the deadline surfaced to the handler as `RpcContext::deadline_millis` |
| 16 | [`gzip`](16-gzip/) | `gunzip` inflating real gzip members (fixed / stored / FNAME) and the client transparently inflating a `grpc-encoding: gzip` reply |
| 17 | [`interceptors`](17-interceptors/) | A unary interceptor chain rewriting request and reply, a short-circuiting guard, and a server-streaming interceptor |
| 18 | [`health`](18-health/) | The `grpc.health.v1.Health` service: `Check` end to end, the hand-coded message codec, and the unknown-service answer |
| 19 | [`reflection`](19-reflection/) | `grpc.reflection.v1.ServerReflection`: `ListServices` and `FileContainingSymbol` driven as bidi calls, descriptors decoded back |
| 20 | [`descriptors`](20-descriptors/) | Building a whole `FileDescriptor`, round-tripping it through the `descriptor.proto` wire form, and packing a `FileDescriptorSet` |
| 21 | [`retry-hedging`](21-retry-hedging/) | The retry policy's should-retry decisions and backoff schedule, and the hedging policy's start / commit decisions |
| 22 | [`keepalive`](22-keepalive/) | The keepalive state machine over a clock: a healthy ping/ack cycle and a missed ACK that times the connection out |
| 23 | [`connectivity-lb`](23-connectivity-lb/) | Sub-connection connectivity states and a connection pool picking a READY backend under `pick_first` / `round_robin` |
| 24 | [`dns-resolver`](24-dns-resolver/) | The DNS codec (RFC 1035): encode an A query, decode a response with A/AAAA records and a compression pointer, read the addresses |
| 25 | [`goaway-shutdown`](25-goaway-shutdown/) | Graceful shutdown with GOAWAY: the last-processed stream id, retryable detection for higher streams, and refusing new streams |
| 26 | [`protocol-errors`](26-protocol-errors/) | RFC 9113 §5.4 fault scope: a stream error answered with RST_STREAM while the connection serves on, a connection error answered with GOAWAY, and the statuses a client builds from a call that ends without trailers |
| 27 | [`server-guards`](27-server-guards/) | What a server refuses: a request whose request line is not gRPC's (415 / 405 / 400), a stream over the advertised concurrency limit, DATA past the receive window, a frame interleaved into a field block, and a call that outruns its `grpc-timeout` |

Everything here is pure and runs on every backend (`moon check --target all`); the native
`@net.Channel` and `@net.GrpcServer` drive these same codecs over a real socket.
