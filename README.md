<div align="center">

# moonrpc

**A real gRPC implementation for MoonBit — `← gRPC`.**

[![Check and Test](https://github.com/Lfan-ke/moonrpc/actions/workflows/ci.yml/badge.svg)](https://github.com/Lfan-ke/moonrpc/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE)
[![mooncakes](https://img.shields.io/badge/mooncakes-Lfan--ke%2Fmoonrpc-brightgreen)](https://mooncakes.io/docs/Lfan-ke/moonrpc)

</div>

`moonrpc` targets **real gRPC** — not gRPC-Web. Where the MoonBit ecosystem lacks the primitives, we build them: the north star is a self-built **HTTP/2 (RFC 7540)** framing layer with stream multiplexing and **HPACK (RFC 7541)**, carrying `application/grpc+proto` over `h2c`.

`v0.5` **serves all four gRPC call kinds** — unary, server-, client-, and bidirectional-streaming — over the self-built **HTTP/2 (h2c)** transport. A pure, all-backend protocol engine (`H2Server`) drives the frame layer, the stream state machine, complete HPACK, and connection- and stream-level flow control honoured across the whole multi-message exchange; a native `moonbitlang/async` socket driver (`GrpcServer`) pumps the bytes:

```moonbit
let server = @net.GrpcServer::new()

// unary: one request, one reply.
server.register("/greet.Greeter/SayHello", req => handle(req))

// server-streaming: one request, an ordered run of replies.
server.register_server_streaming("/count.C/Up", (ctx, req) => [
  first(req), second(req), third(req),
])

// client-streaming: many requests collected, one reply at half-close.
server.register_client_streaming("/sum.S/Add", (ctx, msgs) => fold(msgs))

// bidi: each request echoed as it arrives, a farewell at half-close.
server.register_bidi("/chat.C/Echo", ctx => @moonrpc.BidiHandler::{
  on_message: m => [reply_to(m)],
  on_end: () => [b"bye"],
})

server.serve(port=50051)   // a real gRPC / in-process client gets the replies
```

The handler sees the call's `RpcContext`: the request metadata, the `grpc-timeout` deadline in milliseconds, and slots for response initial and trailing metadata.

## The Channel client

A `Channel` is one long-lived h2c connection; every call multiplexes over it on its own client-allocated stream id, sharing the connection's HPACK and flow-control state.

```moonbit
let chan = @net.Channel::connect("127.0.0.1", 50051)

// unary.
let reply = chan.unary("/greet.Greeter/SayHello", request)
reply.messages[0]   // the reply message; reply.grpc_status is 0 on success

// server-streaming: one request, every framed reply reassembled in order.
let out = chan.server_streaming("/count.C/Up", request)   // out.messages

// client-streaming: many requests, one reply.
let sum = chan.client_streaming("/sum.S/Add", [a, b, c])

// a deadline: sent as grpc-timeout and enforced locally — if it elapses the
// stream is reset and grpc_status comes back DEADLINE_EXCEEDED (4).
let bounded = chan.unary("/slow.S/Wait", request, timeout_millis=Some(200))
```

Under the Channel, `H2Client` is the same kind of pure, transport-independent engine as the server: it produces the request frames and consumes the response frames, so the whole client path is exercised in-memory on every backend against `H2Server`, and only the socket driver is native.

## Health and interceptors

The standard `grpc.health.v1.Health` service registers in one call, and server interceptors wrap unary and server-streaming handlers:

```moonbit
let health = @moonrpc.HealthService::new()
health.set_status("greet.Greeter", @moonrpc.Serving)
server.register_health(health)      // Check + Watch on /grpc.health.v1.Health/*

engine.add_unary_interceptor((ctx, req, next) => {
  // inspect, then proceed — or return without calling next to short-circuit.
  next(ctx, req)
})
```

## Protobuf and Server Reflection

A pure protobuf wire runtime carries the proto3 binary format — varint, zigzag, `fixed32`/`fixed64`, and length-delimited fields — as a `PbWriter`/`PbReader` pair that every message serialises through on every backend:

```moonbit
let w = @moonrpc.PbWriter::new()
w.string_(1, "testing")          // field 1, a length-delimited string
w.int32(2, -1)                   // field 2, a varint (negative -> ten octets)
let r = @moonrpc.PbReader::new(w.to_bytes())
let (field, wire) = r.read_tag() // (1, LengthDelim)
r.read_string()                  // "testing"
```

On top of it, a descriptor model builds and parses the `FileDescriptorProto` / `FileDescriptorSet` fields the reflection path uses — package, message names and fields, services and methods — and the standard `grpc.reflection.v1.ServerReflection` service answers a reflection client over the same h2c transport — `ListServices` to enumerate, `FileContainingSymbol` / `FileByFilename` to describe:

```moonbit
let greeter : @moonrpc.ServiceDescriptor = {
  name: "Greeter",
  methods: [
    { name: "SayHello", input_type: ".greet.HelloRequest",
      output_type: ".greet.HelloReply",
      client_streaming: false, server_streaming: false },
  ],
}
let refl = @moonrpc.ReflectionService::new()
refl.add_file(@moonrpc.FileDescriptor::new("greet.proto", "greet", services=[greeter]))
server.register_reflection(refl)   // a reflection client now lists + describes greet.Greeter
```

Under the hood, everything below `serve` is a **pure, transport-independent engine** — `feed` turns a stream of decoded frames into the frames to write back, so the whole server path (HPACK, flow control, dispatch, HEADERS + DATA + `grpc-status` trailers) is exercised in-memory on **every backend**; only the socket driver is native.

```moonbit
let engine = @moonrpc.H2Server::new()
engine.register("/echo.Echo/Say", req => req)
let out = engine.feed(frame)   // -> [HEADERS(:status 200), DATA(reply), HEADERS(grpc-status:0)]
```

It builds on the load-bearing transport primitives, all verified across every backend:

## The gRPC message framing

```moonbit
let frame = @moonrpc.encode_message(b"hello grpc")
// [0x00][0x00 0x00 0x00 0x0A]["hello grpc"]  — 1-byte flag + 4-byte big-endian length + payload

let (compressed, payload) = @moonrpc.decode_message(frame).unwrap()
// (false, b"hello grpc")
```

This is the *Length-Prefixed-Message* framing shared by gRPC-Web (over HTTP/1.1) and real gRPC (over HTTP/2), so the transport can be swapped underneath it without touching the codec.

## The HTTP/2 frame layer (RFC 7540)

All ten frame types encode to and decode from their exact wire bytes — a byte-level, exhaustively round-trippable codec that the multiplexer will layer on top without touching:

```moonbit
let f = @moonrpc.Frame::Headers(
  stream_id=1, fragment=block, end_stream=true, end_headers=true,
  priority=None, padding=0,
)
let bytes = f.encode()                       // 9-octet header + payload
let (frame, consumed) = @moonrpc.decode_frame(bytes)   // raises Incomplete until whole

@moonrpc.has_connection_preface(buf)         // PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n (§3.5)
```

The stream state machine drives the §5.1 lifecycle (idle / open / half-closed / closed) off `END_STREAM` / push / reset, and rejects illegal transitions:

```moonbit
let s = @moonrpc.Stream::new(1)
s.send(@moonrpc.StreamEvent::Headers(end_stream=false))  // -> Open
s.recv(@moonrpc.StreamEvent::Data(end_stream=true))      // -> HalfClosedRemote
```

## Complete HPACK (RFC 7541)

The full header-compression stack: the 61-entry static table, prefix integers, Huffman coding (Appendix B), and the size-bounded **dynamic table with eviction** — driven by a stateful encoder/decoder pair over the six field representations. Verified byte-for-byte against the Appendix C worked examples.

```moonbit
let enc = @moonrpc.HpackEncoder::new(huffman=true)
let block = enc.encode([{ name: b":method", value: b"GET" },
                        { name: b":authority", value: b"www.example.com" }])

let dec = @moonrpc.HpackDecoder::new()
let headers = dec.decode(block)              // back to the same header list

@moonrpc.huffman_encode(b"www.example.com")  // f1e3 c2e5 f23a 6ba0 ab90 f4ff — §C.4.1
```

## The status model

```moonbit
@moonrpc.Status::code(NotFound)          // 5
@moonrpc.Status::name(Unauthenticated)   // "UNAUTHENTICATED"   (all 17 canonical grpc-status codes)

let m : @moonrpc.Method = { service: "greet.Greeter", name: "SayHello" }
m.path()                                 // "/greet.Greeter/SayHello"
```

## Roadmap (the self-built stack, sequenced to completeness)

`v0` = framing + status + method descriptors; `v0.2` = the first **HPACK** primitives (static table + integer representation + non-Huffman string literals). `v0.3` self-builds the load-bearing transport primitives: the **HTTP/2 frame layer** (all ten types — DATA / HEADERS / PRIORITY / RST_STREAM / SETTINGS / PUSH_PROMISE / PING / GOAWAY / WINDOW_UPDATE / CONTINUATION — with flags and payloads), the connection preface, the **stream state machine** (§5.1 + §5.1.1 id rules), and **complete HPACK** (Huffman coding + the dynamic table with eviction + the stateful encoder/decoder).

`v0.4` makes the server **actually run**: the `H2Server` engine reads frames off a connection, demultiplexes by stream id, drives the per-stream state machine, exchanges SETTINGS, and honours connection- and stream-level flow-control windows with WINDOW_UPDATE; it receives a request stream, dispatches to a registered `(Bytes) -> Bytes` handler, and responds with HEADERS (`:status 200`, `grpc-encoding`) + DATA + trailer HEADERS (`grpc-status`). The native `GrpcServer` binds this engine to real `moonbitlang/async` TCP sockets, proven by an in-process h2c client that makes a unary call end-to-end. Note: the async TLS layer exposes no ALPN, so `h2` runs via **h2c** (prior-knowledge) until an ALPN hook lands upstream.

`v0.5` adds the **streaming** modes on the same engine. A method is registered as one of four cardinalities — unary, server-streaming (`(ctx, req) -> [reply]`), client-streaming (`(ctx, [req]) -> reply`), or bidi (a `BidiHandler` whose `on_message` fires per request message and whose `on_end` fires at half-close). The engine reassembles length-prefixed messages out of the DATA stream, routes each to the handler, and frames every produced message as its own length-prefixed DATA — flow control is honoured across the whole multi-message exchange, so a reply larger than the window splits and resumes on WINDOW_UPDATE. Request **metadata** and the **`grpc-timeout` deadline** are parsed and surfaced to the handler, which can set response initial and trailing metadata. Two in-process h2c clients — a server-streaming call receiving multiple framed replies in order and a client-streaming call sending many messages for one reply — prove it end-to-end, mutation-verified against the stream terminator and the flow-control windows.

`v0.6` adds the client half and the gRPC cross-cutting services. A real **Channel** — a long-lived, multiplexed h2c connection over a real `@socket.Tcp`, driven by a pure `H2Client` engine symmetric to the server — opens streams (client-allocated odd ids), HPACK-encodes request HEADERS, frames request DATA under the send windows, and reassembles the reply. It performs unary, server- and client-streaming calls against a real `GrpcServer` over an actual socket. The **`grpc.health.v1.Health`** service ships with a hand-coded protobuf codec for its two messages; `Check` is fully served, and `Watch` emits the current status once at subscribe time (a stream that also pushes on every later change needs the async engine variant). Server-side **interceptors** wrap unary and server-streaming handlers in an outermost-first chain that can rewrite the request, post-process the reply, or short-circuit. The **`grpc-timeout` deadline** is now enforced client-side: the Channel races the read loop against the timer and, when it elapses, resets the stream and surfaces `DEADLINE_EXCEEDED`.

`v0.6.1` self-builds the **protobuf wire runtime** and **Server Reflection**. A pure `PbWriter`/`PbReader` pair encodes and decodes the proto3 binary format (varint, zigzag, `fixed32`/`fixed64`, length-delimited, tag packing, unknown-field skipping, and truncation/overflow/group-type rejection). A descriptor model builds and parses the `FileDescriptorProto` / `FileDescriptorSet` subset the reflection path needs — package, message names and fields, services and methods — enough for a self-contained proto (enums, nested types, imports, and options are not yet modelled, so a decoded `protoc` set re-encodes only that subset), and the `grpc.reflection.v1.ServerReflection` service (with its `v1alpha` alias) answers `ListServices`, `FileContainingSymbol`, and `FileByFilename` as a bidi stream, returning the real descriptor bytes. An in-process reflection client, over an actual socket, lists and describes a registered service end-to-end, and the varint/descriptor path is mutation-verified.

Delivered since: `grpcurl` interop in CI, `-bin` metadata round-trip, rich errors (`google.rpc.Status`), per-message gzip decompression on both sides, and client-side retry with exponential backoff.

Also delivered on the server: what the engine now **refuses**. A request whose request line is not gRPC's — `:method` other than POST, a `content-type` that does not begin `application/grpc`, a missing `te: trailers` — is answered with an HTTP status (415 / 405 / 400) instead of the 200 a plain HTTP client would read as success. `SETTINGS_MAX_CONCURRENT_STREAMS` is advertised in the server preface and enforced on arrival, with REFUSED_STREAM for the stream over the limit. Received DATA is charged against the connection window before anything can refuse the frame, and overrunning either window is a FLOW_CONTROL_ERROR. A field block must arrive contiguously (§6.10). And the `grpc-timeout` deadline is enforced server-side against a clock the driver installs: a call already out of time never reaches its handler, one whose handler outran it never sends the reply, and `H2Server::tick` closes out a call that expired while it waited. Enforcement is not preemptive — a synchronous handler already running is not interrupted — which is what the concurrent read-demux engine below is for.

Next: gzip response compression (the encode direction), channelz, connection keepalive and pooling, DNS resolution with connectivity-aware load-balancing, and full descriptor fidelity — enums, nested types, imports — with transitive-dependency reflection. The concurrent read-demux engine variant, which underpins interactive bidi, push-based health `Watch`, and preemptive server-side deadlines, is tracked as one piece.

## License

Apache-2.0.
