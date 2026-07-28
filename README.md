<div align="center">

# moonrpc

**A real gRPC implementation for MoonBit — `← gRPC`.**

[![Check and Test](https://github.com/Lfan-ke/moonrpc/actions/workflows/ci.yml/badge.svg)](https://github.com/Lfan-ke/moonrpc/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE)
[![mooncakes](https://img.shields.io/badge/mooncakes-Lfan--ke%2Fmoonrpc-brightgreen)](https://mooncakes.io/docs/Lfan-ke/moonrpc)

</div>

`moonrpc` targets **real gRPC** — not gRPC-Web. Where the MoonBit ecosystem lacks the primitives, we build them: the north star is a self-built **HTTP/2 (RFC 7540)** framing layer with stream multiplexing and **HPACK (RFC 7541)**, carrying `application/grpc+proto` over `h2c`.

`v0.4` **serves a real unary gRPC call** over the self-built **HTTP/2 (h2c)** transport. A pure, all-backend protocol engine (`H2Server`) drives the frame layer, the stream state machine, complete HPACK, and connection- and stream-level flow control; a native `moonbitlang/async` socket driver (`GrpcServer`) pumps the bytes:

```moonbit
let server = @net.GrpcServer::new()
server.register("/greet.Greeter/SayHello", req => handle(req))  // (Bytes) -> Bytes
server.serve(port=50051)   // a real gRPC client / in-process client gets the reply
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

`v0.4` makes the server **actually run**: the `H2Server` engine reads frames off a connection, demultiplexes by stream id, drives the per-stream state machine, exchanges SETTINGS, and honours connection- and stream-level flow-control windows with WINDOW_UPDATE; it receives a request stream (HEADERS with HPACK-decoded `:method` / `:path` / `content-type`, then a length-prefixed DATA message), dispatches to a registered `(Bytes) -> Bytes` handler, and responds with HEADERS (`:status 200`, `grpc-encoding`) + DATA + trailer HEADERS (`grpc-status`). The native `GrpcServer` binds this engine to real `moonbitlang/async` TCP sockets, proven by an in-process h2c client that makes a unary call end-to-end (frame + HPACK codecs on both sides), mutation-verified. Note: the async TLS layer exposes no ALPN, so `h2` runs via **h2c** (prior-knowledge) until an ALPN hook lands upstream.

Next: server / client / bidirectional **streaming**, full **metadata** (incl. `-bin`), `grpc-timeout` **deadlines** + cancel, rich errors, per-message gzip, and interceptors; then the **Channel** client stack, and reflection / health / channelz.

## License

Apache-2.0.
