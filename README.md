<div align="center">

# moonrpc

**A real gRPC implementation for MoonBit — `← gRPC`.**

[![Check and Test](https://github.com/Lfan-ke/moonrpc/actions/workflows/ci.yml/badge.svg)](https://github.com/Lfan-ke/moonrpc/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE)
[![mooncakes](https://img.shields.io/badge/mooncakes-Lfan--ke%2Fmoonrpc-brightgreen)](https://mooncakes.io/docs/Lfan-ke/moonrpc)

</div>

`moonrpc` targets **real gRPC** — not gRPC-Web. Where the MoonBit ecosystem lacks the primitives, we build them: the north star is a self-built **HTTP/2 (RFC 7540)** framing layer with stream multiplexing and **HPACK (RFC 7541)**, carrying `application/grpc+proto` over `h2c`.

`v0` lands the wire primitives every gRPC transport shares, verified across all backends:

## The gRPC message framing

```moonbit
let frame = @moonrpc.encode_message(b"hello grpc")
// [0x00][0x00 0x00 0x00 0x0A]["hello grpc"]  — 1-byte flag + 4-byte big-endian length + payload

let (compressed, payload) = @moonrpc.decode_message(frame).unwrap()
// (false, b"hello grpc")
```

This is the *Length-Prefixed-Message* framing shared by gRPC-Web (over HTTP/1.1) and real gRPC (over HTTP/2), so the transport can be swapped underneath it without touching the codec.

## HPACK primitives (RFC 7541)

The first pieces of the self-built header compression: the 61-entry static table, the prefix-integer representation, and non-Huffman string literals — all pure and byte-level.

```moonbit
@moonrpc.hpack_static_entry(2)              // Some((":method", "GET"))     — RFC index 2

@moonrpc.hpack_encode_int(1337, 5)          // [0x1F, 0x9A, 0x0A]           — §5.1 example
let (value, consumed) = @moonrpc.hpack_decode_int(b"\x1F\x9A\x0A", 0, 5)    // (1337, 3)

@moonrpc.hpack_encode_string(b"custom-key") // [0x0A]["custom-key"]         — H=0, 7-bit length prefix
```

## The status model

```moonbit
@moonrpc.Status::code(NotFound)          // 5
@moonrpc.Status::name(Unauthenticated)   // "UNAUTHENTICATED"   (all 17 canonical grpc-status codes)

let m : @moonrpc.Method = { service: "greet.Greeter", name: "SayHello" }
m.path()                                 // "/greet.Greeter/SayHello"
```

## Roadmap (the self-built stack, sequenced to completeness)

`v0` = framing + status + method descriptors; `v0.2` adds the first **HPACK** primitives (static table + integer representation + non-Huffman string literals). Next, the rest of HPACK (dynamic table + Huffman coding + the header-block field representations); the **HTTP/2 frame layer** (SETTINGS / HEADERS / DATA / RST_STREAM / WINDOW_UPDATE / PING / GOAWAY), the stream state machine, multiplexing, and flow control over `moonbitlang/async` TCP sockets; then unary + server / client / bidirectional streaming, full metadata / deadlines / interceptors, and reflection / health / channelz. Note: the async TLS layer exposes no ALPN, so `h2` runs via **h2c** (prior-knowledge / upgrade) until an ALPN hook lands upstream. A gRPC-Web-over-HTTP/1.1 milestone (servable today by `mooncat`) validates the dispatch/codec/status stack while the h2 layer is built.

## License

Apache-2.0.
