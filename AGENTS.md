`moonrpc` is gRPC for MoonBit: the wire protocol — HTTP/2 framing, HPACK, protobuf, status codes, deadlines, retry and hedging — written from scratch, with a native h2c transport on top. It follows grpc-go in shape and the gRPC and HTTP/2 specs in behaviour.

# Working here

- `moon fmt` before anything else. CI runs `moon fmt && git diff --exit-code`, so an unformatted file fails the build on its own.
- `moon check --target all --deny-warn` is the gate. Warnings are errors, and all four backends (wasm, wasm-gc, js, native) must pass.
- `moon test --target all`. The root suite runs everywhere; `net/` is `supported_targets = "native"` and only runs there.
- `moon info` regenerates `pkg.generated.mbti`. If that file does not change, your edit is not visible to anyone depending on this package, which usually means the refactor was safe. If it does change, read the diff before committing — that is the public interface moving. Only the root package tracks an interface; the examples regenerate their own, which is why those are gitignored.
- CI installs the latest moon on every run, so a toolchain that is behind will disagree with it. Upgrade locally rather than pinning.

# Layout

The root package is pure: no sockets, no async. `frame.mbt`, `hpack.mbt`, `huffman.mbt` and `preface.mbt` are the HTTP/2 codec; `protobuf.mbt`, `descriptor.mbt` and `reflection.mbt` the proto side; `client.mbt` and `server.mbt` the two connection engines that turn frames into calls and back. Policy sits beside them — `retry.mbt`, `hedging.mbt`, `keepalive.mbt`, `loadbalancer.mbt`, `connectivity.mbt`, `interceptor.mbt`, `health.mbt`. `net/` is the only place that touches a socket: `serve.mbt` for the server, `channel.mbt` and `mux_channel.mbt` for the client, `managed_channel.mbt` and `resolver.mbt` for load-balanced dialling. Tests sit beside their subject as `*_wbtest.mbt`; `examples/NN-topic/` are runnable one-file demos.

# Things worth knowing

- The engines are pure state machines: frames in, frames out, no I/O. That is what lets the whole protocol be tested on every backend and driven in-process, and it is worth preserving — a new feature belongs in the engine, with `net/` only carrying bytes.
- A stream that has finished keeps its state but drops its buffers. Callers ask a completed stream for its state, so the map entry has to stay; the request and response bodies do not, or a long-lived connection accumulates one of each per call it has ever served. `H2Client::release` and `H2Server::release` are the full drop, wired in from the drivers with `defer`.
- Closing a socket that another task is parked in `accept` or `read` on does not wake that task — it wedges it, past the reach of cancellation. `graceful_stop` therefore knocks on its own port to wake the accept loop, and the loop closes the listener itself on the way out. Anything else that needs to interrupt a parked driver has to work the same way, through the peer.
- The four call kinds share one path. A bidi handler is a factory per stream, so state belongs in the closure it returns, not in the server.
- `examples/` doubles as the protocol's documentation: each one drives a single mechanism in-process with both engines, no network. A new mechanism should get one.
