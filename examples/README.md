# Examples

A runnable tour of moonrpc's pure codec layer — the bytes a gRPC call is made
of, assembled and taken apart with no server and no socket.

```bash
moon run examples/00-unary-on-the-wire
```

| # | Example | What it shows | Key API |
| --- | --- | --- | --- |
| 00 | [`unary-on-the-wire`](00-unary-on-the-wire/) | Build the HTTP/2 + HPACK + gRPC bytes of one unary `SayHello` request, then decode them straight back | `Method::path`, `PbWriter::string_`, `encode_message` / `decode_message`, `HpackEncoder` / `HpackDecoder`, `Frame::Headers` / `Frame::Data` / `Frame::encode` / `decode_frame`, `PbReader`, `Status` |

Everything here is pure and runs on every backend (`moon check --target all`);
the native `@net.Channel` and `@net.GrpcServer` drive these same codecs over a
real socket.
