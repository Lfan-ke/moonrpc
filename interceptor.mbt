// Server-side interceptors: a chain wrapped around a method handler, the gRPC
// equivalent of Go's `UnaryServerInterceptor` / `StreamServerInterceptor`. An
// interceptor sees the call context and request, may inspect or replace them, and
// calls `next` to invoke the rest of the chain (ending at the registered handler).
// Registration order is outer-to-inner: the first-registered interceptor runs
// first and wraps everything after it.

///|
/// A unary server interceptor: `(ctx, request, next) -> reply`, where `next` is the
/// remainder of the chain. Call `next(ctx, request)` to proceed, or return without
/// calling it to short-circuit.
pub type UnaryInterceptor = (RpcContext, Bytes, (RpcContext, Bytes) -> Bytes) -> Bytes

///|
/// A server-streaming interceptor: `(ctx, request, next) -> replies`. It can
/// pre-process the request, post-process the reply sequence, or short-circuit.
pub type StreamInterceptor = (
  RpcContext,
  Bytes,
  (RpcContext, Bytes) -> Array[Bytes],
) -> Array[Bytes]

///|
/// Fold a unary interceptor chain around a base handler so the first interceptor in
/// `chain` is the outermost. With an empty chain this is the base handler unchanged.
fn compose_unary(
  chain : Array[UnaryInterceptor],
  base : (RpcContext, Bytes) -> Bytes,
) -> (RpcContext, Bytes) -> Bytes {
  let mut h = base
  for i = chain.length() - 1; i >= 0; i = i - 1 {
    let ic = chain[i]
    let next = h
    h = (ctx, req) => ic(ctx, req, next)
  }
  h
}

///|
/// Fold a server-streaming interceptor chain around a base handler, first
/// interceptor outermost.
fn compose_stream(
  chain : Array[StreamInterceptor],
  base : (RpcContext, Bytes) -> Array[Bytes],
) -> (RpcContext, Bytes) -> Array[Bytes] {
  let mut h = base
  for i = chain.length() - 1; i >= 0; i = i - 1 {
    let ic = chain[i]
    let next = h
    h = (ctx, req) => ic(ctx, req, next)
  }
  h
}

///|
/// Add a unary interceptor to the server's chain. Applies to every unary method;
/// interceptors run in registration order, outermost first.
pub fn H2Server::add_unary_interceptor(
  self : H2Server,
  interceptor : UnaryInterceptor,
) -> Unit {
  self.unary_interceptors.push(interceptor)
}

///|
/// Add a server-streaming interceptor to the server's chain.
pub fn H2Server::add_stream_interceptor(
  self : H2Server,
  interceptor : StreamInterceptor,
) -> Unit {
  self.stream_interceptors.push(interceptor)
}
