// The gRPC status model: the 17 canonical `grpc-status` codes, their numeric values
// and canonical names, and the `Method` descriptor whose `path()` is the HTTP/2
// `:path` a call is routed on. This prints the whole status table and a couple of
// method paths.
//
//     moon run examples/06-status-codes

///|
#coverage.skip
fn main {
  let codes : Array[@moonrpc.Status] = [
    Ok,
    Cancelled,
    Unknown,
    InvalidArgument,
    DeadlineExceeded,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
    Unauthenticated,
  ]
  println("grpc-status codes:")
  for s in codes {
    println("  \{s.code()}  \{s.name()}")
  }

  // A path an unmatched route falls back to (UNIMPLEMENTED = 12), and the success
  // code a trailers-only OK reply carries.
  let ok : @moonrpc.Status = Ok
  let unimpl : @moonrpc.Status = Unimplemented
  println(
    "success = \{ok.code()}/\{ok.name()}, unrouted = \{unimpl.code()}/\{unimpl.name()}",
  )

  // A `Method` is `package.Service` + method name; `path()` is the `:path` header the
  // call rides on.
  let greeter : @moonrpc.Method = { service: "greet.Greeter", name: "SayHello" }
  let health : @moonrpc.Method = {
    service: "grpc.health.v1.Health",
    name: "Check",
  }
  println("method path: \{greeter.path()}")
  println("method path: \{health.path()}")
}
