// The DNS resolver over a real UDP round-trip against a mock DNS server on localhost.

///|
// A canned response for example.com -> 93.184.216.34, id 0x2a2a, answer name compressed
// back to the question (0xC00C).
let mock_a_response : Bytes = b"\x2a\x2a\x81\x80\x00\x01\x00\x01\x00\x00\x00\x00\x07example\x03com\x00\x00\x01\x00\x01\xc0\x0c\x00\x01\x00\x01\x00\x00\x01\x2c\x00\x04\x5d\xb8\xd8\x22"

///|
async test "resolver: resolves an A record over a real UDP round-trip against a mock server" {
  @async.with_task_group(g => {
    let server = @socket.UdpServer(@socket.Addr::parse("127.0.0.1:15353"))
    let task = g.spawn(() => {
      let buf = FixedArray::make(512, b'\x00')
      let (_, addr) = server.recvfrom(buf)
      server.sendto(mock_a_response, addr)
    })
    @async.sleep(200)
    let addrs = resolve_a("127.0.0.1", 15353, "example.com")
    assert_eq(addrs, ["93.184.216.34"])
    server.close()
    task.cancel()
  })
}

///|
async test "resolver: a reply whose transaction id doesn't match is rejected" {
  @async.with_task_group(g => {
    let server = @socket.UdpServer(@socket.Addr::parse("127.0.0.1:15354"))
    let task = g.spawn(() => {
      let buf = FixedArray::make(512, b'\x00')
      let (_, addr) = server.recvfrom(buf)
      server.sendto(mock_a_response, addr)
    })
    @async.sleep(200)
    // The canned reply carries id 0x2a2a; asking with a different id must reject it.
    let addrs = resolve_a("127.0.0.1", 15354, "example.com", id=0x0001)
    assert_eq(addrs, [])
    server.close()
    task.cancel()
  })
}
