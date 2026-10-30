# coaperation

`coaperation` is a zero-dependency Go library for CoAP over UDP and plain TCP.
It supports RFC 7252 messages, RFC 8323 stream framing and signaling, RFC 7959
block-wise transfer, IPv6 multicast discovery, Hop-Limit, and CoRE Link Format.

The library keeps options as an ordered `[]coap.Option`. It does not combine or
discard repeated URI options. Option values are byte slices, so binary
`Uri-Query` and application-defined options are safe.

## Network I/O abstraction

Protocol state is independent of concrete sockets. A UDP endpoint needs only the
small `coap.PacketConn` interface:

```go
type PacketConn interface {
	ReadFrom([]byte) (int, net.Addr, error)
	WriteTo([]byte, net.Addr) (int, error)
	LocalAddr() net.Addr
	Close() error
}
```

`net.PacketConn` and `*net.UDPConn` satisfy this interface. Applications can
also provide an in-memory, simulated, tunneled, or instrumented datagram
transport. TCP uses the standard `net.Conn` interface.

## UDP example

Use an existing standard or custom packet connection:

```go
socket, err := net.ListenPacket("udp", ":5683")
if err != nil {
	log.Fatal(err)
}

server, err := coap.NewUDPEndpoint(socket, coap.UDPConfig{
	Handler: coap.HandlerFunc(func(ctx context.Context, request *coap.Message) (*coap.Message, error) {
		return &coap.Message{Code: coap.Content, Payload: []byte("hello")}, nil
	}),
})
if err != nil {
	log.Fatal(err)
}
defer server.Close()
```

The `ListenUDP` helper creates the socket when direct I/O control is not needed.
A client endpoint uses the same abstraction:

```go
socket, err := net.ListenPacket("udp", ":0")
if err != nil {
	log.Fatal(err)
}
client, err := coap.NewUDPEndpoint(socket, coap.UDPConfig{})
if err != nil {
	log.Fatal(err)
}
defer client.Close()

request, target, err := coap.NewRequest(coap.GET, "coap://[::1]/hello", nil)
if err != nil {
	log.Fatal(err)
}
request.Type = coap.Confirmable
request.RemoteAddr, err = net.ResolveUDPAddr("udp", target.Address())
if err != nil {
	log.Fatal(err)
}
response, err := client.Request(ctx, request)
if err != nil {
	log.Fatal(err)
}
log.Printf("response: %s", response.Payload)
```

One `UDPEndpoint` can send and receive requests. `RequestMulticast` returns a
channel of responses until its context ends. `ListenIPv6Multicast` joins
`ff02::fd` on a selected interface and applies a randomized response delay.

## TCP example

Wrap any connected or accepted `net.Conn`, including `net.Pipe` or an
instrumented connection:

```go
conn, err := coap.NewTCPConn(rawConn, coap.TCPConfig{
	MaxMessageSize:    1 << 20,
	BlockWiseTransfer: true,
	Handler:           handler,
})
if err != nil {
	log.Fatal(err)
}
response, err := conn.Request(ctx, &coap.Message{Code: coap.GET})
if err != nil {
	log.Fatal(err)
}
```

Both sides send CSM immediately. Both sides can send requests on the same
connection. Responses are matched by token and can arrive out of order.

## Complete and raw block transfers

`RequestBody` segments Block1 requests and reassembles Block2 responses.
`NewBlockWiseHandler` gives a server handler complete request bodies and
segments its responses. Call `Request` and use `Block` directly when the
application must inspect each raw block.

## CoRE Link Format

The `github.com/asciimoth/coaperation/linkformat` package parses and serializes
link-format documents. It preserves extension attributes and repeated
attributes.
