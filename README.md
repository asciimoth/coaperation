# coaperation

`coaperation` is a zero-dependency Go library for CoAP over UDP and plain TCP.
It supports RFC 7252 messages, RFC 8323 stream framing and signaling, RFC 7959
block-wise transfer, IPv6 multicast discovery, Hop-Limit, and CoRE Link Format.

The library keeps options as an ordered `[]coap.Option`. It does not combine or
discard repeated URI options. Option values are byte slices, so binary
`Uri-Query` and application-defined options are safe.

## UDP example

```go
server, err := coap.ListenUDP("udp", ":5683", coap.UDPConfig{
	Handler: coap.HandlerFunc(func(ctx context.Context, request *coap.Message) (*coap.Message, error) {
		return &coap.Message{Code: coap.Content, Payload: []byte("hello")}, nil
	}),
})
if err != nil {
	log.Fatal(err)
}
defer server.Close()

request, target, err := coap.NewRequest(coap.GET, "coap://[::1]/hello", nil)
if err != nil {
	log.Fatal(err)
}
request.Type = coap.Confirmable
request.RemoteAddr, err = net.ResolveUDPAddr("udp", target.Address())
response, err := server.Request(ctx, request)
```

One `UDPEndpoint` can send and receive requests. `RequestMulticast` returns a
channel of responses until its context ends. `ListenIPv6Multicast` joins
`ff02::fd` on a selected interface and applies a randomized response delay.
`NewUDPEndpoint` accepts the small `PacketConn` interface, so callers can use a
standard `net.PacketConn` or provide a custom datagram transport.

## TCP example

Wrap a connected or accepted `net.Conn`:

```go
conn, err := coap.NewTCPConn(rawConn, coap.TCPConfig{
	MaxMessageSize:    1 << 20,
	BlockWiseTransfer: true,
	Handler:           handler,
})
response, err := conn.Request(ctx, &coap.Message{Code: coap.GET})
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
