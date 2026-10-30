package coap

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestUDPEndpointRequestAndReverseRequest(t *testing.T) {
	server, err := ListenUDP("udp4", "127.0.0.1:0", UDPConfig{Handler: HandlerFunc(func(_ context.Context, m *Message) (*Message, error) {
		return &Message{Code: Content, Payload: append([]byte("server:"), m.Payload...)}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	client, err := ListenUDP("udp4", "127.0.0.1:0", UDPConfig{Handler: HandlerFunc(func(_ context.Context, m *Message) (*Message, error) {
		return &Message{Code: Content, Payload: append([]byte("client:"), m.Payload...)}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	ctx := testContext(t)
	response, err := client.Request(ctx, &Message{Type: Confirmable, Code: PUT, Payload: []byte("x"), RemoteAddr: server.LocalAddr()})
	if err != nil {
		t.Fatal(err)
	}
	if string(response.Payload) != "server:x" {
		t.Fatalf("payload %q", response.Payload)
	}
	response, err = server.Request(ctx, &Message{Type: Confirmable, Code: GET, Payload: []byte("y"), RemoteAddr: client.LocalAddr()})
	if err != nil {
		t.Fatal(err)
	}
	if string(response.Payload) != "client:y" {
		t.Fatalf("payload %q", response.Payload)
	}
}

func TestUDPEndpointLargeMessage(t *testing.T) {
	body := bytes.Repeat([]byte{0x5a}, 40*1024)
	server, err := ListenUDP("udp4", "127.0.0.1:0", UDPConfig{Handler: HandlerFunc(func(_ context.Context, m *Message) (*Message, error) {
		return &Message{Code: Content, Payload: m.Payload}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	client, err := ListenUDP("udp4", "127.0.0.1:0", UDPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	response, err := client.Request(testContext(t), &Message{Type: Confirmable, Code: PUT, Payload: body, RemoteAddr: server.LocalAddr()})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(response.Payload, body) {
		t.Fatalf("received %d bytes", len(response.Payload))
	}
}

func TestTCPBidirectionalOutOfOrderRequests(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	type result struct {
		conn *TCPConn
		err  error
	}
	leftResult, rightResult := make(chan result, 1), make(chan result, 1)
	newSide := func(raw net.Conn, prefix string, out chan<- result) {
		conn, err := NewTCPConn(raw, TCPConfig{BlockWiseTransfer: true, Handler: HandlerFunc(func(_ context.Context, m *Message) (*Message, error) {
			if string(m.Payload) == "slow" {
				time.Sleep(30 * time.Millisecond)
			}
			return &Message{Code: Content, Payload: append([]byte(prefix), m.Payload...)}, nil
		})})
		out <- result{conn, err}
	}
	go newSide(a, "L:", leftResult)
	go newSide(b, "R:", rightResult)
	left, right := <-leftResult, <-rightResult
	if left.err != nil {
		t.Fatal(left.err)
	}
	if right.err != nil {
		t.Fatal(right.err)
	}
	defer func() { _ = left.conn.Close() }()
	defer func() { _ = right.conn.Close() }()
	ctx := testContext(t)

	responses := make(chan string, 3)
	go func() {
		r, e := left.conn.Request(ctx, &Message{Code: GET, Payload: []byte("slow")})
		if e != nil {
			responses <- e.Error()
		} else {
			responses <- string(r.Payload)
		}
	}()
	go func() {
		r, e := left.conn.Request(ctx, &Message{Code: GET, Payload: []byte("fast")})
		if e != nil {
			responses <- e.Error()
		} else {
			responses <- string(r.Payload)
		}
	}()
	go func() {
		r, e := right.conn.Request(ctx, &Message{Code: GET, Payload: []byte("back")})
		if e != nil {
			responses <- e.Error()
		} else {
			responses <- string(r.Payload)
		}
	}()
	want := map[string]bool{"R:slow": true, "R:fast": true, "L:back": true}
	for range 3 {
		got := <-responses
		if !want[got] {
			t.Fatalf("unexpected response %q", got)
		}
		delete(want, got)
	}
	if err := left.conn.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	large := bytes.Repeat([]byte{0x7c}, 40*1024)
	largeResponse, err := left.conn.Request(ctx, &Message{Code: PUT, Payload: large})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(largeResponse.Payload, append([]byte("R:"), large...)) {
		t.Fatalf("large TCP response has %d bytes", len(largeResponse.Payload))
	}
	capabilities, ready := left.conn.Capabilities()
	if !ready || !capabilities.BlockWiseTransfer {
		t.Fatalf("capabilities %#v ready=%v", capabilities, ready)
	}
}

type localRequester struct{ handler Handler }

func (r localRequester) Request(ctx context.Context, request *Message) (*Message, error) {
	request = request.Clone()
	request.LocalAddr = testAddr("server")
	request.RemoteAddr = testAddr("client")
	return r.handler.ServeCOAP(ctx, request)
}

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }

type memoryPacket struct {
	data   []byte
	remote net.Addr
}
type memoryPacketConn struct {
	addr      net.Addr
	peer      *memoryPacketConn
	reads     chan memoryPacket
	closed    chan struct{}
	closeOnce sync.Once
}

func memoryPacketPair() (*memoryPacketConn, *memoryPacketConn) {
	a := &memoryPacketConn{addr: testAddr("memory-a"), reads: make(chan memoryPacket, 16), closed: make(chan struct{})}
	b := &memoryPacketConn{addr: testAddr("memory-b"), reads: make(chan memoryPacket, 16), closed: make(chan struct{})}
	a.peer, b.peer = b, a
	return a, b
}

func (c *memoryPacketConn) ReadFrom(buffer []byte) (int, net.Addr, error) {
	select {
	case packet := <-c.reads:
		return copy(buffer, packet.data), packet.remote, nil
	case <-c.closed:
		return 0, nil, net.ErrClosed
	}
}
func (c *memoryPacketConn) WriteTo(data []byte, _ net.Addr) (int, error) {
	packet := memoryPacket{data: append([]byte(nil), data...), remote: c.addr}
	select {
	case c.peer.reads <- packet:
		return len(data), nil
	case <-c.closed:
		return 0, net.ErrClosed
	case <-c.peer.closed:
		return 0, net.ErrClosed
	}
}
func (c *memoryPacketConn) LocalAddr() net.Addr { return c.addr }
func (c *memoryPacketConn) Close() error        { c.closeOnce.Do(func() { close(c.closed) }); return nil }

func TestUDPEndpointUsesMinimalPacketConn(t *testing.T) {
	a, b := memoryPacketPair()
	server, err := NewUDPEndpoint(a, UDPConfig{Handler: HandlerFunc(func(_ context.Context, request *Message) (*Message, error) {
		return &Message{Code: Content, Payload: append([]byte("memory:"), request.Payload...)}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	client, err := NewUDPEndpoint(b, UDPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	response, err := client.Request(testContext(t), &Message{Type: Confirmable, Code: PUT, Payload: []byte("ok"), RemoteAddr: a.LocalAddr()})
	if err != nil {
		t.Fatal(err)
	}
	if string(response.Payload) != "memory:ok" {
		t.Fatalf("payload %q", response.Payload)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-b.closed:
	default:
		t.Fatal("endpoint did not close the supplied PacketConn")
	}
}

type shortWritePacketConn struct{ *memoryPacketConn }

func (c shortWritePacketConn) WriteTo(data []byte, remote net.Addr) (int, error) {
	if _, err := c.memoryPacketConn.WriteTo(data, remote); err != nil {
		return 0, err
	}
	return len(data) - 1, nil
}

type invalidReadPacketConn struct{ *memoryPacketConn }

func (c invalidReadPacketConn) ReadFrom([]byte) (int, net.Addr, error) { return -1, c.addr, nil }

func TestUDPEndpointRejectsShortPacketWrite(t *testing.T) {
	a, b := memoryPacketPair()
	endpoint, err := NewUDPEndpoint(shortWritePacketConn{a}, UDPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = endpoint.Close(); _ = b.Close() }()
	err = endpoint.Send(&Message{Code: GET, RemoteAddr: b.LocalAddr()})
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("write error: %v", err)
	}
}

func TestUDPEndpointRejectsTypedNilPacketConn(t *testing.T) {
	var conn *memoryPacketConn
	if _, err := NewUDPEndpoint(conn, UDPConfig{}); err == nil {
		t.Fatal("accepted a typed nil PacketConn")
	}
}

func TestUDPEndpointRejectsInvalidPacketReadCount(t *testing.T) {
	a, b := memoryPacketPair()
	endpoint, err := NewUDPEndpoint(invalidReadPacketConn{a}, UDPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = endpoint.Close(); _ = b.Close() }()
	select {
	case <-endpoint.closed:
	case <-time.After(time.Second):
		t.Fatal("invalid read count did not stop endpoint")
	}
	if endpoint.Err() == nil {
		t.Fatal("endpoint did not report invalid read count")
	}
}

func TestCompleteBlockWiseRequestAndResponse(t *testing.T) {
	requestBody := bytes.Repeat([]byte("request-"), 700)
	responseBody := bytes.Repeat([]byte("response-"), 700)
	handler, err := NewBlockWiseHandler(HandlerFunc(func(_ context.Context, request *Message) (*Message, error) {
		if !bytes.Equal(request.Payload, requestBody) {
			t.Fatalf("incomplete request body: %d bytes", len(request.Payload))
		}
		return &Message{Code: Content, Payload: responseBody}, nil
	}), BlockWiseConfig{BlockSize: 128})
	if err != nil {
		t.Fatal(err)
	}
	response, err := RequestBody(testContext(t), localRequester{handler}, &Message{Code: PUT, Payload: requestBody}, TransferConfig{Mode: TransferBlockWise, BlockSize: 256})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(response.Payload, responseBody) {
		t.Fatalf("response body has %d bytes", len(response.Payload))
	}
}

func TestUDPAcknowledgementsAreScopedToPeer(t *testing.T) {
	e := &UDPEndpoint{pending: make(map[string]*udpPending), messageACK: make(map[string]*udpPending)}
	p1 := &udpPending{ack: make(chan struct{}, 1), failures: make(chan error, 1), remote: "peer-1"}
	p2 := &udpPending{ack: make(chan struct{}, 1), failures: make(chan error, 1), remote: "peer-2"}
	if err := e.addPending("one", 7, p1); err != nil {
		t.Fatal(err)
	}
	if err := e.addPending("two", 7, p2); err != nil {
		t.Fatal(err)
	}
	e.handleResponse(&Message{Type: Acknowledgement, MessageID: 7, RemoteAddr: testAddr("peer-1")})
	select {
	case <-p1.ack:
	default:
		t.Fatal("first peer did not receive acknowledgement")
	}
	select {
	case <-p2.ack:
		t.Fatal("second peer received the wrong acknowledgement")
	default:
	}
}

func TestMulticastPendingDoesNotDropBurst(t *testing.T) {
	p := &udpPending{multi: true, notify: make(chan struct{}, 1)}
	for i := range 1000 {
		p.enqueue(&Message{MessageID: uint16(i)})
	}
	for i := range 1000 {
		m := p.dequeue()
		if m == nil || m.MessageID != uint16(i) {
			t.Fatalf("message %d was lost or reordered", i)
		}
	}
	if p.dequeue() != nil {
		t.Fatal("queue is not empty")
	}
}

func TestUDPHandlerContextEndsWithEndpoint(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	server, err := ListenUDP("udp4", "127.0.0.1:0", UDPConfig{Handler: HandlerFunc(func(ctx context.Context, _ *Message) (*Message, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return nil, ctx.Err()
	})})
	if err != nil {
		t.Fatal(err)
	}
	client, err := ListenUDP("udp4", "127.0.0.1:0", UDPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, _ = client.Request(ctx, &Message{Type: Confirmable, Code: GET, RemoteAddr: server.LocalAddr()})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	_ = server.Close()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("handler context was not canceled")
	}
}

func TestUDPSeparateConfirmableResponse(t *testing.T) {
	server, err := ListenUDP("udp4", "127.0.0.1:0", UDPConfig{ACKTimeout: 20 * time.Millisecond, Handler: HandlerFunc(func(context.Context, *Message) (*Message, error) {
		return &Message{Code: Content, Payload: []byte("separate"), Separate: true}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	client, err := ListenUDP("udp4", "127.0.0.1:0", UDPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	response, err := client.Request(testContext(t), &Message{Type: Confirmable, Code: GET, RemoteAddr: server.LocalAddr()})
	if err != nil {
		t.Fatal(err)
	}
	if response.Type != Confirmable || string(response.Payload) != "separate" {
		t.Fatalf("response type=%d payload=%q", response.Type, response.Payload)
	}
}

func TestUDPNStartQueuesConfirmableRequests(t *testing.T) {
	entered, release := make(chan struct{}, 2), make(chan struct{})
	server, err := ListenUDP("udp4", "127.0.0.1:0", UDPConfig{Handler: HandlerFunc(func(context.Context, *Message) (*Message, error) {
		entered <- struct{}{}
		<-release
		return &Message{Code: Content}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	client, err := ListenUDP("udp4", "127.0.0.1:0", UDPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	firstDone := make(chan error, 1)
	go func() {
		_, requestErr := client.Request(testContext(t), &Message{Type: Confirmable, Code: GET, RemoteAddr: server.LocalAddr()})
		firstDone <- requestErr
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first request did not arrive")
	}
	queuedContext, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = client.Request(queuedContext, &Message{Type: Confirmable, Code: GET, RemoteAddr: server.LocalAddr()})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued request error: %v", err)
	}
	select {
	case <-entered:
		t.Fatal("second request bypassed NStart")
	default:
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

type recordingRequester struct {
	request  *Message
	response *Message
}

func (r *recordingRequester) Request(_ context.Context, request *Message) (*Message, error) {
	r.request = request.Clone()
	return r.response.Clone(), nil
}

func TestForcedBlockWiseAddsSingleBlockOption(t *testing.T) {
	r := &recordingRequester{response: &Message{Code: Created}}
	_, err := RequestBody(context.Background(), r, &Message{Code: PUT, Payload: []byte("small")}, TransferConfig{Mode: TransferBlockWise, BlockSize: 64})
	if err != nil {
		t.Fatal(err)
	}
	option, ok := r.request.Option(OptionBlock1)
	if !ok {
		t.Fatal("forced block-wise request has no Block1 option")
	}
	block, err := ParseBlock(option.Value)
	if err != nil || block.Number != 0 || block.More || block.Size != 64 {
		t.Fatalf("block %#v, error %v", block, err)
	}
}

func TestBlockWiseHandlerRejectsWrongSize1(t *testing.T) {
	called := false
	handler, err := NewBlockWiseHandler(HandlerFunc(func(context.Context, *Message) (*Message, error) { called = true; return &Message{Code: Created}, nil }), BlockWiseConfig{})
	if err != nil {
		t.Fatal(err)
	}
	block, _ := (Block{Number: 0, Size: 64}).Option(OptionBlock1)
	request := &Message{Code: PUT, Token: []byte{1}, Payload: []byte("short"), Options: withOptions(nil, block, UintOption(OptionSize1, 20)), LocalAddr: testAddr("server"), RemoteAddr: testAddr("client")}
	response, err := handler.ServeCOAP(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Code != RequestEntityIncomplete || called {
		t.Fatalf("response %s, called=%v", response.Code, called)
	}
}
