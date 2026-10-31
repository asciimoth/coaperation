package coap

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"math/big"
	"net"
	"testing"
	"time"
)

type stubConn struct {
	bytes.Buffer
	writeErr error
	zero     bool
}

type errorWritePacketConn struct{ *memoryPacketConn }

type multicastMemoryPacketConn struct{ *memoryPacketConn }

func (c multicastMemoryPacketConn) ReadPacket(data []byte) (int, net.Addr, net.Addr, bool, error) {
	n, remote, err := c.ReadFrom(data)
	return n, c.LocalAddr(), remote, true, err
}

func (c errorWritePacketConn) WriteTo([]byte, net.Addr) (int, error) {
	return 0, io.ErrClosedPipe
}

type failAfterWritePacketConn struct {
	*memoryPacketConn
	writes int
}

func (c *failAfterWritePacketConn) WriteTo(data []byte, remote net.Addr) (int, error) {
	c.writes++
	if c.writes > 1 {
		return 0, io.ErrClosedPipe
	}
	return c.memoryPacketConn.WriteTo(data, remote)
}

func (c *stubConn) Read([]byte) (int, error) {
	return 0, io.EOF
}
func (c *stubConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	if c.zero {
		return 0, nil
	}
	return c.Buffer.Write(p)
}
func (c *stubConn) Close() error                     { return nil }
func (c *stubConn) LocalAddr() net.Addr              { return testAddr("local") }
func (c *stubConn) RemoteAddr() net.Addr             { return testAddr("remote") }
func (c *stubConn) SetDeadline(time.Time) error      { return nil }
func (c *stubConn) SetReadDeadline(time.Time) error  { return nil }
func (c *stubConn) SetWriteDeadline(time.Time) error { return nil }

func bareTCP(raw net.Conn) *TCPConn {
	ctx, cancel := context.WithCancel(context.Background())
	return &TCPConn{
		conn: raw, cfg: TCPConfig{MaxMessageSize: 64 * 1024}, pending: make(map[string]chan *Message),
		peer: TCPCapabilities{MaxMessageSize: 1152}, peerReady: make(chan struct{}), closed: make(chan struct{}),
		ctx: ctx, cancel: cancel,
	}
}

func TestTCPValidationAndWriteErrors(t *testing.T) {
	if _, err := NewTCPConn(nil, TCPConfig{}); err == nil {
		t.Fatal("NewTCPConn accepted nil")
	}
	a, b := net.Pipe()
	if _, err := NewTCPConn(a, TCPConfig{MaxMessageSize: 1}); err == nil {
		t.Fatal("NewTCPConn accepted a small maximum")
	}
	_ = a.Close()
	_ = b.Close()
	if conn, err := NewTCPConn(&stubConn{}, TCPConfig{MaxMessageSize: 100}); err != nil {
		t.Fatalf("NewTCPConn rejected a valid small maximum: %v", err)
	} else {
		_ = conn.Close()
	}
	if _, err := DialTCP(context.Background(), "tcp", "invalid address", TCPConfig{}); err == nil {
		t.Fatal("DialTCP accepted an invalid address")
	}
	if err := ServeTCP(nil, TCPConfig{}, nil); err == nil {
		t.Fatal("ServeTCP accepted nil")
	}

	raw := &stubConn{}
	c := bareTCP(raw)
	if c.LocalAddr().String() != "local" || c.RemoteAddr().String() != "remote" {
		t.Fatal("unexpected TCP addresses")
	}
	if capabilities, ready := c.Capabilities(); ready || capabilities.MaxMessageSize != 1152 {
		t.Fatalf("capabilities = %#v, ready=%v", capabilities, ready)
	}
	if err := c.Send(nil); err == nil {
		t.Fatal("Send accepted nil")
	}
	if err := c.Send(&Message{Code: GET, Payload: make([]byte, 1200)}); err == nil {
		t.Fatal("Send ignored the peer maximum")
	}
	if err := c.Send(&Message{Code: CSM, Payload: make([]byte, 1200)}); err == nil {
		t.Fatal("signaling Send ignored the peer maximum")
	}
	c.cfg.MaxMessageSize = 20
	if err := c.write(&Message{Code: CSM, Payload: make([]byte, 30)}, false); err == nil {
		t.Fatal("write ignored the local maximum")
	}
	c.cfg.MaxMessageSize = 64 * 1024
	raw.writeErr = io.ErrClosedPipe
	if err := c.Send(&Message{Code: CSM}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write error = %v", err)
	}
	raw.writeErr, raw.zero = nil, true
	if err := c.Send(&Message{Code: CSM}); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("zero write error = %v", err)
	}
	if _, err := NewTCPConn(&stubConn{writeErr: io.ErrClosedPipe}, TCPConfig{}); err == nil {
		t.Fatal("NewTCPConn ignored the initial CSM write error")
	}
}

func TestTCPRequestAndSignalValidation(t *testing.T) {
	noHandlerRaw := &stubConn{}
	noHandler := bareTCP(noHandlerRaw)
	noHandler.handleRequest(&Message{Code: GET, Token: []byte{1}})
	missingHandlerResponse, err := UnmarshalTCP(noHandlerRaw.Bytes(), 0)
	if err != nil || missingHandlerResponse.Code != NotImplemented || !bytes.Equal(missingHandlerResponse.Token, []byte{1}) {
		t.Fatalf("missing handler response = %#v, %v", missingHandlerResponse, err)
	}
	c := bareTCP(&stubConn{})
	if _, err := c.Request(context.Background(), nil); err == nil {
		t.Fatal("Request accepted nil")
	}
	if _, err := c.Request(context.Background(), &Message{Code: Content}); err == nil {
		t.Fatal("Request accepted a response")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Request(canceled, &Message{Code: GET}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request = %v", err)
	}

	if !c.handleSignal(&Message{Code: CSM, Options: []Option{UintOption(OptionMaxMessageSize, 4096), UintOption(OptionBlockWiseTransfer, 0)}}) {
		t.Fatal("valid CSM stopped the connection")
	}
	capabilities, ready := c.Capabilities()
	if !ready || capabilities.MaxMessageSize != 4096 || !capabilities.BlockWiseTransfer {
		t.Fatalf("capabilities = %#v, ready=%v", capabilities, ready)
	}
	if !c.handleSignal(&Message{Code: CSM}) {
		t.Fatal("empty later CSM stopped the connection")
	}
	capabilities, _ = c.Capabilities()
	if capabilities.MaxMessageSize != 4096 || !capabilities.BlockWiseTransfer {
		t.Fatalf("empty later CSM reset capabilities: %#v", capabilities)
	}
	if !c.handleSignal(&Message{Code: CSM, Options: []Option{UintOption(OptionMaxMessageSize, 2048)}}) {
		t.Fatal("updated CSM stopped the connection")
	}
	capabilities, _ = c.Capabilities()
	if capabilities.MaxMessageSize != 2048 || !capabilities.BlockWiseTransfer {
		t.Fatalf("updated CSM reset block-wise capability: %#v", capabilities)
	}
	if !c.handleSignal(&Message{Code: CSM, Options: []Option{
		UintOption(OptionMaxMessageSize, 1024), NewOption(OptionMaxMessageSize, []byte{0xff, 0xff, 0xff, 0xff, 0xff}),
		NewOption(OptionBlockWiseTransfer, nil), NewOption(OptionBlockWiseTransfer, []byte{1}),
	}}) {
		t.Fatal("later occurrences of elective CSM options stopped the connection")
	}
	capabilities, _ = c.Capabilities()
	if capabilities.MaxMessageSize != 1024 || !capabilities.BlockWiseTransfer {
		t.Fatalf("repeated CSM options changed capabilities: %#v", capabilities)
	}
	responses := make(chan *Message, 1)
	c.pending["token"] = responses
	if !c.handleSignal(&Message{Code: Pong, Token: []byte("token")}) {
		t.Fatal("Pong stopped the connection")
	}
	select {
	case <-responses:
	default:
		t.Fatal("Pong was not dispatched")
	}
	if !c.handleSignal(&Message{Code: Pong, Token: []byte("absent")}) {
		t.Fatal("unmatched Pong stopped the connection")
	}
	if !c.handleSignal(&Message{Code: Ping, Token: []byte("ping"), Options: []Option{NewOption(OptionCustody, nil)}}) {
		t.Fatal("Ping stopped the connection")
	}
	pong, err := UnmarshalTCP(c.conn.(*stubConn).Bytes(), 0)
	if err != nil || pong.Code != Pong || string(pong.Token) != "ping" || len(pong.Options) != 0 {
		t.Fatalf("Pong with unearned Custody = %#v, error = %v", pong, err)
	}
	c.conn.(*stubConn).Reset()

	invalid := bareTCP(&stubConn{})
	if invalid.handleSignal(&Message{Code: CSM, Options: []Option{UintOption(OptionMaxMessageSize, 0)}}) {
		t.Fatal("invalid CSM continued")
	}
	if invalid.closedError() == nil {
		t.Fatal("invalid CSM did not close the connection")
	}
	for _, signal := range []*Message{
		{Code: CSM, Options: []Option{NewOption(OptionBlockWiseTransfer, []byte{1})}},
		{Code: CSM, Options: []Option{NewOption(3, nil)}},
		{Code: Ping, Options: []Option{NewOption(3, nil)}},
	} {
		invalid = bareTCP(&stubConn{})
		if invalid.handleSignal(signal) {
			t.Fatalf("invalid signaling message continued: %#v", signal)
		}
		select {
		case <-invalid.closed:
		default:
			t.Fatalf("invalid signaling message did not close: %#v", signal)
		}
	}
	if !c.handleSignal(&Message{Code: CSM, Options: []Option{NewOption(6, nil)}}) {
		t.Fatal("unknown elective signaling option stopped the connection")
	}
	aborted := bareTCP(&stubConn{})
	if aborted.handleSignal(&Message{Code: Abort, Payload: []byte("reason")}) {
		t.Fatal("Abort continued")
	}
	if aborted.err == nil {
		t.Fatal("Abort did not record an error")
	}
	released := bareTCP(&stubConn{})
	if released.handleSignal(&Message{Code: Release}) {
		t.Fatal("Release continued")
	}
	select {
	case <-released.closed:
	case <-time.After(time.Second):
		t.Fatal("idle connection did not close after peer Release")
	}
}

func TestTCPReleaseWaitsForHandlers(t *testing.T) {
	raw := &stubConn{}
	sender := bareTCP(raw)
	defer func() { _ = sender.Close() }()
	if err := sender.Release(); err != nil {
		t.Fatal(err)
	}
	message, err := UnmarshalTCP(raw.Bytes(), 0)
	if err != nil || message.Code != Release {
		t.Fatalf("sent Release = %#v, %v", message, err)
	}
	select {
	case <-sender.closed:
		t.Fatal("sender closed before the peer could respond to Release")
	default:
	}

	receiver := bareTCP(&stubConn{})
	receiver.handlers.Add(1)
	if receiver.handleSignal(&Message{Code: Release}) {
		t.Fatal("received Release did not stop reading")
	}
	select {
	case <-receiver.closed:
		t.Fatal("receiver closed while an earlier handler was active")
	default:
	}
	receiver.handlers.Done()
	select {
	case <-receiver.closed:
	case <-time.After(time.Second):
		t.Fatal("receiver stayed open after the handler finished")
	}
}

func FuzzTCPCSMOptions(f *testing.F) {
	f.Add(byte(2), []byte{0x10, 0})
	f.Add(byte(2), []byte{})
	f.Add(byte(4), []byte{})
	f.Add(byte(4), []byte{1})
	f.Add(byte(3), []byte{})
	f.Add(byte(6), []byte{1})
	f.Fuzz(func(t *testing.T, number byte, value []byte) {
		if len(value) > 8 {
			return
		}
		c := bareTCP(&stubConn{})
		defer func() { _ = c.Close() }()
		c.peer = TCPCapabilities{MaxMessageSize: 4096, BlockWiseTransfer: true}
		valid := number&1 == 0
		expectedMax := uint32(4096)
		switch number {
		case 2:
			valid = len(value) <= 4
			if valid {
				v, _ := NewOption(OptionMaxMessageSize, value).Uint32()
				valid = v != 0
				expectedMax = v
			}
		case 4:
			valid = len(value) == 0
		}
		continued := c.handleSignal(&Message{Code: CSM, Options: []Option{NewOption(OptionNumber(number), value)}})
		if continued != valid {
			t.Fatalf("CSM option %d with %x: continued = %v", number, value, continued)
		}
		if valid {
			got, ready := c.Capabilities()
			if !ready || !got.BlockWiseTransfer || got.MaxMessageSize != expectedMax {
				t.Fatalf("CSM capabilities = %#v, ready = %v", got, ready)
			}
		} else {
			select {
			case <-c.closed:
			default:
				t.Fatal("invalid CSM did not close")
			}
		}
	})
}

func FuzzTCPCSMRepeatedCapabilities(f *testing.F) {
	f.Add(byte(0), uint32(1024), []byte{0xff, 0xff, 0xff, 0xff, 0xff})
	f.Add(byte(1), uint32(2048), []byte{1})
	f.Add(byte(0), uint32(1), []byte{})
	f.Fuzz(func(t *testing.T, number byte, firstValue uint32, secondValue []byte) {
		if len(secondValue) > 8 {
			return
		}
		c := bareTCP(&stubConn{})
		defer func() { _ = c.Close() }()
		var first Option
		var expected TCPCapabilities
		if number&1 == 0 {
			if firstValue == 0 {
				firstValue = 1
			}
			first = UintOption(OptionMaxMessageSize, firstValue)
			expected = TCPCapabilities{MaxMessageSize: firstValue}
		} else {
			first = NewOption(OptionBlockWiseTransfer, nil)
			expected = TCPCapabilities{MaxMessageSize: 1152, BlockWiseTransfer: true}
		}
		if !c.handleSignal(&Message{Code: CSM, Options: []Option{first, NewOption(first.Number, secondValue)}}) {
			t.Fatal("repeated elective option aborted the connection")
		}
		got, ready := c.Capabilities()
		if !ready || got != expected {
			t.Fatalf("capabilities = %#v, want %#v", got, expected)
		}
	})
}

func waitTCPPending(t *testing.T, c *TCPConn) (string, chan *Message) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for key, responses := range c.pending {
			c.mu.Unlock()
			return key, responses
		}
		c.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatal("TCP operation did not register pending state")
	return "", nil
}

func TestTCPRequestWaitOutcomes(t *testing.T) {
	readyTCP := func(raw net.Conn) *TCPConn {
		c := bareTCP(raw)
		close(c.peerReady)
		return c
	}
	t.Run("response", func(t *testing.T) {
		c := readyTCP(&stubConn{})
		done := make(chan error, 1)
		go func() {
			response, err := c.Request(context.Background(), &Message{Code: GET, Token: []byte("fixed")})
			if err == nil && response.Code != Content {
				err = errors.New("unexpected response")
			}
			done <- err
		}()
		_, responses := waitTCPPending(t, c)
		responses <- &Message{Code: Content}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
	t.Run("duplicate token", func(t *testing.T) {
		c := readyTCP(&stubConn{})
		c.pending["fixed"] = make(chan *Message, 1)
		if _, err := c.Request(context.Background(), &Message{Code: GET, Token: []byte("fixed")}); err == nil {
			t.Fatal("accepted a token already in use")
		}
	})
	t.Run("send error", func(t *testing.T) {
		c := readyTCP(&stubConn{writeErr: io.ErrClosedPipe})
		if _, err := c.Request(context.Background(), &Message{Code: GET, Token: []byte("fixed")}); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("context while waiting", func(t *testing.T) {
		c := readyTCP(&stubConn{})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := c.Request(ctx, &Message{Code: GET, Token: []byte("fixed")}); done <- err }()
		waitTCPPending(t, c)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("close while waiting", func(t *testing.T) {
		c := readyTCP(&stubConn{})
		done := make(chan error, 1)
		go func() {
			_, err := c.Request(context.Background(), &Message{Code: GET, Token: []byte("fixed")})
			done <- err
		}()
		waitTCPPending(t, c)
		_ = c.Close()
		if err := <-done; !errors.Is(err, net.ErrClosed) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("context before peer CSM", func(t *testing.T) {
		c := bareTCP(&stubConn{})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		// Use a context whose cancellation is observed in the peer-ready select.
		if _, err := c.Request(ctx, &Message{Code: GET}); !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("close before peer CSM", func(t *testing.T) {
		c := bareTCP(&stubConn{})
		_ = c.Close()
		if _, err := c.Request(context.Background(), &Message{Code: GET}); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestTCPPingWaitOutcomes(t *testing.T) {
	t.Run("non-Pong", func(t *testing.T) {
		c := bareTCP(&stubConn{})
		done := make(chan error, 1)
		go func() { done <- c.Ping(context.Background()) }()
		_, responses := waitTCPPending(t, c)
		responses <- &Message{Code: Content}
		if err := <-done; err == nil {
			t.Fatal("Ping accepted a non-Pong response")
		}
	})
	t.Run("send error", func(t *testing.T) {
		c := bareTCP(&stubConn{writeErr: io.ErrClosedPipe})
		if err := c.Ping(context.Background()); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("context", func(t *testing.T) {
		c := bareTCP(&stubConn{})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- c.Ping(ctx) }()
		waitTCPPending(t, c)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("closed", func(t *testing.T) {
		c := bareTCP(&stubConn{})
		done := make(chan error, 1)
		go func() { done <- c.Ping(context.Background()) }()
		waitTCPPending(t, c)
		_ = c.Close()
		if err := <-done; !errors.Is(err, net.ErrClosed) {
			t.Fatalf("error = %v", err)
		}
	})
	c := bareTCP(&stubConn{writeErr: io.ErrClosedPipe})
	if err := c.Release(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Release error = %v", err)
	}
}

func TestTCPHandlerOutcomesAndClose(t *testing.T) {
	for _, handler := range []Handler{
		nil,
		HandlerFunc(func(context.Context, *Message) (*Message, error) { return nil, nil }),
		HandlerFunc(func(context.Context, *Message) (*Message, error) { return nil, errors.New("handler") }),
		HandlerFunc(func(context.Context, *Message) (*Message, error) { return &Message{Code: Content}, nil }),
	} {
		raw := &stubConn{}
		c := bareTCP(raw)
		c.cfg.Handler = handler
		c.handleRequest(&Message{Code: GET, Token: []byte{1}})
	}
	c := bareTCP(&stubConn{})
	if err := c.Release(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c = bareTCP(&stubConn{})
	if err := c.Abort("diagnostic"); err != nil {
		t.Fatal(err)
	}
	want := errors.New("terminal")
	c = bareTCP(&stubConn{})
	c.fail(want)
	c.fail(errors.New("later"))
	if !errors.Is(c.Close(), want) || !errors.Is(c.closedError(), want) {
		t.Fatalf("terminal error = %v", c.err)
	}
}

func TestDialAndServeTCP(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan *TCPConn, 1)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- ServeTCP(listener, TCPConfig{Handler: HandlerFunc(func(context.Context, *Message) (*Message, error) {
			return &Message{Code: Content, Payload: []byte("served")}, nil
		})}, func(c *TCPConn) { accepted <- c })
	}()
	client, err := DialTCP(testContext(t), "tcp", listener.Addr().String(), TCPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	server := <-accepted
	response, err := client.Request(testContext(t), &Message{Code: GET})
	if err != nil || string(response.Payload) != "served" {
		t.Fatalf("response = %#v, err = %v", response, err)
	}
	_ = client.Close()
	_ = server.Close()
	_ = listener.Close()
	select {
	case err := <-serveDone:
		if err == nil {
			t.Fatal("ServeTCP returned no listener error")
		}
	case <-time.After(time.Second):
		t.Fatal("ServeTCP did not stop")
	}
}

func TestDialAndServeRejectInvalidTCPConfig(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := listener.Accept()
		accepted <- conn
	}()
	if _, err := DialTCP(context.Background(), "tcp", listener.Addr().String(), TCPConfig{MaxMessageSize: 1}); err == nil {
		t.Fatal("DialTCP accepted an invalid TCP configuration")
	}
	if conn := <-accepted; conn != nil {
		_ = conn.Close()
	}
	_ = listener.Close()

	listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- ServeTCP(listener, TCPConfig{MaxMessageSize: 1}, nil) }()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	_ = listener.Close()
	select {
	case err := <-serveDone:
		if err == nil {
			t.Fatal("ServeTCP returned no listener error")
		}
	case <-time.After(time.Second):
		t.Fatal("ServeTCP did not continue after rejecting a connection")
	}
}

func TestTCPRejectsNonCSMFirstMessage(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	type result struct {
		conn *TCPConn
		err  error
	}
	created := make(chan result, 1)
	go func() {
		conn, err := NewTCPConn(a, TCPConfig{})
		created <- result{conn: conn, err: err}
	}()
	reader := bufio.NewReader(b)
	if message, err := ReadTCP(reader, 0); err != nil || message.Code != CSM {
		t.Fatalf("initial message = %#v, %v", message, err)
	}
	c := <-created
	if c.err != nil {
		t.Fatal(c.err)
	}
	wire, err := MarshalTCP(&Message{Code: GET})
	if err != nil {
		t.Fatal(err)
	}
	writeDone := make(chan error, 1)
	go func() { _, err := b.Write(wire); writeDone <- err }()
	if message, err := ReadTCP(reader, 0); err != nil || message.Code != Abort {
		t.Fatalf("abort message = %#v, %v", message, err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.conn.closed:
	case <-time.After(time.Second):
		t.Fatal("connection remained open")
	}
}

func TestTCPIgnoresEmptyKeepalive(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	type result struct {
		conn *TCPConn
		err  error
	}
	created := make(chan result, 1)
	go func() {
		conn, err := NewTCPConn(a, TCPConfig{})
		created <- result{conn, err}
	}()
	if err := b.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if message, err := ReadTCP(bufio.NewReader(b), 0); err != nil || message.Code != CSM {
		t.Fatalf("initial CSM = %#v, %v", message, err)
	}
	client := <-created
	if client.err != nil {
		t.Fatal(client.err)
	}
	defer func() { _ = client.conn.Close() }()
	csm, err := MarshalTCP(&Message{Code: CSM})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(csm); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.conn.peerReady:
	case <-time.After(time.Second):
		t.Fatal("peer CSM was not processed")
	}
	responses := make(chan *Message, 1)
	client.conn.mu.Lock()
	client.conn.pending[""] = responses
	client.conn.mu.Unlock()
	for _, code := range []Code{CodeEmpty, Content} {
		wire, err := MarshalTCP(&Message{Code: code})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.Write(wire); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case response := <-responses:
		if response.Code != Content {
			t.Fatalf("Empty message reached pending response: %#v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("response after Empty message was not delivered")
	}
}

func TestTCPMalformedFrameGetsAbort(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	type result struct {
		conn *TCPConn
		err  error
	}
	created := make(chan result, 1)
	go func() {
		conn, err := NewTCPConn(a, TCPConfig{})
		created <- result{conn, err}
	}()
	reader := bufio.NewReader(b)
	if message, err := ReadTCP(reader, 0); err != nil || message.Code != CSM {
		t.Fatalf("initial message = %#v, %v", message, err)
	}
	c := <-created
	if c.err != nil {
		t.Fatal(c.err)
	}
	if _, err := b.Write([]byte{0x09}); err != nil {
		t.Fatal(err)
	}
	abort, err := ReadTCP(reader, 0)
	if err != nil || abort.Code != Abort {
		t.Fatalf("malformed frame response = %#v, %v", abort, err)
	}
	select {
	case <-c.conn.closed:
	case <-time.After(time.Second):
		t.Fatal("connection remained open after malformed frame")
	}
}

func TestUDPValidationAndTimeouts(t *testing.T) {
	for _, config := range []UDPConfig{
		{MaxDatagramSize: 3}, {MaxDatagramSize: 65536}, {ACKTimeout: -1}, {MaxRetransmit: -1},
		{NStart: -1}, {ExchangeLifetime: -1}, {MulticastLeisure: -1},
	} {
		a, b := memoryPacketPair()
		if _, err := NewUDPEndpoint(a, config); err == nil {
			t.Fatalf("accepted config %#v", config)
		}
		_ = a.Close()
		_ = b.Close()
	}
	if _, err := ListenUDP("udp4", "invalid address", UDPConfig{}); err == nil {
		t.Fatal("ListenUDP accepted an invalid address")
	}
	occupied, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ListenUDP("udp4", occupied.LocalAddr().String(), UDPConfig{}); err == nil {
		t.Fatal("ListenUDP bound an occupied address")
	}
	_ = occupied.Close()
	if _, err := ListenIPv6Multicast("coaperation-no-such-interface", 0, UDPConfig{}); err == nil {
		t.Fatal("ListenIPv6Multicast found a nonexistent interface")
	}
	address := IPv6AllCoAPNodes("eth0", 0)
	if address.Port != DefaultPort || address.Zone != "eth0" || !address.IP.IsMulticast() {
		t.Fatalf("multicast address = %v", address)
	}

	a, b := memoryPacketPair()
	e, err := NewUDPEndpoint(a, UDPConfig{MaxDatagramSize: 8, ACKTimeout: time.Millisecond, MaxRetransmit: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close(); _ = b.Close() }()
	if err := e.Send(nil); err == nil {
		t.Fatal("Send accepted nil")
	}
	if err := e.Send(&Message{Code: GET}); err == nil {
		t.Fatal("Send accepted no remote address")
	}
	if err := e.Send(&Message{Code: GET, Payload: make([]byte, 8), RemoteAddr: b.LocalAddr()}); err == nil {
		t.Fatal("Send accepted an oversized datagram")
	}
	if err := e.Send(&Message{Code: GET, Token: make([]byte, 9), RemoteAddr: b.LocalAddr()}); err == nil {
		t.Fatal("Send accepted a long token")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.Request(canceled, &Message{Code: GET, RemoteAddr: b.LocalAddr()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Request = %v", err)
	}
	if _, err := e.Request(context.Background(), nil); err == nil {
		t.Fatal("Request accepted nil")
	}
	if _, err := e.Request(context.Background(), &Message{Code: Content}); err == nil {
		t.Fatal("Request accepted a response")
	}
	if _, err := e.Request(context.Background(), &Message{Code: GET}); err == nil {
		t.Fatal("Request accepted no remote address")
	}
	if err := e.SendConfirmable(context.Background(), nil); err == nil {
		t.Fatal("SendConfirmable accepted nil")
	}
	if err := e.SendConfirmable(context.Background(), &Message{Code: GET}); err == nil {
		t.Fatal("SendConfirmable accepted a request")
	}
	if err := e.SendConfirmable(canceled, &Message{Code: Content}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled SendConfirmable = %v", err)
	}
	if err := e.SendConfirmable(context.Background(), &Message{Code: Content}); err == nil {
		t.Fatal("SendConfirmable accepted no remote address")
	}
	if err := e.SendConfirmable(context.Background(), &Message{Code: Content, RemoteAddr: b.LocalAddr()}); err == nil {
		t.Fatal("SendConfirmable did not time out")
	}
	if _, err := e.Request(context.Background(), &Message{Code: GET, RemoteAddr: b.LocalAddr()}); err == nil {
		t.Fatal("confirmable Request did not time out")
	}
}

func TestUDPJitterDoesNotWrap(t *testing.T) {
	e := &UDPEndpoint{}
	base := time.Duration(1<<63 - 2)
	for range 100 {
		got := e.jitter(base)
		if got < base {
			t.Fatalf("ACK jitter wrapped: base = %d, got = %d", base, got)
		}
	}
}

func FuzzUDPDurationFraction(f *testing.F) {
	f.Add(uint64(0), uint16(0))
	f.Add(uint64(1), uint16(65535))
	f.Add(uint64(1<<63-1), uint16(65535))
	f.Add(uint64(60*60*24*1e9), uint16(32768))
	f.Fuzz(func(t *testing.T, bits uint64, sample uint16) {
		base := time.Duration(bits & (1<<63 - 1))
		got := durationFraction(base, uint64(sample), 65536)
		want := new(big.Int).SetUint64(uint64(base))
		want.Mul(want, new(big.Int).SetUint64(uint64(sample)))
		want.Div(want, new(big.Int).SetUint64(65536))
		if got != time.Duration(want.Int64()) {
			t.Fatalf("fraction of %d by %d/65536 = %d, want %s", base, sample, got, want)
		}
	})
}

func TestUDPMulticastAndResponseBranches(t *testing.T) {
	a, b := memoryPacketPair()
	server, err := NewUDPEndpoint(a, UDPConfig{Handler: HandlerFunc(func(context.Context, *Message) (*Message, error) {
		return &Message{Code: Content, Payload: []byte("reply")}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewUDPEndpoint(b, UDPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close(); _ = client.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	responses, err := client.RequestMulticast(ctx, &Message{Code: GET, RemoteAddr: a.LocalAddr()})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-responses:
		if string(response.Payload) != "reply" {
			t.Fatalf("payload = %q", response.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("no multicast response")
	}
	cancel()
	select {
	case _, ok := <-responses:
		if ok {
			t.Fatal("multicast response channel remains open")
		}
	case <-time.After(time.Second):
		t.Fatal("multicast response channel did not close")
	}

	for _, message := range []*Message{nil, {Code: Content}} {
		if _, err := client.RequestMulticast(context.Background(), message); err == nil {
			t.Fatal("accepted an invalid multicast request")
		}
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := client.RequestMulticast(canceled, &Message{Code: GET}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled multicast = %v", err)
	}

	p := &udpPending{responses: make(chan *Message, 1), ack: make(chan struct{}, 1), failures: make(chan error, 1), remote: "peer"}
	e := &UDPEndpoint{pending: map[string]*udpPending{"t": p}, messageACK: make(map[string]*udpPending)}
	e.handleResponse(&Message{Type: NonConfirmable, Code: Content, Token: []byte("t"), RemoteAddr: testAddr("wrong")})
	select {
	case <-p.responses:
		t.Fatal("accepted a response from the wrong peer")
	default:
	}
	e.messageACK[udpACKKey("peer", 1)] = p
	e.handleResponse(&Message{Type: Reset, Code: CodeEmpty, MessageID: 1, RemoteAddr: testAddr("peer")})
	select {
	case err := <-p.failures:
		if err == nil {
			t.Fatal("nil reset error")
		}
	default:
		t.Fatal("reset was not dispatched")
	}
}

func TestUDPEmptyConfirmableGetsReset(t *testing.T) {
	a, b := memoryPacketPair()
	e, err := NewUDPEndpoint(a, UDPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close(); _ = b.Close() }()
	wire, err := MarshalUDP(&Message{Type: Confirmable, Code: CodeEmpty, MessageID: 0x1234})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.WriteTo(wire, a.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	select {
	case packet := <-b.reads:
		response, err := UnmarshalUDP(packet.data)
		if err != nil || response.Type != Reset || response.Code != CodeEmpty || response.MessageID != 0x1234 {
			t.Fatalf("reset = %#v, error = %v", response, err)
		}
	case <-time.After(time.Second):
		t.Fatal("no reset for Empty Confirmable message")
	}
}

func TestUDPMulticastIgnoresConfirmableRequest(t *testing.T) {
	a, b := memoryPacketPair()
	calls := make(chan MessageType, 2)
	e, err := NewUDPEndpoint(multicastMemoryPacketConn{a}, UDPConfig{
		Handler: HandlerFunc(func(_ context.Context, request *Message) (*Message, error) {
			calls <- request.Type
			return &Message{Code: Content}, nil
		}),
		MulticastLeisure: time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close(); _ = b.Close() }()
	for _, kind := range []MessageType{Confirmable, NonConfirmable} {
		wire, err := MarshalUDP(&Message{Type: kind, Code: GET, MessageID: uint16(kind) + 1})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.WriteTo(wire, a.LocalAddr()); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case kind := <-calls:
		if kind != NonConfirmable {
			t.Fatalf("handler received multicast type %v", kind)
		}
	case <-time.After(time.Second):
		t.Fatal("valid Non-confirmable multicast request was not handled")
	}
	select {
	case kind := <-calls:
		t.Fatalf("handler received another multicast type %v", kind)
	default:
	}
	select {
	case packet := <-b.reads:
		response, err := UnmarshalUDP(packet.data)
		if err != nil || response.Type != NonConfirmable || response.Code != Content {
			t.Fatalf("multicast response = %#v, %v", response, err)
		}
	case <-time.After(time.Second):
		t.Fatal("valid multicast request received no response")
	}
	select {
	case packet := <-b.reads:
		t.Fatalf("unexpected multicast response: %x", packet.data)
	default:
	}
}

func TestUDPMalformedConfirmableGetsReset(t *testing.T) {
	a, b := memoryPacketPair()
	errorsSeen := make(chan error, 2)
	e, err := NewUDPEndpoint(a, UDPConfig{ErrorHandler: func(err error) { errorsSeen <- err }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close(); _ = b.Close() }()
	for _, wire := range [][]byte{
		{0x49, byte(GET), 0x12, 0x34},       // Reserved token length.
		{0x40, byte(GET), 0x12, 0x35, 0xff}, // Empty payload marker.
	} {
		if _, err := b.WriteTo(wire, a.LocalAddr()); err != nil {
			t.Fatal(err)
		}
		select {
		case packet := <-b.reads:
			reset, err := UnmarshalUDP(packet.data)
			if err != nil || reset.Type != Reset || reset.MessageID != uint16(wire[2])<<8|uint16(wire[3]) {
				t.Fatalf("malformed request reset = %#v, error = %v", reset, err)
			}
		case <-time.After(time.Second):
			t.Fatal("no Reset for malformed Confirmable datagram")
		}
		select {
		case <-errorsSeen:
		case <-time.After(time.Second):
			t.Fatal("malformed datagram error was not reported")
		}
	}
}

func TestUDPPiggybackedACKRequiresMessageID(t *testing.T) {
	e := &UDPEndpoint{pending: make(map[string]*udpPending), messageACK: make(map[string]*udpPending)}
	p := &udpPending{responses: make(chan *Message, 1), ack: make(chan struct{}, 1), remote: "peer"}
	if err := e.addPending("token", 17, p); err != nil {
		t.Fatal(err)
	}
	e.handleResponse(&Message{Type: Acknowledgement, Code: Content, MessageID: 18, Token: []byte("token"), RemoteAddr: testAddr("peer")})
	select {
	case <-p.responses:
		t.Fatal("accepted an ACK with the wrong Message ID")
	default:
	}
	e.handleResponse(&Message{Type: Acknowledgement, Code: Content, MessageID: 17, Token: []byte("token"), RemoteAddr: testAddr("peer")})
	select {
	case response := <-p.responses:
		if response.MessageID != 17 {
			t.Fatalf("response Message ID = %d", response.MessageID)
		}
	default:
		t.Fatal("matching ACK was not dispatched")
	}
}

func TestUDPResponseDeduplicationAndUnexpectedReset(t *testing.T) {
	a, b := memoryPacketPair()
	defer func() { _ = a.Close(); _ = b.Close() }()
	e := &UDPEndpoint{
		conn: a, cfg: UDPConfig{MaxDatagramSize: 65535, ExchangeLifetime: time.Minute},
		pending: make(map[string]*udpPending), messageACK: make(map[string]*udpPending),
		duplicates: make(map[string]duplicateEntry), responseDuplicates: make(map[string]duplicateEntry),
	}
	p := &udpPending{responses: make(chan *Message, 1), ack: make(chan struct{}, 1), remote: "peer"}
	if err := e.addPending("token", 10, p); err != nil {
		t.Fatal(err)
	}
	response := &Message{Type: Confirmable, Code: Content, Token: []byte("token"), MessageID: 20, RemoteAddr: testAddr("peer")}
	e.handleResponse(response)
	first := <-b.reads
	ack, err := UnmarshalUDP(first.data)
	if err != nil || ack.Type != Acknowledgement || ack.MessageID != 20 {
		t.Fatalf("response ACK = %#v, error = %v", ack, err)
	}
	if got := <-p.responses; got.MessageID != 20 {
		t.Fatalf("delivered response Message ID = %d", got.MessageID)
	}
	e.removePending("token", 10)
	e.handleResponse(response)
	repeated := <-b.reads
	if !bytes.Equal(repeated.data, first.data) {
		t.Fatalf("duplicate response ACK %x differs from %x", repeated.data, first.data)
	}
	unexpected := response.Clone()
	unexpected.MessageID = 21
	e.handleResponse(unexpected)
	reset := <-b.reads
	message, err := UnmarshalUDP(reset.data)
	if err != nil || message.Type != Reset || message.MessageID != 21 {
		t.Fatalf("unexpected response Reset = %#v, error = %v", message, err)
	}
	multi := &udpPending{multi: true, notify: make(chan struct{}, 1)}
	e.pending["multi"] = multi
	non := &Message{Type: NonConfirmable, Code: Content, Token: []byte("multi"), MessageID: 30, RemoteAddr: testAddr("other")}
	e.handleResponse(non)
	e.handleResponse(non)
	if multi.dequeue() == nil || multi.dequeue() != nil {
		t.Fatal("duplicate NON response was delivered more than once")
	}
}

func FuzzUDPMulticastResponseDeduplication(f *testing.F) {
	f.Add([]byte{0, 1, 0, 1, 1, 1, 0, 2})
	f.Add([]byte{1, 255, 0, 0, 1, 255})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 128 {
			return
		}
		p := &udpPending{multi: true, notify: make(chan struct{}, 1)}
		e := &UDPEndpoint{
			cfg: UDPConfig{ExchangeLifetime: time.Minute}, pending: map[string]*udpPending{"token": p},
			responseDuplicates: make(map[string]duplicateEntry),
		}
		seen := make(map[[2]byte]bool)
		for i := 0; i+1 < len(data); i += 2 {
			peer := data[i] % 2
			mid := data[i+1]
			seen[[2]byte{peer, mid}] = true
			e.handleResponse(&Message{Type: NonConfirmable, Code: Content, Token: []byte("token"), MessageID: uint16(mid), RemoteAddr: testAddr(string([]byte{'a' + peer}))})
		}
		for range len(seen) {
			if p.dequeue() == nil {
				t.Fatal("lost a distinct NON response")
			}
		}
		if p.dequeue() != nil {
			t.Fatal("delivered a duplicate NON response")
		}
	})
}

func TestUDPPendingCollisionsAndClosedError(t *testing.T) {
	e := &UDPEndpoint{pending: make(map[string]*udpPending), messageACK: make(map[string]*udpPending), peerSlots: make(map[string]chan struct{}), closed: make(chan struct{})}
	p := &udpPending{remote: "peer"}
	if err := e.addPending("token", 1, p); err != nil {
		t.Fatal(err)
	}
	if err := e.addPending("token", 2, p); err == nil {
		t.Fatal("accepted a duplicate token")
	}
	if err := e.addPending("other", 1, p); err == nil {
		t.Fatal("accepted a duplicate message ID")
	}
	e.removePending("token", 1)
	close(e.closed)
	if !errors.Is(e.closedError(), net.ErrClosed) {
		t.Fatalf("closed error = %v", e.closedError())
	}
	if err := e.acquirePeer(context.Background(), "peer"); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed acquire = %v", err)
	}
	e.err = errors.New("terminal read error")
	if e.closedError().Error() != "terminal read error" {
		t.Fatalf("terminal closed error = %v", e.closedError())
	}
}

func TestUDPWaitResponseOutcomes(t *testing.T) {
	newEndpoint := func() *UDPEndpoint { return &UDPEndpoint{closed: make(chan struct{})} }
	t.Run("response", func(t *testing.T) {
		p := &udpPending{responses: make(chan *Message, 1), failures: make(chan error, 1)}
		p.responses <- &Message{Code: Content}
		if response, err := newEndpoint().waitResponse(context.Background(), p); err != nil || response.Code != Content {
			t.Fatalf("response = %#v, err = %v", response, err)
		}
	})
	t.Run("failure", func(t *testing.T) {
		p := &udpPending{responses: make(chan *Message, 1), failures: make(chan error, 1)}
		want := errors.New("failure")
		p.failures <- want
		if _, err := newEndpoint().waitResponse(context.Background(), p); !errors.Is(err, want) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := newEndpoint().waitResponse(ctx, &udpPending{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("closed", func(t *testing.T) {
		e := newEndpoint()
		close(e.closed)
		if _, err := e.waitResponse(context.Background(), &udpPending{}); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestUDPReadLoopReportsRecoverableErrors(t *testing.T) {
	a, b := memoryPacketPair()
	errorsSeen := make(chan error, 3)
	e, err := NewUDPEndpoint(a, UDPConfig{MaxDatagramSize: 4, ErrorHandler: func(err error) { errorsSeen <- err }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close(); _ = b.Close() }()
	a.reads <- memoryPacket{data: []byte{0, 1, 2, 3, 4}, remote: b.addr}
	a.reads <- memoryPacket{data: []byte{0}, remote: b.addr}
	a.reads <- memoryPacket{data: []byte{0x40, 1, 0, 1}}
	for range 3 {
		select {
		case err := <-errorsSeen:
			if err == nil {
				t.Fatal("nil reported error")
			}
		case <-time.After(time.Second):
			t.Fatal("read-loop error was not reported")
		}
	}
}

func TestIPv6PacketMetadata(t *testing.T) {
	if _, ok := parseIPv6PacketInfo([]byte{1, 2, 3}, 5683); ok {
		t.Fatal("parsed malformed packet metadata")
	}
	receiver, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Skipf("IPv6 loopback is unavailable: %v", err)
	}
	defer func() { _ = receiver.Close() }()
	if err := enableIPv6PacketInfo(receiver); err != nil {
		t.Skipf("IPv6 packet information is unavailable: %v", err)
	}
	sender, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sender.Close() }()
	if _, err := sender.WriteToUDP([]byte("packet"), receiver.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	socket := &udpSocket{UDPConn: receiver}
	buffer := make([]byte, 16)
	n, local, remote, multicast, err := socket.ReadPacket(buffer)
	if err != nil || n != len("packet") || local == nil || remote == nil || multicast {
		t.Fatalf("ReadPacket = n %d, local %v, remote %v, multicast %v, err %v", n, local, remote, multicast, err)
	}
}

func TestUDPRequestHandlerBranches(t *testing.T) {
	newEndpoint := func(handler Handler, maxSize int, report chan error) (*UDPEndpoint, *memoryPacketConn) {
		a, b := memoryPacketPair()
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		e := &UDPEndpoint{
			conn: a, cfg: UDPConfig{Handler: handler, MaxDatagramSize: maxSize, ExchangeLifetime: time.Minute},
			pending: make(map[string]*udpPending), messageACK: make(map[string]*udpPending),
			duplicates: make(map[string]duplicateEntry), peerSlots: make(map[string]chan struct{}),
			closed: make(chan struct{}), ctx: ctx, cancel: cancel,
		}
		if report != nil {
			e.cfg.ErrorHandler = func(err error) { report <- err }
		}
		t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
		return e, b
	}
	read := func(t *testing.T, b *memoryPacketConn) *Message {
		t.Helper()
		select {
		case packet := <-b.reads:
			message, err := UnmarshalUDP(packet.data)
			if err != nil {
				t.Fatal(err)
			}
			return message
		case <-time.After(time.Second):
			t.Fatal("no response datagram")
			return nil
		}
	}

	e, b := newEndpoint(nil, 65535, nil)
	e.handleRequest(&Message{Type: NonConfirmable, Code: GET, MessageID: 1, RemoteAddr: testAddr("peer")})
	select {
	case <-b.reads:
		t.Fatal("nil handler replied to a non-confirmable request")
	default:
	}
	e.handleRequest(&Message{Type: Confirmable, Code: GET, MessageID: 2, RemoteAddr: testAddr("peer")})
	if response := read(t, b); response.Type != Acknowledgement || response.Code != CodeEmpty {
		t.Fatalf("empty acknowledgement = %#v", response)
	}
	// A duplicate confirmable request must replay the cached response.
	e.handleRequest(&Message{Type: Confirmable, Code: GET, MessageID: 2, RemoteAddr: testAddr("peer")})
	if response := read(t, b); response.Type != Acknowledgement {
		t.Fatalf("duplicate response = %#v", response)
	}

	e, b = newEndpoint(HandlerFunc(func(context.Context, *Message) (*Message, error) {
		return nil, errors.New("handler failed")
	}), 65535, nil)
	e.handleRequest(&Message{Type: NonConfirmable, Code: GET, MessageID: 3, RemoteAddr: testAddr("peer")})
	if response := read(t, b); response.Code != InternalServerError || response.Type != NonConfirmable {
		t.Fatalf("handler error response = %#v", response)
	}

	e, b = newEndpoint(HandlerFunc(func(context.Context, *Message) (*Message, error) {
		return &Message{Code: Content, Payload: []byte("ok")}, nil
	}), 65535, nil)
	e.cfg.MulticastLeisure = time.Nanosecond
	e.handleRequest(&Message{Type: NonConfirmable, Code: GET, MessageID: 4, Token: []byte{7}, RemoteAddr: testAddr("peer"), Multicast: true})
	if response := read(t, b); response.Code != Content || response.Type != NonConfirmable || !bytes.Equal(response.Token, []byte{7}) {
		t.Fatalf("multicast response = %#v", response)
	}

	e, b = newEndpoint(HandlerFunc(func(context.Context, *Message) (*Message, error) {
		return &Message{Code: Content, Payload: make([]byte, 20)}, nil
	}), 8, nil)
	e.handleRequest(&Message{Type: NonConfirmable, Code: GET, MessageID: 5, RemoteAddr: testAddr("peer")})
	select {
	case <-b.reads:
		t.Fatal("sent an oversized handler response")
	default:
	}
}

func TestUDPNonConfirmableRequestDeduplication(t *testing.T) {
	a, b := memoryPacketPair()
	defer func() { _ = a.Close(); _ = b.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	e := &UDPEndpoint{
		conn: a, cfg: UDPConfig{Handler: HandlerFunc(func(context.Context, *Message) (*Message, error) {
			calls++
			return nil, nil
		}), MaxDatagramSize: 65535, ExchangeLifetime: time.Minute},
		duplicates: make(map[string]duplicateEntry), closed: make(chan struct{}), ctx: ctx, cancel: cancel,
	}
	request := &Message{Type: NonConfirmable, Code: POST, MessageID: 10, RemoteAddr: testAddr("peer-a")}
	e.handleRequest(request)
	e.handleRequest(request)
	if calls != 1 {
		t.Fatalf("duplicate NON request reached handler %d times", calls)
	}
	otherPeer := request.Clone()
	otherPeer.RemoteAddr = testAddr("peer-b")
	e.handleRequest(otherPeer)
	otherID := request.Clone()
	otherID.MessageID++
	e.handleRequest(otherID)
	if calls != 3 {
		t.Fatalf("distinct peer and Message ID calls = %d", calls)
	}
	e.mu.Lock()
	e.duplicates["peer-a/10"] = duplicateEntry{expires: time.Now().Add(-time.Second)}
	e.mu.Unlock()
	e.handleRequest(request)
	e.handleRequest(request)
	if calls != 4 {
		t.Fatalf("expired NON request was not processed once: %d", calls)
	}
}

func FuzzUDPNonConfirmableDeduplication(f *testing.F) {
	f.Add([]byte{0, 1, 0, 1, 1, 1, 0, 2})
	f.Add([]byte{0, 0, 0, 255, 1, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 128 {
			return
		}
		calls := 0
		e := &UDPEndpoint{
			cfg: UDPConfig{ExchangeLifetime: time.Minute, Handler: HandlerFunc(func(context.Context, *Message) (*Message, error) {
				calls++
				return nil, nil
			})},
			duplicates: make(map[string]duplicateEntry),
		}
		seen := make(map[[2]byte]bool)
		for i := 0; i+1 < len(data); i += 2 {
			peer := data[i] % 2
			mid := data[i+1]
			seen[[2]byte{peer, mid}] = true
			e.handleRequest(&Message{Type: NonConfirmable, Code: POST, MessageID: uint16(mid), RemoteAddr: testAddr(string([]byte{'a' + peer}))})
		}
		if calls != len(seen) {
			t.Fatalf("handled %d requests for %d peer/Message ID pairs", calls, len(seen))
		}
	})
}

func TestUDPRequestHandlerWriteFailures(t *testing.T) {
	newEndpoint := func(handler Handler, errorsSeen chan error) (*UDPEndpoint, *memoryPacketConn) {
		a, b := memoryPacketPair()
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		e := &UDPEndpoint{
			conn:    errorWritePacketConn{a},
			cfg:     UDPConfig{Handler: handler, MaxDatagramSize: 65535, ExchangeLifetime: time.Minute, ErrorHandler: func(err error) { errorsSeen <- err }},
			pending: make(map[string]*udpPending), messageACK: make(map[string]*udpPending),
			duplicates: make(map[string]duplicateEntry), peerSlots: make(map[string]chan struct{}),
			closed: make(chan struct{}), ctx: ctx, cancel: cancel,
		}
		t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
		return e, b
	}
	waitError := func(t *testing.T, errorsSeen chan error) {
		t.Helper()
		select {
		case err := <-errorsSeen:
			if err == nil {
				t.Fatal("nil write error")
			}
		case <-time.After(time.Second):
			t.Fatal("write error was not reported")
		}
	}

	errorsSeen := make(chan error, 4)
	e, _ := newEndpoint(nil, errorsSeen)
	e.duplicates["peer/1"] = duplicateEntry{expires: time.Now().Add(time.Minute), response: []byte{0x60, 0, 0, 1}}
	e.handleRequest(&Message{Type: Confirmable, Code: GET, MessageID: 1, RemoteAddr: testAddr("peer")})
	waitError(t, errorsSeen)

	e, _ = newEndpoint(HandlerFunc(func(context.Context, *Message) (*Message, error) {
		return &Message{Code: Content, Separate: true}, nil
	}), errorsSeen)
	e.handleRequest(&Message{Type: Confirmable, Code: GET, MessageID: 2, RemoteAddr: testAddr("peer")})
	waitError(t, errorsSeen)

	e, _ = newEndpoint(HandlerFunc(func(context.Context, *Message) (*Message, error) {
		return &Message{Code: Content}, nil
	}), errorsSeen)
	e.handleRequest(&Message{Type: NonConfirmable, Code: GET, MessageID: 3, RemoteAddr: testAddr("peer")})
	waitError(t, errorsSeen)

	// An invalid option order in a handler response must not emit a datagram.
	a, b := memoryPacketPair()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e = &UDPEndpoint{
		conn: a, cfg: UDPConfig{Handler: HandlerFunc(func(context.Context, *Message) (*Message, error) {
			return &Message{Code: Content, Options: []Option{NewOption(2, nil), NewOption(1, nil)}}, nil
		}), MaxDatagramSize: 65535, ExchangeLifetime: time.Minute},
		pending: make(map[string]*udpPending), messageACK: make(map[string]*udpPending), duplicates: make(map[string]duplicateEntry),
		peerSlots: make(map[string]chan struct{}), closed: make(chan struct{}), ctx: ctx, cancel: cancel,
	}
	e.handleRequest(&Message{Type: NonConfirmable, Code: GET, RemoteAddr: testAddr("peer")})
	select {
	case <-b.reads:
		t.Fatal("sent a response with invalid option order")
	default:
	}
	_ = a.Close()
	_ = b.Close()

	// Closing during multicast leisure must stop the response.
	a, b = memoryPacketPair()
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	e = &UDPEndpoint{
		conn: a, cfg: UDPConfig{Handler: HandlerFunc(func(context.Context, *Message) (*Message, error) {
			return &Message{Code: Content}, nil
		}), MaxDatagramSize: 65535, ExchangeLifetime: time.Minute, MulticastLeisure: time.Hour},
		pending: make(map[string]*udpPending), messageACK: make(map[string]*udpPending), duplicates: make(map[string]duplicateEntry),
		peerSlots: make(map[string]chan struct{}), closed: make(chan struct{}), ctx: ctx, cancel: cancel,
	}
	close(e.closed)
	e.handleRequest(&Message{Type: NonConfirmable, Code: GET, RemoteAddr: testAddr("peer"), Multicast: true})
	_ = a.Close()
	_ = b.Close()
}

func TestUDPNonConfirmableRequest(t *testing.T) {
	a, b := memoryPacketPair()
	server, err := NewUDPEndpoint(a, UDPConfig{Handler: HandlerFunc(func(context.Context, *Message) (*Message, error) {
		return &Message{Code: Content}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewUDPEndpoint(b, UDPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close(); _ = client.Close() }()
	response, err := client.Request(testContext(t), &Message{Type: NonConfirmable, Code: GET, RemoteAddr: a.LocalAddr()})
	if err != nil || response.Type != NonConfirmable {
		t.Fatalf("response = %#v, err = %v", response, err)
	}
}

func TestUDPSendConfirmableWaitOutcomes(t *testing.T) {
	newPair := func(t *testing.T, retransmit int) (*UDPEndpoint, *memoryPacketConn) {
		t.Helper()
		a, b := memoryPacketPair()
		e, err := NewUDPEndpoint(a, UDPConfig{ACKTimeout: time.Millisecond, MaxRetransmit: retransmit})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Close(); _ = b.Close() })
		return e, b
	}
	readSent := func(t *testing.T, b *memoryPacketConn) *Message {
		t.Helper()
		select {
		case packet := <-b.reads:
			message, err := UnmarshalUDP(packet.data)
			if err != nil {
				t.Fatal(err)
			}
			return message
		case <-time.After(time.Second):
			t.Fatal("no outgoing datagram")
			return nil
		}
	}
	reply := func(t *testing.T, b *memoryPacketConn, message *Message) {
		t.Helper()
		wire, err := MarshalUDP(message)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.WriteTo(wire, nil); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("acknowledgement", func(t *testing.T) {
		e, b := newPair(t, 1)
		done := make(chan error, 1)
		go func() {
			done <- e.SendConfirmable(context.Background(), &Message{Code: Content, RemoteAddr: b.LocalAddr()})
		}()
		sent := readSent(t, b)
		reply(t, b, &Message{Type: Acknowledgement, MessageID: sent.MessageID})
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
	t.Run("reset", func(t *testing.T) {
		e, b := newPair(t, 1)
		done := make(chan error, 1)
		go func() {
			done <- e.SendConfirmable(context.Background(), &Message{Code: Content, RemoteAddr: b.LocalAddr()})
		}()
		sent := readSent(t, b)
		reply(t, b, &Message{Type: Reset, MessageID: sent.MessageID})
		if err := <-done; err == nil {
			t.Fatal("reset did not fail the exchange")
		}
	})
	t.Run("context", func(t *testing.T) {
		e, b := newPair(t, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- e.SendConfirmable(ctx, &Message{Code: Content, RemoteAddr: b.LocalAddr()}) }()
		_ = readSent(t, b)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("closed", func(t *testing.T) {
		e, b := newPair(t, 1)
		done := make(chan error, 1)
		go func() {
			done <- e.SendConfirmable(context.Background(), &Message{Code: Content, RemoteAddr: b.LocalAddr()})
		}()
		_ = readSent(t, b)
		_ = e.Close()
		if err := <-done; !errors.Is(err, net.ErrClosed) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("retransmit write", func(t *testing.T) {
		e, b := newPair(t, 1)
		done := make(chan error, 1)
		go func() {
			done <- e.SendConfirmable(context.Background(), &Message{Code: Content, RemoteAddr: b.LocalAddr()})
		}()
		_ = readSent(t, b)
		_ = b.Close()
		if err := <-done; err == nil {
			t.Fatal("retransmit to a closed peer succeeded")
		}
	})
	t.Run("message ID collision", func(t *testing.T) {
		e, b := newPair(t, 1)
		e.mu.Lock()
		e.messageACK[udpACKKey(b.LocalAddr().String(), 77)] = &udpPending{}
		e.mu.Unlock()
		if err := e.SendConfirmable(context.Background(), &Message{Code: Content, MessageID: 77, RemoteAddr: b.LocalAddr()}); err == nil {
			t.Fatal("accepted a message ID already in use")
		}
	})
}

func TestUDPConfirmableRequestWaitOutcomes(t *testing.T) {
	newPair := func(t *testing.T) (*UDPEndpoint, *memoryPacketConn) {
		t.Helper()
		a, b := memoryPacketPair()
		e, err := NewUDPEndpoint(a, UDPConfig{ACKTimeout: 20 * time.Millisecond, MaxRetransmit: 1})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Close(); _ = b.Close() })
		return e, b
	}
	run := func(t *testing.T, responseType MessageType, closeEndpoint bool) error {
		t.Helper()
		e, b := newPair(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := e.Request(ctx, &Message{Type: Confirmable, Code: GET, Token: []byte{1}, MessageID: 88, RemoteAddr: b.LocalAddr()})
			done <- err
		}()
		packet := <-b.reads
		sent, err := UnmarshalUDP(packet.data)
		if err != nil {
			t.Fatal(err)
		}
		if closeEndpoint {
			_ = e.Close()
		} else {
			wire, marshalErr := MarshalUDP(&Message{Type: responseType, MessageID: sent.MessageID})
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			_, _ = b.WriteTo(wire, nil)
			if responseType == Acknowledgement {
				cancel()
			}
		}
		return <-done
	}
	if err := run(t, Reset, false); err == nil {
		t.Fatal("reset did not fail the request")
	}
	if err := run(t, Acknowledgement, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("acknowledged cancellation error = %v", err)
	}
	if err := run(t, Confirmable, true); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed request error = %v", err)
	}
}

func TestUDPRequestRegistrationAndRetransmissionErrors(t *testing.T) {
	newPair := func(t *testing.T, retransmit int) (*UDPEndpoint, *memoryPacketConn) {
		t.Helper()
		a, b := memoryPacketPair()
		e, err := NewUDPEndpoint(a, UDPConfig{ACKTimeout: time.Millisecond, MaxRetransmit: retransmit})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Close(); _ = b.Close() })
		return e, b
	}
	t.Run("token collision", func(t *testing.T) {
		e, b := newPair(t, 1)
		e.mu.Lock()
		e.pending["fixed"] = &udpPending{}
		e.mu.Unlock()
		if _, err := e.Request(context.Background(), &Message{Type: NonConfirmable, Code: GET, Token: []byte("fixed"), RemoteAddr: b.LocalAddr()}); err == nil {
			t.Fatal("accepted a token already in use")
		}
	})
	t.Run("message ID collision", func(t *testing.T) {
		e, b := newPair(t, 1)
		e.mu.Lock()
		e.messageACK[udpACKKey(b.LocalAddr().String(), 42)] = &udpPending{}
		e.mu.Unlock()
		if _, err := e.Request(context.Background(), &Message{Type: NonConfirmable, Code: GET, Token: []byte("unique"), MessageID: 42, RemoteAddr: b.LocalAddr()}); err == nil {
			t.Fatal("accepted a message ID already in use")
		}
	})
	t.Run("initial write", func(t *testing.T) {
		a, b := memoryPacketPair()
		e, err := NewUDPEndpoint(errorWritePacketConn{a}, UDPConfig{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = e.Close(); _ = b.Close() }()
		if _, err := e.Request(context.Background(), &Message{Type: NonConfirmable, Code: GET, Token: []byte("fixed"), RemoteAddr: b.LocalAddr()}); err == nil {
			t.Fatal("wrote to a closed peer")
		}
	})
	t.Run("retransmit write", func(t *testing.T) {
		a, b := memoryPacketPair()
		e, err := NewUDPEndpoint(&failAfterWritePacketConn{memoryPacketConn: a}, UDPConfig{ACKTimeout: time.Millisecond, MaxRetransmit: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = e.Close(); _ = b.Close() }()
		done := make(chan error, 1)
		go func() {
			_, err := e.Request(context.Background(), &Message{Type: Confirmable, Code: GET, Token: []byte("fixed"), RemoteAddr: b.LocalAddr()})
			done <- err
		}()
		<-b.reads
		if err := <-done; err == nil {
			t.Fatal("retransmit to a closed peer succeeded")
		}
	})
	t.Run("one retransmission", func(t *testing.T) {
		e, b := newPair(t, 1)
		done := make(chan error, 1)
		go func() {
			_, err := e.Request(context.Background(), &Message{Type: Confirmable, Code: GET, Token: []byte("fixed"), RemoteAddr: b.LocalAddr()})
			done <- err
		}()
		<-b.reads
		select {
		case <-b.reads:
		case <-time.After(time.Second):
			t.Fatal("request was not retransmitted")
		}
		if err := <-done; err == nil {
			t.Fatal("request did not time out")
		}
	})
	t.Run("NStart context", func(t *testing.T) {
		e, b := newPair(t, 1)
		e.mu.Lock()
		slots := make(chan struct{}, 1)
		slots <- struct{}{}
		e.peerSlots[b.LocalAddr().String()] = slots
		e.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if _, err := e.Request(ctx, &Message{Type: Confirmable, Code: GET, RemoteAddr: b.LocalAddr()}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestUDPMulticastRegistrationErrors(t *testing.T) {
	a, b := memoryPacketPair()
	e, err := NewUDPEndpoint(a, UDPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close(); _ = b.Close() }()
	e.mu.Lock()
	e.pending["fixed"] = &udpPending{}
	e.mu.Unlock()
	if _, err := e.RequestMulticast(context.Background(), &Message{Code: GET, Token: []byte("fixed"), RemoteAddr: b.LocalAddr()}); err == nil {
		t.Fatal("accepted a multicast token already in use")
	}
	e.mu.Lock()
	delete(e.pending, "fixed")
	e.mu.Unlock()
	_ = e.Close()
	a2, b2 := memoryPacketPair()
	e2, err := NewUDPEndpoint(errorWritePacketConn{a2}, UDPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e2.Close(); _ = a2.Close(); _ = b2.Close() }()
	if _, err := e2.RequestMulticast(context.Background(), &Message{Code: GET, Token: []byte("fixed"), RemoteAddr: b2.LocalAddr()}); err == nil {
		t.Fatal("multicast write to a closed peer succeeded")
	}
}
