package coap

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

type TCPCapabilities struct {
	// MaxMessageSize is the complete framed size accepted by the peer.
	MaxMessageSize uint32
	// BlockWiseTransfer reports the peer CSM capability option.
	BlockWiseTransfer bool
}

// TCPConfig controls one CoAP TCP connection.
type TCPConfig struct {
	// Handler receives peer requests.
	Handler Handler
	// MaxMessageSize defaults to 1 MiB and must fit the initial CSM.
	MaxMessageSize uint32
	// BlockWiseTransfer includes the capability in the local CSM.
	BlockWiseTransfer bool
}

func (c TCPConfig) defaults() TCPConfig {
	if c.MaxMessageSize == 0 {
		c.MaxMessageSize = 1 << 20
	}
	return c
}

// TCPConn is a bidirectional CoAP connection. Both peers can issue requests.
type TCPConn struct {
	conn     net.Conn
	cfg      TCPConfig
	reader   *bufio.Reader
	writeMu  sync.Mutex
	handlers sync.WaitGroup

	mu        sync.Mutex
	pending   map[string]chan *Message
	peer      TCPCapabilities
	peerReady chan struct{}
	peerOnce  sync.Once
	closed    chan struct{}
	closeOnce sync.Once
	err       error
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewTCPConn starts CoAP on an existing connected or accepted stream.
func NewTCPConn(conn net.Conn, config TCPConfig) (*TCPConn, error) {
	if conn == nil {
		return nil, errors.New("coap: nil TCP connection")
	}
	config = config.defaults()
	csm := &Message{Code: CSM, Options: []Option{UintOption(OptionMaxMessageSize, config.MaxMessageSize)}}
	if config.BlockWiseTransfer {
		csm.Options = append(csm.Options, UintOption(OptionBlockWiseTransfer, 0))
	}
	csmWire, err := MarshalTCP(csm)
	if err != nil {
		return nil, err
	}
	if uint64(len(csmWire)) > uint64(config.MaxMessageSize) {
		return nil, errors.New("coap: TCP MaxMessageSize is smaller than the initial CSM")
	}
	connectionContext, cancel := context.WithCancel(context.Background())
	c := &TCPConn{conn: conn, cfg: config, reader: bufio.NewReader(conn), pending: make(map[string]chan *Message), peer: TCPCapabilities{MaxMessageSize: 1152}, peerReady: make(chan struct{}), closed: make(chan struct{}), ctx: connectionContext, cancel: cancel}
	go c.readLoop()
	if err := c.write(csm, false); err != nil {
		c.fail(err)
		return nil, err
	}
	return c, nil
}

// DialTCP connects to a plain CoAP TCP peer.
func DialTCP(ctx context.Context, network, address string, config TCPConfig) (*TCPConn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	coapConn, err := NewTCPConn(conn, config)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return coapConn, nil
}

// LocalAddr returns the local stream address.
func (c *TCPConn) LocalAddr() net.Addr { return c.conn.LocalAddr() }

// RemoteAddr returns the peer stream address.
func (c *TCPConn) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }

// Capabilities returns the latest peer CSM settings and whether a CSM arrived.
func (c *TCPConn) Capabilities() (TCPCapabilities, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.peerReady:
		return c.peer, true
	default:
		return c.peer, false
	}
}

// Close stops the connection and cancels active handler contexts.
func (c *TCPConn) Close() error {
	c.closeOnce.Do(func() { c.cancel(); close(c.closed); _ = c.conn.Close() })
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Send writes one raw framed message.
func (c *TCPConn) Send(m *Message) error {
	if m == nil {
		return errors.New("coap: nil message")
	}
	return c.write(m, true)
}

func (c *TCPConn) write(m *Message, honorPeer bool) error {
	b, err := MarshalTCP(m)
	if err != nil {
		return err
	}
	if uint64(len(b)) > uint64(c.cfg.MaxMessageSize) {
		return fmt.Errorf("coap: TCP message size %d exceeds local maximum %d", len(b), c.cfg.MaxMessageSize)
	}
	if honorPeer {
		c.mu.Lock()
		peerMax := c.peer.MaxMessageSize
		c.mu.Unlock()
		if uint64(len(b)) > uint64(peerMax) {
			return fmt.Errorf("coap: TCP message size %d exceeds peer maximum %d", len(b), peerMax)
		}
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	for len(b) > 0 {
		n, err := c.conn.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
		b = b[n:]
	}
	return nil
}

// Request sends a request and matches its response by token.
func (c *TCPConn) Request(ctx context.Context, request *Message) (*Message, error) {
	if request == nil || !request.Code.IsRequest() {
		return nil, errors.New("coap: message code is not a request")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-c.peerReady:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, c.closedError()
	}
	r := request.Clone()
	if len(r.Token) == 0 {
		var err error
		r.Token, err = newToken()
		if err != nil {
			return nil, err
		}
	}
	key := string(r.Token)
	responses := make(chan *Message, 1)
	c.mu.Lock()
	if _, exists := c.pending[key]; exists {
		c.mu.Unlock()
		return nil, errors.New("coap: token is already in use")
	}
	c.pending[key] = responses
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, key); c.mu.Unlock() }()
	if err := c.Send(r); err != nil {
		return nil, err
	}
	select {
	case response := <-responses:
		return response, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, c.closedError()
	}
}

// Ping sends a signaling Ping and waits for its Pong.
func (c *TCPConn) Ping(ctx context.Context) error {
	token, err := newToken()
	if err != nil {
		return err
	}
	responses := make(chan *Message, 1)
	key := string(token)
	c.mu.Lock()
	c.pending[key] = responses
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, key); c.mu.Unlock() }()
	if err := c.Send(&Message{Code: Ping, Token: token}); err != nil {
		return err
	}
	select {
	case m := <-responses:
		if m.Code != Pong {
			return errors.New("coap: non-Pong response to Ping")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return c.closedError()
	}
}

// Release asks the peer to close the connection. Call Close to stop locally.
func (c *TCPConn) Release() error {
	return c.Send(&Message{Code: Release})
}

// Abort sends a signaling Abort with a diagnostic payload and closes.
func (c *TCPConn) Abort(reason string) error {
	err := c.Send(&Message{Code: Abort, Payload: []byte(reason)})
	_ = c.Close()
	return err
}

func (c *TCPConn) closedError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	return net.ErrClosed
}

func (c *TCPConn) fail(err error) {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.mu.Unlock()
	c.closeOnce.Do(func() { c.cancel(); close(c.closed); _ = c.conn.Close() })
}

func (c *TCPConn) readLoop() {
	firstMessage := true
	for {
		m, err := ReadTCP(c.reader, uint64(c.cfg.MaxMessageSize))
		if err != nil {
			select {
			case <-c.closed:
				return
			default:
				if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
					_ = c.Abort("malformed CoAP message")
				}
				c.fail(err)
				return
			}
		}
		m.LocalAddr, m.RemoteAddr = c.conn.LocalAddr(), c.conn.RemoteAddr()
		if firstMessage {
			firstMessage = false
			if m.Code != CSM {
				_ = c.Abort("first peer message is not CSM")
				return
			}
		}
		if m.Code.IsSignaling() {
			if !c.handleSignal(m) {
				return
			}
			continue
		}
		if m.Code.IsRequest() {
			c.handlers.Add(1)
			go func() {
				defer c.handlers.Done()
				c.handleRequest(m)
			}()
			continue
		}
		c.mu.Lock()
		response := c.pending[string(m.Token)]
		c.mu.Unlock()
		if response != nil {
			select {
			case response <- m:
			default:
			}
		}
	}
}

func (c *TCPConn) handleSignal(m *Message) bool {
	for _, option := range m.Options {
		if option.Number&1 != 0 {
			_ = c.Abort("unknown critical signaling option")
			return false
		}
	}
	switch m.Code {
	case CSM:
		c.mu.Lock()
		capabilities := c.peer
		c.mu.Unlock()
		for _, option := range m.Options {
			switch option.Number {
			case OptionMaxMessageSize:
				v, err := option.Uint32()
				if err != nil || v == 0 {
					_ = c.Abort("invalid Max-Message-Size")
					return false
				}
				capabilities.MaxMessageSize = v
			case OptionBlockWiseTransfer:
				if len(option.Value) != 0 {
					_ = c.Abort("invalid Block-Wise-Transfer option")
					return false
				}
				capabilities.BlockWiseTransfer = true
			}
		}
		c.mu.Lock()
		c.peer = capabilities
		c.mu.Unlock()
		c.peerOnce.Do(func() { close(c.peerReady) })
	case Ping:
		_ = c.Send(&Message{Code: Pong, Token: append([]byte(nil), m.Token...)})
	case Pong:
		c.mu.Lock()
		response := c.pending[string(m.Token)]
		c.mu.Unlock()
		if response != nil {
			select {
			case response <- m:
			default:
			}
		}
	case Release:
		go func() {
			c.handlers.Wait()
			_ = c.Close()
		}()
		return false
	case Abort:
		c.fail(fmt.Errorf("coap: peer aborted connection: %s", m.Payload))
		return false
	}
	return true
}

func (c *TCPConn) handleRequest(request *Message) {
	response := &Message{Code: NotImplemented}
	if c.cfg.Handler != nil {
		var err error
		response, err = c.cfg.Handler.ServeCOAP(c.ctx, request)
		if err != nil {
			response = &Message{Code: InternalServerError}
		}
	}
	if response == nil {
		return
	}
	response = response.Clone()
	response.Token = append([]byte(nil), request.Token...)
	response.LocalAddr, response.RemoteAddr = c.conn.LocalAddr(), c.conn.RemoteAddr()
	_ = c.Send(response)
}

// ServeTCP accepts connections until the listener closes. It calls onConn for
// each initialized connection. onConn can retain the connection.
func ServeTCP(listener net.Listener, config TCPConfig, onConn func(*TCPConn)) error {
	if listener == nil {
		return errors.New("coap: nil TCP listener")
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		coapConn, err := NewTCPConn(conn, config)
		if err != nil {
			_ = conn.Close()
			continue
		}
		if onConn != nil {
			go onConn(coapConn)
		}
	}
}
