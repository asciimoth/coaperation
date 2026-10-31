package coap

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type Handler interface {
	// ServeCOAP handles one request. A nil response is allowed.
	ServeCOAP(context.Context, *Message) (*Message, error)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, *Message) (*Message, error)

func (f HandlerFunc) ServeCOAP(ctx context.Context, m *Message) (*Message, error) {
	return f(ctx, m)
}

// PacketConn is the minimal datagram I/O required by UDPEndpoint.
// net.PacketConn and *net.UDPConn implement this interface. An implementation
// must allow one ReadFrom and one WriteTo call at the same time. Close must
// unblock active I/O calls. UDPEndpoint serializes WriteTo calls.
type PacketConn interface {
	ReadFrom([]byte) (int, net.Addr, error)
	WriteTo([]byte, net.Addr) (int, error)
	LocalAddr() net.Addr
	Close() error
}

type packetMetadataReader interface {
	ReadPacket([]byte) (int, net.Addr, net.Addr, bool, error)
}

type UDPConfig struct {
	// Handler receives requests. A nil Handler only dispatches responses.
	Handler Handler
	// MaxDatagramSize defaults to 65535 bytes.
	MaxDatagramSize int
	// ACKTimeout defaults to two seconds.
	ACKTimeout time.Duration
	// MaxRetransmit defaults to four retransmissions.
	MaxRetransmit int
	// NStart limits active Confirmable exchanges per peer. The default is one.
	NStart int
	// ExchangeLifetime defaults to 247 seconds.
	ExchangeLifetime time.Duration
	// MulticastLeisure defaults to five seconds.
	MulticastLeisure time.Duration
	// ErrorHandler receives malformed or oversized incoming datagram errors.
	// The endpoint continues to run after these errors. The handler must return
	// promptly because the endpoint calls it from the read loop.
	ErrorHandler func(error)
}

func (c UDPConfig) defaults() UDPConfig {
	if c.MaxDatagramSize == 0 {
		c.MaxDatagramSize = 65535
	}
	if c.ACKTimeout == 0 {
		c.ACKTimeout = 2 * time.Second
	}
	if c.MaxRetransmit == 0 {
		c.MaxRetransmit = 4
	}
	if c.NStart == 0 {
		c.NStart = 1
	}
	if c.ExchangeLifetime == 0 {
		c.ExchangeLifetime = 247 * time.Second
	}
	if c.MulticastLeisure == 0 {
		c.MulticastLeisure = 5 * time.Second
	}
	return c
}

type udpPending struct {
	responses chan *Message
	ack       chan struct{}
	failures  chan error
	multi     bool
	remote    string
	messageID uint16
	queueMu   sync.Mutex
	queue     []*Message
	notify    chan struct{}
}

func (p *udpPending) enqueue(m *Message) {
	p.queueMu.Lock()
	p.queue = append(p.queue, m)
	p.queueMu.Unlock()
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

func (p *udpPending) dequeue() *Message {
	p.queueMu.Lock()
	defer p.queueMu.Unlock()
	if len(p.queue) == 0 {
		return nil
	}
	m := p.queue[0]
	p.queue[0] = nil
	p.queue = p.queue[1:]
	return m
}

type duplicateEntry struct {
	expires  time.Time
	response []byte
}

// UDPEndpoint sends and receives requests on one UDP socket.
type UDPEndpoint struct {
	conn    PacketConn
	cfg     UDPConfig
	next    atomic.Uint32
	writeMu sync.Mutex

	mu                 sync.Mutex
	pending            map[string]*udpPending
	messageACK         map[string]*udpPending
	duplicates         map[string]duplicateEntry
	responseDuplicates map[string]duplicateEntry
	nextDuplicatePrune time.Time
	peerSlots          map[string]chan struct{}
	closed             chan struct{}
	closeOnce          sync.Once
	errMu              sync.Mutex
	err                error
	ctx                context.Context
	cancel             context.CancelFunc
}

// NewUDPEndpoint starts CoAP on an existing datagram connection. Closing the
// endpoint closes conn.
func NewUDPEndpoint(conn PacketConn, config UDPConfig) (*UDPEndpoint, error) {
	if conn == nil || isNilPacketConn(conn) {
		return nil, errors.New("coap: nil UDP connection")
	}
	config = config.defaults()
	if config.MaxDatagramSize < 4 || config.MaxDatagramSize > 65535 {
		return nil, errors.New("coap: MaxDatagramSize must be from 4 through 65535")
	}
	if config.ACKTimeout < 0 || config.MaxRetransmit < 0 || config.NStart < 0 || config.ExchangeLifetime < 0 || config.MulticastLeisure < 0 {
		return nil, errors.New("coap: UDP timing values, NStart, and MaxRetransmit must not be negative")
	}
	var seed [4]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil, fmt.Errorf("coap: initialize message IDs: %w", err)
	}
	endpointContext, cancel := context.WithCancel(context.Background())
	e := &UDPEndpoint{conn: conn, cfg: config, pending: make(map[string]*udpPending), messageACK: make(map[string]*udpPending), duplicates: make(map[string]duplicateEntry), responseDuplicates: make(map[string]duplicateEntry), peerSlots: make(map[string]chan struct{}), closed: make(chan struct{}), ctx: endpointContext, cancel: cancel}
	e.next.Store(binary.BigEndian.Uint32(seed[:]))
	go e.readLoop()
	return e, nil
}

func isNilPacketConn(conn PacketConn) bool {
	value := reflect.ValueOf(conn)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// ListenIPv6Multicast joins the link-local all-CoAP-nodes group ff02::fd on
// the selected interface. A zero port uses the default CoAP port 5683.
func ListenIPv6Multicast(interfaceName string, port int, config UDPConfig) (*UDPEndpoint, error) {
	if port == 0 {
		port = 5683
	}
	iface, err := net.InterfaceByName(interfaceName)
	if err != nil {
		return nil, err
	}
	group := &net.UDPAddr{IP: net.ParseIP("ff02::fd"), Port: port, Zone: interfaceName}
	conn, err := net.ListenMulticastUDP("udp6", iface, group)
	if err != nil {
		return nil, err
	}
	if err := enableIPv6PacketInfo(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("coap: enable IPv6 packet information: %w", err)
	}
	e, err := NewUDPEndpoint(&udpSocket{UDPConn: conn, multicast: true}, config)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return e, nil
}

// IPv6AllCoAPNodes returns the scoped ff02::fd destination for an interface.
func IPv6AllCoAPNodes(interfaceName string, port int) *net.UDPAddr {
	if port == 0 {
		port = 5683
	}
	return &net.UDPAddr{IP: net.ParseIP("ff02::fd"), Port: port, Zone: interfaceName}
}

// ListenUDP creates a UDP socket and starts a CoAP endpoint.
func ListenUDP(network, address string, config UDPConfig) (*UDPEndpoint, error) {
	addr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP(network, addr)
	if err != nil {
		return nil, err
	}
	e, err := NewUDPEndpoint(conn, config)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return e, nil
}

// LocalAddr returns the endpoint socket address.
func (e *UDPEndpoint) LocalAddr() net.Addr { return e.conn.LocalAddr() }

// Close stops the endpoint and cancels active handler contexts.
func (e *UDPEndpoint) Close() error {
	e.closeOnce.Do(func() { e.cancel(); close(e.closed); _ = e.conn.Close() })
	return e.Err()
}

// Err returns the terminal read error, if one occurred.
func (e *UDPEndpoint) Err() error { e.errMu.Lock(); defer e.errMu.Unlock(); return e.err }

func (e *UDPEndpoint) setError(err error) {
	e.errMu.Lock()
	if e.err == nil {
		e.err = err
	}
	e.errMu.Unlock()
	e.closeOnce.Do(func() { e.cancel(); close(e.closed); _ = e.conn.Close() })
}

func (e *UDPEndpoint) newMessageID() uint16 { return uint16(e.next.Add(1)) }

func newToken() ([]byte, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}

// Send writes one raw UDP message without retransmission.
func (e *UDPEndpoint) Send(m *Message) error {
	if m == nil {
		return errors.New("coap: nil message")
	}
	if m.RemoteAddr == nil {
		return errors.New("coap: UDP message has no RemoteAddr")
	}
	b, err := MarshalUDP(m)
	if err != nil {
		return err
	}
	if len(b) > e.cfg.MaxDatagramSize {
		return fmt.Errorf("coap: datagram size %d exceeds local limit %d", len(b), e.cfg.MaxDatagramSize)
	}
	return e.writeDatagram(b, m.RemoteAddr)
}

func (e *UDPEndpoint) writeDatagram(data []byte, remote net.Addr) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	n, err := e.conn.WriteTo(data, remote)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

// SendConfirmable sends a non-request Confirmable message with RFC 7252
// retransmissions and waits for an acknowledgement or reset. Use Request for
// Confirmable requests.
func (e *UDPEndpoint) SendConfirmable(ctx context.Context, message *Message) error {
	if message == nil {
		return errors.New("coap: nil message")
	}
	if message.Code.IsRequest() {
		return errors.New("coap: use Request to send a Confirmable request")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m := message.Clone()
	if m.RemoteAddr == nil {
		return errors.New("coap: message has no RemoteAddr")
	}
	m.Type = Confirmable
	if m.MessageID == 0 {
		m.MessageID = e.newMessageID()
	}
	if err := e.acquirePeer(ctx, m.RemoteAddr.String()); err != nil {
		return err
	}
	defer e.releasePeer(m.RemoteAddr.String())
	p := &udpPending{ack: make(chan struct{}, 1), failures: make(chan error, 1), remote: m.RemoteAddr.String(), messageID: m.MessageID}
	ackKey := udpACKKey(p.remote, m.MessageID)
	e.mu.Lock()
	if _, exists := e.messageACK[ackKey]; exists {
		e.mu.Unlock()
		return errors.New("coap: message ID is already in use for this peer")
	}
	e.messageACK[ackKey] = p
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		if e.messageACK[ackKey] == p {
			delete(e.messageACK, ackKey)
		}
		e.mu.Unlock()
	}()
	if err := e.Send(m); err != nil {
		return err
	}
	timeout := e.jitter(e.cfg.ACKTimeout)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	transmits := 0
	for {
		select {
		case <-p.ack:
			return nil
		case err := <-p.failures:
			return err
		case <-timer.C:
			if transmits >= e.cfg.MaxRetransmit {
				return errors.New("coap: confirmable message timed out")
			}
			if err := e.Send(m); err != nil {
				return err
			}
			transmits++
			timeout *= 2
			timer.Reset(timeout)
		case <-ctx.Done():
			return ctx.Err()
		case <-e.closed:
			return e.closedError()
		}
	}
}

// Request sends one unicast request. Confirmable requests are retransmitted
// until an acknowledgement or response arrives.
func (e *UDPEndpoint) Request(ctx context.Context, request *Message) (*Message, error) {
	if request == nil {
		return nil, errors.New("coap: nil request")
	}
	if !request.Code.IsRequest() {
		return nil, errors.New("coap: message code is not a request")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r := request.Clone()
	if r.RemoteAddr == nil {
		return nil, errors.New("coap: request has no RemoteAddr")
	}
	if len(r.Token) == 0 {
		var err error
		r.Token, err = newToken()
		if err != nil {
			return nil, err
		}
	}
	if r.MessageID == 0 {
		r.MessageID = e.newMessageID()
	}
	if r.Type == Confirmable {
		if err := e.acquirePeer(ctx, r.RemoteAddr.String()); err != nil {
			return nil, err
		}
		defer e.releasePeer(r.RemoteAddr.String())
	}
	p := &udpPending{responses: make(chan *Message, 1), ack: make(chan struct{}, 1), failures: make(chan error, 1), remote: r.RemoteAddr.String()}
	key := string(r.Token)
	if err := e.addPending(key, r.MessageID, p); err != nil {
		return nil, err
	}
	defer e.removePending(key, r.MessageID)
	if err := e.Send(r); err != nil {
		return nil, err
	}
	if r.Type != Confirmable {
		return e.waitResponse(ctx, p)
	}

	timeout := e.jitter(e.cfg.ACKTimeout)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	acked := false
	transmits := 0
	for {
		select {
		case response := <-p.responses:
			return response, nil
		case <-p.ack:
			acked = true
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
			if acked {
				continue
			}
			if transmits >= e.cfg.MaxRetransmit {
				return nil, errors.New("coap: confirmable request timed out")
			}
			if err := e.Send(r); err != nil {
				return nil, err
			}
			transmits++
			timeout *= 2
			timer.Reset(timeout)
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-p.failures:
			return nil, err
		case <-e.closed:
			return nil, e.closedError()
		}
	}
}

func (e *UDPEndpoint) waitResponse(ctx context.Context, p *udpPending) (*Message, error) {
	select {
	case m := <-p.responses:
		return m, nil
	case err := <-p.failures:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.closed:
		return nil, e.closedError()
	}
}

// RequestMulticast sends a Non-confirmable request and returns all matching
// responses on a channel until ctx ends. The channel is then closed.
func (e *UDPEndpoint) RequestMulticast(ctx context.Context, request *Message) (<-chan *Message, error) {
	if request == nil || !request.Code.IsRequest() {
		return nil, errors.New("coap: invalid multicast request")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r := request.Clone()
	r.Type = NonConfirmable
	if len(r.Token) == 0 {
		var err error
		r.Token, err = newToken()
		if err != nil {
			return nil, err
		}
	}
	if r.MessageID == 0 {
		r.MessageID = e.newMessageID()
	}
	p := &udpPending{responses: make(chan *Message, 1), ack: make(chan struct{}, 1), failures: make(chan error, 1), multi: true, notify: make(chan struct{}, 1)}
	key := string(r.Token)
	if err := e.addPending(key, r.MessageID, p); err != nil {
		return nil, err
	}
	if err := e.Send(r); err != nil {
		e.removePending(key, r.MessageID)
		return nil, err
	}
	out := make(chan *Message)
	go func() {
		defer close(out)
		defer e.removePending(key, r.MessageID)
		for {
			if m := p.dequeue(); m != nil {
				select {
				case out <- m:
				case <-ctx.Done():
					return
				case <-e.closed:
					return
				}
				continue
			}
			select {
			case <-p.notify:
			case <-ctx.Done():
				return
			case <-e.closed:
				return
			}
		}
	}()
	return out, nil
}

func (e *UDPEndpoint) addPending(token string, mid uint16, p *udpPending) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.pending[token]; exists {
		return errors.New("coap: token is already in use")
	}
	ackKey := udpACKKey(p.remote, mid)
	if _, exists := e.messageACK[ackKey]; exists {
		return errors.New("coap: message ID is already in use for this peer")
	}
	p.messageID = mid
	e.pending[token] = p
	e.messageACK[ackKey] = p
	return nil
}

func (e *UDPEndpoint) removePending(token string, mid uint16) {
	e.mu.Lock()
	if p := e.pending[token]; p != nil {
		delete(e.messageACK, udpACKKey(p.remote, mid))
	}
	delete(e.pending, token)
	e.mu.Unlock()
}

func udpACKKey(remote string, mid uint16) string {
	return fmt.Sprintf("%s/%d", remote, mid)
}

func (e *UDPEndpoint) acquirePeer(ctx context.Context, remote string) error {
	e.mu.Lock()
	slots := e.peerSlots[remote]
	if slots == nil {
		slots = make(chan struct{}, e.cfg.NStart)
		e.peerSlots[remote] = slots
	}
	e.mu.Unlock()
	select {
	case slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.closed:
		return e.closedError()
	}
}

func (e *UDPEndpoint) releasePeer(remote string) {
	e.mu.Lock()
	slots := e.peerSlots[remote]
	e.mu.Unlock()
	if slots != nil {
		<-slots
	}
}

func (e *UDPEndpoint) jitter(base time.Duration) time.Duration {
	var b [2]byte
	_, _ = rand.Read(b[:])
	extra := durationFraction(base, uint64(binary.BigEndian.Uint16(b[:])), 131072)
	const maxDuration = time.Duration(1<<63 - 1)
	if base > maxDuration-extra {
		return maxDuration
	}
	return base + extra
}

// durationFraction scales a nonnegative duration with numerator < denominator.
func durationFraction(base time.Duration, numerator, denominator uint64) time.Duration {
	whole := uint64(base) / denominator
	remainder := uint64(base) % denominator
	return time.Duration(whole*numerator + remainder*numerator/denominator)
}

func (e *UDPEndpoint) closedError() error {
	if err := e.Err(); err != nil {
		return err
	}
	return net.ErrClosed
}

func (e *UDPEndpoint) readLoop() {
	bufferSize := e.cfg.MaxDatagramSize
	if bufferSize < 65535 {
		bufferSize++
	}
	buf := make([]byte, bufferSize)
	for {
		var n int
		var remote net.Addr
		local := e.conn.LocalAddr()
		multicast := false
		var err error
		if reader, ok := e.conn.(packetMetadataReader); ok {
			n, local, remote, multicast, err = reader.ReadPacket(buf)
		} else {
			n, remote, err = e.conn.ReadFrom(buf)
		}
		if err != nil {
			select {
			case <-e.closed:
				return
			default:
				e.setError(err)
				return
			}
		}
		if n < 0 || n > len(buf) {
			e.setError(fmt.Errorf("coap: packet connection returned invalid byte count %d", n))
			return
		}
		if n > e.cfg.MaxDatagramSize {
			e.reportError(fmt.Errorf("coap: incoming datagram size exceeds local limit %d", e.cfg.MaxDatagramSize))
			continue
		}
		if remote == nil {
			e.reportError(errors.New("coap: packet connection returned no remote address"))
			continue
		}
		m, err := UnmarshalUDP(buf[:n])
		if err != nil {
			e.reportError(err)
			if !multicast && n >= 4 && buf[0]>>6 == 1 && buf[0]>>4&3 == byte(Confirmable) {
				if resetErr := e.Send(&Message{Type: Reset, MessageID: binary.BigEndian.Uint16(buf[2:4]), RemoteAddr: remote}); resetErr != nil {
					e.reportError(resetErr)
				}
			}
			continue
		}
		m.RemoteAddr, m.LocalAddr, m.Multicast = remote, local, multicast
		if m.Multicast && m.Type != NonConfirmable {
			continue
		}
		if m.Type == Confirmable && m.Code == CodeEmpty {
			if !m.Multicast {
				if err := e.Send(&Message{Type: Reset, Code: CodeEmpty, MessageID: m.MessageID, RemoteAddr: remote}); err != nil {
					e.reportError(err)
				}
			}
			continue
		}
		if m.Code.IsRequest() {
			go e.handleRequest(m)
			continue
		}
		e.handleResponse(m)
	}
}

func (e *UDPEndpoint) reportError(err error) {
	if e.cfg.ErrorHandler != nil {
		e.cfg.ErrorHandler(err)
	}
}

func (e *UDPEndpoint) handleResponse(m *Message) {
	now := time.Now()
	key := udpACKKey(m.RemoteAddr.String(), m.MessageID)
	e.mu.Lock()
	e.pruneDuplicatesLocked(now)
	if m.Code.IsResponse() && (m.Type == Confirmable || m.Type == NonConfirmable) {
		if entry, found := e.responseDuplicates[key]; found && now.Before(entry.expires) {
			e.mu.Unlock()
			if m.Type == Confirmable && len(entry.response) != 0 {
				if err := e.writeDatagram(entry.response, m.RemoteAddr); err != nil {
					e.reportError(err)
				}
			}
			return
		}
	}
	var p *udpPending
	if m.Code == CodeEmpty && (m.Type == Acknowledgement || m.Type == Reset) {
		p = e.messageACK[udpACKKey(m.RemoteAddr.String(), m.MessageID)]
	} else {
		p = e.pending[string(m.Token)]
	}
	e.mu.Unlock()
	if p == nil || (!p.multi && p.remote != "" && p.remote != m.RemoteAddr.String()) {
		if m.Type == Confirmable && m.Code.IsResponse() {
			if err := e.Send(&Message{Type: Reset, MessageID: m.MessageID, RemoteAddr: m.RemoteAddr}); err != nil {
				e.reportError(err)
			}
		}
		return
	}
	if m.Type == Acknowledgement && m.MessageID != p.messageID {
		return
	}
	if m.Type == Confirmable {
		ack, err := MarshalUDP(&Message{Type: Acknowledgement, MessageID: m.MessageID})
		if err != nil {
			e.reportError(err)
			return
		}
		e.mu.Lock()
		if e.responseDuplicates == nil {
			e.responseDuplicates = make(map[string]duplicateEntry)
		}
		e.responseDuplicates[key] = duplicateEntry{expires: now.Add(e.cfg.ExchangeLifetime), response: ack}
		e.mu.Unlock()
		if err := e.writeDatagram(ack, m.RemoteAddr); err != nil {
			e.reportError(err)
		}
	} else if m.Type == NonConfirmable && m.Code.IsResponse() {
		e.mu.Lock()
		if e.responseDuplicates == nil {
			e.responseDuplicates = make(map[string]duplicateEntry)
		}
		e.responseDuplicates[key] = duplicateEntry{expires: now.Add(e.cfg.ExchangeLifetime)}
		e.mu.Unlock()
	}
	if m.Code == CodeEmpty {
		if m.Type == Reset {
			select {
			case p.failures <- errors.New("coap: peer reset exchange"):
			default:
			}
			return
		}
		select {
		case p.ack <- struct{}{}:
		default:
		}
		return
	}
	if p.multi {
		p.enqueue(m)
		return
	}
	select {
	case p.responses <- m:
	default:
	}
}

func (e *UDPEndpoint) pruneDuplicatesLocked(now time.Time) {
	if now.Before(e.nextDuplicatePrune) {
		return
	}
	for key, item := range e.duplicates {
		if !now.Before(item.expires) {
			delete(e.duplicates, key)
		}
	}
	for key, item := range e.responseDuplicates {
		if !now.Before(item.expires) {
			delete(e.responseDuplicates, key)
		}
	}
	interval := e.cfg.ExchangeLifetime / 4
	if interval <= 0 || interval > time.Minute {
		interval = time.Minute
	}
	e.nextDuplicatePrune = now.Add(interval)
}

func (e *UDPEndpoint) handleRequest(request *Message) {
	duplicateKey := request.RemoteAddr.String() + "/" + fmt.Sprint(request.MessageID)
	now := time.Now()
	e.mu.Lock()
	e.pruneDuplicatesLocked(now)
	entry, duplicate := e.duplicates[duplicateKey]
	if !duplicate || !now.Before(entry.expires) {
		e.duplicates[duplicateKey] = duplicateEntry{expires: now.Add(e.cfg.ExchangeLifetime)}
	}
	e.mu.Unlock()
	if duplicate && now.Before(entry.expires) {
		if request.Type == Confirmable && len(entry.response) > 0 {
			if writeErr := e.writeDatagram(entry.response, request.RemoteAddr); writeErr != nil {
				e.reportError(writeErr)
			}
		}
		return
	}
	var response *Message
	var err error
	if e.cfg.Handler != nil {
		response, err = e.cfg.Handler.ServeCOAP(e.ctx, request)
	}
	if err != nil {
		response = &Message{Code: InternalServerError}
	}
	if response == nil && request.Type != Confirmable {
		return
	}
	if response == nil {
		response = &Message{Code: CodeEmpty}
	}
	if request.Multicast && e.cfg.MulticastLeisure > 0 {
		var random [2]byte
		_, _ = rand.Read(random[:])
		delay := durationFraction(e.cfg.MulticastLeisure, uint64(binary.BigEndian.Uint16(random[:])), 65536)
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-e.closed:
			timer.Stop()
			return
		}
	}
	response = response.Clone()
	response.RemoteAddr = request.RemoteAddr
	if response.Code != CodeEmpty {
		response.Token = append([]byte(nil), request.Token...)
	}
	if request.Type == Confirmable && response.Separate && response.Code != CodeEmpty {
		ack := &Message{Type: Acknowledgement, MessageID: request.MessageID, RemoteAddr: request.RemoteAddr}
		ackWire, ackErr := MarshalUDP(ack)
		if ackErr != nil {
			return
		}
		if ackErr = e.writeDatagram(ackWire, request.RemoteAddr); ackErr != nil {
			e.reportError(ackErr)
			return
		}
		e.mu.Lock()
		e.duplicates[duplicateKey] = duplicateEntry{expires: time.Now().Add(e.cfg.ExchangeLifetime), response: ackWire}
		e.mu.Unlock()
		response.Type = Confirmable
		if response.MessageID == 0 {
			response.MessageID = e.newMessageID()
		}
		go func() {
			if sendErr := e.SendConfirmable(e.ctx, response); sendErr != nil && !errors.Is(sendErr, net.ErrClosed) && !errors.Is(sendErr, context.Canceled) {
				e.reportError(sendErr)
			}
		}()
		return
	}
	if request.Type == Confirmable {
		response.Type = Acknowledgement
		response.MessageID = request.MessageID
	} else {
		response.Type = NonConfirmable
		response.MessageID = e.newMessageID()
	}
	b, marshalErr := MarshalUDP(response)
	if marshalErr != nil {
		return
	}
	if len(b) > e.cfg.MaxDatagramSize {
		return
	}
	if writeErr := e.writeDatagram(b, request.RemoteAddr); writeErr != nil {
		e.reportError(writeErr)
	}
	if request.Type == Confirmable {
		e.mu.Lock()
		e.duplicates[duplicateKey] = duplicateEntry{expires: time.Now().Add(e.cfg.ExchangeLifetime), response: b}
		e.mu.Unlock()
	}
}
