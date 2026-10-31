package coap

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"time"
)

type BlockWiseConfig struct {
	// MaxBodySize limits reassembled request and response bodies. The default
	// is 16 MiB.
	MaxBodySize int
	// BlockSize is a power of two from 16 through 1024. The default is 1024.
	BlockSize int
	// StateLifetime controls lazy transfer-state expiry. The default is two
	// minutes.
	StateLifetime time.Duration
}

func (c BlockWiseConfig) defaults() BlockWiseConfig {
	if c.MaxBodySize == 0 {
		c.MaxBodySize = 16 << 20
	}
	if c.BlockSize == 0 {
		c.BlockSize = 1024
	}
	if c.StateLifetime == 0 {
		c.StateLifetime = 2 * time.Minute
	}
	return c
}

type inboundTransfer struct {
	reassembler     BlockReassembler
	expires         time.Time
	expectedSize    uint32
	hasExpectedSize bool
}

type outboundTransfer struct {
	response *Message
	expires  time.Time
}

// BlockWiseHandler adds complete-body Block1 and Block2 handling to a Handler.
// Applications that need raw blocks must use their Handler directly.
type BlockWiseHandler struct {
	next     Handler
	config   BlockWiseConfig
	mu       sync.Mutex
	inbound  map[string]*inboundTransfer
	outbound map[string]*outboundTransfer
}

// NewBlockWiseHandler wraps next with complete-body Block1 and Block2 support.
func NewBlockWiseHandler(next Handler, config BlockWiseConfig) (*BlockWiseHandler, error) {
	if next == nil {
		return nil, errors.New("coap: nil block-wise handler")
	}
	config = config.defaults()
	if _, err := (Block{Size: config.BlockSize}).Option(OptionBlock2); err != nil {
		return nil, err
	}
	if config.MaxBodySize < config.BlockSize {
		return nil, errors.New("coap: MaxBodySize is smaller than BlockSize")
	}
	if uint64(config.MaxBodySize) > math.MaxUint32 {
		return nil, errors.New("coap: MaxBodySize exceeds Size1 and Size2 range")
	}
	if config.StateLifetime < 0 {
		return nil, errors.New("coap: StateLifetime must not be negative")
	}
	return &BlockWiseHandler{next: next, config: config, inbound: make(map[string]*inboundTransfer), outbound: make(map[string]*outboundTransfer)}, nil
}

func (h *BlockWiseHandler) ServeCOAP(ctx context.Context, request *Message) (*Message, error) {
	if request == nil {
		return nil, errors.New("coap: nil block-wise request")
	}
	key := transferKey(request)
	h.expire()
	if blockOption, ok := request.Option(OptionBlock1); ok {
		block, err := ParseBlock(blockOption.Value)
		if err != nil {
			return &Message{Code: BadOption}, nil
		}
		var size1 uint32
		size1Option, hasSize1 := request.Option(OptionSize1)
		if hasSize1 {
			v, err := size1Option.Uint32()
			if err != nil {
				return &Message{Code: BadOption}, nil
			}
			if uint64(v) > uint64(h.config.MaxBodySize) {
				return &Message{Code: RequestEntityTooLarge}, nil
			}
			size1 = v
		}
		h.mu.Lock()
		state := h.inbound[key]
		if block.Number == 0 {
			state = &inboundTransfer{}
			h.inbound[key] = state
		}
		if state == nil {
			h.mu.Unlock()
			return &Message{Code: RequestEntityIncomplete}, nil
		}
		if hasSize1 {
			if state.hasExpectedSize && state.expectedSize != size1 {
				h.mu.Unlock()
				return &Message{Code: RequestEntityIncomplete}, nil
			}
			state.expectedSize = size1
			state.hasExpectedSize = true
		}
		done, addErr := state.reassembler.Add(block, request.Payload)
		state.expires = time.Now().Add(h.config.StateLifetime)
		if addErr != nil {
			delete(h.inbound, key)
			h.mu.Unlock()
			return &Message{Code: RequestEntityIncomplete}, nil
		}
		if len(state.reassembler.body) > h.config.MaxBodySize {
			delete(h.inbound, key)
			h.mu.Unlock()
			return &Message{Code: RequestEntityTooLarge}, nil
		}
		if !done {
			acceptedSize := block.Size
			if acceptedSize > h.config.BlockSize {
				acceptedSize = h.config.BlockSize
			}
			ack, _ := (Block{Number: block.Number, More: true, Size: acceptedSize}).Option(OptionBlock1)
			h.mu.Unlock()
			return &Message{Code: Continue, Options: []Option{ack}}, nil
		}
		body, _ := state.reassembler.Body()
		if state.hasExpectedSize && uint64(state.expectedSize) != uint64(len(body)) {
			delete(h.inbound, key)
			h.mu.Unlock()
			return &Message{Code: RequestEntityIncomplete}, nil
		}
		delete(h.inbound, key)
		h.mu.Unlock()
		request = request.Clone()
		request.Payload = body
		request.Options = withoutOptions(request.Options, OptionBlock1, OptionSize1)
	}

	requestedBlock, hasBlock2, err := requestedBlock2(request)
	if err != nil {
		return &Message{Code: BadOption}, nil
	}
	if hasBlock2 && requestedBlock.Number > 0 {
		h.mu.Lock()
		cached := h.outbound[key]
		h.mu.Unlock()
		if cached == nil {
			return &Message{Code: RequestEntityIncomplete}, nil
		}
		return h.responseBlock(cached.response, requestedBlock)
	}

	response, err := h.next.ServeCOAP(ctx, request)
	if err != nil || response == nil {
		return response, err
	}
	if len(response.Payload) > h.config.MaxBodySize {
		return nil, errors.New("coap: response body exceeds MaxBodySize")
	}
	blockSize := h.config.BlockSize
	if hasBlock2 && requestedBlock.Size < blockSize {
		blockSize = requestedBlock.Size
	}
	if len(response.Payload) <= blockSize && !hasBlock2 {
		return response, nil
	}
	full := response.Clone()
	h.mu.Lock()
	h.outbound[key] = &outboundTransfer{response: full, expires: time.Now().Add(h.config.StateLifetime)}
	h.mu.Unlock()
	if !hasBlock2 {
		requestedBlock = Block{Number: 0, Size: blockSize}
	}
	return h.responseBlock(full, requestedBlock)
}

func requestedBlock2(request *Message) (Block, bool, error) {
	option, ok := request.Option(OptionBlock2)
	if !ok {
		return Block{}, false, nil
	}
	block, err := ParseBlock(option.Value)
	if err != nil {
		return Block{}, true, err
	}
	// RFC 7959 requires a sender to clear More in a Block2 request and a
	// receiver to ignore it. Keep the decoded value out of server logic.
	block.More = false
	return block, true, nil
}

func (h *BlockWiseHandler) responseBlock(full *Message, block Block) (*Message, error) {
	start := block.Offset()
	if block.Size > h.config.BlockSize {
		block.Size = h.config.BlockSize
		block.Number = uint32(start / uint64(block.Size))
	}
	if start >= uint64(len(full.Payload)) && (start != 0 || len(full.Payload) != 0) {
		return &Message{Code: BadOption}, nil
	}
	end := start + uint64(block.Size)
	if end > uint64(len(full.Payload)) {
		end = uint64(len(full.Payload))
	}
	block.More = end < uint64(len(full.Payload))
	option, err := block.Option(OptionBlock2)
	if err != nil {
		return nil, err
	}
	response := full.Clone()
	response.Payload = append([]byte(nil), full.Payload[start:end]...)
	response.Options = withOptions(response.Options, option, UintOption(OptionSize2, uint32(len(full.Payload))))
	return response, nil
}

func transferKey(m *Message) string {
	var key []byte
	appendString := func(value string) {
		key = binary.BigEndian.AppendUint32(key, uint32(len(value)))
		key = append(key, value...)
	}
	local, remote := "", ""
	if m.LocalAddr != nil {
		local = m.LocalAddr.String()
	}
	if m.RemoteAddr != nil {
		remote = m.RemoteAddr.String()
	}
	appendString(local)
	appendString(remote)
	key = append(key, byte(m.Code))
	for _, option := range m.Options {
		switch option.Number {
		case OptionBlock1, OptionBlock2, OptionSize1, OptionSize2:
			continue
		}
		key = binary.BigEndian.AppendUint32(key, uint32(option.Number))
		key = binary.BigEndian.AppendUint32(key, uint32(len(option.Value)))
		key = append(key, option.Value...)
	}
	return string(key)
}

func (h *BlockWiseHandler) expire() {
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	for key, state := range h.inbound {
		if now.After(state.expires) {
			delete(h.inbound, key)
		}
	}
	for key, state := range h.outbound {
		if now.After(state.expires) {
			delete(h.outbound, key)
		}
	}
}
