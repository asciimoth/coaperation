package coap

import (
	"errors"
	"fmt"
)

const MaxBlockNumber uint32 = 0xfffff

// Block describes a Block1 or Block2 option value.
type Block struct {
	Number uint32
	More   bool
	Size   int
}

// ParseBlock decodes and validates a Block1 or Block2 value.
func ParseBlock(value []byte) (Block, error) {
	if len(value) > 3 {
		return Block{}, errors.New("coap: block option is longer than 3 bytes")
	}
	var raw uint32
	for _, b := range value {
		raw = raw<<8 | uint32(b)
	}
	szx := raw & 7
	if szx == 7 {
		return Block{}, errors.New("coap: BERT block size is not supported")
	}
	b := Block{Number: raw >> 4, More: raw&8 != 0, Size: 1 << (szx + 4)}
	if b.Number > MaxBlockNumber {
		return Block{}, errors.New("coap: block number exceeds 20 bits")
	}
	return b, nil
}

// Option encodes b as a Block1 or Block2 option.
func (b Block) Option(number OptionNumber) (Option, error) {
	if number != OptionBlock1 && number != OptionBlock2 {
		return Option{}, errors.New("coap: block option number must be Block1 or Block2")
	}
	if b.Number > MaxBlockNumber {
		return Option{}, errors.New("coap: block number exceeds 20 bits")
	}
	if b.Size < 16 || b.Size > 1024 || b.Size&(b.Size-1) != 0 {
		return Option{}, errors.New("coap: block size must be a power of two from 16 through 1024")
	}
	szx := uint32(0)
	for size := 16; size < b.Size; size <<= 1 {
		szx++
	}
	raw := b.Number<<4 | szx
	if b.More {
		raw |= 8
	}
	return UintOption(number, raw), nil
}

// Offset returns the byte offset of b in the complete body.
func (b Block) Offset() uint64 { return uint64(b.Number) * uint64(b.Size) }

// BlockReassembler validates sequential blocks and keeps the complete body.
// It does not return the body until the last block arrives.
type BlockReassembler struct {
	next uint64
	body []byte
	done bool
}

// Add validates and appends one sequential block.
func (r *BlockReassembler) Add(block Block, payload []byte) (bool, error) {
	if r.done {
		return true, errors.New("coap: block transfer is already complete")
	}
	if block.Offset() != r.next {
		return false, fmt.Errorf("coap: block offset %d, expected %d", block.Offset(), r.next)
	}
	if block.More && len(payload) != block.Size {
		return false, errors.New("coap: non-final block payload does not match its block size")
	}
	if len(payload) > block.Size {
		return false, errors.New("coap: block payload exceeds its block size")
	}
	r.body = append(r.body, payload...)
	r.next += uint64(len(payload))
	r.done = !block.More
	return r.done, nil
}

// Body returns a copy of the body only after the final block arrives.
func (r *BlockReassembler) Body() ([]byte, bool) {
	if !r.done {
		return nil, false
	}
	return append([]byte(nil), r.body...), true
}

// Reset discards all reassembly state.
func (r *BlockReassembler) Reset() {
	r.next, r.body, r.done = 0, nil, false
}

// ValidateHopLimit returns a valid Hop-Limit value from 1 through 255.
func ValidateHopLimit(option Option) (uint8, error) {
	if option.Number != OptionHopLimit {
		return 0, errors.New("coap: option is not Hop-Limit")
	}
	v, err := option.Uint32()
	if err != nil || v == 0 || v > 255 {
		return 0, errors.New("coap: Hop-Limit must be from 1 through 255")
	}
	return uint8(v), nil
}

// DecrementHopLimit returns an option for a forwarded request. It returns
// HopLimitReached when the request must not be forwarded.
func DecrementHopLimit(option Option) (Option, Code, error) {
	v, err := ValidateHopLimit(option)
	if err != nil {
		return Option{}, BadRequest, err
	}
	if v == 1 {
		return Option{}, HopLimitReached, nil
	}
	return UintOption(OptionHopLimit, uint32(v-1)), CodeEmpty, nil
}
