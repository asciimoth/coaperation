package coap

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
)

type Requester interface {
	// Request performs one raw CoAP request and response exchange.
	Request(context.Context, *Message) (*Message, error)
}

// TransferMode selects single-message or block-wise request transfer.
type TransferMode uint8

// Transfer modes for RequestBody.
const (
	TransferAuto TransferMode = iota
	TransferSingleMessage
	TransferBlockWise
)

type TransferConfig struct {
	// Mode defaults to TransferAuto.
	Mode TransferMode
	// BlockSize defaults to 1024 bytes.
	BlockSize int
	// SingleMessageThreshold defaults to 32 KiB.
	SingleMessageThreshold int
}

func (c TransferConfig) defaults() TransferConfig {
	if c.BlockSize == 0 {
		c.BlockSize = 1024
	}
	if c.SingleMessageThreshold == 0 {
		c.SingleMessageThreshold = 32 * 1024
	}
	return c
}

// RequestBody sends a complete request body and returns only a complete
// response body. Use Request directly to access raw block-wise messages.
func RequestBody(ctx context.Context, requester Requester, request *Message, config TransferConfig) (*Message, error) {
	if requester == nil || request == nil {
		return nil, errors.New("coap: nil requester or request")
	}
	config = config.defaults()
	if config.Mode > TransferBlockWise {
		return nil, errors.New("coap: invalid transfer mode")
	}
	if config.SingleMessageThreshold < 0 {
		return nil, errors.New("coap: SingleMessageThreshold must not be negative")
	}
	if _, err := (Block{Size: config.BlockSize}).Option(OptionBlock1); err != nil {
		return nil, err
	}
	request = request.Clone()
	if uint64(len(request.Payload)) > math.MaxUint32 {
		return nil, errors.New("coap: body exceeds Size1 integer range")
	}
	if len(request.Token) == 0 {
		var err error
		request.Token, err = newToken()
		if err != nil {
			return nil, err
		}
	}
	useBlocks := config.Mode == TransferBlockWise || config.Mode == TransferAuto && len(request.Payload) > config.SingleMessageThreshold
	var response *Message
	var err error
	if useBlocks && len(request.Payload) > 0 {
		response, err = sendBlock1(ctx, requester, request, config.BlockSize)
	} else {
		response, err = requester.Request(ctx, request)
	}
	if err != nil {
		return nil, err
	}
	return receiveBlock2(ctx, requester, request, response)
}

func sendBlock1(ctx context.Context, requester Requester, request *Message, blockSize int) (*Message, error) {
	offset := 0
	for {
		if offset%blockSize != 0 {
			return nil, errors.New("coap: server block-size reduction did not align with sent bytes")
		}
		end := offset + blockSize
		if end > len(request.Payload) {
			end = len(request.Payload)
		}
		more := end < len(request.Payload)
		block := Block{Number: uint32(offset / blockSize), More: more, Size: blockSize}
		option, _ := block.Option(OptionBlock1)
		part := request.Clone()
		part.Payload = append([]byte(nil), request.Payload[offset:end]...)
		part.Options = withOptions(part.Options, option, UintOption(OptionSize1, uint32(len(request.Payload))))
		response, err := requester.Request(ctx, part)
		if err != nil {
			return nil, err
		}
		if response.Code == RequestEntityIncomplete || response.Code == RequestEntityTooLarge {
			return response, fmt.Errorf("coap: Block1 transfer rejected with %s", response.Code)
		}
		if more && response.Code != Continue {
			return response, fmt.Errorf("coap: expected 2.31 Continue, got %s", response.Code)
		}
		offset = end
		acknowledged, hasAcknowledgement := response.Option(OptionBlock1)
		if more && !hasAcknowledgement {
			return nil, errors.New("coap: 2.31 Continue response has no Block1 option")
		}
		if hasAcknowledgement {
			accepted, err := ParseBlock(acknowledged.Value)
			if err != nil {
				return nil, err
			}
			if accepted.Size > blockSize {
				return nil, errors.New("coap: server increased Block1 size")
			}
			if offset%accepted.Size != 0 || accepted.Number != uint32(offset/accepted.Size-1) {
				return nil, errors.New("coap: Block1 acknowledgement has an unexpected block number")
			}
			blockSize = accepted.Size
		}
		if !more {
			return response, nil
		}
	}
}

func receiveBlock2(ctx context.Context, requester Requester, original, first *Message) (*Message, error) {
	option, ok := first.Option(OptionBlock2)
	if !ok {
		return first, nil
	}
	block, err := ParseBlock(option.Value)
	if err != nil {
		return nil, err
	}
	var reassembler BlockReassembler
	var expectedSize uint32
	var hasExpectedSize bool
	if size, present := first.Option(OptionSize2); present {
		expectedSize, err = size.Uint32()
		if err != nil {
			return nil, err
		}
		hasExpectedSize = true
	}
	done, err := reassembler.Add(block, first.Payload)
	if err != nil {
		return nil, err
	}
	response := first.Clone()
	for !done {
		nextOffset := block.Offset() + uint64(len(response.Payload))
		if nextOffset%uint64(block.Size) != 0 {
			return nil, errors.New("coap: Block2 offset is not aligned")
		}
		next := Block{Number: uint32(nextOffset / uint64(block.Size)), Size: block.Size}
		nextOption, _ := next.Option(OptionBlock2)
		request := original.Clone()
		request.Payload = nil
		request.Options = withoutOptions(request.Options, OptionBlock1, OptionSize1, OptionBlock2)
		request.Options = withOptions(request.Options, nextOption)
		response, err = requester.Request(ctx, request)
		if err != nil {
			return nil, err
		}
		got, present := response.Option(OptionBlock2)
		if !present {
			return nil, errors.New("coap: Block2 response has no Block2 option")
		}
		block, err = ParseBlock(got.Value)
		if err != nil {
			return nil, err
		}
		if size, present := response.Option(OptionSize2); present {
			value, sizeErr := size.Uint32()
			if sizeErr != nil {
				return nil, sizeErr
			}
			if hasExpectedSize && value != expectedSize {
				return nil, errors.New("coap: Size2 changed during transfer")
			}
			expectedSize, hasExpectedSize = value, true
		}
		done, err = reassembler.Add(block, response.Payload)
		if err != nil {
			return nil, err
		}
	}
	body, _ := reassembler.Body()
	if hasExpectedSize && uint64(expectedSize) != uint64(len(body)) {
		return nil, errors.New("coap: reassembled body length does not match Size2")
	}
	result := response.Clone()
	result.Payload = body
	result.Options = withoutOptions(first.Options, OptionBlock2)
	return result, nil
}

func withoutOptions(options []Option, numbers ...OptionNumber) []Option {
	remove := make(map[OptionNumber]bool, len(numbers))
	for _, n := range numbers {
		remove[n] = true
	}
	result := make([]Option, 0, len(options))
	for _, option := range options {
		if !remove[option.Number] {
			result = append(result, NewOption(option.Number, option.Value))
		}
	}
	return result
}

func withOptions(options []Option, replacements ...Option) []Option {
	numbers := make([]OptionNumber, 0, len(replacements))
	for _, option := range replacements {
		numbers = append(numbers, option.Number)
	}
	result := withoutOptions(options, numbers...)
	for _, option := range replacements {
		result = append(result, NewOption(option.Number, option.Value))
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].Number < result[j].Number })
	return result
}
