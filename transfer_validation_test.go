package coap

import (
	"context"
	"errors"
	"testing"
	"time"
)

type requesterFunc func(context.Context, *Message) (*Message, error)

func (f requesterFunc) Request(ctx context.Context, m *Message) (*Message, error) {
	return f(ctx, m)
}

func blockOption(t *testing.T, number OptionNumber, block Block) Option {
	t.Helper()
	option, err := block.Option(number)
	if err != nil {
		t.Fatal(err)
	}
	return option
}

func TestRequestBodyValidation(t *testing.T) {
	request := &Message{Code: PUT}
	dummy := requesterFunc(func(context.Context, *Message) (*Message, error) { return &Message{Code: Created}, nil })
	for name, tc := range map[string]struct {
		requester Requester
		message   *Message
		config    TransferConfig
	}{
		"nil requester": {nil, request, TransferConfig{}},
		"nil request":   {dummy, nil, TransferConfig{}},
		"mode":          {dummy, request, TransferConfig{Mode: 3}},
		"threshold":     {dummy, request, TransferConfig{SingleMessageThreshold: -1}},
		"block size":    {dummy, request, TransferConfig{BlockSize: 15}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := RequestBody(context.Background(), tc.requester, tc.message, tc.config); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
	want := errors.New("request failed")
	_, err := RequestBody(context.Background(), requesterFunc(func(context.Context, *Message) (*Message, error) {
		return nil, want
	}), request, TransferConfig{})
	if !errors.Is(err, want) {
		t.Fatalf("error = %v", err)
	}
}

func TestSendBlock1Validation(t *testing.T) {
	request := &Message{Code: PUT, Payload: make([]byte, 32)}
	cases := []struct {
		name     string
		response *Message
	}{
		{"rejected incomplete", &Message{Code: RequestEntityIncomplete}},
		{"rejected too large", &Message{Code: RequestEntityTooLarge}},
		{"not continue", &Message{Code: Created}},
		{"missing acknowledgement", &Message{Code: Continue}},
		{"malformed acknowledgement", &Message{Code: Continue, Options: []Option{NewOption(OptionBlock1, []byte{7})}}},
		{"increased size", &Message{Code: Continue, Options: []Option{blockOption(t, OptionBlock1, Block{Size: 32})}}},
		{"wrong number", &Message{Code: Continue, Options: []Option{blockOption(t, OptionBlock1, Block{Number: 1, Size: 16})}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sendBlock1(context.Background(), requesterFunc(func(context.Context, *Message) (*Message, error) {
				return tc.response.Clone(), nil
			}), request, 16)
			if err == nil {
				t.Fatal("expected an error")
			}
		})
	}
	want := errors.New("send failed")
	if _, err := sendBlock1(context.Background(), requesterFunc(func(context.Context, *Message) (*Message, error) {
		return nil, want
	}), request, 16); !errors.Is(err, want) {
		t.Fatalf("error = %v", err)
	}
}

func TestReceiveBlock2Validation(t *testing.T) {
	original := &Message{Code: GET}
	badFirst := []*Message{
		{Code: Content, Options: []Option{NewOption(OptionBlock2, []byte{7})}},
		{Code: Content, Options: []Option{blockOption(t, OptionBlock2, Block{Number: 1, Size: 16})}},
		{Code: Content, Options: []Option{blockOption(t, OptionBlock2, Block{Size: 16}), NewOption(OptionSize2, make([]byte, 5))}},
	}
	for _, first := range badFirst {
		if _, err := receiveBlock2(context.Background(), nil, original, first); err == nil {
			t.Fatalf("accepted first response %#v", first)
		}
	}
	first := &Message{Code: Content, Payload: make([]byte, 16), Options: []Option{
		blockOption(t, OptionBlock2, Block{More: true, Size: 16}), UintOption(OptionSize2, 20),
	}}
	cases := []struct {
		name     string
		response *Message
		err      error
	}{
		{"request error", nil, errors.New("next failed")},
		{"missing block", &Message{Code: Content}, nil},
		{"malformed block", &Message{Code: Content, Options: []Option{NewOption(OptionBlock2, []byte{7})}}, nil},
		{"bad size", &Message{Code: Content, Options: []Option{blockOption(t, OptionBlock2, Block{Number: 1, Size: 16}), NewOption(OptionSize2, make([]byte, 5))}}, nil},
		{"changed size", &Message{Code: Content, Payload: make([]byte, 4), Options: []Option{blockOption(t, OptionBlock2, Block{Number: 1, Size: 16}), UintOption(OptionSize2, 21)}}, nil},
		{"wrong block", &Message{Code: Content, Payload: make([]byte, 4), Options: []Option{blockOption(t, OptionBlock2, Block{Number: 2, Size: 16}), UintOption(OptionSize2, 20)}}, nil},
		{"wrong final size", &Message{Code: Content, Payload: make([]byte, 3), Options: []Option{blockOption(t, OptionBlock2, Block{Number: 1, Size: 16}), UintOption(OptionSize2, 20)}}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := receiveBlock2(context.Background(), requesterFunc(func(context.Context, *Message) (*Message, error) {
				return tc.response, tc.err
			}), original, first)
			if err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestBlockWiseHandlerValidation(t *testing.T) {
	dummy := HandlerFunc(func(context.Context, *Message) (*Message, error) { return &Message{Code: Content}, nil })
	for name, tc := range map[string]struct {
		next   Handler
		config BlockWiseConfig
	}{
		"nil":            {nil, BlockWiseConfig{}},
		"block size":     {dummy, BlockWiseConfig{BlockSize: 15}},
		"small maximum":  {dummy, BlockWiseConfig{BlockSize: 32, MaxBodySize: 16}},
		"large maximum":  {dummy, BlockWiseConfig{MaxBodySize: int(uint64(1) << 32)}},
		"negative state": {dummy, BlockWiseConfig{StateLifetime: -1}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewBlockWiseHandler(tc.next, tc.config); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestBlockWiseHandlerProtocolErrors(t *testing.T) {
	called := 0
	h, err := NewBlockWiseHandler(HandlerFunc(func(context.Context, *Message) (*Message, error) {
		called++
		return &Message{Code: Content, Payload: make([]byte, 17)}, nil
	}), BlockWiseConfig{BlockSize: 16, MaxBodySize: 32})
	if err != nil {
		t.Fatal(err)
	}
	request := func(options ...Option) *Message {
		return &Message{Code: PUT, Token: []byte{1}, LocalAddr: testAddr("local"), RemoteAddr: testAddr("remote"), Options: options}
	}
	checks := []struct {
		name string
		msg  *Message
		code Code
	}{
		{"bad block1", request(NewOption(OptionBlock1, []byte{7})), BadOption},
		{"bad size1", request(blockOption(t, OptionBlock1, Block{Size: 16}), NewOption(OptionSize1, make([]byte, 5))), BadOption},
		{"large size1", request(blockOption(t, OptionBlock1, Block{Size: 16}), UintOption(OptionSize1, 33)), RequestEntityTooLarge},
		{"missing state", request(blockOption(t, OptionBlock1, Block{Number: 1, Size: 16})), RequestEntityIncomplete},
		{"bad block2", request(NewOption(OptionBlock2, []byte{7})), BadOption},
		{"missing cached response", request(blockOption(t, OptionBlock2, Block{Number: 1, Size: 16})), RequestEntityIncomplete},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			response, serveErr := h.ServeCOAP(context.Background(), check.msg)
			if serveErr != nil || response.Code != check.code {
				t.Fatalf("response = %#v, err = %v", response, serveErr)
			}
		})
	}

	oversized, err := NewBlockWiseHandler(HandlerFunc(func(context.Context, *Message) (*Message, error) {
		return &Message{Code: Content, Payload: make([]byte, 17)}, nil
	}), BlockWiseConfig{BlockSize: 16, MaxBodySize: 16})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oversized.ServeCOAP(context.Background(), request()); err == nil {
		t.Fatal("accepted an oversized response body")
	}

	h.inbound["old"] = &inboundTransfer{expires: time.Now().Add(-time.Second)}
	h.outbound["old"] = &outboundTransfer{expires: time.Now().Add(-time.Second)}
	h.expire()
	if len(h.inbound) != 0 || len(h.outbound) != 0 {
		t.Fatal("expired transfer state remains")
	}
	response, err := h.responseBlock(&Message{Code: Content, Payload: []byte("x")}, Block{Number: 1, Size: 16})
	if err != nil || response.Code != BadOption {
		t.Fatalf("out-of-range block = %#v, %v", response, err)
	}
}

func TestBlockWiseHandlerStateAndResponseBranches(t *testing.T) {
	newHandler := func(next Handler, maximum int) *BlockWiseHandler {
		h, err := NewBlockWiseHandler(next, BlockWiseConfig{BlockSize: 16, MaxBodySize: maximum})
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	request := func(number uint32, more bool, payloadSize int, size1 uint32) *Message {
		options := []Option{blockOption(t, OptionBlock1, Block{Number: number, More: more, Size: 16})}
		if size1 != 0 {
			options = append(options, UintOption(OptionSize1, size1))
		}
		return &Message{Code: PUT, Token: []byte{9}, Payload: make([]byte, payloadSize), Options: options}
	}
	dummy := HandlerFunc(func(context.Context, *Message) (*Message, error) { return &Message{Code: Content}, nil })

	h := newHandler(dummy, 64)
	if response, _ := h.ServeCOAP(context.Background(), request(0, true, 16, 32)); response.Code != Continue {
		t.Fatalf("first response = %v", response.Code)
	}
	if response, _ := h.ServeCOAP(context.Background(), request(1, false, 16, 31)); response.Code != RequestEntityIncomplete {
		t.Fatalf("changed Size1 response = %v", response.Code)
	}

	h = newHandler(dummy, 64)
	_, _ = h.ServeCOAP(context.Background(), request(0, true, 16, 0))
	if response, _ := h.ServeCOAP(context.Background(), request(2, false, 1, 0)); response.Code != RequestEntityIncomplete {
		t.Fatalf("nonsequential response = %v", response.Code)
	}

	h = newHandler(dummy, 16)
	_, _ = h.ServeCOAP(context.Background(), request(0, true, 16, 0))
	if response, _ := h.ServeCOAP(context.Background(), request(1, false, 16, 0)); response.Code != RequestEntityTooLarge {
		t.Fatalf("oversized reassembly response = %v", response.Code)
	}

	want := errors.New("handler failure")
	h = newHandler(HandlerFunc(func(context.Context, *Message) (*Message, error) { return nil, want }), 64)
	if response, err := h.ServeCOAP(context.Background(), &Message{Code: GET}); response != nil || !errors.Is(err, want) {
		t.Fatalf("handler error = %#v, %v", response, err)
	}
	h = newHandler(HandlerFunc(func(context.Context, *Message) (*Message, error) { return nil, nil }), 64)
	if response, err := h.ServeCOAP(context.Background(), &Message{Code: GET}); response != nil || err != nil {
		t.Fatalf("nil handler response = %#v, %v", response, err)
	}
	h = newHandler(dummy, 64)
	if response, err := h.ServeCOAP(context.Background(), &Message{Code: GET}); err != nil || response.Code != Content {
		t.Fatalf("small response = %#v, %v", response, err)
	}

	large := newHandler(HandlerFunc(func(context.Context, *Message) (*Message, error) {
		return &Message{Code: Content, Payload: make([]byte, 40)}, nil
	}), 64)
	first, err := large.ServeCOAP(context.Background(), &Message{Code: GET, Token: []byte{3}, Options: []Option{
		blockOption(t, OptionBlock2, Block{More: true, Size: 16}),
	}})
	if err != nil || len(first.Payload) != 16 {
		t.Fatalf("first block = %#v, %v", first, err)
	}
	second, err := large.ServeCOAP(context.Background(), &Message{Code: GET, Token: []byte{3}, Options: []Option{
		blockOption(t, OptionBlock2, Block{Number: 1, Size: 16}),
	}})
	if err != nil || len(second.Payload) != 16 {
		t.Fatalf("cached block = %#v, %v", second, err)
	}
	clamped, err := large.responseBlock(&Message{Code: Content, Payload: make([]byte, 40)}, Block{Size: 32})
	if err != nil || len(clamped.Payload) != 16 {
		t.Fatalf("clamped block = %#v, %v", clamped, err)
	}
}
