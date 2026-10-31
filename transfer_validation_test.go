package coap

import (
	"bytes"
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
	if _, err := RequestBody(context.Background(), requesterFunc(func(context.Context, *Message) (*Message, error) {
		return nil, nil
	}), request, TransferConfig{}); err == nil {
		t.Fatal("accepted a missing response")
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
		{"increased size", &Message{Code: Continue, Options: []Option{blockOption(t, OptionBlock1, Block{More: true, Size: 32})}}},
		{"wrong number", &Message{Code: Continue, Options: []Option{blockOption(t, OptionBlock1, Block{Number: 1, More: true, Size: 16})}}},
		{"premature Continue", &Message{Code: Continue, Options: []Option{blockOption(t, OptionBlock1, Block{Size: 16})}}},
		{"Continue payload", &Message{Code: Continue, Payload: []byte("unexpected"), Options: []Option{blockOption(t, OptionBlock1, Block{More: true, Size: 16})}}},
		{"no response", nil},
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
	if _, err := sendBlock1(context.Background(), requesterFunc(func(context.Context, *Message) (*Message, error) {
		return &Message{Code: Continue, Options: []Option{blockOption(t, OptionBlock1, Block{Number: 0, Size: 16})}}, nil
	}), &Message{Code: PUT, Payload: make([]byte, 16)}, 16); err == nil {
		t.Fatal("accepted Continue after the final Block1 request")
	}
	if _, err := sendBlock1(context.Background(), requesterFunc(func(context.Context, *Message) (*Message, error) {
		return &Message{Code: Created, Options: []Option{blockOption(t, OptionBlock1, Block{More: true, Size: 16})}}, nil
	}), &Message{Code: PUT, Payload: make([]byte, 16)}, 16); err == nil {
		t.Fatal("accepted a final Block1 acknowledgement that expects more blocks")
	}
	if _, err := sendBlock1(context.Background(), requesterFunc(func(context.Context, *Message) (*Message, error) {
		return &Message{Code: Continue, Options: []Option{
			blockOption(t, OptionBlock1, Block{More: true, Size: 16}),
			blockOption(t, OptionBlock2, Block{Size: 16}),
		}}, nil
	}), request, 16); err == nil {
		t.Fatal("accepted a provisional Block1 response with Block2")
	}
	blocks := 0
	response, err := sendBlock1(context.Background(), requesterFunc(func(_ context.Context, part *Message) (*Message, error) {
		option, ok := part.Option(OptionBlock1)
		if !ok {
			t.Fatal("request has no Block1 option")
		}
		block, err := ParseBlock(option.Value)
		if err != nil || block.Number != uint32(blocks) {
			t.Fatalf("block = %#v, error = %v", block, err)
		}
		blocks++
		return &Message{Code: Created, Options: []Option{blockOption(t, OptionBlock1, Block{Number: block.Number, Size: 16})}}, nil
	}), request, 16)
	if err != nil || response.Code != Created || blocks != 2 {
		t.Fatalf("stateless Block1 transfer: response = %#v, blocks = %d, error = %v", response, blocks, err)
	}
}

func TestBlock1NumberRange(t *testing.T) {
	body := make([]byte, (int(MaxBlockNumber)+1)*16+1)
	calls := 0
	requester := requesterFunc(func(context.Context, *Message) (*Message, error) {
		calls++
		return &Message{Code: Continue, Options: []Option{blockOption(t, OptionBlock1, Block{More: true, Size: 16})}}, nil
	})
	if _, err := sendBlock1(context.Background(), requester, &Message{Code: PUT, Payload: body}, 16); err == nil || calls != 0 {
		t.Fatalf("oversized body: calls = %d, error = %v", calls, err)
	}
	if _, err := sendBlock1(context.Background(), requester, &Message{Code: PUT, Payload: body}, 32); err == nil || calls != 1 {
		t.Fatalf("size reduction: calls = %d, error = %v", calls, err)
	}
}

func TestReceiveBlock2Validation(t *testing.T) {
	original := &Message{Code: GET}
	if _, err := receiveBlock2(context.Background(), nil, original, nil); err == nil {
		t.Fatal("accepted a missing first Block2 response")
	}
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
		{"no response", nil, nil},
		{"changed code", &Message{Code: NotFound, Payload: make([]byte, 4), Options: []Option{blockOption(t, OptionBlock2, Block{Number: 1, Size: 16})}}, nil},
		{"changed content format", &Message{Code: Content, Payload: make([]byte, 4), Options: []Option{UintOption(OptionContentFormat, 50), blockOption(t, OptionBlock2, Block{Number: 1, Size: 16})}}, nil},
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

func TestReceiveBlock2ChecksETag(t *testing.T) {
	first := &Message{Code: Content, Payload: make([]byte, 16), Options: []Option{
		NewOption(OptionETag, []byte("one")),
		blockOption(t, OptionBlock2, Block{More: true, Size: 16}),
		UintOption(OptionSize2, 20),
	}}
	for _, tag := range [][]byte{[]byte("two"), nil} {
		secondOptions := []Option{blockOption(t, OptionBlock2, Block{Number: 1, Size: 16}), UintOption(OptionSize2, 20)}
		if tag != nil {
			secondOptions = append([]Option{NewOption(OptionETag, tag)}, secondOptions...)
		}
		_, err := receiveBlock2(context.Background(), requesterFunc(func(context.Context, *Message) (*Message, error) {
			return &Message{Code: Content, Payload: make([]byte, 4), Options: secondOptions}, nil
		}), &Message{Code: GET}, first)
		if err == nil {
			t.Fatalf("accepted changed ETag %q", tag)
		}
	}
	second := &Message{Code: Content, Payload: make([]byte, 4), Options: []Option{
		NewOption(OptionETag, []byte("one")),
		blockOption(t, OptionBlock2, Block{Number: 1, Size: 16}),
		UintOption(OptionSize2, 20),
	}}
	result, err := receiveBlock2(context.Background(), requesterFunc(func(context.Context, *Message) (*Message, error) {
		return second, nil
	}), &Message{Code: GET}, first)
	if err != nil || len(result.Payload) != 20 {
		t.Fatalf("matching ETag result = %#v, error = %v", result, err)
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

func TestBlock2SizeReductionKeepsRequestedOffset(t *testing.T) {
	full := &Message{Code: Content, Payload: []byte("0123456789abcdefghijklmnopqrstuvw")}
	h, err := NewBlockWiseHandler(HandlerFunc(func(context.Context, *Message) (*Message, error) {
		return full, nil
	}), BlockWiseConfig{BlockSize: 16, MaxBodySize: 64})
	if err != nil {
		t.Fatal(err)
	}
	first := &Message{Code: GET, Token: []byte{1}, Options: []Option{blockOption(t, OptionBlock2, Block{Size: 32})}}
	if _, err := h.ServeCOAP(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	request := &Message{Code: GET, Token: []byte{1}, Options: []Option{blockOption(t, OptionBlock2, Block{Number: 1, Size: 32})}}
	response, err := h.ServeCOAP(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	option, ok := response.Option(OptionBlock2)
	if !ok {
		t.Fatal("response has no Block2 option")
	}
	block, err := ParseBlock(option.Value)
	if err != nil || block.Number != 2 || block.Size != 16 || string(response.Payload) != "w" {
		t.Fatalf("block = %#v, payload = %q, error = %v", block, response.Payload, err)
	}
}

func TestBlock1SizeReductionAcknowledgesSentNumber(t *testing.T) {
	body := bytes.Repeat([]byte("a"), 145)
	h, err := NewBlockWiseHandler(HandlerFunc(func(_ context.Context, request *Message) (*Message, error) {
		if !bytes.Equal(request.Payload, body) {
			t.Fatal("handler received the wrong body")
		}
		return &Message{Code: Created}, nil
	}), BlockWiseConfig{BlockSize: 16, MaxBodySize: 256})
	if err != nil {
		t.Fatal(err)
	}
	var numbers []uint32
	requester := requesterFunc(func(ctx context.Context, request *Message) (*Message, error) {
		option, _ := request.Option(OptionBlock1)
		block, err := ParseBlock(option.Value)
		if err != nil {
			t.Fatal(err)
		}
		numbers = append(numbers, block.Number)
		response, err := h.ServeCOAP(ctx, request)
		if len(numbers) == 1 {
			ackOption, ok := response.Option(OptionBlock1)
			if !ok {
				t.Fatal("server omitted Block1 acknowledgement")
			}
			ack, parseErr := ParseBlock(ackOption.Value)
			if parseErr != nil || ack.Number != 0 || ack.Size != 16 {
				t.Fatalf("first acknowledgement = %#v, %v", ack, parseErr)
			}
		}
		return response, err
	})
	response, err := RequestBody(context.Background(), requester, &Message{Code: PUT, Payload: body}, TransferConfig{Mode: TransferBlockWise, BlockSize: 64})
	if err != nil || response.Code != Created {
		t.Fatalf("response = %#v, error = %v", response, err)
	}
	if len(numbers) != 7 || numbers[0] != 0 || numbers[1] != 4 || numbers[len(numbers)-1] != 9 {
		t.Fatalf("request block numbers = %v", numbers)
	}
}

func TestBlockWiseTransferIdentity(t *testing.T) {
	var body []byte
	h, err := NewBlockWiseHandler(HandlerFunc(func(_ context.Context, request *Message) (*Message, error) {
		body = append([]byte(nil), request.Payload...)
		return &Message{Code: Content}, nil
	}), BlockWiseConfig{BlockSize: 16, MaxBodySize: 64})
	if err != nil {
		t.Fatal(err)
	}
	part := func(path string, token byte, number uint32, more bool, contentFormat uint32, payload []byte) *Message {
		return &Message{Code: PUT, Token: []byte{token}, Payload: payload, Options: []Option{
			StringOption(OptionURIPath, path),
			UintOption(OptionContentFormat, contentFormat),
			blockOption(t, OptionBlock1, Block{Number: number, More: more, Size: 16}),
		}}
	}
	first := part("one", 1, 0, true, 0, bytes.Repeat([]byte{'a'}, 16))
	if response, err := h.ServeCOAP(context.Background(), first); err != nil || response.Code != Continue {
		t.Fatalf("first block = %#v, %v", response, err)
	}
	for _, changed := range []*Message{
		part("two", 1, 1, false, 0, []byte("tail")),
		part("one", 1, 1, false, 1, []byte("tail")),
	} {
		response, err := h.ServeCOAP(context.Background(), changed)
		if err != nil || response.Code != RequestEntityIncomplete || body != nil {
			t.Fatalf("accepted changed resource or Content-Format: %#v, %v", response, err)
		}
	}
	// A new token may still continue a transfer to the same resource.
	response, err := h.ServeCOAP(context.Background(), part("one", 2, 1, false, 0, []byte("tail")))
	if err != nil || response.Code != Content || string(body) != "aaaaaaaaaaaaaaaatail" {
		t.Fatalf("new-token continuation = %#v, body %q, error %v", response, body, err)
	}
}

func FuzzBlockWiseRequestBody(f *testing.F) {
	f.Add(byte(0), []byte("a"))
	f.Add(byte(2), bytes.Repeat([]byte("x"), 300))
	f.Add(byte(4), bytes.Repeat([]byte("z"), 512))
	f.Fuzz(func(t *testing.T, sizeByte byte, body []byte) {
		if len(body) > 512 {
			return
		}
		serverBlockSize := 16 << (sizeByte % 5)
		h, err := NewBlockWiseHandler(HandlerFunc(func(_ context.Context, request *Message) (*Message, error) {
			return &Message{Code: Content, Options: []Option{NewOption(OptionETag, []byte("version")), UintOption(OptionContentFormat, 50)}, Payload: append([]byte(nil), request.Payload...)}, nil
		}), BlockWiseConfig{BlockSize: serverBlockSize, MaxBodySize: 512})
		if err != nil {
			t.Fatal(err)
		}
		response, err := RequestBody(context.Background(), localRequester{handler: h}, &Message{Code: PUT, Payload: body}, TransferConfig{Mode: TransferBlockWise, BlockSize: 256})
		if err != nil || response == nil || response.Code != Content || !bytes.Equal(response.Payload, body) {
			t.Fatalf("block-wise round trip: response = %#v, error = %v", response, err)
		}
	})
}

func FuzzBlock1ResponseControl(f *testing.F) {
	f.Add(byte(0), []byte("short"), byte(0))
	f.Add(byte(1), bytes.Repeat([]byte("x"), 100), byte(1))
	f.Add(byte(2), bytes.Repeat([]byte("y"), 200), byte(2))
	f.Add(byte(3), bytes.Repeat([]byte("z"), 512), byte(3))
	f.Fuzz(func(t *testing.T, sizeByte byte, body []byte, flags byte) {
		if len(body) == 0 || len(body) > 512 {
			return
		}
		size := 16 << (sizeByte % 4)
		calls := 0
		response, err := sendBlock1(context.Background(), requesterFunc(func(_ context.Context, part *Message) (*Message, error) {
			calls++
			option, ok := part.Option(OptionBlock1)
			if !ok {
				t.Fatal("request has no Block1 option")
			}
			block, parseErr := ParseBlock(option.Value)
			if parseErr != nil || block.Offset() != uint64((calls-1)*size) {
				t.Fatalf("wrong block offset: %#v, %v", block, parseErr)
			}
			if block.More != (calls*size < len(body)) {
				t.Fatal("wrong request More flag")
			}
			ack := block
			ack.More = block.More || flags&1 != 0
			code := Created
			if block.More {
				code = Continue
			}
			options := []Option{blockOption(t, OptionBlock1, ack)}
			if block.More && flags&2 != 0 {
				options = append(options, blockOption(t, OptionBlock2, Block{Size: 16}))
			}
			return &Message{Code: code, Options: options}, nil
		}), &Message{Code: PUT, Payload: body}, size)
		invalid := flags&1 != 0 || flags&2 != 0 && len(body) > size
		if invalid && err == nil || !invalid && (err != nil || response == nil || response.Code != Created || calls != (len(body)+size-1)/size) {
			t.Fatalf("calls = %d, response = %#v, error = %v, invalid = %v", calls, response, err, invalid)
		}
	})
}
