package coap

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestUDPCodecPreservesOptionsAndPayload(t *testing.T) {
	want := &Message{
		Type: Confirmable, Code: PUT, MessageID: 0x1234, Token: []byte{0, 1, 2, 3},
		Options: []Option{
			StringOption(OptionURIPath, "a"), StringOption(OptionURIPath, "b"),
			NewOption(OptionURIQuery, []byte{0xff, 0, '='}), UintOption(OptionHopLimit, 255),
			StringOption(OptionProxyURI, "coap://host/x"),
		},
		Payload: bytes.Repeat([]byte{0xa5}, 32*1024),
	}
	wire, err := MarshalUDP(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalUDP(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip differs")
	}
}

func TestTCPCodecAllLengthForms(t *testing.T) {
	for _, size := range []int{0, 11, 12, 13, 267, 268, 269, 65802, 65803, 70000} {
		t.Run(string(rune(size)), func(t *testing.T) {
			want := &Message{Code: Content, Token: []byte{1, 2}, Payload: bytes.Repeat([]byte{3}, size)}
			wire, err := MarshalTCP(want)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ReadTCP(bufio.NewReader(bytes.NewReader(wire)), uint64(len(wire)+8))
			if err != nil {
				t.Fatal(err)
			}
			if got.Code != want.Code || !bytes.Equal(got.Token, want.Token) || !bytes.Equal(got.Payload, want.Payload) || !reflect.DeepEqual(got.Options, want.Options) {
				t.Fatalf("round trip differs for payload size %d", size)
			}
		})
	}
}

func TestTCPRFC8323WireVectors(t *testing.T) {
	for _, tc := range []struct {
		message *Message
		wire    []byte
	}{
		{&Message{Code: GET}, []byte{0x00, byte(GET)}},
		{&Message{Code: 0x43, Token: []byte{0x7f}}, []byte{0x01, 0x43, 0x7f}},
		{&Message{Code: Ping, Token: []byte{0x42}}, []byte{0x01, byte(Ping), 0x42}},
		{&Message{Code: GET, Payload: []byte{'x'}}, []byte{0x20, byte(GET), 0xff, 'x'}},
	} {
		wire, err := MarshalTCP(tc.message)
		if err != nil || !bytes.Equal(wire, tc.wire) {
			t.Fatalf("MarshalTCP(%#v) = %x, %v; want %x", tc.message, wire, err, tc.wire)
		}
		got, err := UnmarshalTCP(tc.wire, uint64(len(tc.wire)))
		if err != nil || got.Code != tc.message.Code || !bytes.Equal(got.Token, tc.message.Token) || !bytes.Equal(got.Payload, tc.message.Payload) {
			t.Fatalf("UnmarshalTCP(%x) = %#v, %v", tc.wire, got, err)
		}
	}
}

func TestReadTCPConsecutiveFrames(t *testing.T) {
	reader := bufio.NewReader(bytes.NewReader([]byte{0x00, byte(GET), 0x01, byte(Ping), 0x42}))
	first, err := ReadTCP(reader, 5)
	if err != nil || first.Code != GET {
		t.Fatalf("first frame = %#v, %v", first, err)
	}
	second, err := ReadTCP(reader, 5)
	if err != nil || second.Code != Ping || !bytes.Equal(second.Token, []byte{0x42}) {
		t.Fatalf("second frame = %#v, %v", second, err)
	}
}

func TestTCPShortInputWithLargeDeclaredLength(t *testing.T) {
	if _, err := UnmarshalTCP([]byte{0xf0, 0xff, 0xff, 0xff, 0xff}, 0); err == nil {
		t.Fatal("accepted a truncated frame with a large declared length")
	}
}

func TestTCPEmptyMessageShape(t *testing.T) {
	if message, err := UnmarshalTCP([]byte{0, 0}, 0); err != nil || message.Code != CodeEmpty {
		t.Fatalf("valid Empty message = %#v, %v", message, err)
	}
	for _, message := range []*Message{
		{Code: CodeEmpty, Token: []byte{1}},
		{Code: CodeEmpty, Options: []Option{NewOption(1, nil)}},
		{Code: CodeEmpty, Payload: []byte{1}},
	} {
		if _, err := MarshalTCP(message); err == nil {
			t.Fatalf("encoded malformed Empty message %#v", message)
		}
	}
	for _, wire := range [][]byte{
		{0x01, 0, 1},
		{0x10, 0, 0x10},
		{0x20, 0, 0xff, 1},
	} {
		if _, err := UnmarshalTCP(wire, 0); err == nil {
			t.Fatalf("accepted malformed Empty frame %x", wire)
		}
	}
}

func TestTCPReservedCodeClasses(t *testing.T) {
	for _, code := range []Code{0x20, 0x3f, 0xc0, 0xdf} {
		if _, err := MarshalTCP(&Message{Code: code}); err == nil {
			t.Fatalf("encoded reserved code %s", code)
		}
		if _, err := UnmarshalTCP([]byte{0, byte(code)}, 0); err == nil {
			t.Fatalf("accepted reserved code %s", code)
		}
	}
}

func TestBlockReassemblyWithReducedSize(t *testing.T) {
	var r BlockReassembler
	done, err := r.Add(Block{Number: 0, More: true, Size: 32}, bytes.Repeat([]byte{'a'}, 32))
	if err != nil || done {
		t.Fatalf("first block: done=%v err=%v", done, err)
	}
	done, err = r.Add(Block{Number: 2, More: false, Size: 16}, []byte("tail"))
	if err != nil || !done {
		t.Fatalf("last block: done=%v err=%v", done, err)
	}
	body, ok := r.Body()
	if !ok || len(body) != 36 {
		t.Fatalf("body len=%d complete=%v", len(body), ok)
	}
}

func TestMalformedMessages(t *testing.T) {
	for _, wire := range [][]byte{
		{0x40, 1, 0},          // short UDP header
		{0x49, 1, 0, 1},       // invalid token length
		{0x40, 1, 0, 1, 0xff}, // empty payload
		{0x40, 1, 0, 1, 0xf0}, // reserved delta
		{0x40, 0, 0, 1, 0x10}, // non-empty Empty message
	} {
		if _, err := UnmarshalUDP(wire); err == nil {
			t.Fatalf("accepted malformed wire %x", wire)
		}
	}
}

func TestUDPCodeAndTypeRules(t *testing.T) {
	invalid := []*Message{
		{Code: CodeEmpty, Token: []byte{1}},
		{Code: CodeEmpty, Options: []Option{StringOption(OptionURIPath, "x")}},
		{Code: CodeEmpty, Payload: []byte{1}},
		{Type: Reset, Code: Content},
		{Type: NonConfirmable, Code: CodeEmpty},
		{Type: Acknowledgement, Code: GET},
		{Type: Confirmable, Code: 0x20},
		{Type: Confirmable, Code: 0xc0},
		{Type: Confirmable, Code: CSM},
	}
	for _, message := range invalid {
		if _, err := MarshalUDP(message); err == nil {
			t.Fatalf("encoded invalid message: %#v", message)
		}
	}
	for _, wire := range [][]byte{
		{0x70, byte(Content), 0, 1},
		{0x50, 0, 0, 1},
		{0x60, byte(GET), 0, 1},
		{0x40, byte(CSM), 0, 1},
	} {
		if _, err := UnmarshalUDP(wire); err == nil {
			t.Fatalf("decoded invalid message: %x", wire)
		}
	}
	for _, kind := range []MessageType{Confirmable, Acknowledgement, Reset} {
		wire, err := MarshalUDP(&Message{Type: kind, Code: CodeEmpty})
		if err != nil {
			t.Fatalf("type %d: %v", kind, err)
		}
		if _, err := UnmarshalUDP(wire); err != nil {
			t.Fatalf("type %d round trip: %v", kind, err)
		}
	}
}

func TestMaximumUintOption(t *testing.T) {
	option := UintOption(OptionMaxAge, 0xffffffff)
	if len(option.Value) != 4 {
		t.Fatalf("encoded length %d", len(option.Value))
	}
	got, err := option.Uint32()
	if err != nil || got != 0xffffffff {
		t.Fatalf("value %x, error %v", got, err)
	}
}

func FuzzUnmarshalUDP(f *testing.F) {
	f.Add([]byte{0x40, 0x01, 0x12, 0x34})
	f.Add([]byte{0x44, 0x45, 0, 1, 1, 2, 3, 4, 0xff, 5})
	f.Fuzz(func(t *testing.T, data []byte) {
		message, err := UnmarshalUDP(data)
		if err != nil {
			return
		}
		wire, err := MarshalUDP(message)
		if err != nil {
			t.Fatalf("cannot marshal accepted message: %v", err)
		}
		if _, err := UnmarshalUDP(wire); err != nil {
			t.Fatalf("cannot read round trip: %v", err)
		}
	})
}

func FuzzUnmarshalTCP(f *testing.F) {
	f.Add([]byte{0x00, 0x01})
	f.Add([]byte{0x24, 0x45, 1, 2, 3, 4, 0xff, 5})
	f.Fuzz(func(t *testing.T, data []byte) {
		message, err := UnmarshalTCP(data, 1<<20)
		if err != nil {
			return
		}
		wire, err := MarshalTCP(message)
		if err != nil {
			t.Fatalf("cannot marshal accepted message: %v", err)
		}
		if _, err := UnmarshalTCP(wire, 1<<20); err != nil {
			t.Fatalf("cannot read round trip: %v", err)
		}
	})
}

func FuzzTCPEmptyMessageShape(f *testing.F) {
	f.Add([]byte{}, []byte{}, []byte{})
	f.Add([]byte{1}, []byte{}, []byte{})
	f.Add([]byte{}, []byte{0x10}, []byte{})
	f.Add([]byte{}, []byte{}, []byte{1})
	f.Fuzz(func(t *testing.T, token, options, payload []byte) {
		if len(token) > 8 || len(options) > 32 || len(payload) > 32 {
			return
		}
		body := append([]byte(nil), options...)
		if len(payload) != 0 {
			body = append(body, 0xff)
			body = append(body, payload...)
		}
		header, err := tcpLengthHeader(uint64(len(body)), len(token))
		if err != nil {
			t.Fatal(err)
		}
		wire := append(header, byte(CodeEmpty))
		wire = append(wire, token...)
		wire = append(wire, body...)
		message, err := UnmarshalTCP(wire, 0)
		valid := len(token) == 0 && len(options) == 0 && len(payload) == 0
		if valid && (err != nil || message.Code != CodeEmpty) || !valid && err == nil {
			t.Fatalf("Empty frame %x: message = %#v, error = %v", wire, message, err)
		}
	})
}

func FuzzTCPCodeClass(f *testing.F) {
	for _, code := range []byte{0, 1, 31, 32, 63, 64, 95, 96, 127, 128, 159, 160, 191, 192, 223, 224, 255} {
		f.Add(code)
	}
	f.Fuzz(func(t *testing.T, code byte) {
		reserved := code>>5 == 1 || code>>5 == 6
		wire, marshalErr := MarshalTCP(&Message{Code: Code(code)})
		message, decodeErr := UnmarshalTCP([]byte{0, code}, 0)
		if reserved {
			if marshalErr == nil || decodeErr == nil {
				t.Fatalf("reserved code %x: marshal = %v, decode = %v", code, marshalErr, decodeErr)
			}
			return
		}
		if marshalErr != nil || decodeErr != nil || message.Code != Code(code) || !bytes.Equal(wire, []byte{0, code}) {
			t.Fatalf("code %x: wire = %x, message = %#v, marshal = %v, decode = %v", code, wire, message, marshalErr, decodeErr)
		}
	})
}

func FuzzUnmarshalTCPWithoutSizeLimit(f *testing.F) {
	for _, data := range [][]byte{
		{0x00, byte(GET)},
		{0xf0, 0xff, 0xff, 0xff, 0xff},
		{0xe0, 0xff, 0xff},
		{0xd0, 0xff, byte(Content)},
	} {
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}
		message, err := UnmarshalTCP(data, 0)
		if err != nil {
			return
		}
		wire, err := MarshalTCP(message)
		if err != nil {
			t.Fatalf("cannot encode accepted TCP frame: %v", err)
		}
		if _, err := UnmarshalTCP(wire, 0); err != nil {
			t.Fatalf("cannot decode TCP round trip: %v", err)
		}
	})
}

func FuzzTCPFramingLength(f *testing.F) {
	for _, payload := range [][]byte{
		{}, bytes.Repeat([]byte{'a'}, 11), bytes.Repeat([]byte{'b'}, 12),
		bytes.Repeat([]byte{'c'}, 267), bytes.Repeat([]byte{'d'}, 268),
	} {
		f.Add([]byte{1, 2}, payload)
	}
	f.Fuzz(func(t *testing.T, token, payload []byte) {
		if len(token) > 8 || len(payload) > 4096 {
			return
		}
		wire, err := MarshalTCP(&Message{Code: Content, Token: token, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		length := uint64(wire[0] >> 4)
		headerSize := 1
		switch length {
		case 13:
			length = uint64(wire[1]) + 13
			headerSize = 2
		case 14:
			length = uint64(wire[1])<<8 | uint64(wire[2])
			length += 269
			headerSize = 3
		case 15:
			length = uint64(wire[1])<<24 | uint64(wire[2])<<16 | uint64(wire[3])<<8 | uint64(wire[4])
			length += 65805
			headerSize = 5
		}
		wantLength := uint64(len(payload))
		if len(payload) != 0 {
			wantLength++ // Payload marker.
		}
		if length != wantLength || int(wire[0]&0xf) != len(token) || len(wire) != headerSize+1+len(token)+int(length) {
			t.Fatalf("TCP header describes length %d and token %d; want %d and %d", length, wire[0]&0xf, wantLength, len(token))
		}
		if _, err := UnmarshalTCP(wire, uint64(len(wire))); err != nil {
			t.Fatalf("decoder rejected encoded frame: %v", err)
		}
	})
}

func FuzzOptionFields(f *testing.F) {
	for _, data := range [][]byte{
		{}, {0xd0, 0}, {0xe0, 0, 0}, {0xff},
		{0xdd, 0xff, 0xff, 0xff, 0xff},
		{0x01, 0xff, 0},
	} {
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		options, payload, err := unmarshalOptions(data)
		if err != nil {
			return
		}
		wire, err := marshalOptions(nil, options)
		if err != nil {
			t.Fatalf("cannot encode accepted options: %v", err)
		}
		if len(payload) > 0 {
			wire = append(wire, 0xff)
			wire = append(wire, payload...)
		}
		again, againPayload, err := unmarshalOptions(wire)
		if err != nil || !reflect.DeepEqual(again, options) || !bytes.Equal(againPayload, payload) {
			t.Fatalf("option round trip differs: %v", err)
		}
	})
}

func FuzzBlockOption(f *testing.F) {
	for _, value := range [][]byte{{}, {0}, {7}, {0x19}, {0xff, 0xff, 0xf6}, {0, 0, 0, 0}} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, value []byte) {
		block, err := ParseBlock(value)
		if err != nil {
			return
		}
		option, err := block.Option(OptionBlock2)
		if err != nil {
			t.Fatalf("cannot encode accepted block: %v", err)
		}
		got, err := ParseBlock(option.Value)
		if err != nil || got != block {
			t.Fatalf("block round trip differs: %#v, %v", got, err)
		}
	})
}

func FuzzBlockReassembler(f *testing.F) {
	for _, seed := range []struct {
		number uint32
		size   int
		more   bool
		body   []byte
	}{
		{0, 0, false, nil},
		{0, 15, false, nil},
		{0, 16, false, []byte("last")},
		{0, 24, false, nil},
		{0, 16, true, bytes.Repeat([]byte{'x'}, 16)},
		{0, 1024, false, nil},
		{0, 2048, false, nil},
		{1, 16, false, nil},
		{MaxBlockNumber + 1, 16, false, nil},
	} {
		f.Add(seed.number, seed.size, seed.more, seed.body)
	}
	f.Fuzz(func(t *testing.T, number uint32, size int, more bool, body []byte) {
		if len(body) > 2048 {
			return
		}
		var r BlockReassembler
		block := Block{Number: number, Size: size, More: more}
		done, err := r.Add(block, body)
		validSize := size >= 16 && size <= 1024 && size&(size-1) == 0
		valid := number == 0 && validSize && len(body) <= size && (!more || len(body) == size)
		if !valid {
			if err == nil || done {
				t.Fatalf("accepted invalid block %#v with %d bytes", block, len(body))
			}
			if got, ok := r.Body(); ok || got != nil {
				t.Fatal("invalid block changed completion state")
			}
			return
		}
		if err != nil || done != !more {
			t.Fatalf("valid block %#v: done=%v err=%v", block, done, err)
		}
		got, ok := r.Body()
		if ok != done || (done && !bytes.Equal(got, body)) {
			t.Fatalf("body differs: available=%v, length=%d", ok, len(got))
		}
	})
}

func FuzzBlock2ResponseOffset(f *testing.F) {
	for _, data := range [][]byte{
		{1, 1, 0, 'a'},
		{1, 1, 0, 'a', 'b', 'c'},
		{0, 0, 6},
		{6, 255, 0, 'x'},
	} {
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 3 || len(data) > 4099 {
			return
		}
		requested := Block{Number: uint32(data[1]), Size: 16 << (data[0] % 7)}
		serverSize := 16 << (data[2] % 7)
		body := data[3:]
		h := &BlockWiseHandler{config: BlockWiseConfig{BlockSize: serverSize}}
		response, err := h.responseBlock(&Message{Code: Content, Payload: body}, requested)
		if err != nil {
			t.Fatal(err)
		}
		start := requested.Offset()
		if start >= uint64(len(body)) && (start != 0 || len(body) != 0) {
			if response.Code != BadOption {
				t.Fatalf("out-of-range offset %d returned %s", start, response.Code)
			}
			return
		}
		option, ok := response.Option(OptionBlock2)
		if !ok {
			t.Fatal("response has no Block2 option")
		}
		block, err := ParseBlock(option.Value)
		if err != nil || block.Offset() != start || block.Size > requested.Size || block.Size > serverSize {
			t.Fatalf("block = %#v for requested offset %d: %v", block, start, err)
		}
		end := start + uint64(block.Size)
		if end > uint64(len(body)) {
			end = uint64(len(body))
		}
		if !bytes.Equal(response.Payload, body[start:end]) || block.More != (end < uint64(len(body))) {
			t.Fatalf("wrong Block2 bytes or More flag at offset %d", start)
		}
	})
}

func FuzzUDPTypeCodeTable(f *testing.F) {
	for _, data := range [][]byte{
		{0, 0}, {1, 0}, {2, 1}, {3, byte(Content)},
		{0, byte(CSM)}, {2, byte(Content), 1}, {0, byte(GET), 1, 2, 3},
	} {
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 || len(data) > 11 {
			return
		}
		kind := MessageType(data[0] % 5)
		code := Code(data[1])
		m := &Message{Type: kind, Code: code, Token: append([]byte(nil), data[2:]...)}
		class := code.Class()
		valid := kind <= Reset && len(m.Token) <= 8 && class != 1 && class != 6 && class != 7 &&
			(code != CodeEmpty || kind != NonConfirmable && len(m.Token) == 0) &&
			(kind != Reset || code == CodeEmpty) &&
			(kind != Acknowledgement || !code.IsRequest())
		wire, err := MarshalUDP(m)
		if !valid {
			if err == nil {
				t.Fatalf("encoded invalid type %d, code %s, token length %d", kind, code, len(m.Token))
			}
			return
		}
		if err != nil {
			t.Fatalf("rejected valid type %d, code %s: %v", kind, code, err)
		}
		got, err := UnmarshalUDP(wire)
		if err != nil || got.Type != kind || got.Code != code || !bytes.Equal(got.Token, m.Token) {
			t.Fatalf("UDP round trip = %#v, %v", got, err)
		}
	})
}
