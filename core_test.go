package coap

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestCodeHelpers(t *testing.T) {
	if GET.Detail() != 1 || GET.String() != "0.01" || !Content.IsResponse() || CodeEmpty.IsResponse() {
		t.Fatal("unexpected code classification")
	}
}

func TestMessageHelpers(t *testing.T) {
	if (*Message)(nil).Clone() != nil {
		t.Fatal("a nil message clone must be nil")
	}
	original := &Message{Token: []byte{1}, Payload: []byte{2}, Options: []Option{StringOption(OptionURIPath, "x")}}
	clone := original.Clone()
	clone.Token[0], clone.Payload[0], clone.Options[0].Value[0] = 3, 4, 'y'
	if original.Token[0] != 1 || original.Payload[0] != 2 || string(original.Options[0].Value) != "x" {
		t.Fatal("Clone did not make a deep copy")
	}

	m := new(Message)
	value := []byte("host")
	if err := m.AddOption(NewOption(OptionURIHost, value)); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	if string(m.Options[0].Value) != "host" {
		t.Fatal("AddOption did not copy the value")
	}
	if err := m.AddOption(StringOption(1, "out of order")); !errors.Is(err, ErrOptionOrder) {
		t.Fatalf("AddOption error = %v", err)
	}
	if _, ok := m.Option(OptionBlock1); ok {
		t.Fatal("found an absent option")
	}
}

func TestBlockValidationAndReassemblyErrors(t *testing.T) {
	for _, value := range [][]byte{{0, 0, 0, 0}, {7}} {
		if _, err := ParseBlock(value); err == nil {
			t.Fatalf("ParseBlock accepted %x", value)
		}
	}
	b, err := ParseBlock([]byte{0x19})
	if err != nil || b != (Block{Number: 1, More: true, Size: 32}) {
		t.Fatalf("ParseBlock = %#v, %v", b, err)
	}
	for _, b := range []Block{{Number: MaxBlockNumber + 1, Size: 16}, {Size: 15}, {Size: 24}, {Size: 2048}} {
		if _, err := b.Option(OptionBlock1); err == nil {
			t.Fatalf("Option accepted %#v", b)
		}
	}
	if _, err := (Block{Size: 16}).Option(OptionURIPath); err == nil {
		t.Fatal("Option accepted a non-block option number")
	}
	o, err := (Block{Number: 2, More: true, Size: 64}).Option(OptionBlock2)
	if err != nil || !reflect.DeepEqual(o, UintOption(OptionBlock2, 0x2a)) {
		t.Fatalf("Option = %#v, %v", o, err)
	}

	var r BlockReassembler
	if body, ok := r.Body(); ok || body != nil {
		t.Fatal("incomplete body is available")
	}
	if _, err := r.Add(Block{Number: 1, Size: 16}, nil); err == nil {
		t.Fatal("accepted a nonsequential block")
	}
	if _, err := r.Add(Block{More: true, Size: 16}, []byte("short")); err == nil {
		t.Fatal("accepted a short non-final block")
	}
	if _, err := r.Add(Block{Size: 16}, bytes.Repeat([]byte{'x'}, 17)); err == nil {
		t.Fatal("accepted an oversized final block")
	}
	if done, err := r.Add(Block{Size: 16}, []byte("done")); err != nil || !done {
		t.Fatalf("final block: done=%v err=%v", done, err)
	}
	if _, err := r.Add(Block{Size: 16}, nil); err == nil {
		t.Fatal("accepted a block after completion")
	}
	r.Reset()
	if _, ok := r.Body(); ok {
		t.Fatal("Reset retained completion state")
	}
}

func TestHopLimit(t *testing.T) {
	bad := []Option{
		StringOption(OptionURIPath, "1"),
		UintOption(OptionHopLimit, 0),
		UintOption(OptionHopLimit, 256),
		NewOption(OptionHopLimit, make([]byte, 5)),
	}
	for _, option := range bad {
		if _, err := ValidateHopLimit(option); err == nil {
			t.Fatalf("ValidateHopLimit accepted %#v", option)
		}
		if _, code, err := DecrementHopLimit(option); code != BadRequest || err == nil {
			t.Fatalf("DecrementHopLimit(%#v) = code %v, err %v", option, code, err)
		}
	}
	if _, code, err := DecrementHopLimit(UintOption(OptionHopLimit, 1)); code != HopLimitReached || err != nil {
		t.Fatalf("limit 1 = code %v, err %v", code, err)
	}
	option, code, err := DecrementHopLimit(UintOption(OptionHopLimit, 2))
	value, valueErr := option.Uint32()
	if code != CodeEmpty || err != nil || valueErr != nil || value != 1 {
		t.Fatalf("limit 2 = %#v, code %v, err %v", option, code, err)
	}
}

func TestOptionEncodingBoundariesAndErrors(t *testing.T) {
	if _, err := NewOption(1, make([]byte, 5)).Uint32(); err == nil {
		t.Fatal("Uint32 accepted five bytes")
	}
	options := []Option{
		NewOption(12, bytes.Repeat([]byte{'a'}, 12)),
		NewOption(13, bytes.Repeat([]byte{'b'}, 13)),
		NewOption(269, bytes.Repeat([]byte{'c'}, 269)),
		NewOption(65804, nil),
	}
	wire, err := marshalOptions(nil, options)
	if err != nil {
		t.Fatal(err)
	}
	got, payload, err := unmarshalOptions(wire)
	if err != nil || payload != nil || !reflect.DeepEqual(got, options) {
		t.Fatalf("option round trip failed: %v", err)
	}
	if _, err := marshalOptions(nil, []Option{NewOption(2, nil), NewOption(1, nil)}); !errors.Is(err, ErrOptionOrder) {
		t.Fatalf("order error = %v", err)
	}
	if _, err := marshalOptions(nil, []Option{NewOption(65805, nil)}); err == nil {
		t.Fatal("accepted an excessive option delta")
	}
	for _, wire := range [][]byte{{0xd0}, {0xe0, 0}, {0x0d}, {0x0e, 0}, {0x0f}, {0x01}, {0xff}} {
		if _, _, err := unmarshalOptions(wire); err == nil {
			t.Fatalf("accepted malformed options %x", wire)
		}
	}
}

func TestCodecValidationErrors(t *testing.T) {
	if _, err := MarshalUDP(nil); err == nil {
		t.Fatal("MarshalUDP accepted nil")
	}
	if _, err := MarshalUDP(&Message{Token: make([]byte, 9)}); err == nil {
		t.Fatal("MarshalUDP accepted a long token")
	}
	if _, err := MarshalUDP(&Message{Type: 4}); err == nil {
		t.Fatal("MarshalUDP accepted an invalid type")
	}
	if _, err := MarshalTCP(nil); err == nil {
		t.Fatal("MarshalTCP accepted nil")
	}
	if _, err := MarshalTCP(&Message{Token: make([]byte, 9)}); err == nil {
		t.Fatal("MarshalTCP accepted a long token")
	}
	if _, err := tcpLengthHeader(1, 9); err == nil {
		t.Fatal("tcpLengthHeader accepted a long token")
	}
	if _, err := tcpLengthHeader(1<<33, 0); err == nil {
		t.Fatal("tcpLengthHeader accepted excessive length")
	}

	for _, wire := range [][]byte{{}, {0x10}, {0x19, 1}, {0xd0}, {0xe0, 0}, {0xf0, 0, 0, 0}} {
		if _, err := ReadTCP(bufio.NewReader(bytes.NewReader(wire)), 0); err == nil {
			t.Fatalf("ReadTCP accepted %x", wire)
		}
	}
	valid, err := MarshalTCP(&Message{Code: GET})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadTCP(bufio.NewReader(bytes.NewReader(valid)), 1); err == nil {
		t.Fatal("ReadTCP ignored the size limit")
	}
	if _, err := UnmarshalTCP(append(valid, 0), 0); err == nil {
		t.Fatal("UnmarshalTCP accepted trailing bytes")
	}
	if message, err := UnmarshalTCP(valid, 0); err != nil || message.Code != GET {
		t.Fatalf("UnmarshalTCP = %#v, %v", message, err)
	}
	reader := bytesReader(nil)
	if n, err := reader.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("empty slice read = %d, %v", n, err)
	}
	for _, message := range []*Message{
		{Options: []Option{NewOption(2, nil), NewOption(1, nil)}},
		{Type: Confirmable, Options: []Option{NewOption(2, nil), NewOption(1, nil)}},
	} {
		var marshalErr error
		if message.Type == Confirmable {
			_, marshalErr = MarshalUDP(message)
		} else {
			_, marshalErr = MarshalTCP(message)
		}
		if !errors.Is(marshalErr, ErrOptionOrder) {
			t.Fatalf("marshal order error = %v", marshalErr)
		}
	}
	if _, err := UnmarshalUDP([]byte{0, 0, 0, 0}); err == nil {
		t.Fatal("UnmarshalUDP accepted an old CoAP version")
	}
	if _, err := UnmarshalUDP([]byte{0x41, 1, 0, 0}); err == nil {
		t.Fatal("UnmarshalUDP accepted a truncated token")
	}
	if _, err := MarshalTCP(&Message{Options: []Option{NewOption(2, nil), NewOption(1, nil)}}); !errors.Is(err, ErrOptionOrder) {
		t.Fatalf("MarshalTCP order error = %v", err)
	}
	for _, wire := range [][]byte{{0x00}, {0x10}, {0x20, 1, 0xf0}} {
		if _, err := ReadTCP(bufio.NewReader(bytes.NewReader(wire)), 0); err == nil {
			t.Fatalf("ReadTCP accepted %x", wire)
		}
	}
	if _, err := UnmarshalTCP(nil, 0); err == nil {
		t.Fatal("UnmarshalTCP accepted empty input")
	}
}
