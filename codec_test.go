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
	f.Add([]byte{0x10, 0x01})
	f.Add([]byte{0x64, 0x45, 1, 2, 3, 4, 0xff})
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
