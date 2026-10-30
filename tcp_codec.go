package coap

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

func tcpLengthHeader(length uint64, tkl int) ([]byte, error) {
	if tkl < 0 || tkl > 8 {
		return nil, errors.New("coap: invalid token length")
	}
	switch {
	case length <= 12:
		return []byte{byte(length)<<4 | byte(tkl)}, nil
	case length <= 268:
		return []byte{13<<4 | byte(tkl), byte(length - 13)}, nil
	case length <= 65804:
		return []byte{14<<4 | byte(tkl), byte((length - 269) >> 8), byte(length - 269)}, nil
	case length <= math.MaxUint32+65805:
		b := []byte{15<<4 | byte(tkl)}
		return binary.BigEndian.AppendUint32(b, uint32(length-65805)), nil
	default:
		return nil, errors.New("coap: TCP message exceeds RFC 8323 length limit")
	}
}

// MarshalTCP encodes one RFC 8323 framed message.
func MarshalTCP(m *Message) ([]byte, error) {
	if m == nil {
		return nil, errors.New("coap: nil message")
	}
	if err := validateToken(m.Token); err != nil {
		return nil, err
	}
	body := make([]byte, 0, len(m.Payload)+16)
	var err error
	body, err = marshalOptions(body, m.Options)
	if err != nil {
		return nil, err
	}
	if len(m.Payload) > 0 {
		body = append(body, 0xff)
		body = append(body, m.Payload...)
	}
	header, err := tcpLengthHeader(uint64(len(body)), len(m.Token))
	if err != nil {
		return nil, err
	}
	header = append(header, byte(m.Code))
	header = append(header, m.Token...)
	return append(header, body...), nil
}

// ReadTCP reads one RFC 8323 message from a stream. maxSize limits the full
// framed message size. A zero maxSize uses the RFC maximum.
func ReadTCP(r *bufio.Reader, maxSize uint64) (*Message, error) {
	first, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	tkl := int(first & 0xf)
	headerSize := uint64(1)
	if tkl > 8 {
		return nil, errors.New("coap: invalid TCP token length")
	}
	var length uint64
	switch first >> 4 {
	case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12:
		length = uint64(first >> 4)
	case 13:
		v, e := r.ReadByte()
		if e != nil {
			return nil, e
		}
		length = uint64(v) + 13
		headerSize++
	case 14:
		var b [2]byte
		if _, e := io.ReadFull(r, b[:]); e != nil {
			return nil, e
		}
		length = uint64(binary.BigEndian.Uint16(b[:])) + 269
		headerSize += 2
	case 15:
		var b [4]byte
		if _, e := io.ReadFull(r, b[:]); e != nil {
			return nil, e
		}
		length = uint64(binary.BigEndian.Uint32(b[:])) + 65805
		headerSize += 4
	}
	bodyLength := length + uint64(1+tkl)
	if maxSize != 0 && bodyLength+headerSize > maxSize {
		return nil, fmt.Errorf("coap: TCP message size %d exceeds limit %d", bodyLength+headerSize, maxSize)
	}
	if bodyLength > uint64(int(^uint(0)>>1)) {
		return nil, errors.New("coap: TCP message is too large for this platform")
	}
	body := make([]byte, 0, min(int(bodyLength), 4096))
	var chunk [4096]byte
	for uint64(len(body)) < bodyLength {
		n := min(int(bodyLength-uint64(len(body))), len(chunk))
		read, err := io.ReadFull(r, chunk[:n])
		body = append(body, chunk[:read]...)
		if err != nil {
			return nil, err
		}
	}
	m := &Message{Code: Code(body[0]), Token: append([]byte(nil), body[1:1+tkl]...)}
	m.Options, m.Payload, err = unmarshalOptions(body[1+tkl:])
	if err != nil {
		return nil, err
	}
	return m, nil
}

// UnmarshalTCP decodes exactly one RFC 8323 framed message.
func UnmarshalTCP(data []byte, maxSize uint64) (*Message, error) {
	r := bufio.NewReaderSize(bytesReader(data), len(data))
	m, err := ReadTCP(r, maxSize)
	if err != nil {
		return nil, err
	}
	if r.Buffered() != 0 {
		return nil, errors.New("coap: bytes remain after TCP message")
	}
	return m, nil
}

type sliceReader struct{ b []byte }

func bytesReader(b []byte) *sliceReader { return &sliceReader{b: b} }
func (r *sliceReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}
