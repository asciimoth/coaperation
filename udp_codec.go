package coap

import (
	"encoding/binary"
	"errors"
)

// MarshalUDP encodes one RFC 7252 datagram.
func MarshalUDP(m *Message) ([]byte, error) {
	if m == nil {
		return nil, errors.New("coap: nil message")
	}
	if err := validateToken(m.Token); err != nil {
		return nil, err
	}
	if m.Type > Reset {
		return nil, errors.New("coap: invalid UDP message type")
	}
	b := make([]byte, 4, 4+len(m.Token)+len(m.Payload)+16)
	b[0] = 1<<6 | byte(m.Type)<<4 | byte(len(m.Token))
	b[1] = byte(m.Code)
	binary.BigEndian.PutUint16(b[2:], m.MessageID)
	b = append(b, m.Token...)
	var err error
	b, err = marshalOptions(b, m.Options)
	if err != nil {
		return nil, err
	}
	if len(m.Payload) > 0 {
		b = append(b, 0xff)
		b = append(b, m.Payload...)
	}
	if err := validateUDPCodeAndType(m); err != nil {
		return nil, err
	}
	return b, nil
}

// UnmarshalUDP decodes one complete RFC 7252 datagram.
func UnmarshalUDP(data []byte) (*Message, error) {
	if len(data) < 4 {
		return nil, errors.New("coap: UDP message is shorter than 4 bytes")
	}
	if data[0]>>6 != 1 {
		return nil, errors.New("coap: unsupported CoAP version")
	}
	tkl := int(data[0] & 0xf)
	if tkl > 8 {
		return nil, errors.New("coap: invalid token length")
	}
	if len(data) < 4+tkl {
		return nil, errors.New("coap: truncated token")
	}
	m := &Message{
		Type: MessageType(data[0] >> 4 & 3), Code: Code(data[1]),
		MessageID: binary.BigEndian.Uint16(data[2:4]),
		Token:     append([]byte(nil), data[4:4+tkl]...),
	}
	var err error
	m.Options, m.Payload, err = unmarshalOptions(data[4+tkl:])
	if err != nil {
		return nil, err
	}
	if err := validateUDPCodeAndType(m); err != nil {
		return nil, err
	}
	return m, nil
}

func validateUDPCodeAndType(m *Message) error {
	if class := m.Code.Class(); class == 1 || class == 6 || class == 7 {
		return errors.New("coap: reserved UDP code class")
	}
	if m.Code == CodeEmpty && (len(m.Token) != 0 || len(m.Options) != 0 || len(m.Payload) != 0) {
		return errors.New("coap: empty message has token, options, or payload")
	}
	if m.Type == Reset && m.Code != CodeEmpty {
		return errors.New("coap: reset message is not empty")
	}
	if m.Type == NonConfirmable && m.Code == CodeEmpty {
		return errors.New("coap: non-confirmable message is empty")
	}
	if m.Type == Acknowledgement && m.Code.IsRequest() {
		return errors.New("coap: acknowledgement message contains a request")
	}
	return nil
}
