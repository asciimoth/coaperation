package coap

import (
	"errors"
	"net"
)

type MessageType uint8

// UDP message types.
const (
	Confirmable MessageType = iota
	NonConfirmable
	Acknowledgement
	Reset
)

// Message has common CoAP request and response data. MessageType and
// MessageID are used only by UDP.
type Message struct {
	// Type and MessageID apply only to UDP messages.
	Type      MessageType
	Code      Code
	MessageID uint16
	// Token, Options, and Payload are copied by Clone.
	Token   []byte
	Options []Option
	Payload []byte

	LocalAddr  net.Addr
	RemoteAddr net.Addr
	Multicast  bool
	// Separate asks a UDP server to send this handler result as a separate
	// Confirmable response after an empty acknowledgement.
	Separate bool
}

func (m *Message) Clone() *Message {
	if m == nil {
		return nil
	}
	c := *m
	c.Token = append([]byte(nil), m.Token...)
	c.Payload = append([]byte(nil), m.Payload...)
	c.Options = make([]Option, len(m.Options))
	for i, option := range m.Options {
		c.Options[i] = NewOption(option.Number, option.Value)
	}
	return &c
}

// AddOption appends a copied option and enforces wire order.
func (m *Message) AddOption(option Option) error {
	if len(m.Options) > 0 && option.Number < m.Options[len(m.Options)-1].Number {
		return ErrOptionOrder
	}
	m.Options = append(m.Options, NewOption(option.Number, option.Value))
	return nil
}

// Option returns the first option with number.
func (m *Message) Option(number OptionNumber) (Option, bool) {
	for _, option := range m.Options {
		if option.Number == number {
			return option, true
		}
	}
	return Option{}, false
}

// OptionValues returns copies of all values with number.
func (m *Message) OptionValues(number OptionNumber) [][]byte {
	var values [][]byte
	for _, option := range m.Options {
		if option.Number == number {
			values = append(values, append([]byte(nil), option.Value...))
		}
	}
	return values
}

func validateToken(token []byte) error {
	if len(token) > 8 {
		return errors.New("coap: token is longer than 8 bytes")
	}
	return nil
}
