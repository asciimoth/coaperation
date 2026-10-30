package coap

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// OptionNumber identifies an option in the CoAP option number space.
type OptionNumber uint32

const (
	OptionURIHost       OptionNumber = 3
	OptionETag          OptionNumber = 4
	OptionURIPort       OptionNumber = 7
	OptionURIPath       OptionNumber = 11
	OptionContentFormat OptionNumber = 12
	OptionMaxAge        OptionNumber = 14
	OptionURIQuery      OptionNumber = 15
	OptionHopLimit      OptionNumber = 16
	OptionBlock2        OptionNumber = 23
	OptionBlock1        OptionNumber = 27
	OptionSize2         OptionNumber = 28
	OptionProxyURI      OptionNumber = 35
	OptionSize1         OptionNumber = 60
)

// TCP signaling option numbers are scoped to each signaling code.
const (
	OptionMaxMessageSize     OptionNumber = 2
	OptionBlockWiseTransfer  OptionNumber = 4
	OptionCustody            OptionNumber = 2
	OptionAlternativeAddress OptionNumber = 2
	OptionHoldOff            OptionNumber = 4
	OptionBadCSMOption       OptionNumber = 2
)

// ContentFormatLinkFormat identifies application/link-format.
const ContentFormatLinkFormat uint32 = 40

// Option contains one unmodified option value.
type Option struct {
	Number OptionNumber
	Value  []byte
}

// NewOption copies value into a new option.
func NewOption(number OptionNumber, value []byte) Option {
	return Option{Number: number, Value: append([]byte(nil), value...)}
}

// StringOption creates an option from a string.
func StringOption(number OptionNumber, value string) Option {
	return NewOption(number, []byte(value))
}

// UintOption creates a minimally encoded unsigned integer option.
func UintOption(number OptionNumber, value uint32) Option {
	if value == 0 {
		return Option{Number: number}
	}
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], value)
	i := 0
	for b[i] == 0 {
		i++
	}
	return NewOption(number, b[i:])
}

// Uint32 decodes an unsigned integer option of at most four bytes.
func (o Option) Uint32() (uint32, error) {
	if len(o.Value) > 4 {
		return 0, fmt.Errorf("coap: option %d integer is longer than 4 bytes", o.Number)
	}
	var b [4]byte
	copy(b[4-len(o.Value):], o.Value)
	return binary.BigEndian.Uint32(b[:]), nil
}

var (
	ErrOptionOrder    = errors.New("coap: options are not in nondecreasing number order")
	ErrOptionReserved = errors.New("coap: reserved option field value")
)

func appendOptionField(dst []byte, value uint32) ([]byte, uint8, error) {
	switch {
	case value < 13:
		return dst, uint8(value), nil
	case value < 269:
		return append(dst, byte(value-13)), 13, nil
	case value <= 65804:
		return binary.BigEndian.AppendUint16(dst, uint16(value-269)), 14, nil
	default:
		return nil, 0, fmt.Errorf("coap: option delta or length %d exceeds wire limit", value)
	}
}

func marshalOptions(dst []byte, options []Option) ([]byte, error) {
	previous := OptionNumber(0)
	for _, option := range options {
		if option.Number < previous {
			return nil, ErrOptionOrder
		}
		delta := uint32(option.Number - previous)
		if uint64(len(option.Value)) > math.MaxUint32 {
			return nil, errors.New("coap: option value is too large")
		}
		var deltaExtra, lengthExtra []byte
		var dn, ln uint8
		var err error
		deltaExtra, dn, err = appendOptionField(deltaExtra, delta)
		if err != nil {
			return nil, err
		}
		lengthExtra, ln, err = appendOptionField(lengthExtra, uint32(len(option.Value)))
		if err != nil {
			return nil, err
		}
		dst = append(dst, dn<<4|ln)
		dst = append(dst, deltaExtra...)
		dst = append(dst, lengthExtra...)
		dst = append(dst, option.Value...)
		previous = option.Number
	}
	return dst, nil
}

func readOptionField(nibble byte, src []byte, at *int) (uint32, error) {
	switch nibble {
	case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12:
		return uint32(nibble), nil
	case 13:
		if *at >= len(src) {
			return 0, errors.New("coap: truncated option extension")
		}
		v := uint32(src[*at]) + 13
		(*at)++
		return v, nil
	case 14:
		if len(src)-*at < 2 {
			return 0, errors.New("coap: truncated option extension")
		}
		v := uint32(binary.BigEndian.Uint16(src[*at:])) + 269
		*at += 2
		return v, nil
	default:
		return 0, ErrOptionReserved
	}
}

func unmarshalOptions(src []byte) ([]Option, []byte, error) {
	var options []Option
	var number OptionNumber
	for at := 0; at < len(src); {
		if src[at] == 0xff {
			at++
			if at == len(src) {
				return nil, nil, errors.New("coap: empty payload after payload marker")
			}
			return options, append([]byte(nil), src[at:]...), nil
		}
		header := src[at]
		at++
		delta, err := readOptionField(header>>4, src, &at)
		if err != nil {
			return nil, nil, err
		}
		length, err := readOptionField(header&0xf, src, &at)
		if err != nil {
			return nil, nil, err
		}
		if uint64(length) > uint64(len(src)-at) {
			return nil, nil, errors.New("coap: truncated option value")
		}
		if uint64(number)+uint64(delta) > math.MaxUint32 {
			return nil, nil, errors.New("coap: option number overflow")
		}
		number += OptionNumber(delta)
		value := append([]byte(nil), src[at:at+int(length)]...)
		options = append(options, Option{Number: number, Value: value})
		at += int(length)
	}
	return options, nil, nil
}
