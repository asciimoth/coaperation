// Package coap implements the Constrained Application Protocol features from
// RFC 7252, RFC 7959, and RFC 8323.
package coap

import "fmt"

// Code is an 8-bit CoAP request, response, or signaling code.
type Code uint8

const (
	CodeEmpty Code = 0

	GET Code = 1
	PUT Code = 3

	Created                 Code = 65  // 2.01
	Content                 Code = 69  // 2.05
	Continue                Code = 95  // 2.31
	BadRequest              Code = 128 // 4.00
	Unauthorized            Code = 129 // 4.01
	BadOption               Code = 130 // 4.02
	NotFound                Code = 132 // 4.04
	MethodNotAllowed        Code = 133 // 4.05
	RequestEntityIncomplete Code = 136 // 4.08
	RequestEntityTooLarge   Code = 141 // 4.13
	InternalServerError     Code = 160 // 5.00
	NotImplemented          Code = 161 // 5.01
	ProxyingNotSupported    Code = 165 // 5.05
	HopLimitReached         Code = 168 // 5.08

	CSM     Code = 225 // 7.01
	Ping    Code = 226 // 7.02
	Pong    Code = 227 // 7.03
	Release Code = 228 // 7.04
	Abort   Code = 229 // 7.05
)

func (c Code) Class() uint8      { return uint8(c) >> 5 }
func (c Code) Detail() uint8     { return uint8(c) & 0x1f }
func (c Code) IsRequest() bool   { return c.Class() == 0 && c != CodeEmpty }
func (c Code) IsResponse() bool  { return c.Class() >= 2 && c.Class() <= 5 }
func (c Code) IsSignaling() bool { return c.Class() == 7 }

func (c Code) String() string {
	return fmt.Sprintf("%d.%02d", c.Class(), c.Detail())
}
