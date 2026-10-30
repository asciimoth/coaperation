//go:build !linux

package coap

import "net"

const ipv6PacketInfoBufferSize = 1

func enableIPv6PacketInfo(*net.UDPConn) error              { return nil }
func parseIPv6PacketInfo([]byte, int) (*net.UDPAddr, bool) { return nil, false }
