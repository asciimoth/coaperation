//go:build linux

package coap

import (
	"encoding/binary"
	"net"
	"syscall"
)

const ipv6PacketInfoBufferSize = 128

func enableIPv6PacketInfo(conn *net.UDPConn) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var socketErr error
	if err = raw.Control(func(fd uintptr) {
		socketErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, syscall.IPV6_RECVPKTINFO, 1)
	}); err != nil {
		return err
	}
	return socketErr
}

func parseIPv6PacketInfo(oob []byte, port int) (*net.UDPAddr, bool) {
	messages, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, false
	}
	for _, message := range messages {
		if message.Header.Level != syscall.IPPROTO_IPV6 || message.Header.Type != syscall.IPV6_PKTINFO || len(message.Data) < 20 {
			continue
		}
		ip := append(net.IP(nil), message.Data[:16]...)
		index := int(binary.NativeEndian.Uint32(message.Data[16:20]))
		zone := ""
		if iface, interfaceErr := net.InterfaceByIndex(index); interfaceErr == nil {
			zone = iface.Name
		}
		return &net.UDPAddr{IP: ip, Port: port, Zone: zone}, true
	}
	return nil, false
}
