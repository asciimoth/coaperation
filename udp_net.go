package coap

import "net"

// udpSocket adds destination metadata to the minimal packet interface. The
// endpoint itself does not depend on this concrete socket type.
type udpSocket struct {
	*net.UDPConn
	multicast bool
	oob       [ipv6PacketInfoBufferSize]byte
}

func (c *udpSocket) ReadPacket(payload []byte) (int, net.Addr, net.Addr, bool, error) {
	n, oobn, _, remote, err := c.ReadMsgUDP(payload, c.oob[:])
	local := c.LocalAddr()
	if err != nil {
		return n, local, remote, false, err
	}
	port := 0
	if address, ok := local.(*net.UDPAddr); ok {
		port = address.Port
	}
	if destination, ok := parseIPv6PacketInfo(c.oob[:oobn], port); ok {
		return n, destination, remote, destination.IP.IsMulticast(), nil
	}
	return n, local, remote, c.multicast, nil
}
