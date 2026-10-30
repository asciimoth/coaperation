package coap

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// DefaultPort is the registered port for coap and coap+tcp.
const DefaultPort = 5683

// Target contains the transport destination from a CoAP URI.
type Target struct {
	Scheme string
	Host   string
	Port   uint16
}

// Address returns a host:port address for net.Dial or net.ResolveUDPAddr.
func (t Target) Address() string { return net.JoinHostPort(t.Host, strconv.Itoa(int(t.Port))) }

// NewRequest creates URI options without combining repeated path or query
// components. It supports coap and coap+tcp URIs.
func NewRequest(code Code, rawURI string, payload []byte) (*Message, Target, error) {
	if !code.IsRequest() {
		return nil, Target{}, errors.New("coap: code is not a request method")
	}
	u, err := url.Parse(rawURI)
	if err != nil {
		return nil, Target{}, err
	}
	if u.Scheme != "coap" && u.Scheme != "coap+tcp" {
		return nil, Target{}, fmt.Errorf("coap: unsupported URI scheme %q", u.Scheme)
	}
	if u.User != nil || u.Fragment != "" {
		return nil, Target{}, errors.New("coap: URI user information and fragments are not supported")
	}
	if u.Hostname() == "" {
		return nil, Target{}, errors.New("coap: URI has no host")
	}
	port := uint64(DefaultPort)
	if u.Port() != "" {
		port, err = strconv.ParseUint(u.Port(), 10, 16)
		if err != nil || port == 0 {
			return nil, Target{}, errors.New("coap: invalid URI port")
		}
	}
	target := Target{Scheme: u.Scheme, Host: u.Hostname(), Port: uint16(port)}
	m := &Message{Code: code, Payload: append([]byte(nil), payload...)}
	m.Options = append(m.Options, StringOption(OptionURIHost, target.Host))
	if target.Port != DefaultPort {
		m.Options = append(m.Options, UintOption(OptionURIPort, uint32(target.Port)))
	}
	escapedPath := u.EscapedPath()
	if escapedPath != "" && escapedPath != "/" {
		for _, part := range strings.Split(strings.TrimPrefix(escapedPath, "/"), "/") {
			decoded, decodeErr := url.PathUnescape(part)
			if decodeErr != nil {
				return nil, Target{}, decodeErr
			}
			m.Options = append(m.Options, StringOption(OptionURIPath, decoded))
		}
	}
	if u.RawQuery != "" {
		for _, part := range strings.Split(u.RawQuery, "&") {
			decoded, decodeErr := url.PathUnescape(part)
			if decodeErr != nil {
				return nil, Target{}, decodeErr
			}
			m.Options = append(m.Options, NewOption(OptionURIQuery, []byte(decoded)))
		}
	}
	return m, target, nil
}

// URIPath returns each Uri-Path option as a separate string.
func (m *Message) URIPath() []string {
	values := m.OptionValues(OptionURIPath)
	result := make([]string, len(values))
	for i := range values {
		result[i] = string(values[i])
	}
	return result
}

// URIQueries returns copies of all binary-safe Uri-Query values.
func (m *Message) URIQueries() [][]byte { return m.OptionValues(OptionURIQuery) }
