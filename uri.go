package coap

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"
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
	u, escapedHost, hasEscapedHost, err := parseCoAPURL(rawURI)
	if err != nil {
		return nil, Target{}, err
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "coap" && scheme != "coap+tcp" {
		return nil, Target{}, fmt.Errorf("coap: unsupported URI scheme %q", u.Scheme)
	}
	if u.User != nil || strings.Contains(rawURI, "#") {
		return nil, Target{}, errors.New("coap: URI user information and fragments are not supported")
	}
	host := u.Hostname()
	if hasEscapedHost {
		host = escapedHost
	}
	if host == "" {
		return nil, Target{}, errors.New("coap: URI has no host")
	}
	port := uint64(DefaultPort)
	if u.Port() != "" {
		port, err = strconv.ParseUint(u.Port(), 10, 16)
		if err != nil || port == 0 {
			return nil, Target{}, errors.New("coap: invalid URI port")
		}
	}
	target := Target{Scheme: scheme, Host: host, Port: uint16(port)}
	m := &Message{Code: code, Payload: append([]byte(nil), payload...)}
	ipHost := net.ParseIP(host) != nil
	if address, _, hasZone := strings.Cut(host, "%"); hasZone && net.ParseIP(address) != nil {
		ipHost = true
	}
	if !ipHost {
		target.Host = strings.ToLower(host)
		m.Options = append(m.Options, StringOption(OptionURIHost, target.Host))
	}
	escapedPath := u.EscapedPath()
	if escapedPath != "" && escapedPath != "/" {
		var segments []string
		trailingDot := false
		for _, part := range strings.Split(strings.TrimPrefix(escapedPath, "/"), "/") {
			decoded, decodeErr := url.PathUnescape(part)
			if decodeErr != nil {
				return nil, Target{}, decodeErr
			}
			switch decoded {
			case ".":
				trailingDot = true
				continue
			case "..":
				trailingDot = true
				if len(segments) > 0 {
					segments = segments[:len(segments)-1]
				}
				continue
			}
			trailingDot = false
			segments = append(segments, decoded)
		}
		if trailingDot {
			segments = append(segments, "")
		}
		if len(segments) == 1 && segments[0] == "" {
			segments = nil
		}
		for _, segment := range segments {
			m.Options = append(m.Options, StringOption(OptionURIPath, segment))
		}
	}
	if u.RawQuery != "" || u.ForceQuery {
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

// net/url rejects percent-encoding in a registered host name. Parse the rest
// of the URI with a safe host, then use the decoded host in Uri-Host.
func parseCoAPURL(rawURI string) (*url.URL, string, bool, error) {
	schemeEnd := strings.IndexByte(rawURI, ':')
	if schemeEnd < 0 || !strings.HasPrefix(rawURI[schemeEnd:], "://") {
		u, err := url.Parse(rawURI)
		return u, "", false, err
	}
	start := schemeEnd + 3
	end := len(rawURI)
	if offset := strings.IndexAny(rawURI[start:], "/?#"); offset >= 0 {
		end = start + offset
	}
	authority := rawURI[start:end]
	if !strings.Contains(authority, "%") || strings.HasPrefix(authority, "[") {
		u, err := url.Parse(rawURI)
		return u, "", false, err
	}
	if strings.Contains(authority, "@") {
		return nil, "", false, errors.New("coap: URI user information is not supported")
	}
	host, port := authority, ""
	if colon := strings.LastIndexByte(authority, ':'); colon >= 0 {
		host, port = authority[:colon], authority[colon:]
	}
	for i := 0; i < len(host); i++ {
		c := host[i]
		if c >= 0x80 || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-._~!$&'()*+,;=%", rune(c)) {
			continue
		}
		return nil, "", false, errors.New("coap: invalid URI host")
	}
	decoded, err := url.PathUnescape(host)
	if err != nil || !utf8.ValidString(decoded) {
		return nil, "", false, errors.New("coap: invalid percent-encoded URI host")
	}
	u, err := url.Parse(rawURI[:start] + "coap-host.invalid" + port + rawURI[end:])
	return u, decoded, true, err
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
