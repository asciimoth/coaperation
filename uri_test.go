package coap

import (
	"bytes"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestNewRequestPreservesURIComponents(t *testing.T) {
	m, target, err := NewRequest(GET, "coap+tcp://[2001:db8::1]:7777/a//b?x=1&raw=%FF", nil)
	if err != nil {
		t.Fatal(err)
	}
	if target.Scheme != "coap+tcp" || target.Host != "2001:db8::1" || target.Port != 7777 {
		t.Fatalf("target %#v", target)
	}
	if got, want := m.URIPath(), []string{"a", "", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("path %#v", got)
	}
	queries := m.URIQueries()
	if len(queries) != 2 || string(queries[0]) != "x=1" || !bytes.Equal(queries[1], []byte{'r', 'a', 'w', '=', 0xff}) {
		t.Fatalf("queries %x", queries)
	}
}

func TestNewRequestValidation(t *testing.T) {
	for _, rawURI := range []string{
		"coap://%", "http://example.test", "coap://user@example.test",
		"coap://example.test/x#fragment", "coap:///path", "coap://example.test:0",
		"coap://example.test/x#",
		"coap://example.test:65536", "coap://example.test:bad",
		"coap://example.test/?query=%ZZ",
		"coap://example%ZZ.test", "coap://example%FF.test", "coap://exa mple%41.test",
		"coap:opaque://example%41.test",
	} {
		if _, _, err := NewRequest(GET, rawURI, nil); err == nil {
			t.Fatalf("NewRequest accepted %q", rawURI)
		}
	}
	if _, _, err := NewRequest(Content, "coap://example.test", nil); err == nil {
		t.Fatal("NewRequest accepted a response code")
	}
	m, target, err := NewRequest(GET, "coap://example.test/", []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	if target.Address() != "example.test:5683" || len(m.URIPath()) != 0 || string(m.Payload) != "body" {
		t.Fatalf("request = %#v, target = %#v", m, target)
	}
}

func TestNewRequestURIOptionNormalization(t *testing.T) {
	for _, tc := range []struct {
		uri  string
		host string
		path []string
	}{
		{"coap://EXAMPLE.test:7777/a/./b/../c", "example.test", []string{"a", "c"}},
		{"coap://EX%41MPLE.test:7777/a", "example.test", []string{"a"}},
		{"coap://example.test/a//b/..", "example.test", []string{"a", "", ""}},
		{"coap://example.test/a/%2e%2e/b", "example.test", []string{"b"}},
		{"coap://127.0.0.1:7777/a", "", []string{"a"}},
		{"coap://[2001:db8::1]/a", "", []string{"a"}},
	} {
		m, target, err := NewRequest(GET, tc.uri, nil)
		if err != nil {
			t.Fatalf("%q: %v", tc.uri, err)
		}
		if got := m.URIPath(); !reflect.DeepEqual(got, tc.path) {
			t.Fatalf("%q: path = %#v, want %#v", tc.uri, got, tc.path)
		}
		if option, ok := m.Option(OptionURIHost); (tc.host == "") != !ok || ok && string(option.Value) != tc.host {
			t.Fatalf("%q: Uri-Host = %#v, present = %v", tc.uri, option, ok)
		}
		if _, ok := m.Option(OptionURIPort); ok {
			t.Fatalf("%q: Uri-Port duplicates destination port %d", tc.uri, target.Port)
		}
	}
	m, target, err := NewRequest(GET, "CoAp://EXAMPLE.test?", nil)
	if err != nil || target.Scheme != "coap" || target.Host != "example.test" {
		t.Fatalf("mixed-case URI = %#v, %#v, %v", m, target, err)
	}
	queries := m.URIQueries()
	if len(queries) != 1 || len(queries[0]) != 0 {
		t.Fatalf("empty query = %#v", queries)
	}
}

func FuzzNewRequestPathResolution(f *testing.F) {
	f.Add([]byte("plain"))
	f.Add([]byte("."))
	f.Add([]byte(".."))
	f.Add([]byte("a/b"))
	f.Add([]byte{0xff, '/', '&'})
	f.Fuzz(func(t *testing.T, segment []byte) {
		if len(segment) > 64 {
			return
		}
		part := url.PathEscape(string(segment))
		base, _, err := NewRequest(GET, "coap://example.test/safe/"+part, nil)
		if err != nil {
			t.Fatal(err)
		}
		resolved, _, err := NewRequest(GET, "coap://example.test/safe/extra/../"+part, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(base.URIPath(), resolved.URIPath()) {
			t.Fatalf("dot segment changed path: base %q, resolved %q", base.URIPath(), resolved.URIPath())
		}
		for _, value := range resolved.URIPath() {
			if value == "." || value == ".." {
				t.Fatalf("unresolved path segment %q", value)
			}
		}
	})
}

func FuzzNewRequestEscapedHost(f *testing.F) {
	f.Add([]byte("ample"))
	f.Add([]byte("A/B"))
	f.Add([]byte{0xc3, 0xa9})
	f.Add([]byte{0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64 {
			return
		}
		const hex = "0123456789ABCDEF"
		var uri strings.Builder
		uri.WriteString("coap://x")
		for _, c := range data {
			uri.WriteByte('%')
			uri.WriteByte(hex[c>>4])
			uri.WriteByte(hex[c&15])
		}
		uri.WriteString(".test/a")
		message, target, err := NewRequest(GET, uri.String(), nil)
		if !utf8.Valid(data) {
			if err == nil {
				t.Fatalf("accepted invalid UTF-8 host %x", data)
			}
			return
		}
		want := strings.ToLower("x" + string(data) + ".test")
		if err != nil || target.Host != want {
			t.Fatalf("host %x: target = %#v, error = %v", data, target, err)
		}
		option, ok := message.Option(OptionURIHost)
		if !ok || string(option.Value) != want {
			t.Fatalf("host %x: Uri-Host = %#v, present = %v", data, option, ok)
		}
	})
}
