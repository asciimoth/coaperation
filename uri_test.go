package coap

import (
	"bytes"
	"reflect"
	"testing"
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
		"coap://example.test:65536", "coap://example.test:bad",
		"coap://example.test/?query=%ZZ",
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
