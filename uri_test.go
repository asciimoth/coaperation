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
