package linkformat

import (
	"reflect"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	want := []Link{
		{Target: "/sensors/temp", Attributes: []Attribute{{Name: "rt", Value: "temperature-c", Quoted: true}, {Name: "if", Value: "sensor"}, {Name: "x-vendor", Value: "a,b", Quoted: true}}},
		{Target: "coap://example.test/x", Attributes: []Attribute{{Name: "base", Value: "coap://example.test/", Quoted: true}, {Name: "proxy-scheme", Value: "coap"}}},
	}
	b, err := Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestExtensionGrammar(t *testing.T) {
	input := []byte(`</x>;vendor.name=value/with:punctuation;flag*=UTF-8''hello`)
	links, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Marshal(links)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(input) {
		t.Fatalf("got %q", got)
	}
}

func FuzzParse(f *testing.F) {
	f.Add([]byte(`</x>;rt="sensor"`))
	f.Add([]byte(`<coap://example.test/a,b>;x=a/b`))
	f.Fuzz(func(t *testing.T, data []byte) {
		links, err := Parse(data)
		if err != nil {
			return
		}
		wire, err := Marshal(links)
		if err != nil {
			t.Fatalf("cannot marshal accepted input: %v", err)
		}
		if _, err := Parse(wire); err != nil {
			t.Fatalf("cannot parse round trip: %v", err)
		}
	})
}
