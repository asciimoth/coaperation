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

func TestParseValidation(t *testing.T) {
	if links, err := Parse([]byte(" \t")); err != nil || len(links) != 0 {
		t.Fatalf("empty document = %#v, %v", links, err)
	}
	invalid := []string{
		"x", "<x", "<<x>", "<x> trailing", "<x>;", "<x>;*=x", "<x>;a**=x",
		"<x>;a=", "<x>;a=\"unterminated", "<x>;a=\"bad\\q\"", "<x>;a=\"bad\\",
	}
	for _, input := range invalid {
		if _, err := Parse([]byte(input)); err == nil {
			t.Fatalf("Parse accepted %q", input)
		}
	}
}

func TestMarshalValidationAndValues(t *testing.T) {
	invalid := []Link{
		{Target: "<x>"},
		{Target: "x", Attributes: []Attribute{{}}},
		{Target: "x", Attributes: []Attribute{{Name: "bad name"}}},
		{Target: "x", Attributes: []Attribute{{Name: "*"}}},
		{Target: "x", Attributes: []Attribute{{Name: "a**"}}},
		{Target: "x", Attributes: []Attribute{{Name: "a", Value: "not valid"}}},
	}
	for _, link := range invalid {
		if _, err := Marshal([]Link{link}); err == nil {
			t.Fatalf("Marshal accepted %#v", link)
		}
	}
	link := Link{Attributes: []Attribute{{Name: "rt", Value: "one"}, {Name: "if", Value: "x"}, {Name: "rt", Value: "two"}}}
	if got, want := link.Values("rt"), []string{"one", "two"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Values = %#v, want %#v", got, want)
	}
	wire, err := Marshal([]Link{{Target: "x", Attributes: []Attribute{{Name: "title", Value: `a\"b`, Quoted: true}}}})
	if err != nil || string(wire) != `<x>;title="a\\\"b"` {
		t.Fatalf("quoted Marshal = %q, %v", wire, err)
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
