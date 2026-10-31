package linkformat

import (
	"bytes"
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

func TestQuotedValuePreservesRawBytes(t *testing.T) {
	input := []byte{'<', 'x', '>', ';', 't', 'i', 't', 'l', 'e', '=', '"', 0xff, '"'}
	links, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := Marshal(links)
	if err != nil || string(wire) != string(input) {
		t.Fatalf("raw quoted value changed: %x, %v", wire, err)
	}
}

func TestParseValidation(t *testing.T) {
	if links, err := Parse([]byte(" \t")); err != nil || len(links) != 0 {
		t.Fatalf("empty document = %#v, %v", links, err)
	}
	invalid := []string{
		"x", "<x", "<<x>", "<x> trailing", "<x>;", "<x>;*=x", "<x>;a**=x",
		"<x>;a=", "<x>;a=\"unterminated", "<x>;a=\"bad\\q\"", "<x>;a=\"bad\\",
		"<x y>", "<x%ZZ>", "<x\n>", string([]byte{'<', 0xff, '>'}),
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
		{Target: "x y"},
		{Target: "x%ZZ"},
		{Target: string([]byte{0xff})},
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
	f.Add([]byte{'<', 'x', '>', ';', 't', 'i', 't', 'l', 'e', '=', '"', 0xff, '"'})
	f.Fuzz(func(t *testing.T, data []byte) {
		links, err := Parse(data)
		if err != nil {
			return
		}
		wire, err := Marshal(links)
		if err != nil {
			t.Fatalf("cannot marshal accepted input: %v", err)
		}
		got, err := Parse(wire)
		if err != nil || !reflect.DeepEqual(got, links) {
			t.Fatalf("link-format round trip changed parsed links: got %#v, want %#v, error %v", got, links, err)
		}
	})
}

func FuzzLinkTargetURIReference(f *testing.F) {
	f.Add([]byte("/sensors/temp"))
	f.Add([]byte("coap://example.test/x,y"))
	f.Add([]byte("x y"))
	f.Add([]byte("x%ZZ"))
	f.Add([]byte{0xff})
	f.Fuzz(func(t *testing.T, target []byte) {
		if len(target) > 128 || bytes.ContainsAny(target, "<>") {
			return
		}
		links, parseErr := Parse(append(append([]byte{'<'}, target...), '>'))
		wire, marshalErr := Marshal([]Link{{Target: string(target)}})
		if (parseErr == nil) != (marshalErr == nil) {
			t.Fatalf("target %x: parse = %v, marshal = %v", target, parseErr, marshalErr)
		}
		if parseErr == nil && (len(links) != 1 || links[0].Target != string(target) || !bytes.Equal(wire, append(append([]byte{'<'}, target...), '>'))) {
			t.Fatalf("target %x changed: links = %#v, wire = %x", target, links, wire)
		}
	})
}
