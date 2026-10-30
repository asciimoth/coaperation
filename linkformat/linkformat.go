// Package linkformat parses and serializes the CoRE Link Format from RFC 6690.
package linkformat

import (
	"errors"
	"fmt"
	"strings"
)

// Attribute is one ordered link attribute. Quoted preserves its wire form.
type Attribute struct {
	Name   string
	Value  string
	Quoted bool
}

// Link contains one target and its ordered attributes.
type Link struct {
	Target     string
	Attributes []Attribute
}

// Parse decodes one application/link-format document.
func Parse(data []byte) ([]Link, error) {
	p := parser{s: string(data)}
	var links []Link
	p.space()
	if p.end() {
		return links, nil
	}
	for {
		link, err := p.link()
		if err != nil {
			return nil, err
		}
		links = append(links, link)
		p.space()
		if p.end() {
			return links, nil
		}
		if p.take() != ',' {
			return nil, p.err("expected comma")
		}
		p.space()
	}
}

type parser struct {
	s string
	i int
}

func (p *parser) end() bool { return p.i >= len(p.s) }
func (p *parser) peek() byte {
	if p.end() {
		return 0
	}
	return p.s[p.i]
}
func (p *parser) take() byte {
	c := p.peek()
	if !p.end() {
		p.i++
	}
	return c
}
func (p *parser) space() {
	for p.peek() == ' ' || p.peek() == '\t' {
		p.i++
	}
}
func (p *parser) err(s string) error { return fmt.Errorf("linkformat: %s at byte %d", s, p.i) }

func (p *parser) link() (Link, error) {
	if p.take() != '<' {
		return Link{}, p.err("expected '<'")
	}
	start := p.i
	for !p.end() && p.peek() != '>' {
		if p.peek() == '<' {
			return Link{}, p.err("invalid '<' in link target")
		}
		p.i++
	}
	if p.end() {
		return Link{}, p.err("unterminated link target")
	}
	link := Link{Target: p.s[start:p.i]}
	p.i++
	for {
		p.space()
		if p.peek() != ';' {
			return link, nil
		}
		p.i++
		p.space()
		start = p.i
		for isName(p.peek()) {
			p.i++
		}
		if start == p.i {
			return Link{}, p.err("empty attribute name")
		}
		a := Attribute{Name: p.s[start:p.i]}
		if a.Name == "*" || strings.Count(a.Name, "*") > 1 || strings.Contains(strings.TrimSuffix(a.Name, "*"), "*") {
			return Link{}, p.err("invalid '*' in attribute name")
		}
		if p.peek() == '=' {
			p.i++
			if p.peek() == '"' {
				a.Quoted = true
				p.i++
				var b strings.Builder
				for !p.end() && p.peek() != '"' {
					c := p.take()
					if c == '\\' {
						if p.end() {
							return Link{}, p.err("unterminated quoted attribute")
						}
						c = p.take()
						if c != '\\' && c != '"' {
							return Link{}, p.err("invalid quoted escape")
						}
					}
					b.WriteByte(c)
				}
				if p.end() {
					return Link{}, p.err("unterminated quoted attribute")
				}
				p.i++
				a.Value = b.String()
			} else {
				start = p.i
				for isPToken(p.peek()) {
					p.i++
				}
				if start == p.i {
					return Link{}, p.err("empty attribute value")
				}
				a.Value = p.s[start:p.i]
			}
		}
		link.Attributes = append(link.Attributes, a)
	}
}

func isName(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$&+-.^_`|~*", rune(c))
}

func isPToken(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'()*+-./:<=>?@[]^_`{|}~", rune(c))
}

// Marshal encodes links as an application/link-format document.
func Marshal(links []Link) ([]byte, error) {
	var b strings.Builder
	for i, link := range links {
		if strings.ContainsAny(link.Target, "<>") {
			return nil, errors.New("linkformat: target contains angle bracket")
		}
		if i != 0 {
			b.WriteByte(',')
		}
		b.WriteByte('<')
		b.WriteString(link.Target)
		b.WriteByte('>')
		for _, a := range link.Attributes {
			if a.Name == "" {
				return nil, errors.New("linkformat: empty attribute name")
			}
			for j := range a.Name {
				if !isName(a.Name[j]) {
					return nil, errors.New("linkformat: invalid attribute name")
				}
			}
			if a.Name == "*" || strings.Count(a.Name, "*") > 1 || strings.Contains(strings.TrimSuffix(a.Name, "*"), "*") {
				return nil, errors.New("linkformat: '*' is only valid at the end of an attribute name")
			}
			b.WriteByte(';')
			b.WriteString(a.Name)
			if a.Value != "" || a.Quoted {
				b.WriteByte('=')
				if a.Quoted {
					b.WriteByte('"')
					for j := 0; j < len(a.Value); j++ {
						c := a.Value[j]
						if c == '\\' || c == '"' {
							b.WriteByte('\\')
						}
						b.WriteByte(c)
					}
					b.WriteByte('"')
				} else {
					for j := range a.Value {
						if !isPToken(a.Value[j]) {
							return nil, errors.New("linkformat: unquoted value contains an invalid byte")
						}
					}
					b.WriteString(a.Value)
				}
			}
		}
	}
	return []byte(b.String()), nil
}

// Values returns all values for an attribute name.
func (l Link) Values(name string) []string {
	var values []string
	for _, a := range l.Attributes {
		if a.Name == name {
			values = append(values, a.Value)
		}
	}
	return values
}
