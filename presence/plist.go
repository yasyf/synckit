package presence

import (
	"bytes"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// decodePlist parses an Apple XML plist document and returns its root value the way
// plistlib.loads would. An empty or malformed document is an error, not a silent nil.
func decodePlist(payload []byte) (any, error) {
	dec := xml.NewDecoder(bytes.NewReader(payload))
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("read plist: %w", err)
		}
		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		if start.Name.Local != "plist" {
			continue
		}
		value, err := decodePlistValue(dec)
		if err != nil {
			return nil, err
		}

		for {
			tok, err = dec.Token()
			if err != nil {
				return nil, fmt.Errorf("read plist end: %w", err)
			}
			if text, ok := tok.(xml.CharData); ok && len(bytes.TrimSpace(text)) == 0 {
				continue
			}
			break
		}
		switch el := tok.(type) {
		case xml.EndElement:
			if el.Name.Local != "plist" {
				return nil, fmt.Errorf("plist: expected </plist>, got </%s>", el.Name.Local)
			}
		case xml.StartElement:
			return nil, fmt.Errorf("plist: multiple root values, got <%s>", el.Name.Local)
		default:
			return nil, fmt.Errorf("plist: expected </plist>, got %T", tok)
		}

		for {
			tok, err = dec.Token()
			if errors.Is(err, io.EOF) {
				return value, nil
			}
			if err != nil {
				return nil, fmt.Errorf("read after plist: %w", err)
			}
			if text, ok := tok.(xml.CharData); ok && len(bytes.TrimSpace(text)) == 0 {
				continue
			}
			return nil, fmt.Errorf("plist: unexpected token after </plist>: %T", tok)
		}
	}
}

// decodePlistValue reads the next value element under the cursor (the opening tag of a
// plist value) and returns the decoded Go value.
func decodePlistValue(dec *xml.Decoder) (any, error) {
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("read plist value: %w", err)
		}
		switch el := tok.(type) {
		case xml.StartElement:
			return decodeElement(dec, el)
		case xml.EndElement:
			return nil, fmt.Errorf("plist: expected a value, got </%s>", el.Name.Local)
		}
	}
}

func decodeElement(dec *xml.Decoder, start xml.StartElement) (any, error) {
	switch start.Name.Local {
	case "dict":
		return decodeDict(dec)
	case "array":
		return decodeArray(dec)
	case "true":
		return true, dec.Skip()
	case "false":
		return false, dec.Skip()
	case "string", "data":
		text, err := elementText(dec)
		if err != nil {
			return nil, err
		}
		if start.Name.Local == "data" {
			return decodePlistData(text)
		}
		return text, nil
	case "integer":
		text, err := elementText(dec)
		if err != nil {
			return nil, err
		}
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("plist integer %q: %w", text, err)
		}
		return n, nil
	case "real":
		text, err := elementText(dec)
		if err != nil {
			return nil, err
		}
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, fmt.Errorf("plist real %q: %w", text, err)
		}
		return f, nil
	default:
		return nil, dec.Skip()
	}
}

// decodeDict reads a <dict> body of alternating <key> and value elements into a map,
// until its closing tag.
func decodeDict(dec *xml.Decoder) (map[string]any, error) {
	out := map[string]any{}
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("read plist dict: %w", err)
		}
		switch el := tok.(type) {
		case xml.EndElement:
			return out, nil
		case xml.StartElement:
			if el.Name.Local != "key" {
				return nil, fmt.Errorf("plist dict: expected <key>, got <%s>", el.Name.Local)
			}
			key, err := elementText(dec)
			if err != nil {
				return nil, err
			}
			value, err := decodePlistValue(dec)
			if err != nil {
				return nil, err
			}
			out[key] = value
		}
	}
}

// decodeArray reads an <array> body into a slice, until its closing tag.
func decodeArray(dec *xml.Decoder) ([]any, error) {
	var out []any
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("read plist array: %w", err)
		}
		switch el := tok.(type) {
		case xml.EndElement:
			return out, nil
		case xml.StartElement:
			value, err := decodeElement(dec, el)
			if err != nil {
				return nil, err
			}
			out = append(out, value)
		}
	}
}

// decodePlistData decodes a <data> element's base64 body, dropping the whitespace Apple
// wraps it in so the returned bytes are the decoded content, not the base64 source.
func decodePlistData(text string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(text), ""))
	if err != nil {
		return nil, fmt.Errorf("plist data: %w", err)
	}
	return decoded, nil
}

// elementText reads the character data of the element under the cursor and consumes
// its closing tag.
func elementText(dec *xml.Decoder) (string, error) {
	var b bytes.Buffer
	for {
		tok, err := dec.Token()
		if err != nil {
			return "", fmt.Errorf("read plist text: %w", err)
		}
		switch el := tok.(type) {
		case xml.CharData:
			b.Write(el)
		case xml.EndElement:
			return b.String(), nil
		}
	}
}
