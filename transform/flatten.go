package transforms

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gmbyapa/kafka-connector/connector"
	"github.com/pickme-go/log"
	"strconv"
	"strings"
)

type Flatten struct {
	Type      string
	Delimiter string
}

/*
Flatten a nested data structure, generating names for each field by concatenating the field names at each level with a
configurable delimiter character. Applies to a Struct when a schema is present, or a Map in the case of schemaless data.
The Single Message Transforms (SMT) delimiter is ., which is also the default. Use the concrete transformation type
designed for the record key (Flatten$Key) or key (Flatten$Value).

example 1

"transforms": "flatten",
"transforms.flatten.type": "Flatten$Value",
"transforms.flatten.delimiter": "."

Before:

{
  "content": {
    "id": 42,
    "name": {
      "first": "David",
      "middle": null,
      "last": "Wong"
    }
  }
}

After:

{
  "content.id": 42,
  "content.name.first": "David",
  "content.name.middle": null,
  "content.name.last": "Wong"
}

// https://github.com/jeremywohl/flatten/blob/master/README.md
*/

var flattenLogPrefix = "Flatten SMT"

func (f Flatten) Transform(rec connector.Recode) connector.Recode {
	if strings.Contains(f.Type, "Key") {
		key := f.getJSON(rec.Key())
		if key == nil {
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}
		return NewRec(key, rec.Value(), rec.Topic(), rec.Partition())
	} else if strings.Contains(f.Type, "Value") {
		value := f.getJSON(rec.Value())
		if value == nil {
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}
		return NewRec(rec.Key(), value, rec.Topic(), rec.Partition())
	}
	log.Error(log.WithPrefix(flattenLogPrefix, fmt.Sprintf("unknown SMT type must be (ExtractTopic$Key, ExtractTopic$Value): %v", f.Type)))
	return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
}

func (f Flatten) getJSON(value interface{}) interface{} {
	if !isJSON(value) {
		log.Error(log.WithPrefix(flattenLogPrefix, fmt.Sprintf("unknown type key: %+v", value)))
		return nil
	}
	style := DotStyle
	if f.Delimiter == "_" {
		style = UnderscoreStyle
	}
	val, err := FlattenString(value.(string), "", style)
	if err != nil {
		log.Error(log.WithPrefix(flattenLogPrefix, fmt.Sprintf("out put flat value is null")))
		return nil
	}
	return val
}

// The presentation style of keys.
type SeparatorStyle int

const (
	_ SeparatorStyle = iota

	// Separate nested key components with dots, e.g. "a.b.1.c.d"
	DotStyle

	// Separate with path-like slashes, e.g. a/b/1/c/d
	PathStyle

	// Separate ala Rails, e.g. "a[b][c][1][d]"
	RailsStyle

	// Separate with underscores, e.g. "a_b_1_c_d"
	UnderscoreStyle
)

// Nested input must be a map or slice
var NotValidInputError = errors.New("Not a valid input: map or slice")

// Flatten generates a flat map from a nested one.  The original may include values of type map, slice and scalar,
// but not struct.  Keys in the flat map will be a compound of descending map keys and slice iterations.
// The presentation of keys is set by style.  A prefix is joined to each key.
func Flatten_(nested map[string]interface{}, prefix string, style SeparatorStyle) (map[string]interface{}, error) {
	flatmap := make(map[string]interface{})

	err := flatten(true, flatmap, nested, prefix, style)
	if err != nil {
		return nil, err
	}

	return flatmap, nil
}

// FlattenString generates a flat JSON map from a nested one.  Keys in the flat map will be a compound of
// descending map keys and slice iterations.  The presentation of keys is set by style.  A prefix is joined
// to each key.
func FlattenString(nestedstr, prefix string, style SeparatorStyle) (string, error) {
	var nested map[string]interface{}
	err := json.Unmarshal([]byte(nestedstr), &nested)
	if err != nil {
		return "", err
	}

	flatmap, err := Flatten_(nested, prefix, style)
	if err != nil {
		return "", err
	}

	flatb, err := json.Marshal(&flatmap)
	if err != nil {
		return "", err
	}

	return string(flatb), nil
}

func flatten(top bool, flatMap map[string]interface{}, nested interface{}, prefix string, style SeparatorStyle) error {
	assign := func(newKey string, v interface{}) error {
		switch v.(type) {
		case map[string]interface{}, []interface{}:
			if err := flatten(false, flatMap, v, newKey, style); err != nil {
				return err
			}
		default:
			flatMap[newKey] = v
		}

		return nil
	}

	switch nested.(type) {
	case map[string]interface{}:
		for k, v := range nested.(map[string]interface{}) {
			newKey := enkey(top, prefix, k, style)
			assign(newKey, v)
		}
	case []interface{}:
		for i, v := range nested.([]interface{}) {
			newKey := enkey(top, prefix, strconv.Itoa(i), style)
			assign(newKey, v)
		}
	default:
		return NotValidInputError
	}

	return nil
}

func enkey(top bool, prefix, subkey string, style SeparatorStyle) string {
	key := prefix

	if top {
		key += subkey
	} else {
		switch style {
		case DotStyle:
			key += "." + subkey
		case PathStyle:
			key += "/" + subkey
		case RailsStyle:
			key += "[" + subkey + "]"
		case UnderscoreStyle:
			key += "_" + subkey
		}
	}

	return key
}
