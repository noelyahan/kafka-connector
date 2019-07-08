package transforms

import (
	"fmt"
	"github.com/pickme-go/log"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"mybudget/kafka-connect/connector"
	"strconv"
	"strings"
)

/*
Cast fields (or the entire key or key) to a specific type, updating the schema if one is present.
For example, this can be used to force an integer field into an integer of smaller width.
Only simple primitive types are supported, such as integer, float, boolean, and string.

example 1:

transform configs

"transforms": "cast",
"transforms.cast.type": "Cast$Value",
"transforms.cast.spec": "string"

Before: 63521704.02

After: "63521704.02"

example 2:

transform configs

"transforms": "cast",
"transforms.cast.type": "Cast$Value",
"transforms.cast.spec": "ID:string,score:float64"

"transforms.cast.type": "Cast$Key",
"transforms.cast.spec": "ID:string,score:float64"

Before: {"ID": 46920,"score": 4761}

After: {"ID": "46290","score": 4761.0}

*/
type Cast struct {
	Type  string // key or key
	Spec  []CastProps
}

type CastProps struct {
	Field     string
	FieldType string
}

var castLogPrefix = "Cast SMT"

func (c Cast) Transform(rec connector.Recode) connector.Recode {
	if strings.Contains(c.Type, "Key") {
		key := c.getJSON(rec.Key())
		if key == nil {
			key = rec.Key()
		}
		return NewRec(key, rec.Value(), rec.Topic(), rec.Partition())
	} else if strings.Contains(c.Type, "Value") {
		value := c.getJSON(rec.Value())
		if value == nil {
			value = rec.Value()
		}
		return NewRec(rec.Key(), value, rec.Topic(), rec.Partition())
	}

	log.Error(log.WithPrefix(castLogPrefix, fmt.Sprintf("unknown SMT type must be (Cast$Key, Cast$Value): %v", c.Type)))
	return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
}

func (c Cast) getJSON(value interface{}) interface{} {
	var err error
	if !isJSON(value) {
		for _, spec := range c.Spec {
			value = c.cast(spec.FieldType, value)
		}
	} else {
		for _, spec := range c.Spec {
			newIn := gjson.Get(value.(string), spec.Field).Value()
			if newIn == nil {
				log.Error(log.WithPrefix(castLogPrefix, fmt.Sprintf("no key found, invalid JSON path %v, key: %+v", spec.Field, value)))
				continue
			}
			newIn = c.cast(spec.FieldType, newIn)
			value, err = sjson.Set(value.(string), spec.Field, newIn)
			if err != nil {
				log.Error(log.WithPrefix(castLogPrefix, fmt.Sprintf("error on cast key replacement, check JSON: %v", spec.Field)))
			}
		}
	}
	return value
}


func (c Cast) cast(castType string, v interface{}) interface{} {
	var s string
	var err error
	var i float64
	s = fmt.Sprintf("%v", v)

	switch castType {
	case "string":
		v = s
	case "int8":
		i, err = strconv.ParseFloat(s, 0)
		v = int8(i)
	case "int16":
		i, err = strconv.ParseFloat(s, 0)
		v = int16(i)
	case "int32":
		i, err = strconv.ParseFloat(s, 0)
		v = int32(i)
	case "int64":
		i, err = strconv.ParseFloat(s, 0)
		v = int64(i)
	case "float32":
		i, err = strconv.ParseFloat(s, 0)
		v = float32(i)
	case "float64":
		i, err = strconv.ParseFloat(s, 0)
		v = float64(i)
	case "boolean":
		b := false
		b, err = strconv.ParseBool(s)
		v = bool(b)
	}
	if err != nil {
		log.Error(log.WithPrefix(castLogPrefix, fmt.Sprintf("error on casting the key: %v, type: %v, error: %v", v, castType, err)))
	}
	return v
}
