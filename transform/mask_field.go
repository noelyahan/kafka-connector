package transforms

import (
	"fmt"
	"github.com/gmbyapa/kafka-connector/connector"
	"github.com/pickme-go/log"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"reflect"
	"strings"
)

// Mask specified fields with a valid null key for the field type (i.e. 0, false, empty string, and so on).
type MaskField struct {
	Type   string
	Fields []string
}

var maskFieldLogPrefix = `MaskField SMT`

func (m MaskField) Transform(rec connector.Recode) connector.Recode {
	if strings.Contains(m.Type, "Key") {
		key := m.getJSON(rec.Key())
		if key == nil {
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}
		return NewRec(key, rec.Value(), rec.Topic(), rec.Partition())
	} else if strings.Contains(m.Type, "Value") {
		value := m.getJSON(rec.Value())
		if value == nil {
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}
		return NewRec(rec.Key(), value, rec.Topic(), rec.Partition())
	}

	log.Error(log.WithPrefix(maskFieldLogPrefix, fmt.Sprintf("unknown SMT type must be (MaskField$Key, MaskField$Value): %v", m.Type)))
	return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
}

func (m MaskField) getJSON(value interface{}) interface{} {
	if !isJSON(value) {
		log.Error(log.WithPrefix(maskFieldLogPrefix, fmt.Sprintf("unknown type key: %+v", value)))
		return nil
	}
	var err error
	for _, field := range m.Fields {
		v := gjson.Get(value.(string), field).Value()
		switch reflect.TypeOf(v).String() {
		case "string":
			v = ""
		case "int8":
			v = 0
		case "int16":
			v = 0
		case "int32":
			v = 0
		case "int64":
			v = 0
		case "float32":
			v = 0
		case "float64":
			v = 0
		case "bool":
			v = false
		}
		value, err = sjson.Set(value.(string), field, v)
		if err != nil {
			continue
		}
	}
	return value
}
