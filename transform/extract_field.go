package transforms

import (
	"fmt"
	"github.com/pickme-go/log"
	"github.com/tidwall/gjson"
	"mybudget/kafka-connect/connector"
	"strings"
)

/*
ExtractField pulls a field out of a complex (non-primitive, Map or Struct) key or key and replaces the entire key or key
with the extracted field. Any null values are passed through unmodified. Use the concrete transformation type designed
for the record key (ExtractField$Key) or key (ExtractField$Value).

example 1

"transforms": "extractField",
"transforms.extractField.type":"ExtractField$Key",
"transforms.extractField.field":"id"

Before: {"id": 42, "cost": 4000}

After: 42


*/
type ExtractField struct {
	Type  string
	Field string
}

var extractFieldLogPrefix = "ExtractField SMT"

func (e ExtractField) Transform(rec connector.Recode) connector.Recode {
	if strings.Contains(e.Type, "Key") {
		key := e.getJSON(rec.Key())
		if key == nil {
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}
		return NewRec(key, rec.Value(), rec.Topic(), rec.Partition())
	} else if strings.Contains(e.Type, "Value") {
		value := e.getJSON(rec.Value())
		if value == nil {
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}
		return NewRec(rec.Key(), value, rec.Topic(), rec.Partition())
	}
	log.Error(log.WithPrefix(extractFieldLogPrefix, fmt.Sprintf("unknown SMT type must be (ExtractField$Key, ExtractField$Value): %v", e.Type)))
	return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
}

func (e ExtractField) getJSON(value interface{}) interface{} {
	if !isJSON(value) {
		log.Error(log.WithPrefix(extractFieldLogPrefix, fmt.Sprintf("unknown type key: %+v", value)))
		return nil
	}
	return gjson.Get(value.(string), e.Field).Value()
}
