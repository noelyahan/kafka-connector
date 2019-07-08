package transforms

import (
	"fmt"
	"github.com/pickme-go/log"
	"github.com/tidwall/sjson"
	"mybudget/kafka-connect/connector"
	"strings"
)

type InsertField struct {
	Type string
	Field string
	Value interface{}
}

var insertFieldLogPrefix = `InsertField SMT`

func (i InsertField) Transform(rec connector.Recode) connector.Recode {
	if strings.Contains(i.Type, "Key") {
		key := i.getJSON(rec.Key())
		if key == nil {
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}
		return NewRec(key, rec.Value(), rec.Topic(), rec.Partition())
	}else if strings.Contains(i.Type, "Value") {
		value := i.getJSON(rec.Value())
		if value == nil {
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}
		return NewRec(rec.Key(), value, rec.Topic(), rec.Partition())
	}
	log.Error(log.WithPrefix(insertFieldLogPrefix, fmt.Sprintf("unknown SMT type must be (InsertField$Key, InsertField$Value): %v", i.Type)))
	return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
}

func (i InsertField) getJSON(value interface{}) interface{} {
	if !isJSON(value) {
		log.Error(log.WithPrefix(insertFieldLogPrefix, fmt.Sprintf("unknown type key: %+v", value)))
		return nil
	}
	val, err := sjson.Set(value.(string), i.Field, i.Value)
	if err != nil {
		log.Error(log.WithPrefix(insertFieldLogPrefix, fmt.Sprintf("unknown type key: %+v", err)))
		return nil
	}
	return val
}
