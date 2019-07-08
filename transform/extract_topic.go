package transforms

import (
	"fmt"
	"github.com/pickme-go/log"
	"github.com/tidwall/gjson"
	"mybudget/kafka-connect/connector"
	"strings"
)
/*
	Extract data from a message and use it as the topic name. You can either use the entire key/key
	(which should be a string), or use a field from a map or struct. Use the concrete transformation type designed for
	the record key (ExtractTopic$Key) or key

example 1

Extract a field named f3 from the key, and use it as the topic name. If the field is null or missing, leave the topic name as-is.

transforms.KeyFieldExample.type=ExtractTopic$Value
transforms.KeyFieldExample.field=f3
transforms.KeyFieldExample.skip.missing.or.null=true

*/
type ExtractTopic struct {
	Type string
	Field string
	SkipMissingOrNull bool
}

var extractTopicLogPrefix = "ExtractTopic SMT"

func (et ExtractTopic) Transform(rec connector.Recode) connector.Recode {
	if strings.Contains(et.Type, "Key") {
		key := et.getJSON(rec.Key())
		if key == nil {
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}
		return NewRec(rec.Key(), rec.Value(), key.(string), rec.Partition())
	}else if strings.Contains(et.Type, "Value") {
		value := et.getJSON(rec.Value())
		if value == nil {
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}
		return NewRec(rec.Key(), rec.Value(), value.(string), rec.Partition())
	}
	log.Error(log.WithPrefix(extractTopicLogPrefix, fmt.Sprintf("unknown SMT type must be (ExtractTopic$Key, ExtractTopic$Value): %v", et.Type)))
	return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())

}

func (et ExtractTopic) getJSON(value interface{}) interface{} {
	if !isJSON(value) {
		log.Error(log.WithPrefix(extractTopicLogPrefix, fmt.Sprintf("unknown type key: %+v", value)))
		return nil
	}
	val := gjson.Get(value.(string), et.Field).Value()
	if val == nil && et.SkipMissingOrNull {
		log.Error(log.WithPrefix(extractTopicLogPrefix, fmt.Sprintf("selected field: %v key is null, reset to old topic key", et.Field)))
		return nil
	}
	return et.Field
}