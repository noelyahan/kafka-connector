package transforms

import (
	"fmt"
	"github.com/gmbyapa/kafka-connector/connector"
	"github.com/pickme-go/log"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type ValueToKey struct {
	Fields []string
}

func (v ValueToKey) Transform(rec connector.Recode) connector.Recode {
	if !isJSON(rec.Value()) {
		log.Error(log.WithPrefix(replaceFieldLogPrefix, fmt.Sprintf("unknown type key: %+v", rec.Value())))
		return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
	}
	newKey := `{}`
	for _, field := range v.Fields {
		cVal := gjson.Get(rec.Value().(string), field).Value()
		newKey, _ = sjson.Set(newKey, field, cVal)
	}
	return NewRec(newKey, rec.Value(), rec.Topic(), rec.Partition())
}
