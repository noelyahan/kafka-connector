package transforms

import (
	"encoding/json"
	"fmt"
	"github.com/gmbyapa/kafka-connector/connector"
	"github.com/pickme-go/log"
	"strings"
)

type HoistField struct {
	Type  string
	Field string
}

var hoistFieldLogPrefix = "HoistField SMT"

func (h HoistField) Transform(rec connector.Recode) connector.Recode {
	if strings.Contains(h.Type, "Key") {
		key := h.getJSON(rec.Key())
		if key == nil {
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}
		return NewRec(key, rec.Value(), rec.Topic(), rec.Partition())
	} else if strings.Contains(h.Type, "Value") {
		value := h.getJSON(rec.Value())
		if value == nil {
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}
		return NewRec(rec.Key(), value, rec.Topic(), rec.Partition())
	}

	log.Error(log.WithPrefix(hoistFieldLogPrefix, fmt.Sprintf("unknown SMT type must be (HoistField$Key, HoistField$Value): %v", h.Type)))
	return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
}

func (h HoistField) getJSON(value interface{}) interface{} {
	m := make(map[string]interface{})
	m[h.Field] = value
	b, err := json.Marshal(m)
	if err != nil {
		log.Error(log.WithPrefix(hoistFieldLogPrefix, fmt.Sprintf("unknown SMT type must be (HoistField$Key, HoistField$Value): %v", h.Type)))
		return nil
	}
	return string(b)
}
