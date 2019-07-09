package transforms

import (
	"fmt"
	"github.com/gmbyapa/kafka-connector/connector"
	"github.com/pickme-go/log"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"strings"
)

type ReplaceFieldProps struct {
	Field    string
	NewField string
}
type ReplaceField struct {
	Type            string
	BlackListFields []string
	WhiteList []string
	Renames []ReplaceFieldProps
}

var replaceFieldLogPrefix = `ReplaceField SMT`

func (r ReplaceField) Transform(rec connector.Recode) connector.Recode {
	if strings.Contains(r.Type, "Key") {
		key := r.getJSON(rec.Key())
		if key == nil {
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}
		return NewRec(key, rec.Value(), rec.Topic(), rec.Partition())
	} else if strings.Contains(r.Type, "Value") {
		value := r.getJSON(rec.Value())
		if value == nil {
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}
		return NewRec(rec.Key(), value, rec.Topic(), rec.Partition())
	}

	log.Error(log.WithPrefix(replaceFieldLogPrefix, fmt.Sprintf("unknown SMT type must be (ReplaceField$Key, ReplaceField$Value): %v", r.Type)))
	return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
}

func (r ReplaceField) getJSON(value interface{}) interface{} {
	if !isJSON(value) {
		log.Error(log.WithPrefix(replaceFieldLogPrefix, fmt.Sprintf("unknown type key: %+v", value)))
		return nil
	}
	var err error

	tmpVal := value.(string)


	for _, remove := range r.BlackListFields {
		value, _ = sjson.Delete(value.(string), remove)
	}

	if len(r.BlackListFields) == 0 && len(r.WhiteList) != 0 {
		value = `{}`
	}
	for _, add := range r.WhiteList {
		cVal := gjson.Get(tmpVal, add).Value()
		value, _ = sjson.Set(value.(string), add, cVal)
	}

	for _, rename := range r.Renames {
		cVal := gjson.Get(value.(string), rename.Field).Value()
		if cVal == nil {
			continue
		}
		value, err = sjson.Delete(value.(string), rename.Field)
		if err != nil {
			continue
		}
		value, _ = sjson.Set(value.(string), rename.NewField, cVal)
	}
	return value
}
