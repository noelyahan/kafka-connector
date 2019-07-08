package transforms

import (
	"encoding/json"
	"fmt"
	"github.com/gmbyapa/kafka-connector/connector"
	"github.com/pickme-go/log"
	"strings"
)

/*

Set either the key or key of a message to null. The corresponding schema can also be set to null,
set as optional, checked that it is already optional, or kept as-is. Use the concrete transformation
type designed for the record key or key

example:

transform configs

"transforms": "drop",
transforms.drop.type="Drop$Value"
transforms.drop.schema.behavior="force_optional"
//schema.behavior -> nullify, retain, validate, force_optional


*/
type Drop struct {
	Type           string
	SchemaBehavior string
}

var dropLogPrefix = "Drop SMT"

func (d Drop) Transform(rec connector.Recode) connector.Recode {
	isKey := false
	if strings.Contains(d.Type, "Key") {
		isKey = true
	} else if strings.Contains(d.Type, "Value") {
		isKey = false
	} else {
		log.Error(log.WithPrefix(dropLogPrefix, fmt.Sprintf("unknown SMT type must be (Drop$Key, Drop$Value): %v", d.Type)))
		return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
	}

	switch d.SchemaBehavior {
	case `nullify`:
		if isKey {
			return NewRec(nil, rec.Value(), rec.Topic(), rec.Partition())
		}
		if !isJSON(rec.Value()) {
			log.Error(log.WithPrefix(dropLogPrefix, fmt.Sprintf("unknown type key: %+v", rec.Value())))
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}

		m := make(map[string]interface{})
		err := json.Unmarshal([]byte(rec.Value().(string)), &m)
		if err != nil {
			log.Error(log.WithPrefix(dropLogPrefix, fmt.Sprintf("could not process with nullify behaviour: %+v", rec.Value())))
			return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
		}
		for k := range m {
			m[k] = nil
		}
		b, err := json.Marshal(m)
		if err != nil {

		}
		return NewRec(rec.Key(), string(b), rec.Topic(), rec.Partition())
	case `retain`:
		log.Error(log.WithPrefix(dropLogPrefix, fmt.Sprintf("not implemented the retain behaviour: %+v", rec.Value())))
	case `validate`:
		log.Error(log.WithPrefix(dropLogPrefix, fmt.Sprintf("not implemented the validate behaviour: %+v", rec.Value())))
	case `force_optional`:
		log.Error(log.WithPrefix(dropLogPrefix, fmt.Sprintf("not implemented the force_optional behaviour: %+v", rec.Value())))
	}

	return NewRec(rec.Key(), rec.Value(), rec.Topic(), rec.Partition())
}
