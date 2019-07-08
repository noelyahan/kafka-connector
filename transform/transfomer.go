package transforms

import (
	"encoding/json"
	"fmt"
	"github.com/gmbyapa/kafka-connector/connector"
)

type Transformer interface {
	Transform(rec connector.Recode) connector.Recode
}

// TODO benchmark
func isJSON(v interface{}) bool {
	// TODO v is a struct do it in different way
	var jsonStr map[string]interface{}
	s := fmt.Sprintf("%v", v)
	b := []byte(s)
	err := json.Unmarshal(b, &jsonStr)
	return err == nil
}
