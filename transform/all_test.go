package transforms

import (
	"reflect"
	"testing"
)

func TestAll(t *testing.T)  {
	var k, v interface{}
	k = "1"
	v = `{"age": "12.2324", "height": 100.34412414213412341234123412342134, "user": {"age": "12.456"}}`
	transforms := []Transformer{
		&Cast{`Cast$Value`, []CastProps{{"height", "float32"}}},
		&ExtractField{"ExtractField&Value", "height"},
		&Cast{`Cast$Value`, []CastProps{{"", "string"}}},
		&Cast{`Cast$Value`, []CastProps{{"", "int32"}}},
	}

	rec := NewRec(k, v, "", 0)
	for _, trans := range transforms {
		rec = trans.Transform(rec)
	}

	t.Log(rec.Key(), rec.Value(), reflect.TypeOf(rec.Value()).String())
}
