package transforms

import (
	"reflect"
	"testing"
)

func TestCastKey(t *testing.T) {
	castType := "Cast$Key"
	tests := []struct {
		key  interface{}
		cast Transformer
		out  interface{}
	}{
		{ // TEST 1, with complex json transformers
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&Cast{castType, []CastProps{{"age", "int8"}, {"height", "int8"}, {"user.age", "int8"}}},
			`{"age": 12, "height": 100, "user": {"age": 12}}`},
		{ // TEST 2, with multiple key transformers
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&Cast{castType, []CastProps{{"age", "int8"}, {"age", "string"}, {"height", "int8"}, {"height", "string"}, {"height", "float64"}, {"user.age", "int8"}, {"user.age", "string"}}},
			`{"age": "12", "height": 100, "user": {"age": "12"}}`},
		{ // TEST 3, with single transformer
			`12.34432`,
			&Cast{castType, []CastProps{{"", "int8"}}},
			int8(12)},
		{ // TEST 4, with multiple just a key transformers
			`12.34432`,
			&Cast{castType, []CastProps{{"", "string"}, {"", "int8"}}},
			int8(12)},
		{ // TEST 5, with multiple just a key transformers
			123,
			&Cast{castType, []CastProps{{"", "string"}}},
			`123`},
		{ // TEST 6, with multiple just a key transformers
			`{"ID":46920,"score":4761}`,
			&Cast{castType, []CastProps{{"ID", "string"}, {"score", "float64"}}},
			`{"ID":"46920","score":4761}`},
		//		Error Cases
		{ // TEST 6, invalid json path
			`{"ID":46920,"score":4761}`,
			&Cast{castType, []CastProps{{"ID.score", "string"}}},
			`{"ID":46920,"score":4761}`},
	}

	rec := NewRec(nil, nil, "", 0)
	for id, test := range tests {
		//skip test cases
		//if id != 0 {
		//	continue
		//}
		rec = NewRec(test.key, `{}`, "", 0)
		rec = test.cast.Transform(rec)

		if test.out != rec.Key() {
			t.Errorf("test case: %v, expected type: %v : %v, but got type: %v : %v ", id, reflect.TypeOf(test.out), test.out, reflect.TypeOf(rec.Key()), rec.Key())
		}
	}
}

func TestCastValue(t *testing.T) {
	castType := "Cast$Value"
	tests := []struct {
		value interface{}
		cast  Transformer
		out   interface{}
	}{
		{ // TEST 1, with complex json transformers
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&Cast{castType, []CastProps{{"age", "int8"}, {"height", "int8"}, {"user.age", "int8"}}},
			`{"age": 12, "height": 100, "user": {"age": 12}}`},
		{ // TEST 2, with multiple key transformers
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&Cast{castType, []CastProps{{"age", "int8"}, {"age", "string"}, {"height", "int8"}, {"height", "string"}, {"height", "float64"}, {"user.age", "int8"}, {"user.age", "string"}}},
			`{"age": "12", "height": 100, "user": {"age": "12"}}`},
		{ // TEST 3, with single transformer
			`12.34432`,
			&Cast{castType, []CastProps{{"", "int8"}}},
			int8(12)},
		{ // TEST 4, with multiple just a key transformers
			`12.34432`,
			&Cast{castType, []CastProps{{"", "string"}, {"", "int8"}}},
			int8(12)},
		{ // TEST 5, with multiple just a key transformers
			123,
			&Cast{castType, []CastProps{{"", "string"}}},
			`123`},
		{ // TEST 6, with multiple just a key transformers
			`{"ID":46920,"score":4761}`,
			&Cast{castType, []CastProps{{"ID", "string"}, {"score", "float64"}}},
			`{"ID":"46920","score":4761}`},
		//		Error Cases
		{ // TEST 6, invalid json path
			`{"ID":46920,"score":4761}`,
			&Cast{castType, []CastProps{{"ID.score", "string"}}},
			`{"ID":46920,"score":4761}`},
	}

	rec := NewRec(nil, nil, "", 0)
	for id, test := range tests {
		//skip test cases
		//if id != 0 {
		//	continue
		//}
		rec = NewRec(nil, test.value, "", 0)
		rec = test.cast.Transform(rec)

		if test.out != rec.Value() {
			t.Errorf("test case: %v, expected type: %v : %v, but got type: %v : %v ", id, reflect.TypeOf(test.out), test.out, reflect.TypeOf(rec.Value()), rec.Value())
		}
	}
}
