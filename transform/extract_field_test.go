package transforms

import (
	"reflect"
	"testing"
)
func TestExtractFieldKey_Transform(t *testing.T) {
	castType := "ExtractField&Key"
	tests := []struct {
		key     string
		extract Transformer
		out     interface{}
	}{
		{ // TEST 1, with complex json transformers
			`{"age": "12.2324", "12.32": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&ExtractField{castType, "age"},
			`12.2324`},
		{ // TEST 2, with multiple key transformers
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&ExtractField{castType, "height"},
			`100.34412414213412341234123412342134`},
		{ // TEST 3, with multiple key transformers
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&ExtractField{castType, "user.age"},
			`12.456`},
	}

	rec := NewRec(nil, nil, "", 0)
	for _, test := range tests {
		// skip test cases
		//if id != 1 {
		//	continue
		//}
		rec = NewRec(test.key, `{}`, "", 0)
		rec = test.extract.Transform(rec)

		if test.out != rec.Key() {
			t.Errorf("expected type: %v : %v, but got type: %v : %v ", reflect.TypeOf(test.out), test.out, reflect.TypeOf(rec.Key()), rec.Key())
		}
	}
}

func TestExtractFieldValue_Transform(t *testing.T) {
	castType := "ExtractField&Value"
	tests := []struct {
		value string
		extract  Transformer
		out   interface{}
	}{
		{ // TEST 1, with complex json transformers
			`{"age": "12.2324", "12.32": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&ExtractField{castType, "age"},
			`12.2324`},
		{ // TEST 2, with multiple key transformers
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&ExtractField{castType, "height"},
			`100.34412414213412341234123412342134`},
		{ // TEST 3, with multiple key transformers
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&ExtractField{castType, "user.age"},
			`12.456`},
	}

	rec := NewRec(nil, nil, "", 0)
	for _, test := range tests {
		// skip test cases
		//if id != 1 {
		//	continue
		//}
		rec = NewRec(nil, test.value, "", 0)
		rec = test.extract.Transform(rec)

		if test.out != rec.Value() {
			t.Errorf("expected type: %v : %v, but got type: %v : %v ", reflect.TypeOf(test.out), test.out, reflect.TypeOf(rec.Value()), rec.Value())
		}
	}
}
