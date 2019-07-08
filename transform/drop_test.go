package transforms

import (
	"reflect"
	"testing"
)

func TestDropKey_Transform(t *testing.T) {
	tests := []struct {
		value interface{}
		cast  Transformer
		out   interface{}
	}{
		{ // TEST 1, with complex json transformers
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&Drop{"Drop$Key", "nullify"},
			nil},
	}

	rec := NewRec(nil, nil, "", 0)
	for id, test := range tests {
		// skip test cases
		//if id != 68 {
		//	continue
		//}
		rec = NewRec(nil, test.value, "", 0)
		rec = test.cast.Transform(rec)

		if test.out != rec.Key() {
			t.Errorf("test case: %v, expected type: %v : %v, but got type: %v : %v ", id, reflect.TypeOf(test.out), test.out, reflect.TypeOf(rec.Key()), rec.Key())
		}
	}
}

func TestDropValue_Transform(t *testing.T) {
	tests := []struct {
		value interface{}
		cast  Transformer
		out   interface{}
	}{
		{ // TEST 1, with complex json transformers
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&Drop{"Drop$Value", "nullify"},
			`{"age":null,"height":null,"user":null}`},
	}

	rec := NewRec(nil, nil, "", 0)
	for id, test := range tests {
		// skip test cases
		//if id != 68 {
		//	continue
		//}
		rec = NewRec(nil, test.value, "", 0)
		rec = test.cast.Transform(rec)

		if test.out != rec.Value() {
			t.Errorf("test case: %v, expected type: %v : %v, but got type: %v : %v ", id, reflect.TypeOf(test.out), test.out, reflect.TypeOf(rec.Value()), rec.Value())
		}
	}
}