package transforms

import (
	"strings"
	"testing"
)

func TestValueToKey_Transform(t *testing.T) {
	tests := []struct {
		value string
		extract  Transformer
		out   interface{}
	}{
		{ // TEST 1
			`{"age": "12.2324", "user": {"age": "12.456", "address": {"country": "sl"}}}`,
			&ValueToKey{[]string{"age"}},
			`{"age":"12.2324"}`},
		{ // TEST 2
			`{"age": "12.2324", "user": {"age": "12.456", "address": {"country": "sl"}}}`,
			&ValueToKey{[]string{"user.age"}},
			`{"user":{"age":"12.456"}}`},
		{ // TEST 3
			`{"age": "12.2324", "user": {"age": "12.456", "address": {"country": "sl"}}}`,
			&ValueToKey{[]string{"age", "user.age"}},
			`{"user":{"age":"12.456"},"age":"12.2324"}`},

	}

	rec := NewRec(nil, nil, "", 0)
	for _, test := range tests {
		// skip test cases
		//if id != 1 {
		//	continue
		//}
		rec = NewRec(nil, test.value, "", 0)
		rec = test.extract.Transform(rec)

		v := strings.Replace(rec.Key().(string), " ", "", -1)
		if test.out != v {
			t.Errorf("expected type: %v, but got %v", test.out, v)
		}
	}
}
