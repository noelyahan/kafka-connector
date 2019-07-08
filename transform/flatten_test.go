package transforms

import (
	"strings"
	"testing"
)

func TestFlattenKey_Transform(t *testing.T) {
	transType := "Flatten$Key"

	tests := []struct {
		key string
		extract  Transformer
		out   string
	}{
		{ // TEST 1, with complex json transformers
			`{"age": "12.2324", "12.32": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&Flatten{transType, "."},
			`{"12.32":"100.34412414213412341234123412342134","age":"12.2324","user.age":"12.456"}`},
		{ // TEST 1, with complex json transformers
			`{"user": {"age": "12.456", "name": "test", "nick_names": ["123", "111"]}}`,
			&Flatten{transType, "_"},
			`{"user_age":"12.456", "user_name": "test", "user_nick_names_0":"123","user_nick_names_1":"111"}`},
	}

	rec := NewRec(nil, nil, "", 0)
	for _, test := range tests {
		// skip test cases
		//if id != 1 {
		//	continue
		//}
		rec = NewRec(test.key, nil, "", 0)
		rec = test.extract.Transform(rec)

		if strings.Replace(test.out, " ", "", -1) != rec.Key() {
			t.Errorf("expected type: %v, but got %v", test.out, rec.Value())
		}
	}
}


func TestFlattenValue_Transform(t *testing.T) {
	transType := "Flatten$Value"

	tests := []struct {
		value string
		extract  Transformer
		out   string
	}{
		{ // TEST 1, with complex json transformers
			`{"age": "12.2324", "12.32": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&Flatten{transType, "."},
			`{"12.32":"100.34412414213412341234123412342134","age":"12.2324","user.age":"12.456"}`},
		{ // TEST 1, with complex json transformers
			`{"user": {"age": "12.456", "name": "test", "nick_names": ["123", "111"]}}`,
			&Flatten{transType, "_"},
			`{"user_age":"12.456", "user_name": "test", "user_nick_names_0":"123","user_nick_names_1":"111"}`},
	}

	rec := NewRec(nil, nil, "", 0)
	for _, test := range tests {
		// skip test cases
		//if id != 1 {
		//	continue
		//}
		rec = NewRec(nil, test.value, "", 0)
		rec = test.extract.Transform(rec)

		if strings.Replace(test.out, " ", "", -1) != rec.Value() {
			t.Errorf("expected type: %v, but got %v", test.out, rec.Value())
		}
	}
}
