package transforms

import (
	"testing"
)


func TestMaskFieldKey_Transform(t *testing.T) {
	transType := `MaskField$Key`
	tests := []struct {
		key string
		extract  Transformer
		out   interface{}
	}{
		{ // TEST 1, with complex json transformers
			`{"age": "12.2324", "height": 100.34412414213412341234123412342134, "user": {"age": 12.456, "is_logged": true}}`,
			&MaskField{transType, []string{"age"}},
			`{"age": "", "height": 100.34412414213412341234123412342134, "user": {"age": 12.456, "is_logged": true}}`},
		{ // TEST 2, with complex json transformers
			`{"age": "12.2324", "height": 100.34412414213412341234123412342134, "user": {"age": 12.456, "is_logged": true}}`,
			&MaskField{transType, []string{"age", "height"}},
			`{"age": "", "height": 0, "user": {"age": 12.456, "is_logged": true}}`},
		{ // TEST 3, with complex json transformers
			`{"age": "12.2324", "height": 100.34412414213412341234123412342134, "user": {"age": 12.456, "is_logged": true}}`,
			&MaskField{transType, []string{"age", "height", "user.age", "user.is_logged"}},
			`{"age": "", "height": 0, "user": {"age": 0, "is_logged": false}}`},
	}

	rec := NewRec(nil, nil, "", 0)
	for _, test := range tests {
		// skip test cases
		//if id != 1 {
		//	continue
		//}
		rec = NewRec(test.key, nil, "", 0)
		rec = test.extract.Transform(rec)

		if test.out != rec.Key() {
			t.Errorf("expected type: %v, but got %v", test.out, rec.Value())
		}
	}
}

func TestMaskFieldValue_Transform(t *testing.T) {
	transType := `MaskField$Value`
	tests := []struct {
		value string
		extract  Transformer
		out   interface{}
	}{
		{ // TEST 1, with complex json transformers
			`{"age": "12.2324", "height": 100.34412414213412341234123412342134, "user": {"age": 12.456, "is_logged": true}}`,
			&MaskField{transType, []string{"age"}},
			`{"age": "", "height": 100.34412414213412341234123412342134, "user": {"age": 12.456, "is_logged": true}}`},
		{ // TEST 2, with complex json transformers
			`{"age": "12.2324", "height": 100.34412414213412341234123412342134, "user": {"age": 12.456, "is_logged": true}}`,
			&MaskField{transType, []string{"age", "height"}},
			`{"age": "", "height": 0, "user": {"age": 12.456, "is_logged": true}}`},
		{ // TEST 3, with complex json transformers
			`{"age": "12.2324", "height": 100.34412414213412341234123412342134, "user": {"age": 12.456, "is_logged": true}}`,
			&MaskField{transType, []string{"age", "height", "user.age", "user.is_logged"}},
			`{"age": "", "height": 0, "user": {"age": 0, "is_logged": false}}`},
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
			t.Errorf("expected type: %v, but got %v", test.out, rec.Value())
		}
	}
}
