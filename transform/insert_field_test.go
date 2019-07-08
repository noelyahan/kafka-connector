package transforms

import "testing"


func TestInsertFieldKey_Transform(t *testing.T) {
	transType := `InsertField$Key`
	tests := []struct {
		key string
		trans Transformer
		out   interface{}
	}{
		{ // TEST 1, with complex json transformers
			`{"age": "12.2324", "12.32": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&InsertField{transType, "age", 23},
			`{"age": 23, "12.32": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`},
		{ // TEST 2, with multiple key transformers
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&InsertField{transType, "height", 300},
			`{"age": "12.2324", "height": 300, "user": {"age": "12.456"}}`},
		{ // TEST 3, with multiple key transformers
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&InsertField{transType, "user.age", 23},
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": 23}}`},
	}

	rec := NewRec(nil, nil, "", 0)
	for _, test := range tests {
		// skip test cases
		//if id != 1 {
		//	continue
		//}
		rec = NewRec(test.key, nil, "", 0)
		rec = test.trans.Transform(rec)

		if test.out != rec.Key() {
			t.Errorf("expected type: %v, but got %v", test.out, rec.Value())
		}
	}
}

func TestInsertFieldValue_Transform(t *testing.T) {
	transType := `InsertField$Value`
	tests := []struct {
		value string
		trans Transformer
		out   interface{}
	}{
		{ // TEST 1, with complex json transformers
			`{"age": "12.2324", "12.32": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&InsertField{transType, "age", 23},
			`{"age": 23, "12.32": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`},
		{ // TEST 2, with multiple key transformers
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&InsertField{transType, "height", 300},
			`{"age": "12.2324", "height": 300, "user": {"age": "12.456"}}`},
		{ // TEST 3, with multiple key transformers
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&InsertField{transType, "user.age", 23},
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": 23}}`},
	}

	rec := NewRec(nil, nil, "", 0)
	for _, test := range tests {
		// skip test cases
		//if id != 1 {
		//	continue
		//}
		rec = NewRec(nil, test.value, "", 0)
		rec = test.trans.Transform(rec)

		if test.out != rec.Value() {
			t.Errorf("expected type: %v, but got %v", test.out, rec.Value())
		}
	}
}