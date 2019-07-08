package transforms

import "testing"

func TestHoistFieldKey_Transform(t *testing.T) {
	tests := []struct {
		key interface{}
		extract  Transformer
		out   interface{}
	}{
		{ // TEST 1, with complex json transformers
			12,
			&HoistField{"HoistField$Key", "age"},
			`{"age":12}`},
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

func TestHoistFieldValue_Transform(t *testing.T) {
	tests := []struct {
		value interface{}
		extract  Transformer
		out   interface{}
	}{
		{ // TEST 1, with complex json transformers
			12,
			&HoistField{"HoistField$Value", "age"},
			`{"age":12}`},
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
