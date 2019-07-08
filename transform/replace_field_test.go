package transforms

import (
	"strings"
	"testing"
)

func TestReplaceFieldKey_Transform(t *testing.T) {
	transType := `ReplaceField$Key`
	tests := []struct {
		key string
		extract  Transformer
		out   interface{}
	}{
		{ // TEST 1
			`{"age": "12.2324", "user": {"age": "12.456", "address": {"country": "sl"}}}`,
			&ReplaceField{transType, []string{"age", "user"}, []ReplaceFieldProps{}},
			`{}`},
		{ // TEST 2
			`{"age": "12.2324", "user": {"age": "12.456", "address": {"country": "sl"}}}`,
			&ReplaceField{transType, []string{"age", "user.address"}, []ReplaceFieldProps{}},
			`{"user":{"age":"12.456"}}`},
		{ // TEST 3
			`{"age": "12.2324", "user": {"age": "12.456", "address": {"country": "sl"}}}`,
			&ReplaceField{transType, nil, []ReplaceFieldProps{
				{"age", "Age"},
				{"user", "User"},
				{"User.age", "User.Age"},
				{"User.address", "User.Address"},
			}},
			`{"User":{"Address":{"country":"sl"},"Age":"12.456"},"Age":"12.2324"}`},
		{ // TEST 4
			`{"username": "test", "password": "123"}`,
			&ReplaceField{transType, nil, []ReplaceFieldProps{
				{"username", "user.name"},
				{"password", "user.password"},
			}},
			`{"user":{"password":"123","name":"test"}}`},

	}

	rec := NewRec(nil, nil, "", 0)
	for id, test := range tests {
		//skip test cases
		//if id != 2 {
		//	continue
		//}
		rec = NewRec(test.key, nil, "", 0)
		rec = test.extract.Transform(rec)

		v := strings.Replace(rec.Key().(string), " ", "", -1)
		if test.out != v {
			t.Errorf("%v. expected type: %v, but got %v", id, test.out, v)
		}
	}
}


func TestReplaceFieldValue_Transform(t *testing.T) {
	transType := `ReplaceField$Value`
	tests := []struct {
		value string
		extract  Transformer
		out   interface{}
	}{
		{ // TEST 1
			`{"age": "12.2324", "user": {"age": "12.456", "address": {"country": "sl"}}}`,
			&ReplaceField{transType, []string{"age", "user"}, []ReplaceFieldProps{}},
			`{}`},
		{ // TEST 2
			`{"age": "12.2324", "user": {"age": "12.456", "address": {"country": "sl"}}}`,
			&ReplaceField{transType, []string{"age", "user.address"}, []ReplaceFieldProps{}},
			`{"user":{"age":"12.456"}}`},
		{ // TEST 3
			`{"age": "12.2324", "user": {"age": "12.456", "address": {"country": "sl"}}}`,
			&ReplaceField{transType, nil, []ReplaceFieldProps{
				{"age", "Age"},
				{"user", "User"},
				{"User.age", "User.Age"},
				{"User.address", "User.Address"},
			}},
			`{"User":{"Address":{"country":"sl"},"Age":"12.456"},"Age":"12.2324"}`},
		{ // TEST 4
			`{"username": "test", "password": "123"}`,
			&ReplaceField{transType, nil, []ReplaceFieldProps{
				{"username", "user.name"},
				{"password", "user.password"},
			}},
			`{"user":{"password":"123","name":"test"}}`},

	}

	rec := NewRec(nil, nil, "", 0)
	for id, test := range tests {
		//skip test cases
		//if id != 2 {
		//	continue
		//}
		rec = NewRec(nil, test.value, "", 0)
		rec = test.extract.Transform(rec)

		v := strings.Replace(rec.Value().(string), " ", "", -1)
		if test.out != v {
			t.Errorf("%v. expected type: %v, but got %v", id, test.out, v)
		}
	}
}
