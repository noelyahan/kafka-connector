package transforms

import (
	"testing"
)

func TestExtractTopicKey_Transform(t *testing.T) {
	transType := "ExtractTopic$Key"

	tests := []struct {
		topic   string
		key     string
		extract Transformer
		out     interface{}
	}{
		{ // TEST 1, with complex json transformers
			`topic_1`,
			`{"age": "12.2324", "12.32": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&ExtractTopic{transType, "age", false},
			`age`},
		{ // TEST 2, with multiple key transformers
			`topic_1`,
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&ExtractTopic{transType, "height", false},
			`height`},
		{ // TEST 3, with multiple key transformers
			`topic_1`,
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&ExtractTopic{transType, "user.age", false},
			`user.age`},
		{ // TEST 4, with multiple key transformers
			`topic_1`,
			`{"age": null, "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&ExtractTopic{transType, "age", true},
			`topic_1`},
	}

	rec := NewRec(nil, nil, "", 0)
	for _, test := range tests {
		// skip test cases
		//if id != 1 {
		//	continue
		//}
		rec = NewRec(test.key, nil, test.topic, 0)
		rec = test.extract.Transform(rec)

		if test.out != rec.Topic() {
			t.Errorf("expected type: %v, but got %v", test.out, rec.Topic())
		}
	}
}

// if field is empty
func TestExtractTopicValue_Transform(t *testing.T) {
	transType := "ExtractTopic$Value"

	tests := []struct {
		topic   string
		value   string
		extract Transformer
		out     interface{}
	}{
		{ // TEST 1, with complex json transformers
			`topic_1`,
			`{"age": "12.2324", "12.32": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&ExtractTopic{transType, "age", false},
			`age`},
		{ // TEST 2, with multiple key transformers
			`topic_1`,
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&ExtractTopic{transType, "height", false},
			`height`},
		{ // TEST 3, with multiple key transformers
			`topic_1`,
			`{"age": "12.2324", "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&ExtractTopic{transType, "user.age", false},
			`user.age`},
		{ // TEST 4, with multiple key transformers
			`topic_1`,
			`{"age": null, "height": "100.34412414213412341234123412342134", "user": {"age": "12.456"}}`,
			&ExtractTopic{transType, "age", true},
			`topic_1`},
	}

	rec := NewRec(nil, nil, "", 0)
	for _, test := range tests {
		// skip test cases
		//if id != 1 {
		//	continue
		//}
		rec = NewRec(nil, test.value, test.topic, 0)
		rec = test.extract.Transform(rec)

		if test.out != rec.Topic() {
			t.Errorf("expected type: %v, but got %v", test.out, rec.Topic())
		}
	}
}
