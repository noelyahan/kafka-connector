package transforms

import (
	"testing"
)

func TestRegistry_Get(t *testing.T) {
	reg := NewReg()
	cfg := make(map[string]interface{})

	cfg[`name`] = `connector1`

	cfg[`transforms`] = `Cast1, Cast2, Drop1, Drop2, extractField, extractTopic, flatten, hoistField, insertField, maskField, replaceField, valueToKey`

	// Cast
	cfg[`transforms.Cast1.type`] = `Cast$Value`
	cfg[`transforms.Cast1.spec`] = `string`

	cfg[`transforms.Cast2.type`] = `Cast$Value`
	cfg[`transforms.Cast2.spec`] = `ID:string,score:float64`

	// Drop
	cfg[`transforms.Drop1.type`] = `Drop$Key`
	cfg[`transforms.Drop1.schema.behavior`] = `force_optional`

	cfg[`transforms.Drop2.type`] = `Drop$Key`
	cfg[`transforms.Drop2.schema.behavior`] = `nullify`

	// ExtractField
	cfg[`transforms.extractField.type`] = `ExtractField$Key`
	cfg[`transforms.extractField.field`] = `id`

	// ExtractTopic
	cfg[`transforms.extractTopic.type`] = `ExtractTopic$Value`
	cfg[`transforms.extractTopic.field`] = `id`
	cfg[`transforms.extractTopic.skip.missing.or.null`] = true

	// Flatten
	cfg[`transforms.flatten.type`] = `Flatten$Value`
	cfg[`transforms.flatten.delimiter`] = `_`

	// HoistField
	cfg[`transforms.hoistField.type`] = `HoistField$Value`
	cfg[`transforms.hoistField.field`] = `line`

	// HoistField
	cfg[`transforms.insertField.type`] = `InsertField$Value`
	cfg[`transforms.insertField.static.field`] = `MessageSource`
	cfg[`transforms.insertField.static.key`] = `Kafka Connect framework`

	// MaskField
	cfg[`transforms.maskField.type`] = `MaskField$Value`
	cfg[`transforms.maskField.fields`] = `string_field, f1`

	// ReplaceField
	cfg[`transforms.replaceField.type`] = `ReplaceField$Value`
	cfg[`transforms.replaceField.blacklist`] = `c1`
	cfg[`transforms.replaceField.renames`] = `foo:c1,bar:c2`

	// ValueToKey
	cfg[`transforms.valueToKey.type`] = `ValueToKey`
	cfg[`transforms.valueToKey.fields`] = `userId,city,state`

	reg.Init(cfg)

	//trans := reg.Get("connector1")
	//if len(trans) != len(strings.Split(cfg[`transforms`].(string), ",")) {
	//	t.Errorf("expected 12 transforms but got %v", len(trans))
	//}
}
