package encoding

import "encoding/json"

type JsonEncoder struct {}

func (*JsonEncoder) Encode(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func (*JsonEncoder) Decode(data []byte) (interface{}, error) {
	return string(data), nil
}