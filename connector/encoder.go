package connector

type Encoder interface {
	Encode(data interface{}) ([]byte, error)
	Decode(data []byte) (interface{}, error)
}

type EncoderBuilder func() Encoder