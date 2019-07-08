package kafka_connect

import (
	"errors"
	"fmt"
	"github.com/gmbyapa/kafka-connector/connector"
)

type encoders struct {
	list map[string]connector.Encoder
}

func newConcoders() *encoders {
	return &encoders{
		list: make(map[string]connector.Encoder),
	}
}

func (enc *encoders) Register(name string, encoder connector.Encoder) error {
	if _, ok := enc.list[name]; ok {
		return errors.New(fmt.Sprintf(`encoder [%s] already registered`, name))
	}

	enc.list[name] = encoder
	return nil
}

func (enc *encoders) List() []string { panic(`not yet implemented`) }

func (enc *encoders) Registered(name string) bool {
	_, ok := enc.list[name]
	return ok
}

func (enc *encoders) Get(name string) (connector.EncoderBuilder, error) {
	encoder, ok := enc.list[name]
	if !ok {
		return nil, errors.New(fmt.Sprintf(`encoder [%s] does not exist`, name))
	}

	return func() connector.Encoder {
		return encoder
	}, nil
}
