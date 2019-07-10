package kafka_connect

import (
	"errors"
	"github.com/gmbyapa/kafka-connector/connector"
	"github.com/pickme-go/k-stream/consumer"
	"strings"
)

type configValue struct{ val interface{} }

func (v configValue) Bool() bool             { return v.val.(bool) }
func (v configValue) Int() int               { return v.val.(int) }
func (v configValue) Int8() int8             { return v.val.(int8) }
func (v configValue) Int16() int16           { return v.val.(int16) }
func (v configValue) Int32() int32           { return v.val.(int32) }
func (v configValue) Int64() int64           { return v.val.(int64) }
func (v configValue) Float32() float32       { return v.val.(float32) }
func (v configValue) Float64() float64       { return v.val.(float64) }
func (v configValue) Interface() interface{} { return v.val }
func (v configValue) Map() interface{}       { return v.val }
func (v configValue) String() string         { return v.val.(string) }
func (v configValue) Slice() interface{}     { return v.val }

func validateConnectorConfig(config *connector.Config) error {

	if config.Name == `` {
		return errors.New(`connector name cannot be empty`)
	}

	if config.PluginPath == `` {
		return errors.New(`connector plugin.path cannot be empty`)
	}

	if config.MaxTasks < 1 {
		return errors.New(`connector task.max should be greater than zero`)
	}

	if config.Configs[`encoder.key`] == `` {
		return errors.New(`connector encoder.key cannot be empty`)
	}

	if config.Configs[`encoder.value`] == `` {
		return errors.New(`connector encoder.value cannot be empty`)
	}

	// validate consumerBuilder config
	consumerConfig := consumerConfig(config.Configs)
	if err := consumerConfig.Validate(); err != nil {
		return err
	}

	// validate producer config
	producerConfig := producerConfig(config.Configs)
	if err := producerConfig.Validate(); err != nil {
		return err
	}

	return nil

}

type consumerConfig map[string]interface{}

func (c consumerConfig) Validate() error { return nil }

func (c consumerConfig) Config() (*consumer.Config, error) {
	config := consumer.NewConsumerConfig()
	for name, conf := range c {
		val := configValue{conf}
		switch name {
		case `consumer.bootstrap.servers`:
			config.BootstrapServers = strings.Split(val.String(), `,`)
		case `consumer.host`:
			config.Host = val.String()
		}
	}

	return config, nil
}

type producerConfig map[string]interface{}

func (c producerConfig) Validate() error { return nil }

func (c producerConfig) Config() (*consumer.Config, error) {
	config := consumer.NewConsumerConfig()
	for name, conf := range c {
		val := configValue{conf}
		switch name {
		case `consumer.bootstrap.servers`:
			config.BootstrapServers = strings.Split(val.String(), `,`)
		}
	}
	return config, nil
}
