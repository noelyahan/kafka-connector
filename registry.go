package kafka_connect

import (
	"errors"
	"fmt"
	"github.com/gmbyapa/kafka-connector/connector"
	"github.com/gmbyapa/kafka-connector/encoding"
	"github.com/pickme-go/metrics"
	"sync"
)

type Registry struct {
	plugins        *Plugins
	storage        *connectStorage
	runners        map[string]Runner
	encoders       *encoders
	running        *sync.WaitGroup
	metricReporter metrics.Reporter
}

type RegistryConfig struct {
	plugins        *Plugins
	metricReporter metrics.Reporter
}

func NewRegistry(storage *connectStorage, conf *RegistryConfig, running *sync.WaitGroup) (*Registry, error) {

	encoders := newEncoders()
	if err := encoders.Register(`json`, new(encoding.JsonEncoder)); err != nil {
		return nil, err
	}

	if err := encoders.Register(`string`, new(encoding.StringEncoder)); err != nil {
		return nil, err
	}

	return &Registry{
		plugins:        conf.plugins,
		storage:        storage,
		encoders:       encoders,
		runners:        make(map[string]Runner),
		running:        running,
		metricReporter: conf.metricReporter,
	}, nil
}

// loadAll loads all the existing connectors
func (r *Registry) loadAll() error {
	// get all the existing plugins in the store
	connectors, err := r.storage.GetAll()
	if err != nil {
		return err
	}

	for _, c := range connectors {
		if err := r.loadConnector(c); err != nil {
			Logger.Error(`connect.connectorRegistry`, err)
		}
	}

	return nil
}

func (r *Registry) Connectors() ([]string, error) {
	var list []string
	connectors, err := r.storage.GetAll()
	if err != nil {
		return nil, err
	}
	for c := range connectors {
		list = append(list, c)
	}
	return list, nil
}

func (r *Registry) Connector(name string) (Runner, error) {
	runner, ok := r.runners[name]
	if !ok {
		return nil, errors.New(fmt.Sprintf(`connector [%s] does not exist`, name))
	}

	return runner, nil
}

func (r *Registry) NewConnector(config *RunnerConfig) (Runner, error) {
	c, err := r.storage.Get(config.Connector.Name)
	if err != nil {
		return nil, err
	}

	if c != nil {
		return nil, errors.New(fmt.Sprintf(`connector [%s] already exist`, config.Connector.Name))
	}

	if err := validateConnectorConfig(config.Connector); err != nil {
		return nil, err
	}

	// get plugin from plugins
	p, err := r.plugins.Load(config.Connector.PluginPath)
	if err != nil {
		return nil, err
	}

	if err := r.saveConnector(config, p.Connector); err != nil {
		return nil, err
	}

	if err := r.loadConnector(config); err != nil {
		return nil, err
	}

	return r.Connector(config.Connector.Name)
}

func (r *Registry) saveConnector(config *RunnerConfig, c connector.Connector) error {

	switch c.Type() {
	case connector.ConnectTypeSink:
		config.State = RunnerCreated
		return r.storage.Save(config)
	default:
		return errors.New(fmt.Sprintf(`unsupported connector type %s`, config.Connector.Name))
	}
}

func (r *Registry) loadConnector(config *RunnerConfig) error {
	// get plugin from plugins
	p, err := r.plugins.Load(config.Connector.PluginPath)
	if err != nil {
		return err
	}

	keyEncoder, err := r.encoders.Get(config.Connector.Configs[`encoding.key`].(string))
	if err != nil {
		return err
	}

	valEncoder, err := r.encoders.Get(config.Connector.Configs[`encoding.value`].(string))
	if err != nil {
		return err
	}

	runner := newSinkRunner(config, p.Connector, p.TaskBuilder, keyEncoder, valEncoder, r.metricReporter)
	if err := runner.Init(); err != nil {
		return err
	}

	r.running.Add(1)

	r.runners[config.Connector.Name] = runner

	return runner.Start()
}

func (r *Registry) Reconfigure(name string, config *RunnerConfig) error {
	runner, err := r.Connector(name)
	if err != nil {
		return err
	}

	if err := runner.Reconfigure(config); err != nil {
		return err
	}

	return r.storage.Save(config)
}

func (r *Registry) Stop() error {
	for _, runner := range r.runners {
		if err := runner.Stop(); err != nil {
			Logger.Error(`connect.connectorRegistry`, err)
		}
		r.running.Done()
	}

	return nil
}
