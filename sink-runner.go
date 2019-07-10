package kafka_connect

import (
	"fmt"
	"github.com/gmbyapa/kafka-connector/connector"
	"github.com/gmbyapa/kafka-connector/transform"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/metrics"
	"strings"
)

type sinkRunner struct {
	config          *RunnerConfig
	tasks           []*sinkTaskRunner
	keyEncoder      connector.EncoderBuilder
	valEncoder      connector.EncoderBuilder
	connector       connector.Connector
	taskBuilder     connector.TaskBuilder
	consumerBuilder consumer.Builder
	transformers    *transforms.Registry
	stopped         chan interface{}
	metricReporter  metrics.Reporter
}

func newSinkRunner(
	configs *RunnerConfig,
	connector connector.Connector,
	taskBuilder connector.TaskBuilder,
	keyEncoder, valEncoder connector.EncoderBuilder,
	metricReporter metrics.Reporter) *sinkRunner {
	configs.Connector.Metrics = metricReporter

	return &sinkRunner{
		config:         configs,
		transformers:   transforms.NewReg(),
		connector:      connector,
		keyEncoder:     keyEncoder,
		valEncoder:     valEncoder,
		taskBuilder:    taskBuilder,
		stopped:        make(chan interface{}, 1),
		metricReporter: metricReporter,
	}
}

func (c *sinkRunner) Start() error {
	topics := c.config.Connector.Configs[`topics`]
	for i := 1; i <= c.config.Connector.MaxTasks; i++ {
		task := &sinkTaskRunner{
			id:              fmt.Sprintf(`%d`, i),
			taskBuilder:     c.taskBuilder,
			transforms:      c.transformers,
			keyEncoder:      c.keyEncoder(),
			valEncoder:      c.valEncoder(),
			topics:          strings.Split(topics.(string), `,`),
			connectorConfig: c.config.Connector,
			stopped:         make(chan interface{}, 1),
			metricReporter:  c.metricReporter,
		}
		c.tasks = append(c.tasks, task)
		if err := task.Init(); err != nil {
			return err
		}

		go func(task *sinkTaskRunner) {
			if err := task.Start(); err != nil {
				Logger.Error(``, err)
			}
		}(task)
	}
	return nil
}

func (c *sinkRunner) Stop() error {
	Logger.Info(fmt.Sprintf(`sinkRunner.%s`, c.config.Connector.Name), `stopping...`)
	defer Logger.Info(fmt.Sprintf(`sinkRunner.%s`, c.config.Connector.Name), `stopped`)
	for _, task := range c.tasks {
		if err := task.Stop(); err != nil {
			return err
		}
	}

	return nil
}

func (c *sinkRunner) Connector() connector.Connector {
	return c.connector
}

func (c *sinkRunner) Config() *RunnerConfig {
	return c.config
}

func (c *sinkRunner) State() RunnerState {
	return c.config.State
}

func (c *sinkRunner) Reconfigure(configs *RunnerConfig) error {
	if err := c.Stop(); err != nil {
		return err
	}
	c.config = configs
	return c.Start()
}

func (c *sinkRunner) configure() error {
	// setup consumerBuilder builder
	return nil
}

func (c *sinkRunner) Init() error {
	return c.connector.Init(c.config.Connector)
}
