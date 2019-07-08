package kafka_connect

import (
	"fmt"
	"mybudget/kafka-connect/connector"
)

func NewConnector(configs *connector.Config) (connector.Connector, error) {
	return nil, nil
}

type soiurceConnector struct {
	tasks []connector.Task
}

func (c *soiurceConnector) Configure(configs *connector.Config) error {
	if err := c.Pause(); err != nil {
		return err
	}

	// apply new configurations and restart tasks

	return nil
}

func (c *soiurceConnector) ReConfigure(configs *connector.Config) error {
	if err := c.Pause(); err != nil {
		return err
	}

	// apply new configurations and restart tasks

	return nil
}

func (*soiurceConnector) Pause() error {
	panic("implement me")
}

func (*soiurceConnector) Resume() error {
	panic("implement me")
}

func (c *soiurceConnector) Start() error {
	for _, task := range c.tasks{
		if err := task.Start(); err != nil {
			Logger.Error(`kafkaConnect.connector`, fmt.Sprintf(`taskBuilder start failed due to %s`, err))
		}
	}

	return nil
}

func (c *soiurceConnector) Stop() error {
	for _, task := range c.tasks{
		if err := task.Start(); err != nil {
			Logger.Error(`kafkaConnect.connector`, fmt.Sprintf(`taskBuilder stop failed due to %s`, err))
		}
	}

	return nil
}
