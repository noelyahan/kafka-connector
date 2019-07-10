package kafka_connect

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kafka-connector/connector"
	"github.com/gmbyapa/kafka-connector/transform"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/log"
	"sync"
	"time"
)

type rebelanceHandler struct{}

func (*rebelanceHandler) OnPartitionAssigned(ctx context.Context, assigned []consumer.TopicPartition) {
}
func (*rebelanceHandler) OnPartitionRevoked(ctx context.Context, revoked []consumer.TopicPartition) {}

type sinkTaskRunner struct {
	id              string
	connectorConfig *connector.Config
	consumerBuilder consumer.Builder
	consumer        consumer.Consumer
	keyEncoder      connector.Encoder
	valEncoder      connector.Encoder
	taskBuilder     connector.TaskBuilder
	transforms      *transforms.Registry
	task            connector.SinkTask
	transformers    []transforms.Transformer
	topics          []string
	partitions      chan consumer.Partition
	stopped         chan interface{}
	buffer          *buffer
	state           TaskState
	logger          log.Logger
}

func (tr *sinkTaskRunner) Init() error {

	task, err := tr.taskBuilder.Build()
	if err != nil {
		return err
	}

	logger := tr.setupLogger()

	tr.consumerBuilder = func(config *consumer.Config) (consumer.Consumer, error) {
		consumerConfig := consumerConfig(tr.connectorConfig.Configs)
		conf, err := consumerConfig.Config()
		conf.GroupId = tr.connectorConfig.Name
		conf.Logger = logger
		if err != nil {
			return nil, err
		}

		return consumer.NewConsumer(conf)
	}

	tr.task = task.(connector.SinkTask)

	tr.buffer = NewBuffer(tr.id, 1, 100*time.Microsecond, func(recodes []connector.Recode) {
		if err := tr.task.Process(recodes); err != nil {
			Logger.Error(`kafkaConnect.sinkTaskRunner`, err)
		}
	})

	tr.transformers = tr.transforms.Init(tr.connectorConfig.Configs)

	tr.task.Configure(&connector.TaskConfig{
		TaskId: tr.id,
		Logger: logger,
	})

	c, err := tr.consumerBuilder(nil)
	if err != nil {
		return err
	}
	tr.consumer = c

	return tr.task.Init()
}

func (tr *sinkTaskRunner) Start() error {
	if err := tr.task.Start(); err != nil {
		return err
	}

	partitions, err := tr.consumer.Partitions(tr.topics, new(rebelanceHandler))
	if err != nil {
		return err
	}

	running := new(sync.WaitGroup)
	for p := range partitions {
		running.Add(1)
		go tr.runPartition(p, running)
	}
	running.Wait()
	return nil
}

func (tr *sinkTaskRunner) Stop() error {
	if err := tr.consumer.Close(); err != nil {
		Logger.Error(`kafkaConnect.sinkTaskRunner`, err)
	}
	<-tr.stopped
	tr.state = TaskStopped
	return nil
}

func (tr *sinkTaskRunner) Status() TaskState {
	if len(tr.partitions) == 0 {
		return TaskIdle
	}

	return tr.state
}

func (tr *sinkTaskRunner) runPartition(p consumer.Partition, wg *sync.WaitGroup) {
	defer wg.Done()
	for record := range p.Records() {

		// encode key and value
		key, err := tr.keyEncoder.Decode(record.Key)
		if err != nil {
			Logger.Error(`kafkaConnect.sinkTaskRunner`, err)
		}

		val, err := tr.valEncoder.Decode(record.Value)
		if err != nil {
			Logger.Error(`kafkaConnect.sinkTaskRunner`, err)
		}

		var record connector.Recode = &connectRecord{
			key:       key,
			value:     val,
			topic:     record.Topic,
			partition: record.Partition,
			offset:    record.Offset,
			timestamp: record.Timestamp,
		}

		// apply transformers on the record
		for _, tr := range tr.transformers {
			record = tr.Transform(record)
		}

		tr.buffer.Store(record)
	}

	p.Wait() <- false
	tr.stopped <- true
}

func (tr *sinkTaskRunner) setupLogger() log.PrefixedLogger {
	level := log.ERROR
	filePath := false
	colors := false

	for config, value := range tr.connectorConfig.Configs {
		switch config {
		case `log.level`:
			if value.(string) != string(log.FATAL) {
				level = log.Level(value.(string))
			}
		case `log.filePath`:
			if value == `true` {
				filePath = true
			}
		case `log.colors`:
			if value == `true` {
				colors = true
			}
		}
	}

	return log.Constructor.PrefixedLog(
		log.Prefixed(fmt.Sprintf(`connector.%s.task.%s`, tr.connectorConfig.Name, tr.id)),
		log.WithLevel(level),
		log.WithFilePath(filePath),
		log.WithColors(colors),
	)
}
