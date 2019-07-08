package kafka_connect

import (
	"context"
	"encoding/json"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/producer"
	"time"
)

type Storage interface {
	Save(c Runner) error
	Get(name string) (*RunnerConfig, error)
	GetAll(name string) (map[string]*RunnerConfig, error)
	Delete(name string) error
}

type connectStorage struct {
	connectorConfigs map[string]*RunnerConfig
	synced           chan bool
	conf             *connectStorageConfig
}

type connectStorageConfig struct {
	consumer consumer.PartitionConsumer
	producer producer.Producer
	storageTopic string
}

func NewConnectStorage(conf *connectStorageConfig) *connectStorage {
	s := &connectStorage{
		connectorConfigs: make(map[string]*RunnerConfig),
		synced: make(chan bool, 1),
		conf:conf,
	}
	go s.runSync()
	<- s.synced

	return s
}

func (s *connectStorage) Save(config *RunnerConfig) error {
	key, val, err := s.encode(config)
	if err != nil {
		return nil
	}

	_, _, err = s.conf.producer.Produce(context.Background(), &consumer.Record{
		Key: key,
		Value: val,
		Topic: s.conf.storageTopic,
		Partition: 0, // TODO provide multiple partition support
		Timestamp: time.Now(),
	})
	if err != nil {
		return err
	}

	s.connectorConfigs[config.Connector.Name] = config

	return nil
}

func (s *connectStorage) Get(name string) (*RunnerConfig, error) {
	if s.connectorConfigs[name] == nil {
		return nil, nil
	}
	return s.connectorConfigs[name], nil
}

func (s *connectStorage) GetAll() (map[string]*RunnerConfig, error) {
	return s.connectorConfigs, nil
}

func (s *connectStorage) Delete(name string) error {
	key := []byte(name)
	_, _, err := s.conf.producer.Produce(context.Background(), &consumer.Record{
		Key: key,
		Value: nil,
		Partition: 0, // TODO provide multiple partition support
		Timestamp: time.Now(),
	})
	if err != nil {
		return err
	}

	delete(s.connectorConfigs, name)

	return nil
}

func (s *connectStorage) encode(config *RunnerConfig) (key, val []byte, err error) {
	byt, err := json.Marshal(config)
	if err != nil {
		return nil, nil, err
	}
	return []byte(config.Connector.Name), byt, nil
}

func (s *connectStorage) decode(byt []byte) (*RunnerConfig, error) {
	c := RunnerConfig{}
	if err := json.Unmarshal(byt, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *connectStorage) runSync() {
	Logger.Info(`connectStorage.sync`, `storage syncing...`)
	messages, err := s.conf.consumer.Consume(s.conf.storageTopic, 0, consumer.Earliest)
	if err != nil {
		Logger.Fatal(`connectStorage.sync`, err)
	}

	MLOOP:
	for message := range messages{
		switch m := message.(type) {
		case *consumer.Record:
			config, err := s.decode(m.Value)
			if err == nil {
				s.connectorConfigs[string(m.Key)] = config
				continue MLOOP
			}
			Logger.Error(`connectStorage.sync`, err)

		case *consumer.Error:
			Logger.Error(`connectStorage.sync`, err)

		case *consumer.PartitionEnd:
			Logger.Info(`connectStorage.sync`, `storage synced`)
			s.synced <- true
		}
	}
}
