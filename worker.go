package kafka_connect

import (
	"github.com/Shopify/sarama"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/producer"
	"github.com/pickme-go/metrics"
	"gopkg.in/yaml.v2"
	"net/http"
	"os"
	"sync"
)

type workerConfig struct {
	BootstrapServers  []string `yaml:"bootstrap_servers"`
	WorkerConfigTopic string   `yaml:"worker_config_topic"`
	PluginPath        string   `yaml:"plugin_path"`
	Host              string   `yaml:"host"`
	Metrics           struct {
		Enabled   bool   `json:"enabled"`
		Namespace string `json:"namespace"`
		Host      string `json:"host"`
	} `json:"metrics"`
	AdvertisedHost string `yaml:"advertised_host"`
}

func (wc *workerConfig) validate() error {
	if len(wc.BootstrapServers) == 0 {
		return errors.New(`connect.connectWorker.config`, `workerConfig.BootstrapServers cannot be empty`)
	}

	if wc.WorkerConfigTopic == `` {
		return errors.New(`connect.connectWorker.config`, `workerConfig.WorkerConfigTopic cannot be empty`)
	}

	if wc.PluginPath == `` {
		return errors.New(`connect.connectWorker.config`, `workerConfig.PluginPath cannot be empty`)
	}

	if wc.Metrics.Enabled && wc.Metrics.Host == `` {
		return errors.New(`connect.connectWorker.config`, `workerConfig.Metrics.Host cannot be empty`)
	}

	if wc.Metrics.Enabled && wc.Metrics.Namespace == `` {
		return errors.New(`connect.connectWorker.config`, `workerConfig.Metrics.Namespace cannot be empty`)
	}

	if wc.Host == `` {
		return errors.New(`connect.connectWorker.config`, `workerConfig.Host cannot be empty`)
	}

	if wc.AdvertisedHost == `` {
		return errors.New(`connect.connectWorker.config`, `workerConfig.AdvertisedHost cannot be empty`)
	}
	return nil
}

type connectWorker struct {
	id                string
	config            workerConfig
	connectorRegistry *Registry
	running           *sync.WaitGroup
	metricsReporter   metrics.Reporter
}

func NewConnectWorker() (*connectWorker, error) {
	w := new(connectWorker)
	if err := w.configure(); err != nil {
		return nil, err
	}

	w.id = w.config.Host
	w.running = new(sync.WaitGroup)
	metricsReporter := metrics.PrometheusReporter(w.config.Metrics.Namespace, `connectors`)

	consumerConfig := consumer.NewPartitionConsumerConfig()
	consumerConfig.BootstrapServers = w.config.BootstrapServers
	consumerConfig.Version = sarama.V2_0_0_0
	consumerConfig.MetricsReporter = metricsReporter
	consumerConfig.Logger = Logger
	c, err := consumer.NewPartitionConsumer(consumerConfig)
	if err != nil {
		return nil, err
	}

	p, err := producer.NewProducer(&producer.Config{
		Logger:           Logger,
		MetricsReporter:  metricsReporter,
		BootstrapServers: w.config.BootstrapServers,
	})
	if err != nil {
		return nil, err
	}

	regConfig := &RegistryConfig{
		plugins: NewPlugins(w.config.PluginPath),
	}

	reg, err := NewRegistry(NewConnectStorage(&connectStorageConfig{
		consumer:     c,
		producer:     p,
		storageTopic: w.config.WorkerConfigTopic,
	}), regConfig, w.running)
	if err != nil {
		return nil, err
	}

	// first sync reg with enlisting data
	w.connectorRegistry = reg

	return w, nil
}

func (w *connectWorker) CreateConnector(config *RunnerConfig) (Runner, error) {
	return w.connectorRegistry.NewConnector(config)
}

func (w *connectWorker) Start() error {
	return w.connectorRegistry.loadAll()
}

func (w *connectWorker) Stop() error {
	return w.connectorRegistry.Stop()
}

func (w *connectWorker) Reconfigure(name string, config *RunnerConfig) error {
	return w.connectorRegistry.Reconfigure(name, config)
}

func (w *connectWorker) Wait() {
	w.running.Wait()
}

func (w *connectWorker) Http() *Http {
	return &Http{
		host:         w.config.Host,
		server:       &http.Server{},
		connectorReg: w.connectorRegistry,
	}
}

func (w *connectWorker) configure() error {
	f, err := os.Open(`config.yaml`)
	if err != nil {
		return errors.WithPrevious(err, `connect.connectWorker`, `worker config load failed`)
	}

	d := yaml.NewDecoder(f)
	d.SetStrict(true)

	conf := workerConfig{}
	if err := d.Decode(&conf); err != nil {
		return errors.WithPrevious(err, `connect.connectWorker`, `config decode failed`)
	}

	if err := conf.validate(); err != nil {
		return errors.WithPrevious(err, `connect.connectWorker`, `config validate failed`)
	}

	w.config = conf
	return nil
}
