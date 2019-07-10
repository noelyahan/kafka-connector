package connector

type ConnectType string

const ConnectTypeSource ConnectType = `source`
const ConnectTypeSink ConnectType = `sink`

type Connector interface {
	Init(configs *Config) error
	Pause() error
	Name() string
	Resume() error
	Type() ConnectType
	Start() error
	Stop() error
}
