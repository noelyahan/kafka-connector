package connector

type ConnectType string

const ConnetTypeSource ConnectType = `source`
const ConnetTypeSink ConnectType = `sink`

type Connector interface {
	Init(configs *Config) error
	Pause() error
	Name() string
	Resume() error
	Type() ConnectType
	Start() error
	Stop() error
}