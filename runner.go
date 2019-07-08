package kafka_connect

import "github.com/gmbyapa/kafka-connector/connector"

type RunnerState string

type TaskState string

type RunnerConfig struct {
	Connector *connector.Config `json:"connector"`
	State     RunnerState       `json:"state"`
}

const (
	RunnerCreated RunnerState = `CREATED`
	RunnerRunning RunnerState = `RUNNING`
	RunnerPause   RunnerState = `PAUSED`
	RunnerStopped RunnerState = `STOPPED`
)

const (
	TaskRuning  TaskState = `RUNNING`
	TaskPaused  TaskState = `PAUSED`
	TaskStopped TaskState = `STOPPED`
	TaskIdle    TaskState = `IDLE`
)

type Runner interface {
	Init() error
	Start() error
	Stop() error
	State() RunnerState
	Connector() connector.Connector
	Config() *RunnerConfig
	Reconfigure(configs *RunnerConfig) error
	//Tasks() []connector.Task
}

type RunnerTask interface {
	Start() error
	Status() TaskState
	Stop() error
}
