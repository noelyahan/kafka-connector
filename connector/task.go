package connector

type Task interface {
	Configure(config *TaskConfig)
	Init() error
	Start() error
	Stop() error
}

type ReBalanceHandler interface {
	OnPartitionAssigned(map[string][]int16)
	OnPartitionRevoked(map[string][]int16)
}

type SinkTask interface {
	Task
	OnRebalanced() ReBalanceHandler
	Process([]Recode) error
}

type TaskBuilder interface {
	Build() (Task, error)
}
