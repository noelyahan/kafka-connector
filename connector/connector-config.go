package connector

type Config struct {
	Name       string                 `json:"name"`
	MaxTasks   int                    `json:"task.max"`
	PluginPath string                 `json:"plugin.path"`
	Configs    map[string]interface{} `json:"configs,omitempty"`
	Metrics    MetricsReporter        `json:"-"`
	Logger     Logger                 `json:"-"`
}

type TaskConfig struct {
	TaskId string
	Logger Logger `json:"-"`
}
