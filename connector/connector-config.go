package connector

import "github.com/pickme-go/metrics"

type Config struct {
	Name       string                 `json:"name"`
	MaxTasks   int                    `json:"task.max"`
	PluginPath string                 `json:"plugin.path"`
	Configs    map[string]interface{} `json:"configs,omitempty"`
	Metrics    metrics.Reporter       `json:"-"`
	Logger     Logger                 `json:"-"`
}

type TaskConfig struct {
	Connector *Config `json:"-"`
	TaskId    string  `json:"-"`
	Logger    Logger  `json:"-"`
}
