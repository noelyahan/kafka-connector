package kafka_connect

import (
	"fmt"
	"github.com/gmbyapa/kafka-connector/connector"
	"github.com/pickme-go/errors"
	"os"
	"path/filepath"
	goPlugin "plugin"
	"reflect"
	"strings"
)

type plugin struct {
	TaskBuilder connector.TaskBuilder
	Connector   connector.Connector
}

type Plugins struct {
	path string
	//plugins map[string]*plugin
	plugins []string
}

func NewPlugins(path string) *Plugins {
	return &Plugins{
		path: path,
		//plugins: make(map[string]*plugin),
	}
}

func (p *Plugins) LoadAll() error {
	err := filepath.Walk(p.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		sts := strings.Split(info.Name(), `.`)
		if info.IsDir() || len(sts) != 2 || sts[1] != `so` {
			return nil
		}

		//plugin, err := p.Load(path)
		//if err != nil {
		//	return err
		//}

		p.plugins = append(p.plugins, p.path+path)

		return nil
	})
	if err != nil {
		return errors.WithPrevious(err, `connect.connectWorker`, fmt.Sprintf(`cannot load plugins due to %+v`, err))
	}

	return nil
}

func (p *Plugins) Load(path string) (*plugin, error) {

	plugin := new(plugin)

	plg, err := goPlugin.Open(path)
	if err != nil {
		return nil, err
	}

	// get Connector
	sConector, err := plg.Lookup(`Connector`)
	if err != nil {
		return nil, err
	}

	// get Connector
	sTask, err := plg.Lookup(`Task`)
	if err != nil {
		return nil, err
	}

	connect, ok := (sConector).(*connector.Connector)
	if !ok {
		return nil, errors.New(`connect.connectWorker`, fmt.Sprintf(`invalid Connector type want [Connector] have %s`, reflect.TypeOf(sConector)))
	}

	plugin.Connector = *connect

	switch builder := sTask.(type) {
	case *connector.TaskBuilder:
		plugin.TaskBuilder = *builder
	default:
		return nil, errors.New(`connect.connectWorker`, fmt.Sprintf(`invalid Task type want SinkTaskBuilder or SourceTaskBuilder have %s`, reflect.TypeOf(sTask)))
	}

	return plugin, nil
}
