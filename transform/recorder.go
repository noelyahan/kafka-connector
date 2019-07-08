package transforms

import (
	"github.com/gmbyapa/kafka-connector/connector"
	"time"
)

type ConnectRecord struct {
	key       interface{}
	value     interface{}
	topic     string
	partition int32
	timestamp time.Time
	offset    int64
}

func NewRec(key, value interface{}, topic string, partition int32) connector.Recode {
	return ConnectRecord{key: key, value: value, topic: topic, partition: partition}
}

func (cr ConnectRecord) Key() interface{} {
	return cr.key
}

func (cr ConnectRecord) Value() interface{} {
	return cr.value
}

func (cr ConnectRecord) Offset() int64 {
	return cr.offset
}

func (cr ConnectRecord) Topic() string {
	return cr.topic
}

func (cr ConnectRecord) Partition() int32 {
	return cr.partition
}

func (cr ConnectRecord) Timestamp() time.Time {
	return cr.timestamp
}
