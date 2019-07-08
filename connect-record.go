package kafka_connect

import "time"

type connectRecord struct {
	topic string
	partition int32
	offset int64
	timestamp time.Time
	key interface{}
	value interface{}
}

func (r *connectRecord) Topic() string {
	return r.topic
}

func (r *connectRecord) Partition() int32 {
	return r.partition
}

func (r *connectRecord) Offset() int64 {
	return r.offset
}

func (r *connectRecord) Key() interface{} {
	return r.key
}

func (r *connectRecord) Value() interface{} {
	return r.value
}

func (r *connectRecord) Timestamp() time.Time {
	return r.timestamp
}

