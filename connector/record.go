package connector

import "time"

type Recode interface {
	Topic() string
	Partition() int32
	Offset() int64
	Key() interface{}
	Value() interface{}
	Timestamp() time.Time
}
