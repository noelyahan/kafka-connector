/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package kafka_connect

import (
	"context"
	"fmt"
	"mybudget/kafka-connect/connector"
	"sync"
	"time"
)

// buffer holds a temporary changelog buffer
type buffer struct {
	id            string
	records       []connector.Recode
	mu            *sync.Mutex
	shouldFlush   chan bool
	flushInterval time.Duration
	bufferSize    int
	lastFlushed   time.Time
	onFlush func([]connector.Recode)
	//metrics       struct {
	//	flushLatency metrics.Observer
	//}
}

// NewBuffer creates a new buffer object
func NewBuffer(id string, size int, flushInterval time.Duration, onFlush func([]connector.Recode)) *buffer {
	flush := 1 * time.Second
	if flushInterval != 0 {
		flush = flushInterval
	}

	b := &buffer{
		id:            id,
		records:       make([]connector.Recode, 0, size),
		mu:            new(sync.Mutex),
		bufferSize:    size,
		flushInterval: flush,
		onFlush:onFlush,
		lastFlushed:   time.Now(),
	}

	go b.runFlusher()

	return b
}

// Clear clears the buffer
func (b *buffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.flushAll(); err != nil {
		Logger.Error( `kafkaConnect.buffer`, err)
	}

}

func (b *buffer) Records() []connector.Recode {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.records
}

// Store stores the record in buffer
func (b *buffer) Store(record connector.Recode) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.records = append(b.records, record)

	if len(b.records) >= b.bufferSize {
		b.flush()
	}
}

func (b *buffer) runFlusher() {
	tic := time.NewTicker(b.flushInterval)
	defer tic.Stop()

	for range tic.C {

		if time.Since(b.lastFlushed) <= b.flushInterval {
			continue
		}

		b.mu.Lock()
		l := len(b.records)
		b.mu.Unlock()

		if l > 0 {
			b.flush()
			println(`flushed`)
		}
	}
}

func (b *buffer) flush() {
	if err := b.flushAll(); err != nil {
		Logger.ErrorContext(context.Background(), `kafkaConnect.buffer`, err)
	}

	Logger.Trace(`kafkaConnect.buffer`, `buffer flushed`)
}

func (b *buffer) flushAll() error {
	//begin := time.Now()
	//defer func(t time.Time) {
	//	b.metrics.flushLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), nil)
	//}(begin)


	if len(b.records) < 1 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// punch method
	b.onFlush(b.records)

	b.reset()

	return nil
}

func (b *buffer) reset() {
	b.records = make([]connector.Recode, 0, b.bufferSize)
	b.lastFlushed = time.Now()
}

func (b *buffer) Close() {
	// flush existing buffer
	Logger.Info(`kafkaConnect.buffer`, fmt.Sprintf(`flushing buffer... on %s`, b.id))

	if err := b.flushAll(); err != nil {
		Logger.Error(`kafkaConnect.buffer`, err)
	}
}
