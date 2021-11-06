package local

import (
	"log"
	"time"

	"github.com/ltwonders/gevent"
)

const (
	defaultMaxQueued         = int64(2000)
	defaultTickerInterval    = 1 * time.Second // polling check events of topic
	defaultParallelThreshold = 20              // go-routine threshold for each topic
	defaultMaxRetry          = int32(3)        // max retry count
)

var (
	maxQueuedEvents   = defaultMaxQueued
	tickerInterval    = defaultTickerInterval
	parallelThreshold = defaultParallelThreshold
	maxRetry          = defaultMaxRetry
)

func parse(options ...gevent.Option) {
	for _, option := range options {
		switch op := option.(type) {
		case gevent.OptionMaxQueued:
			maxQueuedEvents = op.MaxQueued
		case gevent.OptionTickerInterval:
			tickerInterval = op.Interval
		case gevent.OptionParallelRoutines:
			parallelThreshold = op.Threshold
		case gevent.OptionMaxRetry:
			maxRetry = op.Retry
		default:
			log.Printf("not support option type [%s] for local dispatcher\n", op)
		}
	}
}
