package redis

import (
	"log"
	"time"

	"github.com/ltwonders/gevent"
)

const (
	defaultParallelRoutines = 40
	defaultTickerInterval   = 1 * time.Second
	defaultMaxRetry         = int32(3)
	defaultMaxTaskExecution = 50 * time.Second
)

var (
	//max handling events count for each topic while ticker run
	maxHandlingEvents      = defaultParallelRoutines
	tickerInterval         = defaultTickerInterval
	maxRetry               = defaultMaxRetry
	maxTaskExecuteDuration = defaultMaxTaskExecution
)

func parse(options ...gevent.Option) {
	for _, option := range options {
		switch op := option.(type) {
		case gevent.OptionTickerInterval:
			tickerInterval = op.Interval
		case gevent.OptionParallelRoutines:
			maxHandlingEvents = op.Threshold
		case gevent.OptionMaxRetry:
			maxRetry = op.Retry
		case gevent.OptionTaskMaxDuration:
			maxTaskExecuteDuration = op.MaxDuration
		default:
			log.Printf("option type : %t", op)
		}
	}
}
