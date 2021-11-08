package gevent

import (
	"log"
	"time"
)

// Option option for different localDispatcher config
type Option interface{}

// Options parsed config for dispatchers
type Options struct {
	MaxQueuedEvents        int64
	TickerInterval         time.Duration
	ParallelThreshold      int
	MaxRetry               int32
	MaxTaskExecuteDuration time.Duration
}

//OptionMaxQueued indicates how many events should be queued, especially valid for local localDispatcher
type OptionMaxQueued struct {
	MaxQueued int64
}

//OptionTickerInterval indicates interval for localDispatcher ticks
type OptionTickerInterval struct {
	Interval time.Duration
}

//OptionParallelRoutines indicates routines will launch for handling each topic
type OptionParallelRoutines struct {
	Threshold int
}

//OptionMaxRetry indicates max retry count if handler failed
type OptionMaxRetry struct {
	Retry int32
}

//OptionTaskMaxDuration indicates max duration which the task will execute
type OptionTaskMaxDuration struct {
	MaxDuration time.Duration
}

func WithMaxQueued(m int64) Option {
	return OptionMaxQueued{MaxQueued: m}
}

func WithTickerInterval(d time.Duration) Option {
	return OptionTickerInterval{Interval: d}
}

//WithParallelThreshold  set go-routines threshold for parallel running in a topic, suggest between [10,40]
func WithParallelThreshold(threshold int) Option {
	return OptionParallelRoutines{Threshold: threshold}
}

func WithMaxRetry(retry int32) Option {
	return OptionMaxRetry{Retry: retry}
}

func WithTaskMaxDuration(d time.Duration) Option {
	return OptionTaskMaxDuration{MaxDuration: d}
}

func parse(opts Options, options ...Option) Options {
	for _, option := range options {
		switch op := option.(type) {
		case OptionMaxQueued:
			opts.MaxQueuedEvents = op.MaxQueued
		case OptionTickerInterval:
			opts.TickerInterval = op.Interval
		case OptionParallelRoutines:
			opts.ParallelThreshold = op.Threshold
		case OptionMaxRetry:
			opts.MaxRetry = op.Retry
		case OptionTaskMaxDuration:
			opts.MaxTaskExecuteDuration = op.MaxDuration
		default:
			log.Printf("not support option type [%s] for local localDispatcher\n", op)
		}
	}
	return opts
}
