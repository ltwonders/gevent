package gevent

import "time"

//OptionMaxQueued indicates how many events should be queued, especially valid for local dispatcher
type OptionMaxQueued struct {
	MaxQueued int64
}

//OptionTickerInterval indicates interval for dispatcher ticks
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
