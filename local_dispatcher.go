package gevent

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.uber.org/atomic"
)

const (
	stateLocalRunning  = 1
	stateLocalStopped  = 2
	stateLocalHandling = 3
)

//localDispatcher local localDispatcher, support multi topic and each topic with multi handlers
type localDispatcher struct {
	events  map[Topic]*localEvents
	total   *atomic.Int64
	state   *atomic.Int32
	changed chan *localEvents
	options Options
}

var (
	localInst *localDispatcher
	localOnce sync.Once
)

// Dispatch dispatch a event to specific topic, there may be time duration between executing and dispatching
func (d *localDispatcher) Dispatch(ctx context.Context, topic Topic, evt Event) error {
	if d.isStopped() {
		return ErrDispatcherNotWorking
	}

	//check max queued size
	if d.total.Load() >= d.options.MaxQueuedEvents {
		return ErrMaxQueuedEventsReached
	}

	//local localDispatcher should register handlers first
	events := d.events[topic]
	if nil == events || len(events.handlers) == 0 {
		return ErrNoHandlerFound
	}

	//add emit time
	emitAt := time.Now()
	if delayedEvt, ok := evt.(DelayedEvent); ok {
		emitAt = emitAt.Add(delayedEvt.Delayed())
	}

	events.PushEvent(ctx, &DispatchedEvent{Event: evt, EmitAt: emitAt})

	d.total.Inc()

	d.changed <- events

	return nil
}

//Register add a handler func to specific topic
func (d *localDispatcher) Register(ctx context.Context, topic Topic, handler HandleFunc) error {
	if d.isStopped() {
		return ErrDispatcherNotWorking
	}
	if nil == d.events[topic] {
		d.events[topic] = newEvents(d.options.ParallelThreshold)
	}
	events := d.events[topic]
	return events.AddHandleFunc(ctx, handler)
}

func (d *localDispatcher) Remove(ctx context.Context, topic Topic, handler HandleFunc) bool {
	if d.isStopped() {
		return false
	}
	events := d.events[topic]
	if nil == events {
		return false
	}
	return events.RemoveHandleFunc(ctx, handler)
}

func (d *localDispatcher) Stop(ctx context.Context) {
	d.state.Swap(stateLocalStopped)
	log.Println("local localDispatcher stopped")
}

//emit Start consuming events of a topic, go-routines count will be restricted with parallelThreshold
// Ends until no event can emit by current time
func (d *localDispatcher) emit(ctx context.Context, events *localEvents, wg *sync.WaitGroup) {
	if nil != wg {
		defer wg.Done()
	}
	if d.isStopped() || nil == events {
		return
	}

	n := time.Now()
	e := events.PopEvent(ctx)
	for {
		//quit if no event or the first event should emit after now
		if nil == e || e.EmitAt.After(n) {
			events.PushEvent(ctx, e)
			return
		}
		//start consume with go-routines
		go func(de *DispatchedEvent) {
			events.parallel <- true
			defer func() {
				<-events.parallel
			}()
			handled := d.handle(ctx, de, events.handlers)
			// return the heap if max retry not reached
			if !handled {
				if de.Retry < d.options.MaxRetry {
					de.Retry = atomic.NewInt32(de.Retry).Inc()
					events.PushEvent(ctx, de)
					return
				}
				//will panic current go-routine
				panic(fmt.Sprintf("event [%v] handle failed after max retry", de))
			}
			d.total.Dec()
		}(e)
		e = events.PopEvent(ctx)
	}
}

func (d *localDispatcher) handle(ctx context.Context, e *DispatchedEvent, handlers []HandleFunc) bool {
	if d.isStopped() || nil == e || len(handlers) == 0 {
		return false
	}

	handled := false
	for _, handle := range handlers {
		handled = true
		if err := handle(ctx, e.Event); nil != err {
			handled = false
			break
		}
	}
	return handled
}

func (d *localDispatcher) isStopped() bool {
	return d.state.Load() == stateLocalStopped
}

//start initialize a forever-run go-routine ticker to check topics with events,
//ticker depends on the option of tickerInterval
func (d *localDispatcher) start(ctx context.Context) {
	ticker := time.NewTicker(d.options.TickerInterval)
	d.state.Swap(stateLocalRunning)
	go func() {
		for {
			select {
			case <-ticker.C:
				if d.state.Load() == stateLocalStopped {
					return
				}
				if d.state.Load() == stateLocalHandling {
					//if ticker is still handling, break select
					break
				}
				d.state.Store(stateLocalHandling)
				//polling the events with interval
				wg := &sync.WaitGroup{}
				wg.Add(len(d.events))
				for _, events := range d.events {
					go d.emit(ctx, events, wg)
				}
				wg.Wait()
				d.state.Store(stateLocalRunning)
			case events := <-d.changed:
				//new event dispatch
				d.emit(ctx, events, nil)
			case <-ctx.Done():
				log.Println("local localDispatcher context done")
				return
			}
		}
	}()
}

//LocalInit create local localDispatcher as need, init will default options if no options pass
func LocalInit(options ...Option) {
	localOnce.Do(func() {
		localDoInit(options...)
	})
}

func localDoInit(options ...Option) {
	if nil != localInst {
		panic(ErrDuplicateInitialized)
	}
	opts := parse(defaultLocalOptions(), options...)

	localInst = &localDispatcher{
		events:  map[Topic]*localEvents{},
		total:   atomic.NewInt64(0),
		state:   atomic.NewInt32(stateLocalStopped),
		changed: make(chan *localEvents),
		options: opts,
	}
	localInst.start(context.Background())
}

func defaultLocalOptions() Options {
	return Options{
		MaxQueuedEvents:   int64(2000),
		TickerInterval:    1 * time.Second, // polling check events of topic
		ParallelThreshold: 200,             // go-routine threshold for each topic
		MaxRetry:          int32(3),        // max retry count
	}
}

func Local() *localDispatcher {
	if nil == localInst {
		panic("local localDispatcher not initialized")
	}
	return localInst
}

func DispatchLocal(ctx context.Context, tpc Topic, evt Event) error {
	return Local().Dispatch(ctx, tpc, evt)
}
