package local

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ltwonders/gevent"
	"go.uber.org/atomic"
)

const (
	stateRunning  = 1
	stateStopped  = 2
	stateHandling = 3
)

//dispatcher local dispatcher, support multi topic and each topic with multi handlers
type dispatcher struct {
	events  map[gevent.Topic]*localEvents
	total   *atomic.Int64
	state   *atomic.Int32
	changed chan *localEvents
}

var (
	localInst *dispatcher
	localOnce sync.Once
)

// Dispatch dispatch a event to specific topic, there may be time duration between executing and dispatching
func (d *dispatcher) Dispatch(ctx context.Context, topic gevent.Topic, evt gevent.Event) error {
	if d.isStopped() {
		return gevent.ErrDispatcherNotWorking
	}

	//check max queued size
	if d.total.Load() >= maxQueuedEvents {
		return gevent.ErrMaxQueuedEventsReached
	}

	//local dispatcher should register handlers first
	events := d.events[topic]
	if nil == events || len(events.handlers) == 0 {
		return gevent.ErrNoHandlerFound
	}

	//add emit time
	emitAt := time.Now()
	if delayedEvt, ok := evt.(gevent.DelayedEvent); ok {
		emitAt = emitAt.Add(delayedEvt.Delayed())
	}

	events.PushEvent(ctx, &gevent.DispatchedEvent{Event: evt, EmitAt: emitAt})

	d.total.Inc()

	d.changed <- events

	return nil
}

//Register add a handler func to specific topic
func (d *dispatcher) Register(ctx context.Context, topic gevent.Topic, handler gevent.HandleFunc) error {
	if d.isStopped() {
		return gevent.ErrDispatcherNotWorking
	}
	if nil == d.events[topic] {
		d.events[topic] = newEvents(parallelThreshold)
	}
	events := d.events[topic]
	return events.AddHandleFunc(ctx, handler)
}

func (d *dispatcher) Remove(ctx context.Context, topic gevent.Topic, handler gevent.HandleFunc) bool {
	if d.isStopped() {
		return false
	}
	events := d.events[topic]
	if nil == events {
		return false
	}
	return events.RemoveHandleFunc(ctx, handler)
}

func (d *dispatcher) Stop(ctx context.Context) {
	d.state.Swap(stateStopped)
	log.Println("local dispatcher stopped")
}

//emit Start consuming events of a topic, go-routines count will be restricted with parallelThreshold
// Ends until no event can emit by current time
func (d *dispatcher) emit(ctx context.Context, events *localEvents, wg *sync.WaitGroup) {
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
		go func(de *gevent.DispatchedEvent) {
			events.parallel <- true
			defer func() {
				<-events.parallel
			}()
			handled := d.handle(ctx, de, events.handlers)
			// return the heap if max retry not reached
			if !handled {
				if de.Retry < maxRetry {
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

func (d *dispatcher) handle(ctx context.Context, e *gevent.DispatchedEvent, handlers []gevent.HandleFunc) bool {
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

func (d *dispatcher) isStopped() bool {
	return d.state.Load() == stateStopped
}

//start initialize a forever-run go-routine ticker to check topics with events,
//ticker depends on the option of tickerInterval
func (d *dispatcher) start(ctx context.Context) {
	ticker := time.NewTicker(tickerInterval)
	d.state.Swap(stateRunning)
	go func() {
		for {
			select {
			case <-ticker.C:
				if d.state.Load() == stateStopped {
					return
				}
				if d.state.Load() == stateHandling {
					//if ticker is still handling, break select
					break
				}
				d.state.Store(stateHandling)
				//polling the events with interval
				wg := &sync.WaitGroup{}
				wg.Add(len(d.events))
				for _, events := range d.events {
					go d.emit(ctx, events, wg)
				}
				wg.Wait()
				d.state.Store(stateRunning)
			case events := <-d.changed:
				//new event dispatch
				d.emit(ctx, events, nil)
			case <-ctx.Done():
				log.Println("local dispatcher context done")
				return
			}
		}
	}()
}

//Init create local dispatcher as need, init will default options if no options pass
func Init(options ...gevent.Option) *dispatcher {
	localOnce.Do(func() {
		doInit(options...)
	})
	return localInst
}

func doInit(options ...gevent.Option) {
	if nil != localInst {
		panic(gevent.ErrDuplicateInitialized)
	}
	parse(options...)

	localInst = &dispatcher{
		events:  map[gevent.Topic]*localEvents{},
		total:   atomic.NewInt64(0),
		state:   atomic.NewInt32(stateStopped),
		changed: make(chan *localEvents),
	}
	localInst.start(context.Background())
}

func Dispatcher() *dispatcher {
	if nil == localInst {
		panic("local dispatcher not initialized")
	}
	return localInst
}

func Dispatch(ctx context.Context, tpc gevent.Topic, evt gevent.Event) error {
	return Dispatcher().Dispatch(ctx, tpc, evt)
}
