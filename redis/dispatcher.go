package redis

import (
	"context"
	"crypto/md5"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/ltwonders/gevent"
	"go.uber.org/atomic"
)

const (
	stateRunning  = 1 //dispatcher is running
	stateStopped  = 2
	stateHandling = 3 //ticker running for consuming
)

//dispatcher use redis to persist handlers, handlers are registered to the dispatcher which will ticker check latest handlers
type dispatcher struct {
	clientWrapper *clientWrapper
	handlers      map[gevent.Topic][]gevent.HandleFunc
	state         *atomic.Int32
	sync.RWMutex
}

//Dispatch push an event to redis
func (d *dispatcher) Dispatch(ctx context.Context, topic gevent.Topic, evt gevent.Event) error {
	if d.isStopped() {
		return gevent.ErrDispatcherNotWorking
	}
	//examine emit time
	emitAt := time.Now()
	if delayedEvt, ok := evt.(gevent.DelayedEvent); ok {
		emitAt = emitAt.Add(delayedEvt.Delayed())
	}
	emitEvt := &gevent.DispatchedEvent{Event: evt, EmitAt: emitAt}

	//dispatch events should lock while handler change, concurrency dispatch is allowed
	d.RLock()
	defer d.RUnlock()
	return d.clientWrapper.ZAdd(ctx, topic, emitEvt)
}

//Register add a handler to consume specific topic
func (d *dispatcher) Register(ctx context.Context, topic gevent.Topic, handler gevent.HandleFunc) error {
	if d.isStopped() {
		return gevent.ErrDispatcherNotWorking
	}

	d.Lock()
	defer d.Unlock()

	if _, ok := d.handlers[topic]; !ok {
		d.handlers[topic] = []gevent.HandleFunc{}
	}
	for tpc, handlers := range d.handlers {
		if tpc == topic {
			d.handlers[tpc] = append(handlers, handler)
		}
	}
	return nil
}

// Remove remove a handler of specific topic from dispatcher
func (d *dispatcher) Remove(ctx context.Context, topic gevent.Topic, handler gevent.HandleFunc) bool {
	if d.isStopped() {
		return false
	}
	d.Lock()
	defer d.Unlock()
	for tpc, handlers := range d.handlers {
		if tpc == topic {
			for i, h := range handlers {
				if reflect.ValueOf(h).Pointer() == reflect.ValueOf(handler).Pointer() {
					d.handlers[tpc] = append(handlers[:i], handlers[i+1:]...)
					return true
				}
			}
		}
	}
	return false
}

//emit start handling events of topic:
// once ticker time will list [0,maxHandlingEvents] of topic, for each events will be handled by a go-routine
func (d *dispatcher) emit(ctx context.Context, tpc gevent.Topic, handlers []gevent.HandleFunc, wg *sync.WaitGroup) {
	defer wg.Done()

	if d.isStopped() || len(handlers) == 0 {
		return
	}

	events, err := d.clientWrapper.ZRangeByScore(ctx, tpc, 0, maxHandlingEvents)
	if nil != err {
		panic(fmt.Sprintf("get events from topic [%s] failed: %s", tpc, err))
	}

	if len(events) == 0 {
		return
	}

	wgTopic := &sync.WaitGroup{}
	wgTopic.Add(len(events))
	for _, evt := range events {
		go d.handle(ctx, tpc, evt, wgTopic)
	}
	wgTopic.Wait()

}

//handle events for a topic, will panic if max retry have reached
func (d *dispatcher) handle(ctx context.Context, tpc gevent.Topic, evt *gevent.DispatchedEvent, wg *sync.WaitGroup) bool {
	defer wg.Done()
	handlers := d.handlers[tpc]
	//if the low score does not need handle, skip all the events until next ticker
	if d.isStopped() || nil == evt || evt.EmitAt.After(time.Now()) || len(handlers) == 0 {
		return false
	}

	//lock if there is a handler is handling
	lockKey := fmt.Sprintf("%s:%s:%s", tpc, "handling", fmt.Sprintf("%x", md5.Sum([]byte(evt.JsonStable()))))
	lockValue := strconv.FormatInt(rand.Int63(), 10)
	if !d.clientWrapper.DLock(ctx, lockKey, lockValue, maxTaskExecuteDuration) {
		return false
	}
	defer d.clientWrapper.DUnlock(ctx, lockKey, lockValue)

	handled := false
	for _, handle := range handlers {
		if err1 := handle(ctx, evt.Event); nil != err1 {
			log.Printf("handle event [%+v] failed: %s", evt, err1)
			handled = false
			break
		}
		handled = true
	}

	//remove the event first
	if err2 := d.clientWrapper.ZRem(ctx, tpc, evt); nil != err2 {
		panic(fmt.Sprintf("remove event [%+v] failed: %s", evt, err2))
	}
	if !handled {
		//put event back to retry if lower than max retry, !!!may be have concurrency problem
		if evt.Retry < maxRetry {
			evt.Retry = evt.Retry + 1
			if err3 := d.clientWrapper.ZAdd(ctx, tpc, evt); nil != err3 {
				panic(fmt.Sprintf("event [%v] handle failed after max retry: %s", evt, err3))
			}
		}
	}

	return handled
}

//start ticker running go-routine to check events, check if
func (d *dispatcher) start(ctx context.Context) {
	ticker := time.NewTicker(tickerInterval)
	d.state = atomic.NewInt32(stateRunning)
	go func() {
		for {
			select {
			case <-ticker.C:
				if d.isStopped() {
					//if dispatcher has been stopped, shut down go-routine
					return
				}
				if d.isHandling() || len(d.handlers) == 0 {
					//if ticker is still handling or no handlers, break select
					break
				}
				d.state.Store(stateHandling)
				wg := &sync.WaitGroup{}
				wg.Add(len(d.handlers))
				for tpc, handlers := range d.handlers {
					go d.emit(ctx, tpc, handlers, wg)
				}
				wg.Wait()
				d.state.Store(stateRunning)
			case <-ctx.Done():
				log.Printf("context done")
				return
			}
		}
	}()
}

// Stop terminate the redis dispatcher
func (d *dispatcher) Stop(ctx context.Context) {
	d.state.Store(stateStopped)
}

func (d *dispatcher) isStopped() bool {
	return d.state.Load() == stateStopped
}

func (d *dispatcher) isHandling() bool {
	return d.state.Load() == stateHandling
}

//New create a new redis dispatcher
func New(client Client, opts ...gevent.Option) gevent.Dispatcher {
	return NewWithContext(context.Background(), client, opts...)
}

//NewWithContext create a redis dispatcher that relates to ctx, if ctx is cancelled, dispatcher will stop
func NewWithContext(ctx context.Context, client Client, opts ...gevent.Option) gevent.Dispatcher {
	if nil == client {
		panic("no client specified")
	}
	parse(opts...)
	inst := &dispatcher{
		handlers:      map[gevent.Topic][]gevent.HandleFunc{},
		clientWrapper: &clientWrapper{client: client},
	}
	inst.start(ctx)
	return inst
}
