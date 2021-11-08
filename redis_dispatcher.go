package gevent

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

	"go.uber.org/atomic"
)

const (
	stateRedisRunning  = 1
	stateRedisStopped  = 2
	stateRedisHandling = 3
)

//redisDispatcher use redis to persist handlers, handlers are registered to the redisDispatcher which will ticker check latest handlers
type redisDispatcher struct {
	clientWrapper *clientWrapper
	handlers      map[Topic][]HandleFunc
	state         *atomic.Int32
	options       Options
	sync.RWMutex
}

//Dispatch push an event to redis
func (d *redisDispatcher) Dispatch(ctx context.Context, topic Topic, evt Event) error {
	if d.isStopped() {
		return ErrDispatcherNotWorking
	}
	//examine emit time
	emitAt := time.Now()
	if delayedEvt, ok := evt.(DelayedEvent); ok {
		emitAt = emitAt.Add(delayedEvt.Delayed())
	}
	emitEvt := &DispatchedEvent{Event: evt, EmitAt: emitAt}

	//dispatch events should lock while handler change, concurrency dispatch is allowed
	d.RLock()
	defer d.RUnlock()
	return d.clientWrapper.ZAdd(ctx, topic, emitEvt)
}

//Register add a handler to consume specific topic
func (d *redisDispatcher) Register(ctx context.Context, topic Topic, handler HandleFunc) error {
	if d.isStopped() {
		return ErrDispatcherNotWorking
	}

	d.Lock()
	defer d.Unlock()

	if _, ok := d.handlers[topic]; !ok {
		d.handlers[topic] = []HandleFunc{}
	}
	for tpc, handlers := range d.handlers {
		if tpc == topic {
			d.handlers[tpc] = append(handlers, handler)
		}
	}
	return nil
}

// Remove remove a handler of specific topic from redisDispatcher
func (d *redisDispatcher) Remove(ctx context.Context, topic Topic, handler HandleFunc) bool {
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
func (d *redisDispatcher) emit(ctx context.Context, tpc Topic, handlers []HandleFunc, wg *sync.WaitGroup) {
	defer wg.Done()

	if d.isStopped() || len(handlers) == 0 {
		return
	}

	events, err := d.clientWrapper.ZRangeByScore(ctx, tpc, 0, d.options.ParallelThreshold)
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
func (d *redisDispatcher) handle(ctx context.Context, tpc Topic, evt *DispatchedEvent, wg *sync.WaitGroup) bool {
	defer wg.Done()
	handlers := d.handlers[tpc]
	//if the low score does not need handle, skip all the events until next ticker
	if d.isStopped() || nil == evt || evt.EmitAt.After(time.Now()) || len(handlers) == 0 {
		return false
	}

	//lock if there is a handler is handling
	lockKey := fmt.Sprintf("%s:%s:%s", tpc, "handling", fmt.Sprintf("%x", md5.Sum([]byte(evt.JsonStable()))))
	lockValue := strconv.FormatInt(rand.Int63(), 10)
	if !d.clientWrapper.DLock(ctx, lockKey, lockValue, d.options.MaxTaskExecuteDuration) {
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
		if evt.Retry < d.options.MaxRetry {
			evt.Retry = evt.Retry + 1
			if err3 := d.clientWrapper.ZAdd(ctx, tpc, evt); nil != err3 {
				panic(fmt.Sprintf("event [%v] handle failed after max retry: %s", evt, err3))
			}
		}
	}

	return handled
}

//start ticker running go-routine to check events, check if
func (d *redisDispatcher) start(ctx context.Context) {
	ticker := time.NewTicker(d.options.TickerInterval)
	d.state = atomic.NewInt32(stateRedisRunning)
	go func() {
		for {
			select {
			case <-ticker.C:
				if d.isStopped() {
					//if redisDispatcher has been stopped, shut down go-routine
					return
				}
				if d.isHandling() || len(d.handlers) == 0 {
					//if ticker is still handling or no handlers, break select
					break
				}
				d.state.Store(stateRedisHandling)
				wg := &sync.WaitGroup{}
				wg.Add(len(d.handlers))
				for tpc, handlers := range d.handlers {
					go d.emit(ctx, tpc, handlers, wg)
				}
				wg.Wait()
				d.state.Store(stateRedisRunning)
			case <-ctx.Done():
				log.Printf("context done")
				return
			}
		}
	}()
}

// Stop terminate the redis redisDispatcher
func (d *redisDispatcher) Stop(ctx context.Context) {
	d.state.Store(stateRedisStopped)
}

func (d *redisDispatcher) isStopped() bool {
	return d.state.Load() == stateRedisStopped
}

func (d *redisDispatcher) isHandling() bool {
	return d.state.Load() == stateRedisHandling
}

//NewRedis create a new redis redisDispatcher
func NewRedis(client RedisClient, opts ...Option) Dispatcher {
	return NewRedisWithContext(context.Background(), client, opts...)
}

//NewRedisWithContext create a redis redisDispatcher that relates to ctx, if ctx is cancelled, redisDispatcher will stop
func NewRedisWithContext(ctx context.Context, client RedisClient, opts ...Option) Dispatcher {
	if nil == client {
		panic("no client specified")
	}
	options := parse(defaultRedisOptions(), opts...)
	inst := &redisDispatcher{
		handlers:      map[Topic][]HandleFunc{},
		clientWrapper: &clientWrapper{client: client},
		options:       options,
	}
	inst.start(ctx)
	return inst
}

func defaultRedisOptions() Options {
	return Options{
		MaxQueuedEvents:        int64(2000),
		TickerInterval:         1 * time.Second, // polling check events of topic
		ParallelThreshold:      40,              // go-routine threshold for each topic
		MaxRetry:               int32(3),        // max retry count
		MaxTaskExecuteDuration: 50 * time.Second,
	}
}
