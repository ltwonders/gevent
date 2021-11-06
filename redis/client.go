package redis

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/ltwonders/gevent"
)

// Client Redis clientWrapper to add\rem\range events,
// better to also implements gevent.DistributeLocker to control event handling and event removing
type Client interface {
	//ZAdd add event to a sorted set, the score should be the time that event will emit
	ZAdd(ctx context.Context, key string, score float64, evt interface{}) error
	//ZRem remove event from a sorted set, better to use evt and score to avoid remove the wrong event
	ZRem(ctx context.Context, key string, score float64, evt interface{}) error
	//ZRangeByScore list (stop - start) events while score are lower than scoreTo
	ZRangeByScore(ctx context.Context, key string, scoreTo float64, start, stop int) ([]string, error)
}

//clientWrapper client clientWrapper to generally and better handing events,which will accept events instance and lock
type clientWrapper struct {
	client Client
	sync.RWMutex
}

func (w *clientWrapper) ZAdd(ctx context.Context, tpc gevent.Topic, evt *gevent.DispatchedEvent) error {
	w.Lock()
	defer w.Unlock()

	return w.client.ZAdd(ctx, tpc.String(), float64(evt.EmitAt.UnixNano()), evt.JsonStable())
}

func (w *clientWrapper) ZRem(ctx context.Context, tpc gevent.Topic, evt *gevent.DispatchedEvent) error {
	w.Lock()
	defer w.Unlock()

	h := fmt.Sprintf("%x", md5.Sum([]byte(evt.JsonStable())))
	key := fmt.Sprintf("%s:%s:%s", tpc, "removing", h)
	value := strconv.FormatInt(rand.Int63(), 10)
	if !w.DLock(ctx, key, value, 100*time.Millisecond) {
		return gevent.ErrConcurrentModificationEvent
	}
	defer w.DUnlock(ctx, key, value)

	return w.client.ZRem(ctx, tpc.String(), float64(evt.EmitAt.UnixNano()), evt.JsonStable())
}

//ZRangeByScore list events
func (w *clientWrapper) ZRangeByScore(ctx context.Context, tpc gevent.Topic, start, stop int) ([]*gevent.DispatchedEvent, error) {
	w.Lock()
	defer w.Unlock()
	var events []*gevent.DispatchedEvent
	raws, err := w.client.ZRangeByScore(ctx, tpc.String(), float64(time.Now().UnixNano()), start, stop)
	if nil != err {
		return events, err
	}
	for _, raw := range raws {
		evt := &gevent.DispatchedEvent{}
		if err0 := json.Unmarshal([]byte(raw), evt); nil != err0 {
			log.Printf("unmarshal event [%s] failed : %s", raw, err0)
			continue
		}
		events = append(events, evt)
	}
	return events, nil
}

//DLock use distribute lock, always return true if client does not implement gevent.DistributeLocker
func (w *clientWrapper) DLock(ctx context.Context, key, value string, expire time.Duration) bool {
	//check client have implement locker interface or not
	if locker, ok := w.client.(gevent.DistributeLocker); ok {
		return locker.DLock(ctx, key, value, expire)
	}
	return true
}

//DUnlock remove distribute lock, always return true if client does not implement gevent.DistributeLocker
func (w *clientWrapper) DUnlock(ctx context.Context, key, value string) bool {
	//check client have implement locker interface or not
	if locker, ok := w.client.(gevent.DistributeLocker); ok {
		return locker.DUnlock(ctx, key, value)
	}
	return true
}

//ClientSimple a redis client uses redigo, can be replaced by other redis clientWrapper
type ClientSimple struct {
	Pool *redis.Pool
	sync.Mutex
}

func (s *ClientSimple) getConn() redis.Conn {
	s.Lock()
	defer s.Unlock()
	return s.Pool.Get()
}

//ZAdd use ZADD to add evt to key with score
func (s *ClientSimple) ZAdd(ctx context.Context, key string, score float64, evt interface{}) error {
	v, e := redis.Int64(s.getConn().Do("ZADD", key, score, evt))
	if nil != e {
		return e
	}
	if v == 0 {
		return fmt.Errorf("add event failed key: %s,evt: %s", key, evt)
	}
	return nil
}

//ZRem use ZREM to remove evt with score from key
func (s *ClientSimple) ZRem(ctx context.Context, key string, score float64, evt interface{}) error {
	v, e := redis.Int64(s.getConn().Do("ZREM", key, score, evt))
	if nil != e {
		return e
	}
	if v == 0 {
		return fmt.Errorf("remove failed,key: %s,evt: %s", key, evt)
	}
	return nil
}

//ZRangeByScore use ZRANGEBYSCORE list (stop - start) events from -inf to scoreTo, returns json-marshalled string slice
func (s *ClientSimple) ZRangeByScore(ctx context.Context, key string, scoreTo float64, start int, stop int) ([]string, error) {
	r, e := redis.Strings(s.getConn().Do("ZRANGEBYSCORE", key, "-inf", strconv.FormatFloat(scoreTo, 'g', 0, 64), "LIMIT", start, stop))
	if nil != e {
		return nil, e
	}

	return r, nil
}

//DLock lock with setnx or set px or set ex
func (s *ClientSimple) DLock(ctx context.Context, key, value string, expire time.Duration) bool {
	var locked string
	var err error
	if expire == 0 {
		locked, err = redis.String(s.getConn().Do("setnx", key, value))
	} else {
		if expire < time.Second && expire%time.Second != 0 {
			locked, err = redis.String(s.getConn().Do("set", key, value, "px", int64(expire/time.Millisecond), "nx"))
		} else {
			locked, err = redis.String(s.getConn().Do("set", key, value, "ex", int64(expire/time.Second), "nx"))
		}
	}
	return nil == err && locked == "OK"
}

const cadScript = `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`

var cadCommand = redis.NewScript(1, cadScript)

//DUnlock unlock with lua script
func (s *ClientSimple) DUnlock(ctx context.Context, key, value string) bool {
	res, err := cadCommand.Do(s.getConn(), key, value)
	return nil == err && res.(int64) > 0
}
