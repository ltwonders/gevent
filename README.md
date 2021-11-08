[gevent](https://github.com/ltwonders/gevent) imply go-event which tries to make event handling easier.

### What does gevent want to do

1. Async execute jobs safely without too many go routines.
2. Support delayed events to execute.
3. Support to config executing options

### Main usage scenarios:

1. Separate side business from main, like system log which should be async.
2. Use delayed event to confirm order status or pay status.
3. Decouple domain events to avoid cycling call.
4. Notify downstream while domain event happening.

### Attention

1. Instant event may delay a few milliseconds while dispatching.
2. Local events are not durable, use redis durable model for distribute systems.

### TODO

1. Support use rmq etc.
2. Benchmark

### Any question or suggestion, please let me know.

### How to use：

```bash
    go get github.com/ltwonders/gevent
```

Example:

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/gomodule/redigo/redis"
	"github.com/ltwonders/gevent"
)

func main() {
	ctx := context.Background()
	localDispatcher := gevent.LocalInit()
	s, err0 := miniredis.Run()
	if err0 != nil {
		panic(err0)
	}
	defer s.Close()

	pool := &redis.Pool{
		MaxIdle: 2,
		Dial:    func() (redis.Conn, error) { return redis.Dial("tcp", s.Addr()) },
	}
	redisDispatcher := gevent.NewRedisWithContext(ctx, &gevent.ClientSimple{Pool: pool})

	//annoy func to handle event
	var annoyFunc = func(ctx context.Context, evt gevent.Event) error {
		log.Printf("instant event [%+v] start at %+v", evt, time.Now().Second())
		time.Sleep(2 * time.Second)
		log.Printf("instant event [%+v] finish at %+v", evt, time.Now().Second())
		return nil
	}
	if err1 := localDispatcher.Register(ctx, "instant", annoyFunc); nil != err1 {
		log.Printf("fail to register local handler")
	}

	type instantEvent struct {
		ID int
	}

	// dispatch events
	for i := 1; i <= 100; i++ {
		inst := &instantEvent{ID: i}
		if err := gevent.Dispatch(ctx, "instant", inst, localDispatcher, redisDispatcher); nil != err {
			log.Printf("dispatch failed: %s", err)
		}
	}
	if err2 := redisDispatcher.Register(ctx, "instant", annoyFunc); nil != err2 {
		log.Printf("fail to register redis handler")
	}

	time.Sleep(200 * time.Second)
}

```

more examples, see example package
