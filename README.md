### Features

1. gevent imply go-event,a simple async event handler package
2. async executing and avoid too much go-routines
3. support delayed execute simply

### Main usage scenarios:

1. Side business separating from main workflow, like system log etc.
2. Delayed event to check result except polling, like checking pay result status
3. Decouple layer event without cycling call
4. Notify downstream while domain event happening,like after pay finished etc.

### Attention

1. Instant event may be delay although, pass definite time if need accurately handle time
2. Local dispatcher does not persist events, use redis or rmq to persist

### TODO

1. support use rmq etc.
2. benchmark

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
	rclient "github.com/gomodule/redigo/redis"
	"github.com/ltwonders/gevent"
	"github.com/ltwonders/gevent/local"
	"github.com/ltwonders/gevent/redis"
)

func main() {
	ctx := context.Background()
	localDispatcher := local.Init()
	s, err0 := miniredis.Run()
	if err0 != nil {
		panic(err0)
	}
	defer s.Close()

	pool := &rclient.Pool{
		MaxIdle: 2,
		Dial:    func() (rclient.Conn, error) { return rclient.Dial("tcp", s.Addr()) },
	}
	redisDispatcher := redis.NewWithContext(ctx, &redis.ClientSimple{Pool: pool})

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

### Any question or suggestion, please let me know.
