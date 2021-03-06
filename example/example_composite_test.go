package example

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	rclient "github.com/gomodule/redigo/redis"
	"github.com/ltwonders/gevent"
)

func Test_Example_Composite(t *testing.T) {
	ctx := context.Background()
	gevent.LocalInit()
	s, err0 := miniredis.Run()
	if err0 != nil {
		panic(err0)
	}
	defer s.Close()

	pool := &rclient.Pool{
		MaxIdle: 2,
		Dial:    func() (rclient.Conn, error) { return rclient.Dial("tcp", s.Addr()) },
	}
	redisDispatcher := gevent.NewRedisWithContext(ctx, &gevent.ClientSimple{Pool: pool})

	if err1 := gevent.Local().Register(ctx, delayedTopic, annoyFunc); nil != err1 {
		log.Printf("fail to register local handler")
	}

	// dispatch events
	for i := 1; i <= 100; i++ {
		inst := &delayedEvent{ID: i, DelayDuration: time.Duration(i) * time.Second}
		if err := gevent.Dispatch(ctx, delayedTopic, inst, gevent.Local(), redisDispatcher); nil != err {
			log.Printf("dispatch failed: %s", err)
		}
	}
	if err2 := redisDispatcher.Register(ctx, delayedTopic, annoyFunc); nil != err2 {
		log.Printf("fail to register redis handler")
	}

	time.Sleep(200 * time.Second)
}
