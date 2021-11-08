package example

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/gomodule/redigo/redis"
	"github.com/ltwonders/gevent"
)

func Test_Example_Redis(t *testing.T) {
	ctx := context.Background()
	s, err0 := miniredis.Run()
	if err0 != nil {
		panic(err0)
	}
	defer s.Close()

	pool := &redis.Pool{
		MaxIdle: 2,
		Dial:    func() (redis.Conn, error) { return redis.Dial("tcp", s.Addr()) },
	}
	holder := gevent.NewRedis(&gevent.ClientSimple{Pool: pool}, gevent.WithParallelThreshold(10))

	type instantEvent struct {
		ID int `json:"id"`
	}

	if err := holder.Register(ctx, instantTopic, annoyFunc); nil != err {
		log.Printf("register handler failed : %s", err)
	}

	// use strut to handle event
	if err := holder.Register(ctx, delayedTopic, gevent.ToHandleFunc(&delayedEventHandler{})); nil != err {
		log.Printf("register handler failed : %s", err)
	}

	// dispatch instant events
	for i := 1; i <= 100; i++ {
		inst := &instantEvent{ID: i}
		if err := holder.Dispatch(ctx, instantTopic, inst); nil != err {
			log.Printf("dispatch failed: %s", err)
		} else {
			log.Printf("dispatched instant event: %v", inst)
		}
	}

	// dispatch delayed events
	for i := 1; i <= 100; i++ {
		delayed := time.Duration(i) * time.Second
		evt := &delayedEvent{DelayDuration: delayed, ID: i}
		if err := gevent.Dispatch(ctx, delayedTopic, evt, holder); nil != err {
			log.Printf("dispatch failed: %s", err)
		} else {
			log.Printf("dispatched delay event: %v", evt)
		}
	}

	time.Sleep(200 * time.Second)
	holder.Stop(ctx)
	if err := holder.Dispatch(ctx, instantTopic, &instantEvent{ID: 1024}); nil != err {
		log.Printf("dispatch failed: %s", err)
	}
	log.Printf("success")
}
