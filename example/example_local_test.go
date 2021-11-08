package example

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/ltwonders/gevent"
)

func Test_Example_Local(t *testing.T) {
	ctx := context.Background()
	gevent.LocalInit(gevent.WithParallelThreshold(20))

	type instantEvent struct {
		ID int
	}

	if err := gevent.Local().Register(ctx, instantTopic, annoyFunc); nil != err {
		log.Printf("register handler failed : %s", err)
	}

	// use strut to handle event
	if err := gevent.Local().Register(ctx, delayedTopic, gevent.ToHandleFunc(&delayedEventHandler{})); nil != err {
		log.Printf("register handler failed : %s", err)
	}

	// dispatch instant events
	for i := 1; i <= 100; i++ {
		inst := &instantEvent{ID: i}
		if err := gevent.DispatchLocal(ctx, instantTopic, inst); nil != err {
			log.Printf("dispatch failed: %s", err)
		}
	}

	// dispatch delayed events
	for i := 1; i <= 100; i++ {
		delayed := time.Duration(i) * time.Second
		evt := &delayedEvent{DelayDuration: delayed, ID: i}
		if err := gevent.Dispatch(ctx, delayedTopic, evt, gevent.Local()); nil != err {
			log.Printf("dispatch failed: %s", err)
		}
	}

	time.Sleep(100 * time.Second)
	gevent.Local().Stop(ctx)
	if err := gevent.Local().Dispatch(ctx, instantTopic, &instantEvent{ID: 1024}); nil != err {
		log.Printf("dispatch failed: %s", err)
	}
	log.Printf("success")
}
