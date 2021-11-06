package example

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/ltwonders/gevent"
	"github.com/ltwonders/gevent/local"
)

func Test_Example_Local(t *testing.T) {
	ctx := context.Background()
	local.Init(gevent.WithParallelThreshold(20))

	type instantEvent struct {
		ID int
	}

	if err := local.Dispatcher().Register(ctx, instantTopic, annoyFunc); nil != err {
		log.Printf("register handler failed : %s", err)
	}

	// use strut to handle event
	if err := local.Dispatcher().Register(ctx, delayedTopic, gevent.ToHandleFunc(&delayedEventHandler{})); nil != err {
		log.Printf("register handler failed : %s", err)
	}

	// dispatch instant events
	for i := 1; i <= 100; i++ {
		inst := &instantEvent{ID: i}
		if err := local.Dispatch(ctx, instantTopic, inst); nil != err {
			log.Printf("dispatch failed: %s", err)
		}
	}

	// dispatch delayed events
	for i := 1; i <= 100; i++ {
		delayed := time.Duration(i) * time.Second
		evt := &delayedEvent{DelayDuration: delayed, ID: i}
		if err := gevent.Dispatch(ctx, delayedTopic, evt, local.Dispatcher()); nil != err {
			log.Printf("dispatch failed: %s", err)
		}
	}

	time.Sleep(100 * time.Second)
	local.Dispatcher().Stop(ctx)
	if err := local.Dispatcher().Dispatch(ctx, instantTopic, &instantEvent{ID: 1024}); nil != err {
		log.Printf("dispatch failed: %s", err)
	}
	log.Printf("success")
}
