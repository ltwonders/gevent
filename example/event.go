package example

import (
	"context"
	"log"
	"time"

	"github.com/ltwonders/gevent"
)

const (
	instantTopic gevent.Topic = "instant"
	delayedTopic gevent.Topic = "delayed"
)

type delayedEvent struct {
	ID            int           `json:"id"`
	DelayDuration time.Duration `json:"delay_duration"`
}

func (pe delayedEvent) Delayed() time.Duration {
	return pe.DelayDuration
}

type delayedEventHandler struct{}

func (p delayedEventHandler) Handle(ctx context.Context, evt gevent.Event) error {
	log.Printf("delayed event [%+v] start at %+v", evt, time.Now().Second())
	time.Sleep(2 * time.Second)
	log.Printf("delayed event [%+v] finish at %+v", evt, time.Now().Second())
	return nil
}

//annoy func to handle event
var annoyFunc = func(ctx context.Context, evt gevent.Event) error {
	log.Printf("instant event [%+v] start at %+v", evt, time.Now().Second())
	time.Sleep(2 * time.Second)
	log.Printf("instant event [%+v] finish at %+v", evt, time.Now().Second())
	return nil
}
