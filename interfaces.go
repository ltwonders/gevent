package gevent

import (
	"context"
	"time"
)

// Event indicate a event to dispatch, should marked with json marshall tag and be idempotent handling
type Event interface{}

// Topic each event has a topic to dispatch
type Topic string

func (t Topic) String() string {
	return string(t)
}

// DelayedEvent indicate event that needs be handled delay
type DelayedEvent interface {
	Delayed() time.Duration
	Event
}

// Handler interface to handle a event, strut can implement this
// need convert to func use ToHandleFunc() while registering to a localDispatcher
type Handler interface {
	Handle(ctx context.Context, evt Event) error
}

// HandleFunc simply func to handle func
type HandleFunc func(ctx context.Context, evt Event) error

// ToHandleFunc convert interface Handler to func for easy invoke
func ToHandleFunc(handler Handler) HandleFunc {
	return func(ctx context.Context, evt Event) error {
		return handler.Handle(ctx, evt)
	}
}

// Dispatcher allow dispatching event, registering handler and remove handler
type Dispatcher interface {

	// Dispatch dispatch an event to special topic
	Dispatch(ctx context.Context, tpc Topic, evt Event) error

	// Register register a handle func
	Register(ctx context.Context, tpc Topic, handler HandleFunc) error

	// Remove remove a handle func
	Remove(ctx context.Context, tpc Topic, handler HandleFunc) bool

	// Stop stop localDispatcher
	Stop(ctx context.Context)
}

//DistributeLocker a distribute locker will lock while handling or removing event to avoid duplicate operation
//it is better to be implemented if services are distribute deployed
type DistributeLocker interface {

	//DLock a distribute lock to hold event removing and avoid duplicating consume
	DLock(ctx context.Context, key, value string, expiration time.Duration) bool

	//DUnlock remove lock after finishing, should use cas or lua script
	DUnlock(ctx context.Context, key, value string) bool
}

//Dispatch dispatch a event to a topic, support multi dispatchers to receive events
//should keep idempotent for event handling
func Dispatch(ctx context.Context, topic Topic, evt Event, dispatcher Dispatcher, dispatchers ...Dispatcher) error {
	if "" == topic.String() {
		return ErrNoTopicSpecified
	}
	if nil == evt {
		return ErrNoEventSpecified
	}
	if nil == dispatcher {
		return ErrNoDispatcherSpecified
	}

	allDispatchers := append(dispatchers, dispatcher)
	for _, disp := range allDispatchers {
		if err := disp.Dispatch(ctx, topic, evt); nil != err {
			return err
		}
	}
	return nil
}
