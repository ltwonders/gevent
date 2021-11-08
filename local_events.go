package gevent

import (
	"container/heap"
	"context"
	"reflect"
	"sync"
)

//localEvents each topic owns one
type localEvents struct {
	handlers []HandleFunc
	heap     *DispatchedHeap
	parallel chan bool
	sync.Mutex
}

func newEvents(parallel int) *localEvents {
	return &localEvents{
		parallel: make(chan bool, parallel),
	}
}

func (e *localEvents) AddHandleFunc(ctx context.Context, handler HandleFunc) error {
	e.Lock()
	defer e.Unlock()
	if nil == handler {
		return ErrNoHandlerFound
	}
	e.handlers = append(e.handlers, handler)
	return nil
}

func (e *localEvents) RemoveHandleFunc(ctx context.Context, handler HandleFunc) bool {
	e.Lock()
	defer e.Unlock()
	for i, h := range e.handlers {
		if reflect.ValueOf(h).Pointer() == reflect.ValueOf(handler).Pointer() {
			e.handlers = append(e.handlers[:i], e.handlers[i+1:]...)
			return true
		}
	}
	return false
}

func (e *localEvents) PushEvent(ctx context.Context, evt *DispatchedEvent) {
	if nil == evt {
		return
	}
	e.Lock()
	defer e.Unlock()
	if nil == e.heap {
		e.heap = &DispatchedHeap{}
	}
	heap.Push(e.heap, evt)
}

func (e *localEvents) PopEvent(ctx context.Context) *DispatchedEvent {
	e.Lock()
	defer e.Unlock()
	if nil == e.heap || e.heap.Len() == 0 {
		return nil
	}
	if ee, ok := heap.Pop(e.heap).(*DispatchedEvent); ok {
		return ee
	}
	return nil
}
