package gevent

import "errors"

var (
	ErrDispatcherNotWorking        = errors.New("dispatcher is not working")
	ErrNoDispatcherSpecified       = errors.New("no dispatcher specified")
	ErrNoTopicSpecified            = errors.New("no topic specified")
	ErrNoEventSpecified            = errors.New("no Event specified")
	ErrNoHandlerFound              = errors.New("no handler found")
	ErrDuplicateInitialized        = errors.New("duplicate initialized dispatcher")
	ErrMaxQueuedEventsReached      = errors.New("max queued heap reached")
	ErrConcurrentModificationEvent = errors.New("concurrent modification event")
)
