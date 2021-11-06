package gevent

import (
	"encoding/json"
	"time"
)

//DispatchedEvent event after dispatched, with emit time and event body
type DispatchedEvent struct {
	EmitAt time.Time `json:"emit_at"`
	Event  Event     `json:"event"`
	Retry  int32     `json:"retry"`
}

//JsonStable convert event to stable ordered json by marshalling event twice to keep the same json output
func (e DispatchedEvent) JsonStable() string {
	byteEvt, _ := json.Marshal(&e)
	commMap := make(map[string]interface{})
	_ = json.Unmarshal(byteEvt, &commMap)
	mapByteEvt, _ := json.Marshal(commMap)
	return string(mapByteEvt)
}

// DispatchedHeap event heap, use heap.Push, heap.Pop to push and pop element
// the event happens nearly current time will be pop first
type DispatchedHeap []*DispatchedEvent

func (h DispatchedHeap) Len() int           { return len(h) }
func (h DispatchedHeap) Less(i, j int) bool { return h[i].EmitAt.Before(h[j].EmitAt) }
func (h DispatchedHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *DispatchedHeap) Push(x interface{}) {
	*h = append(*h, x.(*DispatchedEvent))
}

func (h *DispatchedHeap) Pop() interface{} {
	old := *h
	n := len(old)
	if n == 0 {
		return nil
	}
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
