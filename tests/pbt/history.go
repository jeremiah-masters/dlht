package pbt

import "sync/atomic"

// OpResult is the observed outcome of one map operation.
type OpResult[V any] struct {
	Found    bool
	Value    V
	Updated  bool
	Computed bool // LoadOrCompute only: fn ran (exactly once)
}

// TimedOp is one operation with its observed result and real-time interval.
// StartSeq/EndSeq come from a shared SeqCounter, so EndSeq(a) < StartSeq(b)
// means a completed before b was invoked.
type TimedOp[K comparable, V any] struct {
	Op       Op[K, V]
	Result   OpResult[V]
	StartSeq uint64
	EndSeq   uint64
	ThreadId int
}

// SeqCounter hands out globally ordered timestamps for TimedOp intervals.
type SeqCounter struct {
	counter atomic.Uint64
}

func (s *SeqCounter) Start() uint64 {
	return s.counter.Add(1)
}

func (s *SeqCounter) End() uint64 {
	return s.counter.Add(1)
}

// keyState is the abstract per-key state used by the Porcupine model.
type keyState[V any] struct {
	exists bool
	value  V
}
