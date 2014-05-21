package disruptor

import (
	"math"
	"sync/atomic"
)

type SharedWriter struct {
	capacity           int64
	gate               int64 // TODO: this will most likely need to be a cursor
	shift              uint8
	committedSequences []int32
	readerBarrier      Barrier
	writerCursor       *Cursor
}

func NewSharedWriter(writerCursor *Cursor, capacity int64, readerBarrier Barrier) *SharedWriter {
	assertPowerOfTwo(capacity)

	shift := uint8(math.Log2(float64(capacity)))
	buffer := initializeCommittedSequences(capacity)

	return &SharedWriter{
		capacity:           capacity,
		gate:               InitialSequenceValue,
		shift:              shift,
		committedSequences: buffer,
		readerBarrier:      readerBarrier,
		writerCursor:       writerCursor,
	}
}
func initializeCommittedSequences(capacity int64) []int32 {
	buffer := make([]int32, capacity)
	for i := range buffer {
		buffer[i] = int32(InitialSequenceValue)
	}
	return buffer
}

func (this *SharedWriter) Reserve(count int64) (int64, int64) {
	if count <= 0 {
		panic("Reservation must be a positive number.")
	} else if count > this.capacity {
		panic("Reservation cannot exceed the capacity.")
	}

	for {
		previous := this.writerCursor.Load()
		next := previous + count
		wrap := next - this.capacity

		if wrap > this.gate {
			min := this.readerBarrier.Load()
			if wrap > min {
				return 0, Gating
			}

			this.gate = min // doesn't matter which write wins, BUT will most likely need to be a Cursor
		}

		if atomic.CompareAndSwapInt64(&this.writerCursor.value, previous, next) {
			return previous + 1, next
		}
	}
}

func (this *SharedWriter) Commit(lower, upper int64) {
	for mask := this.capacity - 1; lower <= upper; lower++ {
		this.committedSequences[lower&mask] = int32(lower >> this.shift)
	}
}

func (this *SharedWriter) Load() int64 {
	upper := this.writerCursor.Load()

	for mask := this.capacity - 1; upper >= 0; upper-- {
		if this.committedSequences[upper&mask] == int32(upper>>this.shift) {
			return upper
		}
	}

	return upper
}