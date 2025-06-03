package disruptor_test

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time" // Added for timeout and cleanup sleep

	"github.com/smartystreets-prototypes/go-disruptor"
)

const (
	DPTestRingBufferSize = 16
	DPTestRingBufferMask = DPTestRingBufferSize - 1
	// Define a cleanup duration similar to disruptor.DisruptorCleanup
	DPTestCleanupDuration = time.Millisecond * 10
)

var dpTestRingBuffer [DPTestRingBufferSize]int64

type SimpleConsumer struct {
	sequence int64 // Stores the highest sequence processed
	consumed map[int64]bool // Tracks individual consumed items
	// t            *testing.T // Optional: if you need to make assertions from within Consume
}

func NewSimpleConsumer() *SimpleConsumer {
	return &SimpleConsumer{
		sequence: disruptor.InitialSequenceValue,
		consumed: make(map[int64]bool),
	}
}

// Consume populates the dpTestRingBuffer and updates the consumer's state
func (sc *SimpleConsumer) Consume(lower, upper int64) {
	for seq := lower; seq <= upper; seq++ {
		// In this test, the primary role of Consume is to mark sequences as processed
		// and update the consumer's own sequence counter.
		// The dpTestRingBuffer is populated by the writer in this test.
		sc.consumed[seq] = true
		atomic.StoreInt64(&sc.sequence, seq)
	}
}

func (sc *SimpleConsumer) GetSequence() int64 {
	return atomic.LoadInt64(&sc.sequence)
}

func (sc *SimpleConsumer) IsConsumed(seq int64) bool {
	// Assuming non-concurrent map access for IsConsumed relative to Consume for a single instance,
	// or that IsConsumed is called after all Consume operations are finished.
	return sc.consumed[seq]
}

func TestDiamondPattern(t *testing.T) {
	defer time.Sleep(DPTestCleanupDuration) // Use a cleanup period

	c1 := NewSimpleConsumer()
	c2 := NewSimpleConsumer()
	c3 := NewSimpleConsumer()

	// The controller manages the lifecycle of the Disruptor components
	controller := disruptor.
		Configure(DPTestRingBufferSize). // Ring buffer size
		WithConsumerGroup(c1, c2).     // C1 and C2 run in parallel
		WithConsumerGroup(c3).         // C3 runs after C1 and C2 complete for a given sequence
		Build()

	// Start the disruptor components
	controller.Start()
	defer controller.Stop() // Ensure cleanup

	writer := controller.Writer()
	// We will publish sequences from 0 up to DPTestRingBufferSize - 1
	highestPublished := int64(DPTestRingBufferSize - 1)

	// Production loop
	// Here, 's' represents the value we want to publish and also implicitly the sequence number
	// in this simplified test case because Reserve(1) will return sequential numbers starting from 0.
	for s := int64(0); s <= highestPublished; s++ {
		reservedSequence := writer.Reserve(1) // Reserve a single slot

		// Ensure the reserved sequence matches our loop variable 's'.
		// This is a key assumption for this test's direct mapping of 's' to sequence.
		if reservedSequence != s {
			t.Fatalf("Reserved sequence %d does not match expected sequence %d. Test logic error.", reservedSequence, s)
			return
		}

		dpTestRingBuffer[reservedSequence&DPTestRingBufferMask] = s // Store 's' in the ring buffer at the reserved slot
		writer.Commit(reservedSequence, reservedSequence)          // Commit the reserved sequence
	}

	// Verification loop
	// Wait for C3 to process all messages
	timeout := time.After(time.Second * 5) // 5-second timeout
	for c3.GetSequence() < highestPublished {
		select {
		case <-timeout:
			t.Errorf("Timeout waiting for C3 to process all messages. C3 sequence: %d, expected: %d", c3.GetSequence(), highestPublished)
			t.Logf("C1 sequence: %d, C2 sequence: %d", c1.GetSequence(), c2.GetSequence())
			// Check if C1 or C2 also got stuck, which would prevent C3 from progressing.
			if c1.GetSequence() < highestPublished || c2.GetSequence() < highestPublished {
				t.Errorf("C1 (%d) or C2 (%d) did not complete, preventing C3 from completing.", c1.GetSequence(), c2.GetSequence())
			}
			return // Exit test on timeout
		default:
			runtime.Gosched() // Yield to allow other goroutines to run
		}
	}

	// Assertions
	for s := int64(0); s <= highestPublished; s++ {
		if !c1.IsConsumed(s) {
			t.Errorf("C1 did not consume sequence %d. C1 sequence: %d", s, c1.GetSequence())
		}
		if !c2.IsConsumed(s) {
			t.Errorf("C2 did not consume sequence %d. C2 sequence: %d", s, c2.GetSequence())
		}
		if !c3.IsConsumed(s) {
			t.Errorf("C3 did not consume sequence %d. C3 sequence: %d", s, c3.GetSequence())
		}
		// Value check: dpTestRingBuffer was populated by the writer using 's' as value at index 's & mask'.
		if val := dpTestRingBuffer[s&DPTestRingBufferMask]; val != s {
			t.Errorf("Value in ringBuffer for sequence %d is incorrect. Expected %d, got %d", s, s, val)
		}
	}
}
