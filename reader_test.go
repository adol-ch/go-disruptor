package disruptor

import (
	"sync/atomic"
	"testing"
	"time"
)

// MockConsumer is a mock implementation of the Consumer interface.
type MockConsumer struct {
	consumedCalls int64
	lower         int64
	upper         int64
	// t can be used for assertions within the consumer if needed
	// t *testing.T
}

func (mc *MockConsumer) Consume(lower, upper int64) {
	atomic.StoreInt64(&mc.lower, lower)
	atomic.StoreInt64(&mc.upper, upper)
	atomic.AddInt64(&mc.consumedCalls, 1)
}

func (mc *MockConsumer) GetConsumedCalls() int64 {
	return atomic.LoadInt64(&mc.consumedCalls)
}

func (mc *MockConsumer) GetLower() int64 {
	return atomic.LoadInt64(&mc.lower)
}

func (mc *MockConsumer) GetUpper() int64 {
	return atomic.LoadInt64(&mc.upper)
}

// MockBarrier is a mock implementation of the Barrier interface.
type MockBarrier struct {
	readVal int64
}

func (mb *MockBarrier) Read(lowerConsidered int64) int64 { // Match actual Barrier interface
	return atomic.LoadInt64(&mb.readVal)
}

func (mb *MockBarrier) SetRead(val int64) {
	atomic.StoreInt64(&mb.readVal, val)
}

// TestReaderStartStop tests the Start and Stop methods of the Reader.
// (This test was implemented in the previous subtask and is kept for completeness)
func TestReaderStartStop(t *testing.T) {
	readCursor := NewCursor()
	writtenCursor := NewCursor()

	mockBarrier := &MockBarrier{}
	mockConsumer := &MockConsumer{}

	mockBarrier.SetRead(InitialSequenceValue)
	writtenCursor.Store(InitialSequenceValue)

	reader := NewReader(readCursor, writtenCursor, mockBarrier, mockConsumer)

	reader.Start() // Internally sets ready=true and starts goroutine

	// To check if reader is "ready" (active), we observe its behavior.
	// If Stop() works, consumer should not be called afterwards even if data becomes available.

	reader.Stop() // Internally sets ready=false

	// Conditions for receive loop to exit when !ready:
	// lower > upstream.Read(lower) AND lower > written.Load()
	// We ensure these are met if ready is false.
	mockBarrier.SetRead(InitialSequenceValue - 1) // Ensure upstream.Read returns less than current 'lower'
	writtenCursor.Store(InitialSequenceValue - 1) // Ensure written.Load returns less than current 'lower'

	// Give time for the goroutine to process the Stop signal and exit.
	// The original sleep was 50ms, let's keep it similar.
	time.Sleep(50 * time.Millisecond)

	initialCalls := mockConsumer.GetConsumedCalls()

	// Try to make the reader consume something if it were still running (i.e., if Stop failed)
	writtenCursor.Store(1)      // Say sequence 0, 1 are "written"
	mockBarrier.SetRead(1) // Barrier says sequence 0, 1 are "available"

	time.Sleep(50 * time.Millisecond) // Wait to see if consumer is called

	finalCalls := mockConsumer.GetConsumedCalls()

	if finalCalls > initialCalls {
		t.Errorf("Consumer was called after Stop(). Expected %d calls, got %d. This implies the receive loop did not terminate as expected or 'ready' was not respected.", initialCalls, finalCalls)
	}
}

// TestReaderConsumesMessages tests if the reader correctly consumes messages.
func TestReaderConsumesMessages(t *testing.T) {
	readCursor := NewCursor() // Starts at -1 (InitialSequenceValue)
	writtenCursor := NewCursor()

	mockBarrier := &MockBarrier{}
	mockConsumer := &MockConsumer{}

	// Configure the scenario:
	// We want the consumer to process sequences from 0 up to 4.
	// So, 'lower' should be 0 (InitialSequenceValue + 1)
	// and 'upper' should be 4.
	const expectedUpperSequence int64 = 4
	const expectedLowerSequence int64 = 0 // InitialSequenceValue + 1

	writtenCursor.Store(expectedUpperSequence)
	mockBarrier.SetRead(expectedUpperSequence)

	reader := NewReader(readCursor, writtenCursor, mockBarrier, mockConsumer)
	reader.Start()

	// Allow time for the reader's goroutine to process the messages.
	// This duration might need adjustment based on system speed and scheduler.
	time.Sleep(50 * time.Millisecond)

	reader.Stop() // Stop the reader before assertions to stabilize state

	// Assertions
	if calls := mockConsumer.GetConsumedCalls(); calls == 0 {
		t.Errorf("Expected consumer to be called, but it wasn't. ConsumedCalls: %d", calls)
	} else if calls > 1 {
		// The current reader implementation should batch this into a single call
		// for a contiguous sequence. If it calls more, it might be processing one by one.
		// For this test, we expect one call for the batch 0-4.
		t.Logf("Note: Consumer was called %d times. Expected 1 call for the batch.", calls)
	}


	if lower := mockConsumer.GetLower(); lower != expectedLowerSequence {
		t.Errorf("Expected consumer Lower to be %d, got %d", expectedLowerSequence, lower)
	}

	if upper := mockConsumer.GetUpper(); upper != expectedUpperSequence {
		t.Errorf("Expected consumer Upper to be %d, got %d", expectedUpperSequence, upper)
	}

	if currentRead := readCursor.Load(); currentRead != expectedUpperSequence {
		t.Errorf("Expected read cursor to be updated to %d, got %d", expectedUpperSequence, currentRead)
	}
}
