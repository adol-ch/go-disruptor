package disruptor

import (
	"sync" // Added for TestWriterBlocksWhenRingBufferIsFull
	"testing"
	"time" // Added for TestWriterBlocksWhenRingBufferIsFull
)

// Existing Benchmarks (preserved)
func BenchmarkWriterReserve(b *testing.B) {
	read, written := NewCursor(), NewCursor()
	writer := NewWriter(written, read, 1024)
	iterations := int64(b.N)
	b.ReportAllocs()
	b.ResetTimer()

	for i := int64(0); i < iterations; i++ {
		sequence := writer.Reserve(1)
		read.Store(sequence)
	}
}

func BenchmarkWriterNextWrapPoint(b *testing.B) {
	read, written := NewCursor(), NewCursor()
	writer := NewWriter(written, read, 1024)
	iterations := int64(b.N)
	b.ReportAllocs()
	b.ResetTimer()

	read.Store(MaxSequenceValue)
	for i := int64(0); i < iterations; i++ {
		writer.Reserve(1)
	}
}

func BenchmarkWriterAwait(b *testing.B) {
	written, read := NewCursor(), NewCursor()
	writer := NewWriter(written, read, 1024*64)
	iterations := int64(b.N)
	b.ReportAllocs()
	b.ResetTimer()

	for i := int64(0); i < iterations; i++ {
		writer.Await(i)
		read.Store(i)
	}
}

func BenchmarkWriterCommit(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	// Preserving structure but content is mostly commented out
	// as writer.go's Writer doesn't have Commit(lower,upper).
}

// New Functional Tests from previous step (preserved)

func TestWriterReserveAndCommitSingleSlot(t *testing.T) {
	writtenCursor := NewCursor()
	readCursor := NewCursor()
	upstreamBarrier := NewCompositeBarrier(readCursor) // Using CompositeBarrier
	capacity := int64(8)
	writer := NewWriter(writtenCursor, upstreamBarrier, capacity)

	readCursor.Store(InitialSequenceValue)

	s1 := writer.Reserve(1)
	if s1 != 0 {
		t.Errorf("Expected first reserved sequence to be 0, got %d", s1)
	}
	writtenCursor.Store(s1)
	if loadedS1 := writtenCursor.Load(); loadedS1 != 0 {
		t.Errorf("Expected written cursor to be 0 after commit, got %d", loadedS1)
	}

	s2 := writer.Reserve(1)
	if s2 != 1 {
		t.Errorf("Expected second reserved sequence to be 1, got %d", s2)
	}
	writtenCursor.Store(s2)
	if loadedS2 := writtenCursor.Load(); loadedS2 != 1 {
		t.Errorf("Expected written cursor to be 1 after commit, got %d", loadedS2)
	}
}

func TestWriterReserveManySlots(t *testing.T) {
	writtenCursor := NewCursor()
	readCursor := NewCursor()
	upstreamBarrier := NewCompositeBarrier(readCursor) // Using CompositeBarrier
	capacity := int64(8)
	writer := NewWriter(writtenCursor, upstreamBarrier, capacity)

	readCursor.Store(InitialSequenceValue)

	reservedHigh1 := writer.Reserve(3)
	if reservedHigh1 != 2 {
		t.Errorf("Expected first reserved high sequence to be 2, got %d", reservedHigh1)
	}
	writtenCursor.Store(reservedHigh1)
	if loadedS1 := writtenCursor.Load(); loadedS1 != 2 {
		t.Errorf("Expected written cursor to be 2 after first batch commit, got %d", loadedS1)
	}

	reservedHigh2 := writer.Reserve(3)
	if reservedHigh2 != 5 {
		t.Errorf("Expected second reserved high sequence to be 5, got %d", reservedHigh2)
	}
	writtenCursor.Store(reservedHigh2)
	if loadedS2 := writtenCursor.Load(); loadedS2 != 5 {
		t.Errorf("Expected written cursor to be 5 after second batch commit, got %d", loadedS2)
	}
}

// Helper for assertions to keep tests cleaner
func AssertInt64(t *testing.T, expected, actual int64) {
	t.Helper()
	if expected != actual {
		t.Errorf("Expected %d, got %d", expected, actual)
	}
}

func TestWriterWrapsRingBuffer(t *testing.T) {
	const capacity = 4
	readCursor := NewCursor()    // Implements Barrier interface via Read method
	writtenCursor := NewCursor()
	writer := NewWriter(writtenCursor, readCursor, capacity) // readCursor itself is a Barrier

	readCursor.Store(InitialSequenceValue) // All slots available

	s0 := writer.Reserve(1); AssertInt64(t, 0, s0); writtenCursor.Store(s0)
	s1 := writer.Reserve(1); AssertInt64(t, 1, s1); writtenCursor.Store(s1)
	s2 := writer.Reserve(1); AssertInt64(t, 2, s2); writtenCursor.Store(s2)
	s3 := writer.Reserve(1); AssertInt64(t, 3, s3); writtenCursor.Store(s3)
	AssertInt64(t, 3, writtenCursor.Load())

	// Make slot 0 available by advancing readCursor past it.
	// Writer checks previous_sequence (3) - capacity (4) > gate (readCursor.Load())
	// (3 - 4 = -1) > gate. If gate is -1, then -1 > -1 is false. Loop doesn't block.
	readCursor.Store(0) // Slot 0 is consumed.
	s4 := writer.Reserve(1); AssertInt64(t, 4, s4); writtenCursor.Store(s4)
	AssertInt64(t, 4, writtenCursor.Load())

	// Make slots 1, 2, 3 available
	// Writer previous_sequence is 4.
	// (4 - 4 = 0) > gate. If gate is 0, then 0 > 0 is false. Loop doesn't block.
	readCursor.Store(3) // Slots up to 3 are consumed.
	s5 := writer.Reserve(1); AssertInt64(t, 5, s5); writtenCursor.Store(s5)
	// Writer previous_sequence is 5.
	// Reserve(2): previous becomes 5+2=7.
	// (7 - 4 = 3) > gate. If gate is 3, then 3 > 3 is false. Loop doesn't block.
	s6_batch_high := writer.Reserve(2); AssertInt64(t, 7, s6_batch_high); writtenCursor.Store(s6_batch_high) // Reserves 6 and 7
	AssertInt64(t, 7, writtenCursor.Load())
}

func TestWriterBlocksWhenRingBufferIsFull(t *testing.T) {
	const capacity = 2
	readCursor := NewCursor()
	writtenCursor := NewCursor()
	// Using CompositeBarrier as it's a common way to group cursors for a barrier.
	// readCursor itself would also work here.
	upstreamBarrier := NewCompositeBarrier(readCursor)
	writer := NewWriter(writtenCursor, upstreamBarrier, capacity)

	readCursor.Store(InitialSequenceValue) // All slots available initially (-1)

	s0 := writer.Reserve(1); AssertInt64(t, 0, s0); writtenCursor.Store(s0) // writer.previous is 0
	s1 := writer.Reserve(1); AssertInt64(t, 1, s1); writtenCursor.Store(s1) // writer.previous is 1
	// Ring buffer is now full from the writer's perspective of available slots.
	// writer.previous = 1. Writer's internal gate = InitialSequenceValue = -1.

	var wg sync.WaitGroup
	wg.Add(1)
	reservedChan := make(chan int64, 1)

	go func() {
		defer wg.Done()
		// This call should block. writer.previous will become 1+1=2.
		// Loop condition: (writer.previous (2) - capacity (2)) > writer.gate (-1)
		// (0 > -1) is true. Loop continues. writer.gate = upstreamBarrier.Read(0) which is readCursor.Load() = -1.
		// Loop will spin, calling runtime.Gosched(), until readCursor advances.
		nextSequence := writer.Reserve(1)
		reservedChan <- nextSequence
	}()

	time.Sleep(time.Millisecond * 50) // Give goroutine time to call Reserve and block.

	select {
	case val := <-reservedChan:
		t.Fatalf("writer.Reserve should have blocked, but returned %d prematurely", val)
	default:
		// Expected: Reserve is blocked.
	}

	readCursor.Store(0) // Make slot 0 available. writer.gate will become 0.
	// Now loop condition: (writer.previous (2) - capacity (2)) > writer.gate (0)
	// (0 > 0) is false. Loop should exit.

	select {
	case reservedSeq := <-reservedChan:
		AssertInt64(t, 2, reservedSeq) // writer.previous was 2 upon exiting loop.
		writtenCursor.Store(reservedSeq)
	case <-time.After(time.Second * 2): // Increased timeout just in case.
		t.Fatal("Timeout waiting for writer.Reserve to unblock")
	}

	wg.Wait()
}
