package disruptor

import "testing"

func BenchmarkCompositeBarrierRead(b *testing.B) {
	barrier := NewCompositeBarrier(NewCursor(), NewCursor(), NewCursor(), NewCursor())

	iterations := int64(b.N)
	b.ReportAllocs()
	b.ResetTimer()

	for i := int64(0); i < iterations; i++ {
		barrier.Read(0)
	}
}

func TestCompositeBarrierBehavesLikeSingleCursorWhenAlone(t *testing.T) {
	cursor := NewCursor()
	barrier := NewCompositeBarrier(cursor)

	cursor.Store(5)
	if sequence := barrier.Read(0); sequence != 5 {
		t.Errorf("Expected barrier to read 5, got %d", sequence)
	}

	cursor.Store(10)
	if sequence := barrier.Read(0); sequence != 10 {
		t.Errorf("Expected barrier to read 10, got %d", sequence)
	}
}

func TestCompositeBarrierReturnsMinimumOfMultipleCursors(t *testing.T) {
	c1, c2, c3 := NewCursor(), NewCursor(), NewCursor()
	barrier := NewCompositeBarrier(c1, c2, c3)

	c1.Store(10)
	c2.Store(5)
	c3.Store(12)
	if sequence := barrier.Read(0); sequence != 5 {
		t.Errorf("Expected minimum 5, got %d", sequence)
	}

	c2.Store(15) // c1=10, c2=15, c3=12
	if sequence := barrier.Read(0); sequence != 10 {
		t.Errorf("Expected minimum 10, got %d", sequence)
	}

	c1.Store(20) // c1=20, c2=15, c3=12 -> This comment was slightly off, c3 is still 12
	// c3.Store(18) // c1=20, c2=15, c3=18 -> Let's use the values from the problem description for this step
	// Correcting based on original intent: c1=20, c2=15 (from previous step), c3 updated to 18
	c3.Store(18) // Now state is c1=20, c2=15, c3=18
	if sequence := barrier.Read(0); sequence != 15 { // Minimum of (20, 15, 18) is 15
		t.Errorf("Expected minimum 15, got %d", sequence)
	}
}

func TestNewCompositeBarrierPanicsWithNoCursors(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("NewCompositeBarrier did not panic when called with no cursors")
		} else {
			// Optionally, check the panic message
			expectedPanicMsg := "At least one upstream cursor is required."
			if r != expectedPanicMsg {
				t.Errorf("Expected panic message '%s', got '%v'", expectedPanicMsg, r)
			}
		}
	}()
	NewCompositeBarrier() // This call should panic
}

// TestCompositeBarrierReadReturnsActualMinimum tests that Read returns the actual minimum,
// not influenced by the 'lower' parameter (which is 'noop' in CompositeBarrier.Read).
func TestCompositeBarrierReadReturnsActualMinimum(t *testing.T) {
	c1, c2 := NewCursor(), NewCursor()
	barrier := NewCompositeBarrier(c1, c2)

	c1.Store(10)
	c2.Store(12)

	// Even if caller is interested in sequence 5, actual minimum is 10.
	if sequence := barrier.Read(5); sequence != 10 {
		t.Errorf("Expected barrier.Read(5) to return 10, got %d", sequence)
	}

	// If caller is interested in sequence 11, actual minimum is still 10.
	// The barrier itself doesn't advance or wait based on 'lower'.
	if sequence := barrier.Read(11); sequence != 10 {
		t.Errorf("Expected barrier.Read(11) to return 10, got %d", sequence)
	}

	// If caller is interested in sequence 0, actual minimum is 10.
	if sequence := barrier.Read(0); sequence != 10 {
		t.Errorf("Expected barrier.Read(0) to return 10, got %d", sequence)
	}
}
