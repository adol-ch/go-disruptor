package disruptor

import (
	"runtime"
	"time"
)

type Reader struct {
	read     *Cursor
	written  *Cursor
	upstream Barrier
	consumer Consumer
	ready    bool
}

func NewReader(read, written *Cursor, upstream Barrier, consumer Consumer) *Reader {
	return &Reader{
		read:     read,
		written:  written,
		upstream: upstream,
		consumer: consumer,
		ready:    false,
	}
}

func (this *Reader) Start() {
	this.ready = true
	go this.receive()
}
func (this *Reader) Stop() {
	this.ready = false
}

const (
	SpinTries            = 300
	YieldTries           = 100
	InitialSleepDuration = 50 * time.Microsecond
	MaxSleepDuration     = 10 * time.Millisecond
)

func (this *Reader) receive() {
	previous := this.read.Load()
	// Old idling, gating counters removed.
	// Initialize new counters for spin/yield/sleep state.
	gatingCounter := 0 // Counter for progressive backoff steps during gating
	idlingCounter := 0 // Counter for progressive backoff steps during idling
	currentSleepDuration := InitialSleepDuration

	for {
		lower := previous + 1
		upper := this.upstream.Read(lower) // Check what the upstream barrier allows us to read

		if lower <= upper { // Successfully claimed a sequence range
			this.consumer.Consume(lower, upper)
			this.read.Store(upper)
			previous = upper

			// Reset counters and sleep duration on successful consumption
			gatingCounter = 0
			idlingCounter = 0
			currentSleepDuration = InitialSleepDuration
		} else {
			// No sequence range available from upstream barrier, check what's written by the producer
			writtenUpper := this.written.Load()
			if lower <= writtenUpper { // Gating: items are written, but upstream (e.g. other consumers) is behind
				gatingCounter++
				idlingCounter = 0 // Reset idling state

				if gatingCounter <= SpinTries {
					// Spin phase - do nothing, just loop quickly
				} else if gatingCounter <= SpinTries+YieldTries {
					runtime.Gosched() // Yield phase
				} else {
					time.Sleep(currentSleepDuration) // Sleep phase
					currentSleepDuration *= 2
					if currentSleepDuration > MaxSleepDuration {
						currentSleepDuration = MaxSleepDuration
					}
				}
			} else if this.ready { // Idling: no items available from writer, and we are supposed to be running
				idlingCounter++
				gatingCounter = 0 // Reset gating state

				// Similar phased backoff for idling
				if idlingCounter <= SpinTries {
					// Spin phase
				} else if idlingCounter <= SpinTries+YieldTries {
					runtime.Gosched() // Yield phase
				} else {
					time.Sleep(currentSleepDuration) // Sleep phase
					currentSleepDuration *= 2
					if currentSleepDuration > MaxSleepDuration {
						currentSleepDuration = MaxSleepDuration
					}
				}
			} else { // Not ready to run, so exit
				break
			}
		}
		// The runtime.Gosched() at the end of the original loop is removed,
		// as yielding is now part of the wait strategy.
	}
}
