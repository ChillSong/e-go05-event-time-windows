// Package clock abstracts the processing-time clock.
//
// Code that calls time.Now() directly cannot be tested deterministically: the
// answer changes on every run. Everything in this project that needs "what time
// is it for the system" asks a Clock instead, and the caller decides which one:
//
//   - Real: the wall clock, for a live input where events arrive in real time.
//   - Manual: a clock that only moves when told to. Tests drive it step by step,
//     and the file replay drives it with each event's arrival_time, which is the
//     processing clock this exercise defines.
package clock

import (
	"sync"
	"time"
)

// Clock reports the current processing time.
type Clock interface {
	Now() time.Time
}

// Real is the wall clock.
type Real struct{}

// Now returns time.Now().
func (Real) Now() time.Time { return time.Now() }

// Manual is a clock that only changes when Set or Advance is called. It never
// moves backwards: Set to an earlier instant is ignored, because a processing
// clock that runs backwards would let the watermark regress.
type Manual struct {
	mu sync.Mutex
	t  time.Time
}

// NewManual creates a manual clock stopped at t.
func NewManual(t time.Time) *Manual { return &Manual{t: t} }

// Now returns the current manual time.
func (m *Manual) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.t
}

// Set moves the clock to t, if t is not earlier than the current time.
func (m *Manual) Set(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t.After(m.t) {
		m.t = t
	}
}

// Advance moves the clock forward by d.
func (m *Manual) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d > 0 {
		m.t = m.t.Add(d)
	}
}
