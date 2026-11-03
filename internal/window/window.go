// Package window groups events into 5-minute event-time windows per country,
// decides when a window is final, and corrects windows that were already
// published while a late event is still allowed.
//
// Two clocks, two jobs:
//
//   - event_time (what the device says happened) decides WHICH window an event
//     belongs to. A window is [start, end): an event at exactly `end` belongs to
//     the next window.
//   - processing time (the injected Clock — in the file replay, arrival_time)
//     decides WHEN the processor believes a window is complete.
//
// Lifecycle of a window, driven by the watermark W:
//
//	open ──(W >= end)──> emitted, provisional ──(W >= end + lateness)──> final, state freed
//	                        │      ▲
//	                        └──────┘  late event within lateness: correction, revision++
//
// An event whose window already reached end + lateness is dropped as too late:
// counted, its amount kept, and handed to the sink — never silently lost. An
// event whose event_time is ahead of the processing clock by more than the
// tolerance comes from a broken device clock: it goes to quarantine and never
// touches the watermark.
package window

import (
	"fmt"
	"sort"
	"time"

	"e-go05/internal/clock"
)

// Event is one payment confirmation. Amounts are integer cents so that sums are
// exact and comparable to the batch truth without floating-point noise.
type Event struct {
	ID        string
	Country   string
	Cents     int64
	EventTime time.Time
}

// Status of a published window version.
type Status string

const (
	Provisional Status = "provisional"
	Final       Status = "final"
)

// Result is one published version of a (window, country) cell.
type Result struct {
	WindowStart time.Time
	WindowEnd   time.Time
	Country     string
	Events      int64
	Cents       int64
	Revision    int
	Status      Status
	PublishedAt time.Time // processing time
}

// Reason an event was not counted.
type Reason string

const (
	TooLate     Reason = "too_late"          // its window was already final
	FutureClock Reason = "future_event_time" // device clock ahead of processing time
)

// Rejected is an event that did not enter the aggregate, with why.
type Rejected struct {
	Event  Event
	Reason Reason
	At     time.Time // processing time
	Detail string
}

// Sink receives every publication and every rejected event.
type Sink interface {
	Publish(rs []Result) error
	Reject(r Rejected) error
}

// Config holds the time policy. See decisions.md for each value.
type Config struct {
	Size            time.Duration // window length (5 min)
	AllowedLateness time.Duration // how long after W passes `end` a window can still be corrected
	ClockTolerance  time.Duration // event_time later than now + this = broken clock
	IdleTimeout     time.Duration // W never lags processing time by more than this
	PublishEvery    time.Duration // coalesce corrections: publish dirty cells at most this often

	// NaiveWatermark is an EXPERIMENT, not a policy: W = max event_time of every
	// event, with no quarantine, no processing-time cap and no idle rule. It
	// exists only to measure the failure the exercise warns about (one broken
	// clock jumps W hours ahead and closes every real window early).
	NaiveWatermark bool
}

// Stats are the counters the run reports.
type Stats struct {
	Accepted         int64
	Corrections      int64 // accepted events that landed in an already emitted window
	Late             int64
	LateCents        int64
	Quarantined      int64
	QuarantinedCents int64
	Publications     int64 // result rows handed to the sink
	Finalized        int   // windows finalized
	MaxOpenWindows   int
	MaxOpenCells     int
	FinalizeDelays   []time.Duration // per window: processing time from `end` to its final version
}

type cell struct {
	events, cents int64
	revision      int
	dirty         bool
}

type win struct {
	start, end time.Time
	cells      map[string]*cell
	emitted    bool
}

// Processor is the windowing state machine. It is not safe for concurrent use:
// a stream is processed in arrival order by a single goroutine.
type Processor struct {
	cfg  Config
	clk  clock.Clock
	sink Sink

	wm          time.Time // watermark, never moves backwards
	maxEvent    time.Time // max event_time among ACCEPTED events
	windows     map[int64]*win
	openCells   int
	lastPublish time.Time
	stats       Stats
}

// New creates a processor. The clock is injected: tests pass a manual clock,
// the file replay passes one driven by arrival_time.
func New(cfg Config, clk clock.Clock, sink Sink) *Processor {
	if cfg.Size <= 0 {
		cfg.Size = 5 * time.Minute
	}
	return &Processor{cfg: cfg, clk: clk, sink: sink, windows: make(map[int64]*win)}
}

// Watermark returns the current watermark.
func (p *Processor) Watermark() time.Time { return p.wm }

// Stats returns a copy of the counters.
func (p *Processor) Stats() Stats {
	s := p.stats
	s.FinalizeDelays = append([]time.Duration(nil), p.stats.FinalizeDelays...)
	return s
}

// OpenWindows is how many windows currently hold state.
func (p *Processor) OpenWindows() int { return len(p.windows) }

// WindowStart returns the start of the [start, start+size) window holding t.
func (p *Processor) WindowStart(t time.Time) time.Time {
	size := p.cfg.Size.Milliseconds()
	ms := t.UnixMilli()
	start := ms - ms%size
	if ms%size < 0 { // floor for instants before 1970, kept for completeness
		start -= size
	}
	return time.UnixMilli(start).UTC()
}

// Process handles one event at the current processing time.
func (p *Processor) Process(ev Event) error {
	now := p.clk.Now()
	if !p.cfg.NaiveWatermark {
		p.advance(now) // processing time alone can move W (idle rule)
	}

	// Broken clock: an event cannot have happened after the moment it arrived.
	if !p.cfg.NaiveWatermark && ev.EventTime.After(now.Add(p.cfg.ClockTolerance)) {
		p.stats.Quarantined++
		p.stats.QuarantinedCents += ev.Cents
		if err := p.sink.Reject(Rejected{
			Event: ev, Reason: FutureClock, At: now,
			Detail: fmt.Sprintf("event_time %s ahead of processing time", ev.EventTime.Sub(now).Round(time.Millisecond)),
		}); err != nil {
			return err
		}
		return p.fire(now)
	}

	start := p.WindowStart(ev.EventTime)
	end := start.Add(p.cfg.Size)

	// Too late: its window already reached end + lateness and was finalized.
	if !end.Add(p.cfg.AllowedLateness).After(p.wm) {
		p.stats.Late++
		p.stats.LateCents += ev.Cents
		if err := p.sink.Reject(Rejected{
			Event: ev, Reason: TooLate, At: now,
			Detail: fmt.Sprintf("window %s final; arrived %s after event_time", start.Format("15:04"), now.Sub(ev.EventTime).Round(time.Millisecond)),
		}); err != nil {
			return err
		}
		return p.fire(now)
	}

	key := start.UnixMilli()
	w := p.windows[key]
	if w == nil {
		w = &win{start: start, end: end, cells: make(map[string]*cell)}
		p.windows[key] = w
	}
	c := w.cells[ev.Country]
	if c == nil {
		c = &cell{}
		w.cells[ev.Country] = c
		p.openCells++
	}
	c.events++
	c.cents += ev.Cents
	p.stats.Accepted++
	if w.emitted {
		c.dirty = true
		p.stats.Corrections++
	}
	if len(p.windows) > p.stats.MaxOpenWindows {
		p.stats.MaxOpenWindows = len(p.windows)
	}
	if p.openCells > p.stats.MaxOpenCells {
		p.stats.MaxOpenCells = p.openCells
	}

	// Only an accepted event's event_time can push the watermark.
	if ev.EventTime.After(p.maxEvent) {
		p.maxEvent = ev.EventTime
	}
	if p.cfg.NaiveWatermark {
		if p.maxEvent.After(p.wm) {
			p.wm = p.maxEvent // the trap: whatever the device claims, W follows
		}
	} else {
		p.advance(now)
	}
	return p.fire(now)
}

// advance recomputes the watermark:
//
//	W = max( min(maxEvent, now), now - IdleTimeout )
//
//   - min(maxEvent, now): event time drives W, but it can never get ahead of the
//     processing clock. Quarantine already keeps broken clocks out of maxEvent;
//     this cap is a second, independent guard.
//   - now - IdleTimeout: if event time stops advancing (the sale ended but
//     stragglers keep arriving), processing time still moves W, so the last
//     windows close instead of waiting for end of input.
//
// W is monotonic: it never moves backwards.
func (p *Processor) advance(now time.Time) {
	eventWM := p.maxEvent
	if eventWM.After(now) {
		eventWM = now
	}
	cand := eventWM
	if idle := now.Add(-p.cfg.IdleTimeout); idle.After(cand) {
		cand = idle
	}
	if cand.After(p.wm) {
		p.wm = cand
	}
}

// fire applies the watermark to every open window and publishes what changed.
func (p *Processor) fire(now time.Time) error {
	return p.transition(now, false)
}

// Close is end of input: every remaining window is finalized now.
func (p *Processor) Close() error {
	return p.transition(p.clk.Now(), true)
}

func (p *Processor) transition(now time.Time, endOfInput bool) error {
	keys := make([]int64, 0, len(p.windows))
	for k := range p.windows {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	publishDirty := endOfInput || p.cfg.PublishEvery <= 0 || now.Sub(p.lastPublish) >= p.cfg.PublishEvery
	var out []Result

	for _, k := range keys {
		w := p.windows[k]
		switch {
		case endOfInput || !w.end.Add(p.cfg.AllowedLateness).After(p.wm):
			// Final: one last version per cell, then free the state.
			out = p.publishCells(out, w, Final, now, false)
			p.stats.Finalized++
			p.stats.FinalizeDelays = append(p.stats.FinalizeDelays, now.Sub(w.end))
			p.openCells -= len(w.cells)
			delete(p.windows, k)

		case !w.emitted && !w.end.After(p.wm):
			// First (provisional) version.
			w.emitted = true
			out = p.publishCells(out, w, Provisional, now, false)

		case w.emitted && publishDirty:
			// Corrections accumulated since the last publication.
			out = p.publishCells(out, w, Provisional, now, true)
		}
	}
	if publishDirty {
		p.lastPublish = now
	}
	if len(out) == 0 {
		return nil
	}
	p.stats.Publications += int64(len(out))
	return p.sink.Publish(out)
}

// publishCells appends one new version per cell (only dirty ones if onlyDirty).
// Every published version bumps the cell's revision.
func (p *Processor) publishCells(out []Result, w *win, st Status, now time.Time, onlyDirty bool) []Result {
	countries := make([]string, 0, len(w.cells))
	for c := range w.cells {
		countries = append(countries, c)
	}
	sort.Strings(countries)
	for _, country := range countries {
		c := w.cells[country]
		if onlyDirty && !c.dirty {
			continue
		}
		c.revision++
		c.dirty = false
		out = append(out, Result{
			WindowStart: w.start, WindowEnd: w.end, Country: country,
			Events: c.events, Cents: c.cents, Revision: c.revision,
			Status: st, PublishedAt: now,
		})
	}
	return out
}
