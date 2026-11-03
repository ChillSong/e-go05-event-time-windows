// Package tests holds the deterministic, clock-injected tests for the window
// processor. None of them calls time.Now(): every instant comes from a manual
// clock that the test moves explicitly, so each run sees exactly the same times.
package tests

import (
	"fmt"
	"testing"
	"time"

	"e-go05/internal/clock"
	"e-go05/internal/window"
)

// memSink records every publication and keeps the latest version per cell, the
// same way the Postgres upsert does.
type memSink struct {
	pubs     []window.Result
	latest   map[string]window.Result
	rejected []window.Rejected
}

func newSink() *memSink { return &memSink{latest: map[string]window.Result{}} }

func (s *memSink) Publish(rs []window.Result) error {
	for _, r := range rs {
		s.pubs = append(s.pubs, r)
		s.latest[key(r.WindowStart, r.Country)] = r
	}
	return nil
}

func (s *memSink) Reject(r window.Rejected) error {
	s.rejected = append(s.rejected, r)
	return nil
}

func key(start time.Time, country string) string {
	return start.UTC().Format("15:04:05") + "/" + country
}

var t0 = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return t0.Add(d) }

func cfg(lateness time.Duration) window.Config {
	return window.Config{
		Size:            5 * time.Minute,
		AllowedLateness: lateness,
		ClockTolerance:  time.Minute,
		IdleTimeout:     time.Minute,
		PublishEvery:    0, // publish corrections immediately: easier to assert
	}
}

// harness wires a processor to a manual clock and a memory sink.
type harness struct {
	t   *testing.T
	clk *clock.Manual
	snk *memSink
	p   *window.Processor
	n   int
}

func newHarness(t *testing.T, c window.Config) *harness {
	clk := clock.NewManual(t0)
	snk := newSink()
	return &harness{t: t, clk: clk, snk: snk, p: window.New(c, clk, snk)}
}

// send delivers an event that happened at eventAt and arrives at arriveAt.
func (h *harness) send(country string, cents int64, eventAt, arriveAt time.Time) {
	h.t.Helper()
	h.clk.Set(arriveAt)
	h.n++
	if err := h.p.Process(window.Event{
		ID: fmt.Sprintf("ev%d", h.n), Country: country, Cents: cents, EventTime: eventAt,
	}); err != nil {
		h.t.Fatalf("process: %v", err)
	}
}

func (h *harness) latest(start time.Time, country string) window.Result {
	h.t.Helper()
	r, ok := h.snk.latest[key(start, country)]
	if !ok {
		h.t.Fatalf("no publication for %s", key(start, country))
	}
	return r
}

// --- borde de ventana ------------------------------------------------------

func TestWindowBoundaryIsHalfOpen(t *testing.T) {
	h := newHarness(t, cfg(time.Minute))
	edge := at(5 * time.Minute)
	if got := h.p.WindowStart(edge.Add(-time.Millisecond)); !got.Equal(t0) {
		t.Fatalf("10:04:59.999 must fall in [10:00,10:05), got %s", got)
	}
	if got := h.p.WindowStart(edge); !got.Equal(edge) {
		t.Fatalf("10:05:00.000 is the END of [10:00,10:05): must open [10:05,10:10), got %s", got)
	}

	h.send("MX", 100, edge.Add(-time.Millisecond), edge.Add(-time.Millisecond))
	h.send("MX", 200, edge, edge)
	h.send("MX", 1, at(20*time.Minute), at(20*time.Minute)) // push W past both windows

	if r := h.latest(t0, "MX"); r.Events != 1 || r.Cents != 100 {
		t.Fatalf("[10:00,10:05) want 1 event / 100c, got %d / %d", r.Events, r.Cents)
	}
	if r := h.latest(edge, "MX"); r.Events != 1 || r.Cents != 200 {
		t.Fatalf("[10:05,10:10) want 1 event / 200c, got %d / %d", r.Events, r.Cents)
	}
}

// --- emision cuando el watermark pasa el fin --------------------------------

func TestWindowEmittedWhenWatermarkPassesEnd(t *testing.T) {
	h := newHarness(t, cfg(time.Minute))
	h.send("MX", 100, at(time.Minute), at(time.Minute))
	h.send("MX", 50, at(4*time.Minute+59*time.Second), at(4*time.Minute+59*time.Second))
	if len(h.snk.pubs) != 0 {
		t.Fatalf("W=10:04:59 < end 10:05: nothing must be published yet, got %d", len(h.snk.pubs))
	}
	h.send("AR", 1, at(5*time.Minute), at(5*time.Minute)) // W reaches 10:05
	r := h.latest(t0, "MX")
	if r.Status != window.Provisional || r.Revision != 1 || r.Cents != 150 {
		t.Fatalf("want provisional rev1 150c, got %s rev%d %dc", r.Status, r.Revision, r.Cents)
	}
}

// --- dato tardio que corrige -------------------------------------------------

func TestLateEventWithinLatenessCorrectsWindow(t *testing.T) {
	h := newHarness(t, cfg(2*time.Minute))
	h.send("MX", 100, at(time.Minute), at(time.Minute))
	h.send("MX", 1, at(6*time.Minute), at(6*time.Minute)) // emit [10:00,10:05) as rev1
	if r := h.latest(t0, "MX"); r.Revision != 1 || r.Cents != 100 {
		t.Fatalf("want rev1 100c, got rev%d %dc", r.Revision, r.Cents)
	}

	// Happened at 10:03, arrives at 10:06:30: W=10:06:30 < end+lateness=10:07.
	h.send("MX", 40, at(3*time.Minute), at(6*time.Minute+30*time.Second))

	r := h.latest(t0, "MX")
	if r.Revision != 2 || r.Cents != 140 || r.Events != 2 || r.Status != window.Provisional {
		t.Fatalf("correction: want provisional rev2 140c 2 events, got %s rev%d %dc %d", r.Status, r.Revision, r.Cents, r.Events)
	}
	if s := h.p.Stats(); s.Corrections != 1 || s.Late != 0 {
		t.Fatalf("want 1 correction 0 late, got %d / %d", s.Corrections, s.Late)
	}
}

// --- dato demasiado tardio ---------------------------------------------------

func TestEventBeyondLatenessIsDroppedAndCounted(t *testing.T) {
	h := newHarness(t, cfg(2*time.Minute))
	h.send("MX", 100, at(time.Minute), at(time.Minute))
	h.send("MX", 1, at(8*time.Minute), at(8*time.Minute)) // W=10:08 >= 10:07: final

	if r := h.latest(t0, "MX"); r.Status != window.Final {
		t.Fatalf("W past end+lateness: window must be final, got %s", r.Status)
	}
	before := h.latest(t0, "MX")

	// Happened at 10:02, arrives at 10:08:10: its window is already final.
	h.send("MX", 777, at(2*time.Minute), at(8*time.Minute+10*time.Second))

	if after := h.latest(t0, "MX"); after != before {
		t.Fatalf("a too-late event must not change a final window: %+v -> %+v", before, after)
	}
	s := h.p.Stats()
	if s.Late != 1 || s.LateCents != 777 {
		t.Fatalf("want 1 late event worth 777c, got %d / %dc", s.Late, s.LateCents)
	}
	last := h.snk.rejected[len(h.snk.rejected)-1]
	if last.Reason != window.TooLate || last.Event.Cents != 777 {
		t.Fatalf("rejected event must keep reason and amount, got %s %dc", last.Reason, last.Event.Cents)
	}
}

func TestLatenessBoundaryIsExact(t *testing.T) {
	// Final exactly when W == end + lateness. One ms before, still correctable.
	h := newHarness(t, cfg(2*time.Minute))
	h.send("MX", 100, at(time.Minute), at(time.Minute))
	h.send("MX", 1, at(7*time.Minute-time.Millisecond), at(7*time.Minute-time.Millisecond))
	h.send("MX", 5, at(2*time.Minute), at(7*time.Minute-time.Millisecond))
	if s := h.p.Stats(); s.Late != 0 {
		t.Fatalf("W one ms before end+lateness: event must be accepted, got %d late", s.Late)
	}
	h.send("MX", 1, at(7*time.Minute), at(7*time.Minute)) // W == end + lateness: final
	h.send("MX", 5, at(2*time.Minute), at(7*time.Minute))
	if s := h.p.Stats(); s.Late != 1 {
		t.Fatalf("W == end+lateness: window final, event must be late, got %d late", s.Late)
	}
}

// --- reloj roto ----------------------------------------------------------------

func TestBrokenClockQuarantinedAndWatermarkUntouched(t *testing.T) {
	h := newHarness(t, cfg(2*time.Minute))
	h.send("MX", 100, at(time.Minute), at(time.Minute))
	wmBefore := h.p.Watermark()

	// Device says 12:00 but the event arrives at 10:01:30: two hours in the future.
	h.send("CO", 999, at(2*time.Hour), at(time.Minute+30*time.Second))

	if wm := h.p.Watermark(); wm.After(at(time.Minute + 30*time.Second)) {
		t.Fatalf("a broken clock must not advance W: was %s, now %s", wmBefore, wm)
	}
	s := h.p.Stats()
	if s.Quarantined != 1 || s.QuarantinedCents != 999 {
		t.Fatalf("want 1 quarantined worth 999c, got %d / %dc", s.Quarantined, s.QuarantinedCents)
	}
	r := h.snk.rejected[0]
	if r.Reason != window.FutureClock || r.Detail == "" {
		t.Fatalf("quarantine must carry a reason and detail, got %q %q", r.Reason, r.Detail)
	}
	// The window it claimed (12:00) must not exist, and 10:00 must still be open.
	if h.p.OpenWindows() != 1 {
		t.Fatalf("only [10:00,10:05) must hold state, got %d windows", h.p.OpenWindows())
	}
	if len(h.snk.pubs) != 0 {
		t.Fatalf("no window may close because of a broken clock, got %d publications", len(h.snk.pubs))
	}
}

func TestEventWithinToleranceCannotPushWatermarkAheadOfClock(t *testing.T) {
	// 30s ahead: inside the 1-min tolerance, so it is accepted — but W is capped
	// by the processing clock and cannot run ahead of it.
	h := newHarness(t, cfg(2*time.Minute))
	now := at(4*time.Minute + 50*time.Second)
	h.send("MX", 100, now.Add(30*time.Second), now)
	if wm := h.p.Watermark(); wm.After(now) {
		t.Fatalf("W must never exceed processing time: W=%s now=%s", wm, now)
	}
	if s := h.p.Stats(); s.Quarantined != 0 || s.Accepted != 1 {
		t.Fatalf("within tolerance: want accepted, got accepted=%d quarantined=%d", s.Accepted, s.Quarantined)
	}
}

// --- estado liberado al finalizar ------------------------------------------------

func TestFinalizedWindowsReleaseState(t *testing.T) {
	h := newHarness(t, cfg(time.Minute))
	for m := 0; m < 60; m++ { // one event per minute for an hour, always on time
		h.send("MX", 1, at(time.Duration(m)*time.Minute), at(time.Duration(m)*time.Minute))
	}
	if n := h.p.OpenWindows(); n > 2 {
		t.Fatalf("with 1-min lateness at most ~2 windows may stay open, got %d", n)
	}
	s := h.p.Stats()
	if s.MaxOpenWindows > 2 {
		t.Fatalf("max open windows must stay bounded, got %d", s.MaxOpenWindows)
	}
	if s.Finalized < 10 {
		t.Fatalf("an hour of data must finalize ~11 windows, got %d", s.Finalized)
	}
}

// --- ciclo completo de revisiones ------------------------------------------------

func TestRevisionSequenceProvisionalCorrectionFinal(t *testing.T) {
	h := newHarness(t, cfg(2*time.Minute))
	h.send("PE", 10, at(time.Minute), at(time.Minute))
	h.send("PE", 1, at(5*time.Minute), at(5*time.Minute))                  // rev1 provisional
	h.send("PE", 20, at(2*time.Minute), at(5*time.Minute+10*time.Second))  // rev2 correction
	h.send("PE", 30, at(3*time.Minute), at(5*time.Minute+20*time.Second))  // rev3 correction
	h.send("PE", 1, at(7*time.Minute), at(7*time.Minute))                  // rev4 final

	var got []string
	for _, r := range h.snk.pubs {
		if r.WindowStart.Equal(t0) {
			got = append(got, fmt.Sprintf("%s:%d:%d", r.Status, r.Revision, r.Cents))
		}
	}
	want := []string{"provisional:1:10", "provisional:2:30", "provisional:3:60", "final:4:60"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("revision sequence:\n got  %v\n want %v", got, want)
	}
}

// --- inactividad: el tiempo de evento se estanca -------------------------------

func TestIdleRuleClosesWindowsWhenEventTimeStalls(t *testing.T) {
	// The sale ends at 10:05 (no event happens after), but stragglers keep
	// arriving. Without the idle rule W would stay at 10:05 forever and the last
	// window would never be final.
	h := newHarness(t, cfg(2*time.Minute))
	h.send("MX", 100, at(4*time.Minute), at(4*time.Minute))
	h.send("MX", 5, at(4*time.Minute+30*time.Second), at(4*time.Minute+31*time.Second))
	// Straggler from 10:01 arriving at 10:09: W = max(10:04:30, 10:09-1min) = 10:08 >= 10:07.
	h.send("MX", 7, at(time.Minute), at(9*time.Minute))
	if r := h.latest(t0, "MX"); r.Status != window.Final {
		t.Fatalf("idle rule must finalize [10:00,10:05) once processing time moves on, got %s", r.Status)
	}
	if s := h.p.Stats(); s.Late != 1 {
		t.Fatalf("the 10:09 straggler is past end+lateness+idle: want late, got %d", s.Late)
	}
}

// --- fin de entrada y monotonia ------------------------------------------------

func TestCloseFinalizesEverything(t *testing.T) {
	h := newHarness(t, cfg(30*time.Minute))
	h.send("MX", 100, at(time.Minute), at(time.Minute))
	h.send("AR", 200, at(12*time.Minute), at(12*time.Minute))
	if err := h.p.Close(); err != nil {
		t.Fatal(err)
	}
	if h.p.OpenWindows() != 0 {
		t.Fatalf("end of input must free all state, %d windows left", h.p.OpenWindows())
	}
	for _, k := range []string{key(t0, "MX"), key(at(10*time.Minute), "AR")} {
		if h.snk.latest[k].Status != window.Final {
			t.Fatalf("%s must be final after Close", k)
		}
	}
}

func TestManualClockNeverRunsBackwards(t *testing.T) {
	c := clock.NewManual(at(10 * time.Minute))
	c.Set(at(5 * time.Minute))
	if !c.Now().Equal(at(10 * time.Minute)) {
		t.Fatalf("Set to an earlier time must be ignored, clock now %s", c.Now())
	}
	c.Advance(time.Second)
	if !c.Now().Equal(at(10*time.Minute + time.Second)) {
		t.Fatalf("Advance: got %s", c.Now())
	}
}

func TestRealClockImplementsInterface(t *testing.T) {
	var _ clock.Clock = clock.Real{}
	var _ clock.Clock = clock.NewManual(t0)
}

// --- la trampa: watermark ingenuo -------------------------------------------------

func TestNaiveWatermarkIsBrokenByOneBadClock(t *testing.T) {
	// Documents WHY the guards exist: with W = max event_time seen, a single
	// device claiming 12:00 closes every real window and the next on-time event
	// is thrown away as "too late".
	c := cfg(2 * time.Minute)
	c.NaiveWatermark = true
	h := newHarness(t, c)
	h.send("MX", 100, at(time.Minute), at(time.Minute))
	h.send("CO", 999, at(2*time.Hour), at(time.Minute+30*time.Second)) // broken clock
	if wm := h.p.Watermark(); !wm.Equal(at(2 * time.Hour)) {
		t.Fatalf("naive W follows the broken clock to 12:00, got %s", wm)
	}
	h.send("MX", 50, at(time.Minute+40*time.Second), at(time.Minute+41*time.Second)) // on time!
	if s := h.p.Stats(); s.Late != 1 {
		t.Fatalf("naive W must wrongly drop an on-time event, got %d late", s.Late)
	}
}
