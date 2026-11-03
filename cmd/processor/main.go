// Command processor replays the late-events log in arrival order through the
// event-time window processor, with the allowed lateness given by flag, and
// writes every window version, every rejected event and the run's metrics to
// Postgres.
//
// There is no broker this week: arrival_time IS the processing clock. Each line
// moves an injected manual clock to its arrival_time before the event is
// processed, so the run is deterministic and as fast as the code allows.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"e-go05/internal/clock"
	"e-go05/internal/store"
	"e-go05/internal/window"
)

type line struct {
	EventID     string      `json:"event_id"`
	Country     string      `json:"country"`
	AmountUSD   json.Number `json:"amount_usd"`
	EventTime   string      `json:"event_time"`
	ArrivalTime string      `json:"arrival_time"`
}

func main() {
	file := flag.String("file", "data/eventos_tardios.jsonl", "JSONL log in arrival order")
	dsn := flag.String("dsn", "postgres://go05:go05@localhost:5436/go05?sslmode=disable", "Postgres DSN")
	lateness := flag.Duration("lateness", 2*time.Minute, "allowed lateness after the watermark passes a window's end")
	tolerance := flag.Duration("clock-tolerance", time.Minute, "event_time later than arrival_time + this = broken clock")
	idle := flag.Duration("idle", time.Minute, "watermark never lags processing time by more than this")
	publishEvery := flag.Duration("publish-every", time.Second, "coalesce corrections: publish at most this often (processing time)")
	runID := flag.String("run-id", "", `run identifier (default "lat-<lateness>")`)
	resetRun := flag.Bool("reset-run", false, "delete this run's rows before processing")
	naive := flag.Bool("naive-watermark", false, "EXPERIMENT: W = max event_time seen, no quarantine/cap/idle (demonstrates the broken-clock trap)")
	flag.Parse()

	if *runID == "" {
		*runID = "lat-" + short(*lateness)
	}

	ctx := context.Background()
	sink, err := store.Open(ctx, *dsn, *runID)
	if err != nil {
		log.Fatal(err)
	}
	if *resetRun {
		if err := sink.ResetRun(); err != nil {
			log.Fatalf("reset run: %v", err)
		}
	}

	f, err := os.Open(*file)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer f.Close()

	cfg := window.Config{
		Size:            5 * time.Minute,
		AllowedLateness: *lateness,
		ClockTolerance:  *tolerance,
		IdleTimeout:     *idle,
		PublishEvery:    *publishEvery,
		NaiveWatermark:  *naive,
	}
	clk := clock.NewManual(time.Time{})
	proc := window.New(cfg, clk, sink)

	log.Printf("[%s] lateness=%s clock-tolerance=%s idle=%s publish-every=%s naive-watermark=%v",
		*runID, *lateness, *tolerance, *idle, *publishEvery, *naive)

	t0 := time.Now()
	sc := bufio.NewScanner(f) // streaming: one line in memory at a time
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var n int64
	var maxHeap uint64
	var ms runtime.MemStats

	for sc.Scan() {
		var l line
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			log.Fatalf("line %d: %v", n+1, err)
		}
		evAt, err := time.Parse(time.RFC3339Nano, l.EventTime)
		if err != nil {
			log.Fatalf("line %d event_time: %v", n+1, err)
		}
		arrAt, err := time.Parse(time.RFC3339Nano, l.ArrivalTime)
		if err != nil {
			log.Fatalf("line %d arrival_time: %v", n+1, err)
		}
		cents, err := parseCents(l.AmountUSD.String())
		if err != nil {
			log.Fatalf("line %d amount: %v", n+1, err)
		}

		clk.Set(arrAt) // arrival_time is this system's processing clock
		if err := proc.Process(window.Event{ID: l.EventID, Country: l.Country, Cents: cents, EventTime: evAt}); err != nil {
			log.Fatalf("process line %d: %v", n+1, err)
		}
		n++
		if n%200_000 == 0 {
			runtime.ReadMemStats(&ms)
			if ms.HeapInuse > maxHeap {
				maxHeap = ms.HeapInuse
			}
			log.Printf("[%s] %dk lines  W=%s  open windows=%d", *runID, n/1000,
				proc.Watermark().Format("15:04:05"), proc.OpenWindows())
		}
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("scan: %v", err)
	}
	if err := proc.Close(); err != nil {
		log.Fatalf("close: %v", err)
	}
	if err := sink.Flush(); err != nil {
		log.Fatalf("flush: %v", err)
	}
	elapsed := time.Since(t0)

	st := proc.Stats()
	p50, p95, pmax := percentiles(st.FinalizeDelays)
	if err := sink.SaveMetrics(store.Metrics{
		AllowedLateness: *lateness, ClockTolerance: *tolerance, IdleTimeout: *idle,
		Lines: n, Stats: st, P50: p50, P95: p95, Max: pmax,
		MaxHeapMB: float64(maxHeap) / (1 << 20), Elapsed: elapsed,
	}); err != nil {
		log.Fatalf("metrics: %v", err)
	}
	if err := sink.Close(); err != nil {
		log.Fatalf("close sink: %v", err)
	}

	fmt.Printf("\n==================== %s (lateness %s) ====================\n", *runID, *lateness)
	fmt.Printf("  lineas leidas:            %d\n", n)
	fmt.Printf("  aceptados:                %d  (correcciones a ventanas ya emitidas: %d)\n", st.Accepted, st.Corrections)
	fmt.Printf("  descartados por tardios:  %d  USD %s\n", st.Late, usd(st.LateCents))
	fmt.Printf("  cuarentena reloj roto:    %d  USD %s\n", st.Quarantined, usd(st.QuarantinedCents))
	fmt.Printf("  ventanas finalizadas:     %d  (max abiertas a la vez: %d ventanas / %d celdas)\n",
		st.Finalized, st.MaxOpenWindows, st.MaxOpenCells)
	fmt.Printf("  demora de finalizacion:   p50=%s p95=%s max=%s\n", p50.Round(time.Millisecond), p95.Round(time.Millisecond), pmax.Round(time.Millisecond))
	fmt.Printf("  versiones publicadas:     %d\n", st.Publications)
	fmt.Printf("  heap maximo:              %.1f MB\n", float64(maxHeap)/(1<<20))
	fmt.Printf("  tiempo de corrida:        %s\n\n", elapsed.Round(time.Millisecond))
}

// parseCents converts "113.54" to 11354 exactly, with no floating point.
func parseCents(s string) (int64, error) {
	whole, frac, _ := strings.Cut(s, ".")
	if len(frac) > 2 {
		return 0, fmt.Errorf("more than 2 decimals in %q", s)
	}
	for len(frac) < 2 {
		frac += "0"
	}
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, err
	}
	c, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, err
	}
	return w*100 + c, nil
}

func usd(cents int64) string { return fmt.Sprintf("%d.%02d", cents/100, cents%100) }

// percentiles returns p50, p95 (nearest rank) and max.
func percentiles(d []time.Duration) (time.Duration, time.Duration, time.Duration) {
	if len(d) == 0 {
		return 0, 0, 0
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	rank := func(p float64) time.Duration {
		i := int(p*float64(len(s))+0.999999) - 1
		if i < 0 {
			i = 0
		}
		return s[i]
	}
	return rank(0.50), rank(0.95), s[len(s)-1]
}

// short formats durations without trailing zero units: 10s, 2m, 30m, 1h.
// (Only the zero units of a compound value are trimmed: "10s" stays "10s".)
func short(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}
