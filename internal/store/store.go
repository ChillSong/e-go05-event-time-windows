// Package store is the Postgres sink for the window processor.
//
// Window results are upserted by (run_id, window_start, country). A corrected
// window overwrites its row with a higher revision. The upsert only applies when
// the incoming revision is >= the stored one, so replaying the file (or a stale
// writer) can never move a final window back to an older version: running the
// processor twice leaves exactly the same rows.
package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"e-go05/internal/window"
)

const schema = `
CREATE TABLE IF NOT EXISTS window_results (
    run_id        TEXT        NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    country       TEXT        NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    events        BIGINT      NOT NULL,
    gmv_usd       NUMERIC(18,2) NOT NULL,
    revision      INT         NOT NULL,
    status        TEXT        NOT NULL,
    published_at  TIMESTAMPTZ NOT NULL,  -- processing time of this version
    PRIMARY KEY (run_id, window_start, country)
);

CREATE TABLE IF NOT EXISTS rejected_events (
    run_id        TEXT        NOT NULL,
    event_id      TEXT        NOT NULL,
    reason        TEXT        NOT NULL,  -- too_late | future_event_time (cuarentena)
    country       TEXT        NOT NULL,
    amount_usd    NUMERIC(18,2) NOT NULL,
    event_time    TIMESTAMPTZ NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL,
    detail        TEXT,
    PRIMARY KEY (run_id, event_id)
);

CREATE TABLE IF NOT EXISTS run_metrics (
    run_id              TEXT PRIMARY KEY,
    allowed_lateness_s  DOUBLE PRECISION NOT NULL,
    clock_tolerance_s   DOUBLE PRECISION NOT NULL,
    idle_timeout_s      DOUBLE PRECISION NOT NULL,
    lines               BIGINT NOT NULL,
    accepted            BIGINT NOT NULL,
    corrections         BIGINT NOT NULL,
    late_events         BIGINT NOT NULL,
    late_gmv_usd        NUMERIC(18,2) NOT NULL,
    quarantined         BIGINT NOT NULL,
    quarantined_gmv_usd NUMERIC(18,2) NOT NULL,
    finalized_windows   INT NOT NULL,
    max_open_windows    INT NOT NULL,
    max_open_cells      INT NOT NULL,
    finalize_p50_s      DOUBLE PRECISION NOT NULL,
    finalize_p95_s      DOUBLE PRECISION NOT NULL,
    finalize_max_s      DOUBLE PRECISION NOT NULL,
    publications        BIGINT NOT NULL,
    max_heap_mb         DOUBLE PRECISION NOT NULL,
    elapsed_s           DOUBLE PRECISION NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

const upsertWindow = `
INSERT INTO window_results
    (run_id, window_start, country, window_end, events, gmv_usd, revision, status, published_at)
VALUES ($1, $2, $3, $4, $5, $6::numeric / 100, $7, $8, $9)
ON CONFLICT (run_id, window_start, country) DO UPDATE SET
    window_end   = EXCLUDED.window_end,
    events       = EXCLUDED.events,
    gmv_usd      = EXCLUDED.gmv_usd,
    revision     = EXCLUDED.revision,
    status       = EXCLUDED.status,
    published_at = EXCLUDED.published_at
WHERE window_results.revision <= EXCLUDED.revision`

const rejectFlush = 5000

// Sink writes to Postgres. Window publications are queued as separate
// statements in one pgx.Batch (one round trip per batch); rejected events are
// buffered and written as multi-row inserts.
type Sink struct {
	ctx   context.Context
	conn  *pgx.Conn
	runID string

	pending  *pgx.Batch
	rejects  []window.Rejected
	maxBatch int
}

// Open connects and creates the schema if needed.
func Open(ctx context.Context, dsn, runID string) (*Sink, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w (is Postgres up? docker compose up -d)", err)
	}
	if _, err := conn.Exec(ctx, schema); err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("schema: %w", err)
	}
	return &Sink{ctx: ctx, conn: conn, runID: runID, pending: &pgx.Batch{}, maxBatch: 500}, nil
}

// ResetRun deletes every row of this run (for a deliberately fresh start).
func (s *Sink) ResetRun() error {
	for _, t := range []string{"window_results", "rejected_events", "run_metrics"} {
		if _, err := s.conn.Exec(s.ctx, "DELETE FROM "+t+" WHERE run_id = $1", s.runID); err != nil {
			return err
		}
	}
	return nil
}

// Publish implements window.Sink.
func (s *Sink) Publish(rs []window.Result) error {
	for _, r := range rs {
		s.pending.Queue(upsertWindow, s.runID, r.WindowStart, r.Country, r.WindowEnd,
			r.Events, r.Cents, r.Revision, string(r.Status), r.PublishedAt)
	}
	if s.pending.Len() >= s.maxBatch {
		return s.flushWindows()
	}
	return nil
}

// Reject implements window.Sink.
func (s *Sink) Reject(r window.Rejected) error {
	s.rejects = append(s.rejects, r)
	if len(s.rejects) >= rejectFlush {
		return s.flushRejects()
	}
	return nil
}

// Flush writes everything still buffered.
func (s *Sink) Flush() error {
	if err := s.flushWindows(); err != nil {
		return err
	}
	return s.flushRejects()
}

func (s *Sink) flushWindows() error {
	if s.pending.Len() == 0 {
		return nil
	}
	err := s.conn.SendBatch(s.ctx, s.pending).Close()
	s.pending = &pgx.Batch{}
	if err != nil {
		return fmt.Errorf("upsert windows: %w", err)
	}
	return nil
}

func (s *Sink) flushRejects() error {
	if len(s.rejects) == 0 {
		return nil
	}
	const cols = 8
	var b strings.Builder
	b.WriteString(`INSERT INTO rejected_events
		(run_id, event_id, reason, country, amount_usd, event_time, processed_at, detail) VALUES `)
	args := make([]any, 0, len(s.rejects)*cols)
	for i, r := range s.rejects {
		if i > 0 {
			b.WriteByte(',')
		}
		n := i * cols
		fmt.Fprintf(&b, "($%d,$%d,$%d,$%d,$%d::numeric/100,$%d,$%d,$%d)", n+1, n+2, n+3, n+4, n+5, n+6, n+7, n+8)
		args = append(args, s.runID, r.Event.ID, string(r.Reason), r.Event.Country, r.Event.Cents,
			r.Event.EventTime, r.At, r.Detail)
	}
	b.WriteString(" ON CONFLICT (run_id, event_id) DO NOTHING")
	_, err := s.conn.Exec(s.ctx, b.String(), args...)
	s.rejects = s.rejects[:0]
	if err != nil {
		return fmt.Errorf("insert rejected: %w", err)
	}
	return nil
}

// Metrics is the per-run summary written to run_metrics.
type Metrics struct {
	AllowedLateness, ClockTolerance, IdleTimeout time.Duration
	Lines                                        int64
	Stats                                        window.Stats
	P50, P95, Max                                time.Duration
	MaxHeapMB                                    float64
	Elapsed                                      time.Duration
}

// SaveMetrics upserts the run summary.
func (s *Sink) SaveMetrics(m Metrics) error {
	st := m.Stats
	_, err := s.conn.Exec(s.ctx, `
		INSERT INTO run_metrics (run_id, allowed_lateness_s, clock_tolerance_s, idle_timeout_s, lines,
			accepted, corrections, late_events, late_gmv_usd, quarantined, quarantined_gmv_usd,
			finalized_windows, max_open_windows, max_open_cells, finalize_p50_s, finalize_p95_s,
			finalize_max_s, publications, max_heap_mb, elapsed_s, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::numeric/100,$10,$11::numeric/100,$12,$13,$14,$15,$16,$17,$18,$19,$20, now())
		ON CONFLICT (run_id) DO UPDATE SET
			allowed_lateness_s = EXCLUDED.allowed_lateness_s, clock_tolerance_s = EXCLUDED.clock_tolerance_s,
			idle_timeout_s = EXCLUDED.idle_timeout_s, lines = EXCLUDED.lines, accepted = EXCLUDED.accepted,
			corrections = EXCLUDED.corrections, late_events = EXCLUDED.late_events,
			late_gmv_usd = EXCLUDED.late_gmv_usd, quarantined = EXCLUDED.quarantined,
			quarantined_gmv_usd = EXCLUDED.quarantined_gmv_usd, finalized_windows = EXCLUDED.finalized_windows,
			max_open_windows = EXCLUDED.max_open_windows, max_open_cells = EXCLUDED.max_open_cells,
			finalize_p50_s = EXCLUDED.finalize_p50_s, finalize_p95_s = EXCLUDED.finalize_p95_s,
			finalize_max_s = EXCLUDED.finalize_max_s, publications = EXCLUDED.publications,
			max_heap_mb = EXCLUDED.max_heap_mb, elapsed_s = EXCLUDED.elapsed_s, updated_at = now()`,
		s.runID, m.AllowedLateness.Seconds(), m.ClockTolerance.Seconds(), m.IdleTimeout.Seconds(), m.Lines,
		st.Accepted, st.Corrections, st.Late, st.LateCents, st.Quarantined, st.QuarantinedCents,
		st.Finalized, st.MaxOpenWindows, st.MaxOpenCells, m.P50.Seconds(), m.P95.Seconds(), m.Max.Seconds(),
		st.Publications, m.MaxHeapMB, m.Elapsed.Seconds())
	return err
}

// Close flushes and disconnects.
func (s *Sink) Close() error {
	err := s.Flush()
	s.conn.Close(s.ctx)
	return err
}
