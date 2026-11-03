package main

import (
	"testing"
	"time"
)

func TestParseCentsExact(t *testing.T) {
	cases := map[string]int64{"113.54": 11354, "8.00": 800, "900.00": 90000, "8.5": 850, "12": 1200, "0.01": 1}
	for in, want := range cases {
		got, err := parseCents(in)
		if err != nil || got != want {
			t.Errorf("parseCents(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	if _, err := parseCents("1.234"); err == nil {
		t.Error("3 decimals must be rejected, not rounded")
	}
}

func TestShortDuration(t *testing.T) {
	cases := map[time.Duration]string{10 * time.Second: "10s", 2 * time.Minute: "2m", 30 * time.Minute: "30m", time.Hour: "1h"}
	for in, want := range cases {
		if got := short(in); got != want {
			t.Errorf("short(%s) = %q, want %q", in, got, want)
		}
	}
}
