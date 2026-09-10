package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestECSRejectDrainRetainsUnsentAndPartialLogs(t *testing.T) {
	for _, scenario := range []string{"missing", "partial", "offline"} {
		t.Run(scenario, func(t *testing.T) {
			cfg, path := durableFixture(t)
			sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
			defer sink.Close()
			cfg.SinkURL = sink.URL
			if scenario != "missing" {
				line := rejectFixtureLine("fixture")
				if scenario == "partial" {
					line = strings.TrimSuffix(line, "\n")
				}
				if err := os.WriteFile(path, []byte(line), 0600); err != nil {
					t.Fatal(err)
				}
			}
			state, err := loadDurableState(cfg)
			if err != nil {
				t.Fatal(err)
			}
			r := durableRunner{cfg: cfg, loc: time.UTC, client: sink.Client(), state: state, save: func(s durableState) error { return saveDurableState(cfg.StatePath, s) }}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if err := r.drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unverified EOF/ACK accepted: %v", err)
			}
			if scenario == "offline" && len(r.state.Pending) == 0 {
				t.Fatal("undelivered frozen batch discarded")
			}
			if scenario == "partial" {
				for _, file := range r.state.Files {
					if file.Offset != 0 {
						t.Fatal("partial tail consumed")
					}
				}
			}
		})
	}
}

func TestLightsailShutdownDoesNotRunECSDrain(t *testing.T) {
	r := durableRunner{}
	if err := r.shutdown(); err != nil {
		t.Fatal(err)
	}
}
