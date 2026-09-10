package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func durableFixture(t *testing.T) (Config, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "state"), 0700); err != nil {
		t.Fatal(err)
	}
	cfg := Config{StatePath: filepath.Join(dir, "state", "checkpoint.json"), LogGlob: filepath.Join(dir, "*.log"), Node: "ecs-reject-test", SinkURL: "https://monitor.invalid/internal/rejections/v2", SinkToken: "fixture", LogTimezone: "Asia/Shanghai", FlushSeconds: 5}
	path := filepath.Join(dir, "oneapi-1.log")
	return cfg, path
}

func TestDurableRejectSaveFailureBeforePOSTAndAfterACK(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			cfg, path := durableFixture(t)
			posts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { posts++; b, _ := io.ReadAll(r.Body); fullRejectACK(w, b) }))
			defer server.Close()
			cfg.SinkURL = server.URL
			writeFixture(t, path, rejectFixtureLine("m"))
			initial, err := loadDurableState(cfg)
			if err != nil {
				t.Fatal(err)
			}
			saves := 0
			runner := durableRunner{cfg: cfg, loc: time.UTC, client: server.Client(), state: initial, save: func(s durableState) error {
				saves++
				if saves == failAt {
					return errors.New("injected checkpoint fsync failure")
				}
				return saveDurableState(cfg.StatePath, s)
			}}
			if _, err := runner.turn(context.Background()); err == nil {
				t.Fatal("ambiguous persistence failure was not fatal")
			}
			if failAt == 1 && (posts != 0 || len(runner.state.Files) != 0) {
				t.Fatal("sent or advanced before durable checkpoint")
			}
			if failAt == 2 {
				restored, err := loadDurableState(cfg)
				if err != nil || posts != 1 || len(restored.Pending) == 0 || !bytes.Equal(restored.Pending, runner.state.Pending) {
					t.Fatalf("ACK save failure discarded pending: %v", err)
				}
			}
		})
	}
}

func rejectFixtureLine(model string) string {
	return "[ERR] 2026/09/09 - 12:34:56 | fixture | user 7 | No available channel for model " + model + " under group test-group (distributor)\n"
}

func prepareFixture(t *testing.T, cfg Config) durableState {
	t.Helper()
	s, err := loadDurableState(cfg)
	if err != nil {
		t.Fatal(err)
	}
	loc, err := time.LoadLocation(cfg.LogTimezone)
	if err != nil {
		t.Fatal(err)
	}
	s, changed, err := prepareDurableBatch(cfg, s, loc)
	if err != nil || !changed {
		t.Fatalf("prepare: changed=%v err=%v", changed, err)
	}
	if err := saveDurableState(cfg.StatePath, s); err != nil {
		t.Fatal(err)
	}
	return s
}

func writeFixture(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}

func fullRejectACK(w http.ResponseWriter, b []byte) {
	var p durablePayload
	_ = json.Unmarshal(b, &p)
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "version": 2, "node": p.Node, "batch_id": p.BatchID, "payload_hash": durableHash(b), "accepted": len(p.Samples), "rejected": 0})
}

func TestDurableRejectRestartRetriesExactBatchAfterLostACK(t *testing.T) {
	cfg, path := durableFixture(t)
	var attempts [][]byte
	receipts := map[string]bool{}
	count := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		attempts = append(attempts, b)
		var p durablePayload
		_ = json.Unmarshal(b, &p)
		if !receipts[p.BatchID] {
			receipts[p.BatchID] = true
			count += len(p.Samples)
		}
		if len(attempts) == 1 {
			http.Error(w, "lost ACK", 502)
			return
		}
		fullRejectACK(w, b)
	}))
	defer server.Close()
	cfg.SinkURL = server.URL
	writeFixture(t, path, rejectFixtureLine("m1"))
	s := prepareFixture(t, cfg)
	if err := postDurableBatch(context.Background(), cfg, server.Client(), s); err == nil {
		t.Fatal("expected failed ACK")
	}
	// Simulate process death. New bytes must not join the pending payload.
	writeFixture(t, path, rejectFixtureLine("m1")+rejectFixtureLine("m2"))
	restored, err := loadDurableState(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := postDurableBatch(context.Background(), cfg, server.Client(), restored); err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 || !bytes.Equal(attempts[0], attempts[1]) || count != 1 {
		t.Fatal("retry changed or double-counted batch")
	}
	restored.Pending, restored.PendingHash = nil, ""
	if err := saveDurableState(cfg.StatePath, restored); err != nil {
		t.Fatal(err)
	}
	next := prepareFixture(t, cfg)
	var p durablePayload
	if err := json.Unmarshal(next.Pending, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Samples) != 1 || p.Samples[0].Model != "m2" || p.Samples[0].BucketTs != time.Date(2026, 9, 9, 4, 34, 0, 0, time.UTC).Unix() {
		t.Fatalf("cursor/time incorrect: %+v", p)
	}
}

func TestDurableRejectInvalidACKRetainsCheckpoint(t *testing.T) {
	for _, body := range []string{`<html>login</html>`, `{"ok":true}`, `{"ok":true,"version":2,"accepted":1}`, `{"ok":false}`} {
		t.Run(body, func(t *testing.T) {
			cfg, path := durableFixture(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) }))
			defer server.Close()
			cfg.SinkURL = server.URL
			writeFixture(t, path, rejectFixtureLine("m"))
			s := prepareFixture(t, cfg)
			before, _ := os.ReadFile(cfg.StatePath)
			if err := postDurableBatch(context.Background(), cfg, server.Client(), s); err == nil {
				t.Fatal("accepted incomplete ACK")
			}
			after, _ := os.ReadFile(cfg.StatePath)
			if !bytes.Equal(before, after) {
				t.Fatal("failed ACK mutated checkpoint")
			}
		})
	}
}

func TestDurableRejectPartialTailRotationAndTruncation(t *testing.T) {
	cfg, path := durableFixture(t)
	line := rejectFixtureLine("m1")
	writeFixture(t, path, line+strings.TrimSuffix(rejectFixtureLine("m2"), "\n"))
	s := prepareFixture(t, cfg)
	for _, c := range s.Files {
		if c.Offset != int64(len(line)) {
			t.Fatal("committed partial tail")
		}
	}
	s.Pending, s.PendingHash = nil, ""
	if err := saveDurableState(cfg.StatePath, s); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, filepath.Join(filepath.Dir(path), "rotated.log")); err != nil {
		t.Fatal(err)
	}
	writeFixture(t, path, rejectFixtureLine("m3"))
	s = prepareFixture(t, cfg)
	var p durablePayload
	_ = json.Unmarshal(s.Pending, &p)
	if len(p.Samples) != 1 || p.Samples[0].Model != "m3" || len(s.Files) != 2 {
		t.Fatalf("rotation: %+v", p)
	}
	s.Pending, s.PendingHash = nil, ""
	if err := saveDurableState(cfg.StatePath, s); err != nil {
		t.Fatal(err)
	}
	writeFixture(t, path, "")
	if _, _, err := prepareDurableBatch(cfg, s, time.UTC); err == nil {
		t.Fatal("silently reset truncated cursor")
	}
}

func TestDurableRejectLockAndCorruptionFailClosed(t *testing.T) {
	cfg, path := durableFixture(t)
	lock, err := lockDurableState(cfg.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := lockDurableState(cfg.StatePath); err == nil {
		other.Close()
		t.Fatal("two owners acquired checkpoint")
	}
	lock.Close()
	writeFixture(t, path, rejectFixtureLine("m"))
	s := prepareFixture(t, cfg)
	other := cfg
	other.Node = "another-task"
	if _, err := loadDurableState(other); err == nil {
		t.Fatal("cross-task checkpoint reuse allowed")
	}
	s.PendingHash = strings.Repeat("0", 64)
	if err := saveDurableState(cfg.StatePath, s); err != nil {
		t.Fatal(err)
	}
	if _, err := loadDurableState(cfg); err == nil {
		t.Fatal("corrupt payload accepted")
	}
}

func TestDurableRejectCheckpointFailureDoesNotCommitCursor(t *testing.T) {
	cfg, path := durableFixture(t)
	writeFixture(t, path, rejectFixtureLine("m"))
	s, err := loadDurableState(cfg)
	if err != nil {
		t.Fatal(err)
	}
	next, _, err := prepareDurableBatch(cfg, s, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(cfg.StatePath, 0700); err != nil {
		t.Fatal(err)
	}
	if err := saveDurableState(cfg.StatePath, next); err == nil {
		t.Fatal("expected checkpoint rename failure")
	}
	if len(s.Files) != 0 || len(s.Pending) != 0 {
		t.Fatal("uncommitted scan changed original state")
	}
}

func TestDurableRejectRejectsMissingTimeOversizeAndUnreadDeletion(t *testing.T) {
	for name, line := range map[string]string{
		"missing-time": "| user 7 | No available channel for model m under group g (distributor)\n",
		"oversize":     strings.Repeat("x", durableMaxLineBytes+1) + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			cfg, path := durableFixture(t)
			writeFixture(t, path, line)
			s, _ := loadDurableState(cfg)
			if _, _, err := prepareDurableBatch(cfg, s, time.UTC); err == nil {
				t.Fatal("silently skipped invalid record")
			}
		})
	}
	cfg, path := durableFixture(t)
	writeFixture(t, path, strings.Repeat("irrelevant\n", durableMaxLines+1))
	s := prepareFixture(t, cfg)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := prepareDurableBatch(cfg, s, time.UTC); err == nil {
		t.Fatal("unread source removal not detected")
	}
}

func TestDurableRejectConfigAndRedirect(t *testing.T) {
	cfg, _ := durableFixture(t)
	if _, err := validateDurableConfig(cfg); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Config){func(c *Config) { c.LogTimezone = "" }, func(c *Config) { c.SinkURL = "http://monitor.invalid/internal/rejections/v2" }, func(c *Config) { c.Node = "task/invalid" }, func(c *Config) { c.StatePath = "relative" }, func(c *Config) { c.SinkURL = "https://monitor.invalid/internal/rejections" }} {
		bad := cfg
		mutate(&bad)
		if _, err := validateDurableConfig(bad); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://other.invalid", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	p, _ := json.Marshal(durablePayload{Node: cfg.Node, BatchID: strings.Repeat("1", 32), Samples: []Sample{{Count: 1}}})
	cfg.SinkURL = server.URL
	err := postDurableBatch(context.Background(), cfg, server.Client(), durableState{Pending: p, PendingHash: durableHash(p)})
	if err == nil || !strings.Contains(fmt.Sprint(err), "307") {
		t.Fatalf("redirect followed: %v", err)
	}
}

func TestDurableRejectChecksEveryACKIdentityField(t *testing.T) {
	for _, field := range []string{"node", "batch_id", "payload_hash", "accepted", "rejected", "version", "missing-rejected"} {
		t.Run(field, func(t *testing.T) {
			cfg, path := durableFixture(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				var p durablePayload
				_ = json.Unmarshal(b, &p)
				ack := map[string]any{"ok": true, "version": 2, "node": p.Node, "batch_id": p.BatchID, "payload_hash": durableHash(b), "accepted": len(p.Samples), "rejected": 0}
				switch field {
				case "node", "batch_id", "payload_hash":
					ack[field] = "wrong"
				case "accepted":
					ack[field] = 0
				case "rejected", "version":
					ack[field] = 1
				case "missing-rejected":
					delete(ack, "rejected")
				}
				_ = json.NewEncoder(w).Encode(ack)
			}))
			defer server.Close()
			cfg.SinkURL = server.URL
			writeFixture(t, path, rejectFixtureLine("m"))
			s := prepareFixture(t, cfg)
			if err := postDurableBatch(context.Background(), cfg, server.Client(), s); err == nil {
				t.Fatalf("accepted bad %s", field)
			}
		})
	}
}

func TestDurableRejectReadBudgetDoesNotLosePartialBuffer(t *testing.T) {
	cfg, path := durableFixture(t)
	body := strings.Repeat(strings.Repeat("x", 64<<10)+"\n", 100)
	writeFixture(t, path, body)
	s := prepareFixture(t, cfg)
	for _, c := range s.Files {
		if c.Offset <= 0 || c.Offset > durableReadBudget {
			t.Fatalf("unbounded read: %d", c.Offset)
		}
	}
	if len(s.Pending) != 0 {
		t.Fatal("unrelated lines became requests")
	}
	s = prepareFixture(t, cfg)
	for _, c := range s.Files {
		if c.Offset != int64(len(body)) {
			t.Fatalf("partial buffer lost: %d/%d", c.Offset, len(body))
		}
	}
}
