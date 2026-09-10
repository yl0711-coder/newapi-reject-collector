package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	_ "time/tzdata" // scratch images need the configured log timezone too.
)

var durableNodePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func validateDurableConfig(cfg Config) (*time.Location, error) {
	if cfg.ECSSocket != "" && (!filepath.IsAbs(cfg.ECSSocket) || !strings.HasPrefix(cfg.Node, "ecs-") || len(cfg.Node) != 52 || cfg.StatePath == "") {
		return nil, errors.New("ECS socket requires an independent durable ECS source")
	}
	u, err := url.Parse(cfg.SinkURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !strings.HasSuffix(u.Path, "/internal/rejections/v2") {
		return nil, errors.New("durable reject sink must be HTTPS /internal/rejections/v2 without credentials/query/fragment")
	}
	if cfg.SinkToken == "" || !durableNodePattern.MatchString(cfg.Node) || !filepath.IsAbs(cfg.StatePath) || !filepath.IsAbs(cfg.LogGlob) || cfg.FlushSeconds < 5 || cfg.FlushSeconds > 3600 {
		return nil, errors.New("durable reject requires token, bounded node/interval and absolute state/log paths")
	}
	if cfg.LogTimezone == "" || cfg.LogTimezone == "Local" {
		return nil, errors.New("COLLECTOR_LOG_TIMEZONE must explicitly name the source log timezone")
	}
	if filepath.Dir(cfg.StatePath) == filepath.Dir(cfg.LogGlob) {
		return nil, errors.New("durable state and source logs must use separate directories")
	}
	return time.LoadLocation(cfg.LogTimezone)
}

func postDurableBatch(ctx context.Context, cfg Config, client *http.Client, state durableState) error {
	if len(state.Pending) == 0 {
		return nil
	}
	var payload durablePayload
	if json.Unmarshal(state.Pending, &payload) != nil || durableHash(state.Pending) != state.PendingHash {
		return errors.New("frozen reject payload checksum mismatch")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.SinkURL, bytes.NewReader(state.Pending))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.SinkToken)
	// Do not accept redirects (including a login HTML page returning 200), or
	// forward a source credential to another host. Keep the caller's transport.
	cl := *client
	cl.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusAccepted {
		b, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
		if err != nil {
			return err
		}
		return validateArchiveReceipt(cfg, payload.Node, payload.BatchID, state.PendingHash, b)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("reject sink returned HTTP %d; frozen batch retained", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8193))
	if err != nil {
		return err
	}
	var ack struct {
		OK          bool   `json:"ok"`
		Node        string `json:"node"`
		BatchID     string `json:"batch_id"`
		PayloadHash string `json:"payload_hash"`
		Accepted    int    `json:"accepted"`
		Rejected    *int   `json:"rejected"`
		Version     int    `json:"version"`
	}
	if len(b) > 8192 || json.Unmarshal(b, &ack) != nil || !ack.OK || ack.Version != 2 || ack.Node != payload.Node || ack.BatchID != payload.BatchID || ack.PayloadHash != state.PendingHash || ack.Accepted != len(payload.Samples) || ack.Rejected == nil || *ack.Rejected != 0 {
		return errors.New("reject ACK incomplete or mismatched; frozen batch retained")
	}
	return nil
}

func runDurableCollector(ctx context.Context, cfg Config, cl *http.Client) error {
	loc, err := validateDurableConfig(cfg)
	if err != nil {
		return err
	}
	lock, err := lockDurableState(cfg.StatePath)
	if err != nil {
		return err
	}
	defer lock.Close()
	state, err := loadDurableState(cfg)
	if err != nil {
		return err
	}
	runner := durableRunner{cfg: cfg, loc: loc, client: cl, state: state, save: func(s durableState) error { return saveDurableState(cfg.StatePath, s) }}
	for {
		if err := ctx.Err(); err != nil {
			return runner.shutdown()
		}
		delay, err := runner.turn(ctx)
		if err != nil {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return runner.shutdown()
		case <-timer.C:
		}
	}
}

// Persistence is injected to test BOTH crash boundaries, without weakening the
// production filesystem implementation. An ambiguous save error is fatal; callers
// must reopen durable state before another turn rather than reuse memory.
type durableRunner struct {
	cfg    Config
	loc    *time.Location
	client *http.Client
	state  durableState
	save   func(durableState) error
}

func (r *durableRunner) turn(ctx context.Context) (time.Duration, error) {
	delay := time.Second
	if len(r.state.Pending) == 0 {
		next, changed, err := prepareDurableBatch(r.cfg, r.state, r.loc)
		if err != nil {
			return 0, err
		}
		if changed {
			if err := r.save(next); err != nil {
				return 0, err
			}
			r.state = next
			delay = 250 * time.Millisecond // bounded catch-up, not one batch/minute.
		}
	}
	if len(r.state.Pending) == 0 {
		return delay, nil
	}
	if err := postDurableBatch(ctx, r.cfg, r.client, r.state); err != nil {
		slog.Warn("reject batch pending; source reading paused", "err", err)
		return time.Duration(r.cfg.FlushSeconds) * time.Second, nil
	}
	next := r.state
	next.Pending, next.PendingHash = nil, ""
	if err := r.save(next); err != nil {
		return 0, err
	}
	r.state = next
	return delay, nil
}
