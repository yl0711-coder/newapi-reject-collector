package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const ecsShutdownBudget = 15 * time.Second
const ecsDrainRetry = 250 * time.Millisecond

func (r *durableRunner) shutdown() error {
	if r.cfg.ECSSocket == "" {
		return nil
	} // Preserve Lightsail behavior.
	ctx, cancel := context.WithTimeout(context.Background(), ecsShutdownBudget)
	defer cancel()
	return r.drain(ctx)
}

// A successful drain describes observed files only. It does not assert that
// the producer has stopped or authorize publishing complete source coverage.
func (r *durableRunner) drain(ctx context.Context) error {
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("ECS reject drain incomplete: %w", errors.Join(err, lastErr))
		}
		if _, err := r.turn(ctx); err != nil {
			return err
		} // Never reuse ambiguous in-memory state after save failure.
		lastErr = r.observedEOF()
		if lastErr == nil {
			return nil
		}
		timer := time.NewTimer(ecsDrainRetry)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (r *durableRunner) observedEOF() error {
	if len(r.state.Pending) != 0 {
		return errors.New("reject batch still awaiting durable receipt")
	}
	paths, err := filepath.Glob(r.cfg.LogGlob)
	if err != nil {
		return err
	}
	if len(paths) == 0 || len(paths) > durableMaxFiles {
		return errors.New("reject source inventory unavailable")
	}
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		key, err := durableFileID(info)
		if err != nil {
			return err
		}
		cur, exists := r.state.Files[key]
		if !exists || cur.Offset != info.Size() || cur.ObservedSize != info.Size() {
			return errors.New("reject observed log EOF not reached")
		}
	}
	return nil
}
