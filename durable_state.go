package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const (
	durableStateVersion  = 1
	durableStateMaxBytes = 8 << 20
	durableMaxFiles      = 4096
	durableMaxLines      = 1000
	durableReadBudget    = 4 << 20
	durableMaxLineBytes  = 256 << 10
	durableAnchorBytes   = 128
)

// Cursor advancement and the entire frozen payload are one atomic checkpoint.
// No raw log content or credentials are written. A single pending batch bounds
// memory/disk use: an unavailable sink pauses reading, it never drops overflow.
type durableState struct {
	Version     int                    `json:"version"`
	Binding     string                 `json:"binding"`
	Files       map[string]durableFile `json:"files"`
	Pending     json.RawMessage        `json:"pending,omitempty"`
	PendingHash string                 `json:"pending_hash,omitempty"`
}

type durableFile struct {
	Offset       int64  `json:"offset"`
	ObservedSize int64  `json:"observed_size"`
	Anchor       string `json:"anchor"`
}

type durablePayload struct {
	Node      string   `json:"node"`
	BatchID   string   `json:"batch_id"`
	CreatedAt int64    `json:"created_at"`
	Samples   []Sample `json:"samples"`
}

func durableHash(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func durableBinding(cfg Config) string {
	// Tokens may rotate, but changing source/sink/glob/time interpretation may
	// not silently reuse a previous source's cursor or pending batch.
	b, _ := json.Marshal([]string{cfg.Node, cfg.SinkURL, cfg.LogGlob, cfg.LogTimezone})
	return durableHash(b)
}

func loadDurableState(cfg Config) (durableState, error) {
	s := durableState{Version: durableStateVersion, Binding: durableBinding(cfg), Files: map[string]durableFile{}}
	f, err := os.Open(cfg.StatePath)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return s, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, durableStateMaxBytes+1))
	if err != nil {
		return s, err
	}
	if len(b) > durableStateMaxBytes {
		return s, errors.New("durable checkpoint exceeds size limit; original retained")
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var trailing any
	if dec.Decode(&s) != nil || !errors.Is(dec.Decode(&trailing), io.EOF) {
		return s, errors.New("invalid durable checkpoint; original retained")
	}
	if s.Version != durableStateVersion || s.Binding != durableBinding(cfg) || s.Files == nil || len(s.Files) > durableMaxFiles {
		return s, errors.New("durable checkpoint identity/limits mismatch; original retained")
	}
	for key, cursor := range s.Files {
		if len(key) != 64 || cursor.Offset < 0 || cursor.ObservedSize < cursor.Offset || (cursor.Offset > 0 && len(cursor.Anchor) != 64) {
			return s, errors.New("invalid durable cursor; original retained")
		}
	}
	if len(s.Pending) == 0 {
		if s.PendingHash != "" {
			return s, errors.New("unexpected pending hash")
		}
	} else {
		var p durablePayload
		if durableHash(s.Pending) != s.PendingHash || json.Unmarshal(s.Pending, &p) != nil || p.Node != cfg.Node || len(p.BatchID) != 32 || len(p.Samples) == 0 || len(p.Samples) > durableMaxLines {
			return s, errors.New("invalid frozen reject batch; original retained")
		}
	}
	return s, nil
}

func saveDurableState(path string, s durableState) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if len(b) > durableStateMaxBytes {
		return errors.New("durable checkpoint budget exceeded; reading stopped")
	}
	return writeDurableAtomic(path, b)
}

func writeDurableAtomic(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".reject-checkpoint-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func lockDurableState(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("durable checkpoint already owned: %w", err)
	}
	// Closing releases the advisory lock, including process death. Never delete
	// the lock pathname: a new inode would permit two simultaneous owners.
	return f, nil
}
