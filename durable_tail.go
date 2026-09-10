package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"syscall"
	"time"
)

var rejectLogTimestamp = regexp.MustCompile(`^\[ERR\] (\d{4}/\d{2}/\d{2} - \d{2}:\d{2}:\d{2}) \|`)

func durableFileID(st os.FileInfo) (string, error) {
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.Mode().IsRegular() {
		return "", errors.New("reject log is not a supported regular file")
	}
	return durableHash([]byte(fmt.Sprintf("reject-file-v1:%d:%d", stat.Dev, stat.Ino))), nil
}

func durableAnchor(f *os.File, offset int64) (string, error) {
	if offset == 0 {
		return "", nil
	}
	n := min(offset, int64(durableAnchorBytes))
	b := make([]byte, n)
	if _, err := f.ReadAt(b, offset-n); err != nil {
		return "", err
	}
	return durableHash(b), nil
}

// scanDurableFile only commits newline-terminated records. Partial tails remain
// on disk. Truncation/inode reuse fails closed instead of resetting a cursor and
// potentially counting old bytes twice. Append-only rotation is supported.
func scanDurableFile(path, expectedID string, old durableFile, loc *time.Location, budget int, byteBudget *int64) (durableFile, []Sample, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return old, nil, 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return old, nil, 0, err
	}
	actualID, err := durableFileID(st)
	if err != nil || actualID != expectedID {
		return old, nil, 0, errors.New("reject source replaced during scan; retry after review")
	}
	if st.Size() < old.Offset {
		return old, nil, 0, errors.New("reject source truncated; cursor retained, gap requires review")
	}
	anchor, err := durableAnchor(f, old.Offset)
	if err != nil {
		return old, nil, 0, err
	}
	if anchor != old.Anchor {
		return old, nil, 0, errors.New("reject source content changed; cursor retained, gap requires review")
	}
	if _, err := f.Seek(old.Offset, io.SeekStart); err != nil {
		return old, nil, 0, err
	}
	next := old
	next.ObservedSize = st.Size()
	limit := min(st.Size()-old.Offset, *byteBudget)
	limited := &io.LimitedReader{R: f, N: limit}
	defer func() { *byteBudget -= limit - limited.N }()
	reader := bufio.NewReaderSize(limited, durableMaxLineBytes+1)
	var samples []Sample
	lines := 0
	for lines < budget {
		line, err := reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			return old, nil, 0, errors.New("reject log line exceeds limit; cursor retained")
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return old, nil, 0, err
		}
		if len(line) > durableMaxLineBytes {
			return old, nil, 0, errors.New("reject log line exceeds limit; cursor retained")
		}
		if reason, model, group, ok := parseLine(string(line)); ok {
			match := rejectLogTimestamp.FindSubmatch(line)
			if len(match) != 2 {
				return old, nil, 0, errors.New("reject event timestamp missing; refusing collection-time backfill")
			}
			ts, err := time.ParseInLocation("2006/01/02 - 15:04:05", string(match[1]), loc)
			if err != nil {
				return old, nil, 0, errors.New("reject event timestamp invalid")
			}
			if len(model) > 128 || len(group) > 64 || len(reason) > 64 || ts.Unix() <= 0 {
				return old, nil, 0, errors.New("reject event outside receiver contract; cursor retained")
			}
			samples = append(samples, Sample{BucketTs: ts.Unix() / 60 * 60, Reason: reason, Model: model, Group: group, Count: 1})
		}
		next.Offset += int64(len(line))
		lines++
	}
	next.Anchor, err = durableAnchor(f, next.Offset)
	return next, samples, lines, err
}

// Every matching file is examined, not only newestMatch: a rolled-away file
// can still contain unacknowledged records. No state pruning without a separate
// retained-log/receipt policy. Hitting a bound stops rather than silently skips.
func prepareDurableBatch(cfg Config, state durableState, loc *time.Location) (durableState, bool, error) {
	if len(state.Pending) != 0 {
		return state, false, errors.New("pending reject batch must be acknowledged before reading more logs")
	}
	paths, err := filepath.Glob(cfg.LogGlob)
	if err != nil {
		return state, false, err
	}
	if len(paths) > durableMaxFiles {
		return state, false, errors.New("reject file inventory exceeds budget")
	}
	sort.Strings(paths)
	next := state
	next.Files = make(map[string]durableFile, len(state.Files))
	for k, v := range state.Files {
		next.Files[k] = v
	}
	budget := durableMaxLines
	byteBudget := int64(durableReadBudget)
	seen := map[string]bool{}
	var samples []Sample
	for _, path := range paths {
		st, err := os.Lstat(path)
		if err != nil {
			return state, false, err
		}
		key, err := durableFileID(st)
		if err != nil {
			return state, false, err
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		if budget == 0 || byteBudget < durableMaxLineBytes+1 {
			continue
		}
		cursor, rows, consumed, err := scanDurableFile(path, key, next.Files[key], loc, budget, &byteBudget)
		if err != nil {
			return state, false, err
		}
		next.Files[key] = cursor
		if len(next.Files) > durableMaxFiles {
			return state, false, errors.New("reject cursor inventory exceeds budget")
		}
		samples = append(samples, rows...)
		budget -= consumed
	}
	for key, cursor := range state.Files {
		if !seen[key] && cursor.Offset < cursor.ObservedSize {
			return state, false, errors.New("reject source disappeared with unread bytes; checkpoint retained")
		}
	}
	if len(samples) > 0 {
		id, err := newBatchID()
		if err != nil {
			return state, false, err
		}
		next.Pending, err = json.Marshal(durablePayload{Node: cfg.Node, BatchID: id, CreatedAt: time.Now().Unix(), Samples: samples})
		if err != nil {
			return state, false, err
		}
		next.PendingHash = durableHash(next.Pending)
	}
	a, _ := json.Marshal(state)
	b, _ := json.Marshal(next)
	return next, !bytes.Equal(a, b), nil
}
