// tail.go:轻量文件 tail——跟随最新日志文件、处理轮转(new-api 每次重启换新文件名)与截断。
// 纯 stdlib、轮询式(每秒),零依赖。
package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"time"
)

// tailLoop 跟随 glob 匹配到的【最新】文件,把每行交给 onLine。
// 首次启动跳过历史(只采新行);检测到更新的文件(轮转)则切换并从头读。
func tailLoop(ctx context.Context, glob string, onLine func(string)) {
	var (
		path     string
		f        *os.File
		offset   int64
		leftover []byte
		first    = true
	)
	defer func() {
		if f != nil {
			_ = f.Close()
		}
	}()

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}

		newest := newestMatch(glob)
		if newest == "" {
			continue
		}
		if newest != path { // 首次,或轮转到新文件
			if f != nil {
				_ = f.Close()
			}
			nf, err := os.Open(newest)
			if err != nil {
				continue
			}
			f, path, leftover = nf, newest, nil
			if first { // 首启:跳过历史,只采之后的新行
				if st, e := f.Stat(); e == nil {
					offset = st.Size()
				}
				first = false
			} else { // 轮转:新文件从头读
				offset = 0
			}
		}

		st, err := f.Stat()
		if err != nil {
			continue
		}
		if st.Size() < offset { // 文件被截断 → 从头
			offset, leftover = 0, nil
		}
		if st.Size() == offset {
			continue // 无新内容
		}
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			continue
		}
		buf, err := io.ReadAll(f)
		if err != nil {
			continue
		}
		offset += int64(len(buf))

		data := make([]byte, 0, len(leftover)+len(buf))
		data = append(data, leftover...)
		data = append(data, buf...)
		for {
			i := bytes.IndexByte(data, '\n')
			if i < 0 {
				break
			}
			line := data[:i]
			if len(line) > 0 {
				onLine(string(line))
			}
			data = data[i+1:]
		}
		leftover = append([]byte(nil), data...) // 残留半行,留到下轮
	}
}

// newestMatch 返回 glob 里修改时间最新的文件(new-api 当前活跃日志)。
func newestMatch(glob string) string {
	files, _ := filepath.Glob(glob)
	var newest string
	var newestMod time.Time
	for _, fn := range files {
		st, err := os.Stat(fn)
		if err != nil {
			continue
		}
		if st.ModTime().After(newestMod) {
			newestMod, newest = st.ModTime(), fn
		}
	}
	return newest
}
