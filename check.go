// check.go:校验模式(`-check <文件>`)。对一个日志文件跑解析,打印识别到的拒绝与计数,
// 不 tail、不推送。用于【验证日志格式/正则是否匹配你的 new-api 版本】——尤其升级后自检。
package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
)

func runCheck(path string) int {
	f, err := os.Open(path)
	if err != nil {
		fmt.Println("打不开文件:", err)
		return 1
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024) // 容纳长行

	var total, matched int
	counts := map[string]int{}
	for sc.Scan() {
		total++
		if reason, model, group, ok := parseLine(sc.Text()); ok {
			matched++
			counts[reason+" | "+model+" | "+group]++
		}
	}
	if err := sc.Err(); err != nil {
		fmt.Println("读取出错:", err)
		return 1
	}

	fmt.Printf("扫描 %d 行,识别到 %d 条前置拒绝。\n", total, matched)
	if matched == 0 {
		fmt.Println("⚠️ 没匹配到任何拒绝。若该文件确实含拒绝行,说明你的 new-api 版本日志格式可能不同 —— 需更新 collector.go 的 rules 正则。")
		return 0
	}

	type kv struct {
		k string
		n int
	}
	rows := make([]kv, 0, len(counts))
	for k, n := range counts {
		rows = append(rows, kv{k, n})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].n > rows[j].n })
	fmt.Println("原因 | 模型 | 分组  →  次数:")
	for _, r := range rows {
		fmt.Printf("  %s  →  %d\n", r.k, r.n)
	}
	return 0
}
