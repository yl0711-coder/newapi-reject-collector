// collector.go:解析 new-api 日志里的「前置拒绝」事件,按分钟聚合。
//
// 针对 new-api v1.0.0-rc.4 的日志格式(见 README)。日志格式随版本可能变化,
// 改正则只需动 rules。所有规则:捕获组 1 = 模型,捕获组 2 = 分组。
package main

import (
	"regexp"
	"strings"
	"sync"
)

// Sample 是一条按分钟聚合的拒绝计数,推送给中心监控。
type Sample struct {
	BucketTs int64  `json:"bucket_ts"` // 分钟桶(unix 秒,对齐到 60)
	Reason   string `json:"reason"`    // 拒绝原因,如 no_available_channel
	Model    string `json:"model"`     // 模型名
	Group    string `json:"group"`     // 分组(线路)
	Count    int64  `json:"count"`
}

// rule 一条日志匹配规则:正则 + 原因。正则必须 capture 组1=模型、组2=分组。
type rule struct {
	re     *regexp.Regexp
	reason string
}

// rules 是要识别的前置拒绝类型。MVP 只做「无可用渠道」(最痛、最干净)。
// 后续可加限流/余额/认证等(各加一条,确保组1=模型、组2=分组)。
var rules = []rule{
	// new-api v1.0.0-rc.4 distributor 拒绝行(真实用户请求,带 "| user N |"):
	//   [ERR] ... | <reqid> | user 1 | No available channel for model <M> under group <G> (distributor)
	// "| user N |" 前缀把它和「channel test bad response」健康测试噪音区分开。
	{
		re:     regexp.MustCompile(`\| user \d+ \| No available channel for model (\S+) under group (.+?) \(distributor\)`),
		reason: "no_available_channel",
	},
}

// parseLine 从一行日志抽出拒绝事件;非拒绝行返回 ok=false。
func parseLine(line string) (reason, model, group string, ok bool) {
	if strings.Contains(line, "channel test bad response") {
		return "", "", "", false // 后台渠道健康测试,不是真实用户拒绝,排除
	}
	for _, r := range rules {
		if m := r.re.FindStringSubmatch(line); m != nil {
			return r.reason, m[1], strings.TrimSpace(m[2]), true
		}
	}
	return "", "", "", false
}

// agg 是按 (分钟桶 × 原因 × 模型 × 分组) 的内存计数器。
type aggKey struct {
	bucket               int64
	reason, model, group string
}

type agg struct {
	mu      sync.Mutex
	data    map[aggKey]int64
	maxKeys int // 推送长期失败时的内存上限,超了丢弃保护(0=默认 1 万)
}

func newAgg() *agg { return &agg{data: map[aggKey]int64{}, maxKeys: 10000} }

// add 累加一次拒绝(now 为当前 unix 秒,内部对齐到分钟桶)。
func (a *agg) add(now int64, reason, model, group string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.data) >= a.maxKeys {
		return // 保护:中心长期不可达时不无限涨内存
	}
	a.data[aggKey{now / 60 * 60, reason, model, group}]++
}

// drain 取出并清空当前聚合。
func (a *agg) drain() []Sample {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]Sample, 0, len(a.data))
	for k, c := range a.data {
		out = append(out, Sample{BucketTs: k.bucket, Reason: k.reason, Model: k.model, Group: k.group, Count: c})
	}
	a.data = map[aggKey]int64{}
	return out
}

// merge 把推送失败的样本合并回来,下批重试(不丢数据)。
func (a *agg) merge(samples []Sample) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, s := range samples {
		if len(a.data) >= a.maxKeys {
			return
		}
		a.data[aggKey{s.BucketTs, s.Reason, s.Model, s.Group}] += s.Count
	}
}
