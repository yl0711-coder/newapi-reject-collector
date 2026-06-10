package main

import "testing"

func TestParseLine(t *testing.T) {
	cases := []struct {
		name             string
		line             string
		wantOK           bool
		wantModel, group string
	}{
		{
			name:      "真实无可用渠道(用户请求)",
			line:      `[ERR] 2026/05/12 - 22:14:54 | 202605121414547826593578268d9d6RD2gTzAZ | user 1 | No available channel for model claude-sonnet-4-5 under group AZ-Claude-CH1 (distributor)`,
			wantOK:    true,
			wantModel: "claude-sonnet-4-5",
			group:     "AZ-Claude-CH1",
		},
		{
			name:   "渠道健康测试噪音(必须排除)",
			line:   `[SYS] 2026/05/12 - 14:57:07 | channel test bad response: channel_id=1 name=AWS-CH1 type=1 model=claude-opus-4-7 status=503 err=bad response status code 503, message: No available channel for model claude-opus-4-7 under group aws (distributor) (request id: x), body: {}`,
			wantOK: false,
		},
		{
			name:   "普通成功/无关行",
			line:   `[INFO] 2026/05/12 - 15:13:03 | reqid | record error log: userId=1, channelId=2, modelName=claude-sonnet-4-6`,
			wantOK: false,
		},
		{
			name:      "分组名带空格也能抽",
			line:      `[ERR] 2026/06/10 - 10:00:00 | rid | user 7 | No available channel for model gpt-5.2 under group cq codex pro (distributor)`,
			wantOK:    true,
			wantModel: "gpt-5.2",
			group:     "cq codex pro",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reason, model, group, ok := parseLine(c.line)
			if ok != c.wantOK {
				t.Fatalf("ok=%v want %v", ok, c.wantOK)
			}
			if !ok {
				return
			}
			if reason != "no_available_channel" {
				t.Errorf("reason=%q", reason)
			}
			if model != c.wantModel {
				t.Errorf("model=%q want %q", model, c.wantModel)
			}
			if group != c.group {
				t.Errorf("group=%q want %q", group, c.group)
			}
		})
	}
}

func TestAgg(t *testing.T) {
	a := newAgg()
	now := int64(1_900_000_037) // 落在某分钟桶
	a.add(now, "no_available_channel", "gpt-5.2", "g1")
	a.add(now+5, "no_available_channel", "gpt-5.2", "g1") // 同分钟桶 → 累加
	a.add(now, "no_available_channel", "claude", "g2")

	s := a.drain()
	if len(s) != 2 {
		t.Fatalf("drain 应得 2 个桶,得 %d", len(s))
	}
	total := int64(0)
	for _, x := range s {
		total += x.Count
		if x.BucketTs%60 != 0 {
			t.Errorf("桶未对齐到分钟: %d", x.BucketTs)
		}
	}
	if total != 3 {
		t.Errorf("总计数应 3,得 %d", total)
	}
	if len(a.drain()) != 0 {
		t.Error("drain 后应清空")
	}

	// merge 回灌(推送失败重试)
	a.merge(s)
	if got := a.drain(); len(got) != 2 {
		t.Errorf("merge 后应有 2 桶,得 %d", len(got))
	}
}
