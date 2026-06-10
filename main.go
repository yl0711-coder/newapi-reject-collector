// Command newapi-reject-collector 是一个【通用】的 new-api 前置拒绝日志采集器。
//
// 它 tail new-api 的本地日志文件,把「无可用渠道」等【没进 logs 表】的前置拒绝
// 抽成 (分钟桶 × 原因 × 模型 × 分组) 计数,定期 POST 给中心监控汇总展示。
//
// 纯旁路、零侵入:只读日志文件,不碰 new-api、不连数据库。每个 new-api 节点部署一份。
//
// 针对 new-api v1.0.0-rc.4 的日志格式开发(见 README;日志格式随版本可能变化)。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	checkFile := flag.String("check", "", "校验模式:对给定日志文件跑解析并打印识别到的拒绝(不 tail、不推送),用于验证正则是否匹配你的 new-api 版本")
	flag.Parse()
	if *checkFile != "" {
		os.Exit(runCheck(*checkFile))
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg := loadConfig()
	if cfg.SinkURL == "" {
		slog.Error("缺少 COLLECTOR_SINK_URL(中心监控接收地址)")
		os.Exit(1)
	}
	slog.Info("采集器启动",
		"log_glob", cfg.LogGlob, "sink", cfg.SinkURL, "node", cfg.Node, "flush_s", cfg.FlushSeconds)

	a := newAgg()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// tail 日志 → 解析 → 累加
	go tailLoop(ctx, cfg.LogGlob, func(line string) {
		if reason, model, group, ok := parseLine(line); ok {
			a.add(time.Now().Unix(), reason, model, group)
		}
	})

	// 定期推送
	flushLoop(ctx, a, cfg)
	slog.Info("采集器退出")
}

// Config 全部从环境变量读取。
type Config struct {
	LogGlob      string // COLLECTOR_LOG_GLOB:要 tail 的日志(支持通配),默认 new-api 默认路径
	SinkURL      string // COLLECTOR_SINK_URL:中心监控接收地址(必填)
	SinkToken    string // COLLECTOR_SINK_TOKEN:推送鉴权 Bearer(可空)
	Node         string // COLLECTOR_NODE:本节点标识,默认 hostname
	FlushSeconds int    // COLLECTOR_FLUSH_SECONDS:聚合推送间隔,默认 60
}

func loadConfig() Config {
	node := env("COLLECTOR_NODE", "")
	if node == "" {
		node, _ = os.Hostname()
	}
	fs, _ := strconv.Atoi(env("COLLECTOR_FLUSH_SECONDS", "60"))
	if fs < 5 {
		fs = 60
	}
	return Config{
		LogGlob:      env("COLLECTOR_LOG_GLOB", "/app/logs/oneapi-*.log"),
		SinkURL:      env("COLLECTOR_SINK_URL", ""),
		SinkToken:    env("COLLECTOR_SINK_TOKEN", ""),
		Node:         node,
		FlushSeconds: fs,
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// flushLoop 每 FlushSeconds 取出聚合并推送;推送失败则合并回下批,不丢数据。
func flushLoop(ctx context.Context, a *agg, cfg Config) {
	cl := &http.Client{Timeout: 10 * time.Second}
	tick := time.NewTicker(time.Duration(cfg.FlushSeconds) * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			if s := a.drain(); len(s) > 0 { // 退出前最后冲一次
				_ = postSamples(cl, cfg, s)
			}
			return
		case <-tick.C:
			samples := a.drain()
			if len(samples) == 0 {
				continue
			}
			if err := postSamples(cl, cfg, samples); err != nil {
				slog.Warn("推送失败,合并回下批重试", "err", err, "samples", len(samples))
				a.merge(samples)
			} else {
				slog.Info("已推送", "samples", len(samples))
			}
		}
	}
}

func postSamples(cl *http.Client, cfg Config, samples []Sample) error {
	body, err := json.Marshal(map[string]any{"node": cfg.Node, "samples": samples})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, cfg.SinkURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.SinkToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.SinkToken)
	}
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("接收端返回 %d", resp.StatusCode)
	}
	return nil
}
