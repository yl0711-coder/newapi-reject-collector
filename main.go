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
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
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
	cl, err := newHTTPClient(cfg)
	if err != nil {
		slog.Error("初始化 HTTPS 客户端失败", "err", err)
		os.Exit(1)
	}
	slog.Info("采集器启动",
		"log_glob", cfg.LogGlob, "sink", cfg.SinkURL, "node", cfg.Node, "flush_s", cfg.FlushSeconds,
		"ca_file", cfg.CAFile)

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
	flushLoop(ctx, a, cfg, cl)
	slog.Info("采集器退出")
}

// Config 全部从环境变量读取。
type Config struct {
	LogGlob      string // COLLECTOR_LOG_GLOB:要 tail 的日志(支持通配),默认 new-api 默认路径
	SinkURL      string // COLLECTOR_SINK_URL:中心监控接收地址(必填)
	SinkToken    string // COLLECTOR_SINK_TOKEN:推送鉴权 Bearer(可空)
	Node         string // COLLECTOR_NODE:本节点标识,默认 hostname
	FlushSeconds int    // COLLECTOR_FLUSH_SECONDS:聚合推送间隔,默认 60
	CAFile       string // COLLECTOR_CA_FILE:额外信任的 PEM 根证书路径(可空)
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
		CAFile:       env("COLLECTOR_CA_FILE", ""),
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// newHTTPClient 创建推送客户端。CAFile 仅用于追加一个私有 CA，绝不跳过 TLS 校验。
func newHTTPClient(cfg Config) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.CAFile != "" {
		pemData, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("读取 COLLECTOR_CA_FILE %q: %w", cfg.CAFile, err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if ok := roots.AppendCertsFromPEM(pemData); !ok {
			return nil, fmt.Errorf("COLLECTOR_CA_FILE %q 不含有效 PEM 证书", cfg.CAFile)
		}
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{}
		} else {
			transport.TLSClientConfig = transport.TLSClientConfig.Clone()
		}
		transport.TLSClientConfig.RootCAs = roots
	}
	return &http.Client{Timeout: 10 * time.Second, Transport: transport}, nil
}

type sampleBatch struct {
	ID      string
	Samples []Sample
}

func newBatchID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func takeBatch(a *agg) (*sampleBatch, error) {
	samples := a.drain()
	if len(samples) == 0 {
		return nil, nil
	}
	id, err := newBatchID()
	if err != nil {
		a.merge(samples)
		return nil, err
	}
	return &sampleBatch{ID: id, Samples: samples}, nil
}

// flushOne 只在没有待确认批次时取新数据。网络失败（包括服务端已落库但响应丢失）
// 时保留原 batch_id 原样重试，配合中心批次台账实现 exactly-once 累加。
func flushOne(a *agg, pending *sampleBatch, cfg Config, cl *http.Client) (*sampleBatch, error) {
	if pending == nil {
		var err error
		pending, err = takeBatch(a)
		if err != nil || pending == nil {
			return pending, err
		}
	}
	if err := postSamples(cl, cfg, pending.ID, pending.Samples); err != nil {
		return pending, err
	}
	slog.Info("已推送", "samples", len(pending.Samples), "batch_id", pending.ID)
	return nil, nil
}

// flushLoop 每 FlushSeconds 取出聚合并推送；推送失败保留同一批次下次重试。
// 新到事件继续留在 agg 中，不会和未确认批次混合后换 ID，避免响应丢失造成重复计数。
func flushLoop(ctx context.Context, a *agg, cfg Config, cl *http.Client) {
	tick := time.NewTicker(time.Duration(cfg.FlushSeconds) * time.Second)
	defer tick.Stop()
	var pending *sampleBatch
	for {
		select {
		case <-ctx.Done():
			// 退出前最多冲两批：先完成既有待确认批次，再发送停机前新积累的一批。
			for attempts := 0; attempts < 2; attempts++ {
				var err error
				pending, err = flushOne(a, pending, cfg, cl)
				if err != nil || pending != nil {
					break
				}
			}
			return
		case <-tick.C:
			var err error
			pending, err = flushOne(a, pending, cfg, cl)
			if err != nil {
				count := 0
				if pending != nil {
					count = len(pending.Samples)
				}
				slog.Warn("推送失败,保留原批次重试", "err", err, "samples", count)
			}
		}
	}
}

func postSamples(cl *http.Client, cfg Config, batchID string, samples []Sample) error {
	if batchID == "" {
		return fmt.Errorf("batch_id 不能为空")
	}
	body, err := json.Marshal(map[string]any{"node": cfg.Node, "batch_id": batchID, "samples": samples})
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
