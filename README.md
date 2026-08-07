# newapi-reject-collector

> **new-api 前置拒绝日志采集器** —— 把「无可用渠道」等**没进 `logs` 表**的前置拒绝,从日志里抽出来、按分钟聚合、推给中心监控。

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## 这是干什么的

[new-api](https://github.com/QuantumNous/new-api) 处理请求分两段:**先前置检查(鉴权、选渠道、限流、余额)→ 选中渠道后才调用上游并写 `logs` 表**。

所以「**前置就被拒**」的请求(典型:**无可用渠道**)**根本不进 `logs` 表**——后台和任何读 `logs` 的监控都看不到,只在 new-api 的**本地日志文件**里打一行。用户却实实在在收到了 503。

本采集器**纯旁路 tail 那个日志文件**,把这些拒绝抽成 `(分钟桶 × 原因 × 模型 × 分组)` 计数,定期 `POST` 给中心监控展示/告警。**不改 new-api、不连数据库、零侵入。**

```
new-api 本地日志 ──tail+正则──► 采集器 ──按分钟聚合──► POST ──► 中心监控(展示「被拒请求」+ 告警)
```

每个跑 new-api 的节点部署一份(拒绝日志是各节点本地的)。

## ⚠️ 版本兼容(重要)

**本采集器针对 new-api `v1.0.0-rc.4` 的日志格式开发并测试。** 日志文案随 new-api 版本可能变化,届时只需调整 `collector.go` 里的 `rules` 正则。

当前锁定识别的日志行格式(`reason=no_available_channel`):
```
[ERR] <时间> | <reqid> | user <N> | No available channel for model <模型> under group <分组> (distributor)
```
- 正则:`\| user \d+ \| No available channel for model (\S+) under group (.+?) \(distributor\)`(组1=模型,组2=分组)。
- **会主动排除** `channel test bad response` 开头的行(那是后台渠道健康测试,不是真实用户拒绝)。

> 升级 new-api 后,请抽查几行真实拒绝日志确认正则仍匹配;不匹配就更新 `rules`(每条规则:捕获组1=模型、组2=分组)。

## 配置(环境变量)

| 变量 | 说明 | 默认 |
|---|---|---|
| `COLLECTOR_SINK_URL` | 中心监控接收地址,如 `http://monitor:8090/internal/rejections` | **必填** |
| `COLLECTOR_LOG_GLOB` | 要 tail 的 new-api 日志(支持通配,跟随最新文件 + 轮转) | `/app/logs/oneapi-*.log` |
| `COLLECTOR_SINK_TOKEN` | 推送鉴权 Bearer Token(中心校验);留空=不带 | 留空 |
| `COLLECTOR_NODE` | 本节点标识(展示用,如 `master`/`slave`) | 主机名 |
| `COLLECTOR_FLUSH_SECONDS` | 聚合推送间隔(秒,≥5) | `60` |
| `COLLECTOR_CA_FILE` | 额外信任的 PEM 根证书文件。用于 HTTPS 接收端使用私有 CA 的场景；证书应只读挂载到容器。 | 留空(仅内置公共根证书) |

镜像内置标准公共根证书包，因此 Let’s Encrypt、Cloudflare 等公开 HTTPS 证书无需额外配置。仅当接收端使用私有 CA 时，挂载其**根证书**（不是私钥），并设置 `COLLECTOR_CA_FILE`。采集器会把它追加到信任链；不会关闭或跳过 TLS 证书校验：

```bash
-v /opt/nexusapi/deploy/certs/monitor-internal-ca.crt:/etc/collector/monitor-ca.crt:ro \
-e COLLECTOR_CA_FILE=/etc/collector/monitor-ca.crt
```

## 推送协议

每 `FLUSH_SECONDS` 向 `COLLECTOR_SINK_URL` POST:
```jsonc
{
  "node": "slave",
  "batch_id": "0f45d34c44e34e34998cf64d246e45fa",
  "samples": [
    {"bucket_ts": 1781234560, "reason": "no_available_channel",
     "model": "claude-sonnet-4-5", "group": "AZ-Claude-CH1", "count": 5}
  ]
}
```
- `bucket_ts`:分钟桶(unix 秒,对齐 60)。
- `batch_id`:每个待确认批次的唯一 ID。网络失败时原样复用，中心按节点 + 批次去重，避免“中心已写入但响应丢失”导致重复计数。
- 推送失败会保留原批次重试；期间的新事件留在下一批，不丢数据(中心短暂不可达无碍)。长期不可达时仍有内存上限保护。

升级时请**先升级采集器，再升级中心 Monitor**：旧 Monitor 会忽略新增的 `batch_id`，而新版 Monitor 会要求该字段；这个顺序不会产生采集空窗。

## 部署(Docker,每个 new-api 节点一份)

```bash
docker run -d --name newapi-reject-collector \
  -v /opt/nexusapi/deploy/data/logs:/app/logs:ro \   # 挂 new-api 日志目录(只读)
  -e COLLECTOR_SINK_URL='http://<监控地址>/internal/rejections' \
  -e COLLECTOR_SINK_TOKEN='<与监控约定的 token>' \
  -e COLLECTOR_NODE='slave' \
  ghcr.io/yl0711-coder/newapi-reject-collector:latest
```
- `:ro` 只读挂载,采集器只读日志、绝不写。
- 资源极小(纯 stdlib、scratch 镜像、内存个位数 MB),2C2G 无压力。

## 测试 / 升级后自检

```bash
go test ./...                          # 单元测试(解析 + 聚合)

# 校验模式:对一个真实日志文件跑解析,打印识别到的拒绝(不 tail、不推送)
./collector -check /path/to/oneapi-xxx.log
```
`-check` 是**验证正则是否匹配你的 new-api 版本**的工具:它扫描文件、打印识别到的 `原因 | 模型 | 分组 → 次数`。
- 升级 new-api 后,拿一个**确实发生过拒绝**的日志文件跑 `-check`,若识别到拒绝且模型/分组正确 → 正则仍有效;若显示"没匹配到" → 日志格式变了,需更新 `collector.go` 的 `rules`。
- 部署前也用它确认你的日志路径/格式对得上。

> 示例(对真实日志):`扫描 200 行,识别到 159 条前置拒绝` → 按 `模型 | 分组` 列出各自次数。

## 构建

```bash
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o collector .   # 二进制
docker build -t newapi-reject-collector .                          # 镜像
```

## 安全

- **只读**日志文件,不写、不改、不连任何库。
- 日志拒绝行只含 模型/分组/用户id 等路由信息,**不含密钥**;采集器也只抽 模型/分组/原因/计数 推出。
- 与中心监控间用 `COLLECTOR_SINK_TOKEN` 鉴权,建议同时限制接收接口仅内网可达。
- 镜像内置公共 CA 信任包；HTTPS 使用私有 CA 时通过 `COLLECTOR_CA_FILE` 显式信任其根证书；**禁止**以 `InsecureSkipVerify` 等方式绕过证书校验。

## License

[MIT](LICENSE)
