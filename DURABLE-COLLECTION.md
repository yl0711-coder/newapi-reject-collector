# 持久拒绝日志采集（ECS 任务独立身份，默认关闭）

这是 ECS 自动扩缩容适配的 reject lane 实现。ECS 模式由 Monitor 仓库的 `ecslogagent`
绑定真实 Task/容器身份，通过私有 Unix socket 转发冻结批次，并使用任务外归档做退出补传。
所有 ECS 开关默认关闭；未设置 `COLLECTOR_STATE_PATH` 时，Lightsail 原采集行为完全不变。

## 协议与状态

- 新模式从匹配日志的开头或持久游标继续读，检查所有匹配文件，不只读最新文件。仅支持追加式日志；完整换名轮转按设备/inode 和读取位置指纹续读。
- 每次最多处理 1000 行、读取 4 MiB；单行上限 256 KiB，已跟踪文件上限 4096，状态上限 8 MiB。达到限制报错并保留状态，不丢弃后伪报成功。
- 先把“新游标 + 完整冻结批次”写入同一个私有 checkpoint，文件 fsync、原子 rename、目录 fsync 成功后才发送。只有一个 pending 批次；网络失败时暂停继续读取，而不是让内存队列无限增长。
- 重启从 checkpoint 恢复同一 ID、同一正文；HTTP 409 不改编号。一次只允许一个进程持有 checkpoint 文件锁。
- 新接收地址是 `/internal/rejections/v2`。ACK 必须匹配 node、batch_id、完整正文 SHA-256、协议版本、accepted 数量和 rejected=0。HTML 登录页、重定向、旧版不完整 ACK 都不能清除 pending。
- Monitor 在同一个事务写事实和去重凭据。v1 仍使用原有规范化行哈希；v2 对完整冻结正文做哈希。同号异正文仍返回 409。
- 成功 ACK 后本地清理状态如果落盘失败，进程报错退出。原 pending 保留或已原子清除，两种重启结果都安全；不能继续使用不确定的内存状态。
- 时间使用日志中 `[ERR] YYYY/MM/DD - HH:MM:SS` 与明确配置的源时区；无法识别时间的拒绝行会停止并保留游标，不能把补传记到采集当前时间。原解析器支持的拒绝类型没有扩大，不冒充“全部错误日志”。
- v2 自动重试窗口是批次生成后的 24 小时；Monitor 拒绝过期批次且不清除采集器状态。去重凭据至少保留 48 小时，即使分钟事实配置只保留一天，仍覆盖重试和时钟偏差。超期应走受控修复，不得改 `created_at`、ID 后重新发。

## 配置合同（不是生产启用指令）

| 配置 | 要求 |
| --- | --- |
| `COLLECTOR_STATE_PATH` | 独立绝对路径，例如 `/collector-state/<source>/checkpoint.json`；不能与源日志同目录 |
| `COLLECTOR_LOG_GLOB` | 只读源日志绝对路径，必须包含轮转后待补传的文件 |
| `COLLECTOR_LOG_TIMEZONE` | 明确的真实源时区，例如 `UTC` 或 `Asia/Shanghai`，不是凭感觉选择；scratch 镜像已内嵌时区数据 |
| `COLLECTOR_SINK_URL` | HTTPS，路径以 `/internal/rejections/v2` 结尾；不允许 URL 内凭证、query 或 fragment |
| `COLLECTOR_SINK_TOKEN` | Lightsail 持久模式才由部署配置；ECS 模式由 `ecslogagent` 每个任务注入短期私有令牌，不手工共享 |
| `COLLECTOR_NODE` | 独立来源名；本补丁不会根据自报 Task ID 自动授予权限 |
| `COLLECTOR_FLUSH_SECONDS` | 新模式网络失败后的重试间隔，5～3600 秒；有进展时分批追赶，空闲时低频轮询 |

先升级 Monitor 接收端，再在隔离验证环境启用新采集模式。**不要直接对已有 Lightsail 历史日志启用新模式**：新状态从头读取，可能与旧方式已经接收的记录重叠。切换必须有明确边界及新身份配置。

## 故障与边界

- 截断、已读取位置指纹变化、检测到仍有未读字节的文件消失、checkpoint 损坏、来源配置变化、满盘或写入失败：报错保留原状态，不能自动清空或重置。
- 采集器错误会在自身日志和 Monitor ECS 来源状态中显示；心跳正常只代表采集存活，不伪装成业务覆盖完整。
- 支持进程崩溃恢复。Fargate 任务销毁前通过停机收尾和任务外归档保留冻结批次；
  未取得可验证归档回执的批次不会清除本地 pending。
- 不保证强杀前尚未封存的尾部、不在 glob 中的已轮转文件、发现前已被清理的日志、被原地修改但无法通过读取位置指纹识别的历史内容完整。生产日志保留和外部持久化不能用代码重试替代。
- 文件清单不自动删除旧游标，以免重新发现旧文件后二次采集。达到 4096 个来源文件时需要受控归档治理，不通过清空 checkpoint 处理。

## 回滚

默认未启用不涉及采集数据迁移。隔离启用后，先停止采集器并保留 checkpoint/源日志，确认 pending 已处理或交接，再回退镜像。不能简单取消 `COLLECTOR_STATE_PATH` 让旧模式跳到 EOF，否则会放弃未确认数据。旧 Monitor 不识别 v2 接收口，回滚接收端前必须保留 v2 批次恢复能力。

## 本地复验

```sh
GOTOOLCHAIN=go1.26.6 go test -race -count=3 ./...
GOTOOLCHAIN=go1.26.6 go vet ./...
```

Monitor 仓库还提供 `TestRejectCollectorProcessRestartAgainstRealMonitor`：需显式设置 `MONITOR_TEST_REJECT_COLLECTOR_BIN` 为本地编译的采集器绝对路径。测试使用临时 TLS 服务、临时 SQLite、合成日志及清空后的子进程环境，不连 AWS 或线上。

## 生产发布边界

`Dockerfile.ecs-isolated` 同时适用于隔离验收和受控生产发布，但必须显式传入已扫描的
`ECS_AGENT_IMAGE@sha256:...`，不默认下载、不接受 `latest`。Monitor 先部署兼容接收端，但保持
ECS 生产双开关关闭；再创建精确 Task Definition 允许列表和归档边界，先单任务验收，最后才滚动更新。
发布不得更改 NewAPI/Nginx 镜像、业务环境变量、ALB 流量或数据库结构。
