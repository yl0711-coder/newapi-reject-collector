# 多阶段构建:静态编译,运行镜像极小(scratch)。
FROM golang:1.26.6-alpine3.23 AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /collector .

# scratch 不带任何系统根证书。采集器访问 HTTPS 接收端时仍必须校验证书，
# 因此只复制公开 CA 信任包，不复制任何私钥或系统工具。
FROM alpine:3.23 AS certs
RUN apk add --no-cache ca-certificates

FROM scratch
COPY --from=build /collector /collector
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
# 运行时挂载 new-api 日志目录(只读),并通过环境变量配置:
#   COLLECTOR_LOG_GLOB / COLLECTOR_SINK_URL / COLLECTOR_SINK_TOKEN / COLLECTOR_NODE / COLLECTOR_CA_FILE
ENTRYPOINT ["/collector"]
