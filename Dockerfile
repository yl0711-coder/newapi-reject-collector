# 多阶段构建:静态编译,运行镜像极小(scratch)。
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /collector .

FROM scratch
COPY --from=build /collector /collector
# 运行时挂载 new-api 日志目录(只读),并通过环境变量配置:
#   COLLECTOR_LOG_GLOB / COLLECTOR_SINK_URL / COLLECTOR_SINK_TOKEN / COLLECTOR_NODE
ENTRYPOINT ["/collector"]
