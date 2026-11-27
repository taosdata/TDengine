# tdsqlsmith 使用说明

## 1. 项目概览

`tdsqlsmith` 用于面向 TDengine 的 SQL 生成与稳定性测试，提供三类入口：

- `run`: 执行测试任务并产出 `run_report.json`
- `serve`: 启动 API + Console 服务
- `replay`: 重放失败样例

当前版本已将语料和规则内置到程序中（`internal/corpusdata`），运行时不再依赖外部 `sqlparse` 仓库目录。

## 2. 项目目录与作用

| 目录/文件 | 作用 |
|---|---|
| `cmd/tdsqlsmith/` | 命令行入口，解析参数并分发到 `run/replay/serve`。 |
| `internal/run/` | 核心运行流程（任务执行、覆盖统计、报告写入、崩溃处理）。 |
| `internal/serve/` | Web 服务层，提供 API 与前端静态资源服务。 |
| `internal/queryrules/` | 查询规则目录解析与规则命中跟踪。 |
| `internal/branchmodel/` | 分支用例类型定义与覆盖模型。 |
| `internal/corpusdata/` | 内置语料与语法文件（通过 `go:embed` 编译进程序）。 |
| `internal/report/` | 运行报告数据结构与读写。 |
| `internal/crashguard/` | 崩溃保护、快照与故障上下文记录。 |
| `web/console/` | 前端控制台源码（Vue3 + TypeScript）。 |
| `internal/serve/webdist/` | 前端构建产物目录（供后端嵌入，默认不提交生成文件）。 |
| `run_parent_child_test.sh` | 长时运行脚本，统一参数并产出会话日志/报告。 |
| `run_web_service.sh` | Web 服务启停与状态管理脚本。 |
| `Makefile` | 统一的初始化、构建、打包命令入口。 |
| `bin/` | 本地编译产物和打包文件输出目录。 |
| `out/` | 运行时报告与日志输出目录。 |

## 3. 快速开始

### 3.1 初始化依赖

```bash
make init
```

### 3.2 构建

```bash
make build
```

构建结果：

- 后端二进制：`bin/tdsqlsmith`
- 前端静态资源：`internal/serve/webdist/`（用于 `go:embed`）

### 3.3 打包分发

```bash
make package
```

输出：

- `bin/tdsqlsmith-<timestamp>.tar.gz`
- 包内包含：`tdsqlsmith`、`run_parent_child_test.sh`、`run_web_service.sh`

## 4. 命令行用法

```bash
tdsqlsmith run [flags]
tdsqlsmith serve [flags]
tdsqlsmith replay [flags]
```

可执行：

```bash
./bin/tdsqlsmith --help
```

## 5. run_parent_child_test.sh

该脚本用于快速启动一次带统一参数的 `run` 任务，并把输出集中到单独目录。

### 5.1 用法

```bash
./run_parent_child_test.sh <duration>
```

示例：

```bash
./run_parent_child_test.sh 30s
./run_parent_child_test.sh 10m
./run_parent_child_test.sh 2h
```

### 5.2 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `TDSQLSMITH_BIN` | `${ROOT_DIR}/tdsqlsmith` | 二进制路径或命令名 |
| `DSN` | `root:taosdata@tcp(127.0.0.1:6030)/` | 连接串 |
| `STMT_TIMEOUT` | `2s` | 单条 SQL 超时 |
| `MUTATION_LEVEL` | `1` | SQL 变异强度 |
| `EXEC_PROFILE` | `balanced` | 执行策略（`strict/balanced/aggressive`） |
| `CHILD_CASES` | `1000000000` | 生成条数上限 |

### 5.3 固定附加参数

脚本会固定传入：

- `--cleanup-success-run-dir=true`
- `--stop-when-covered=false`
- `--verbose`

### 5.4 产物路径

- `out/pc_YYYYMMDD_HHMMSS/parent_child.log`
- `out/pc_YYYYMMDD_HHMMSS/run_report.json`

## 6. run_web_service.sh

用于启动和管理 `tdsqlsmith serve`。

### 6.1 用法

```bash
./run_web_service.sh [start|stop|status|restart] [--daemon]
```

### 6.2 常用示例

```bash
# 前台启动
./run_web_service.sh

# 后台启动
./run_web_service.sh start --daemon

# 查看状态
./run_web_service.sh status

# 停止
./run_web_service.sh stop
```

### 6.3 主要环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `TDSQLSMITH_BIN` | `tdsqlsmith` | 二进制路径或命令名 |
| `LISTEN` | `0.0.0.0:18080` | 监听地址 |
| `API_TOKEN` | `tdsqlsmith-dev-token` | API token |
| `DATA_DIR` | `$(pwd)/data` | 服务状态目录 |
| `OUT_DIR` | `$(pwd)/out` | 报告目录 |
| `ALLOW_ORIGIN` | `*` | CORS 来源 |
| `LOG_FILE` | `$(pwd)/tdsqlsmith-web.log` | 后台日志文件 |
| `PID_FILE` | `$(pwd)/tdsqlsmith-web.pid` | 后台 PID 文件 |

## 7. 测试命令

```bash
# 默认全量测试
go test ./...

# 含 integration tag 的测试
go test -tags=integration ./...
```

## 8. 说明

- `internal/serve/webdist` 下的前端编译产物不建议入库；仓库仅保留占位文件用于 `go:embed` 编译。
- 若 `run` 无法连接数据库，请先确认 `taosd` 处于可用状态并且 DSN 正确。
