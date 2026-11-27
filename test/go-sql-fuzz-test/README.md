# go-sql-fuzz-test

基于 Go 的 TDengine SQL 解析与 Fuzz 测试仓库，当前包含两个核心子项目：

- `sqlparse`：TDengine SQL 解析器（GoYacc 语法 + AST + 大量语法回归测试）
- `tdsqlsmith`：SQL 生成、执行、覆盖跟踪、崩溃检测与回放工具

## 仓库结构

```text
.
├── README.md
├── sqlparse/      # SQL parser 模块（module: sqlparser）
└── tdsqlsmith/    # Fuzz runner 模块（module: tdsqlsmith）
```

## 1. sqlparse（解析器）

目标：让 Go 版本解析器与 `lemon/sql.y` 的语法/行为保持一致，为后续生成与执行提供稳定 parse gate。

关键目录：

- `sqlparse/td_sql.y`：GoYacc 语法主入口
- `sqlparse/sql.go`：由 `goyacc` 生成
- `sqlparse/stmt_*.go`、`sqlparse/expr_*.go`：AST 节点实现
- `sqlparse/testdata/sql_corpus/`：语料与矩阵测试数据

常用命令（在 `sqlparse/` 下执行）：

```bash
# 重新生成解析器
/tmp/bin/goyacc -o sql.go -v y.output td_sql.y

# 运行测试
go test ./... -count=1
```

说明：`lemon/` 目录作为基线，不应直接修改。

## 2. tdsqlsmith（Fuzz 测试引擎）

目标：面向 TDengine 执行 SQL 生成与稳定性测试，输出运行报告并支持 Web 查看与失败重放。

核心子命令：

- `run`：执行 fuzz 任务并输出 `run_report.json`
- `serve`：启动 API + Console
- `replay`：重放失败样例

关键目录：

- `tdsqlsmith/cmd/tdsqlsmith/`：CLI 入口
- `tdsqlsmith/internal/run/`：运行主流程
- `tdsqlsmith/internal/report/`：报告结构与写入
- `tdsqlsmith/internal/serve/`：后端 API 与静态资源服务
- `tdsqlsmith/web/console/`：前端控制台（Vue3 + TS）

## 环境要求

- Go
  - `sqlparse`：Go 1.25+
  - `tdsqlsmith`：Go 1.26+
- Node.js + npm（构建 `tdsqlsmith` 前端）
- 可访问的 TDengine 实例（非 `--dry-run` 场景）

## 快速开始

### A. 仅验证解析器

```bash
cd sqlparse
go test ./... -count=1
```

### B. 构建 tdsqlsmith

```bash
cd tdsqlsmith
make init
make build
```

构建产物：

- `tdsqlsmith/bin/tdsqlsmith`

### C. 运行一次 fuzz

前置约束：

- 不能存在由 `systemd` 启动并托管的 `taosd`（如 `systemctl start taosd`）。
- `tdsqlsmith` 会以子进程方式拉起并管理 `taosd`，若系统中已有 `systemd` 管理实例，可能导致端口/进程管理冲突。

```bash
cd tdsqlsmith
TDSQLSMITH_BIN=./bin/tdsqlsmith ./run_parent_child_test.sh 10m
```

常用参数：

- `DSN`：覆盖默认连接串，例如 `DSN="root:taosdata@tcp(127.0.0.1:6030)/"`
- `STMT_TIMEOUT`：单条 SQL 超时（默认 `2s`）
- `MUTATION_LEVEL`：变异强度（默认 `1`）
- `EXEC_PROFILE`：执行策略 `strict|balanced|aggressive`（默认 `balanced`）
- `CHILD_CASES`：生成条数上限（默认 `1000000000`）

## 常用命令索引

```bash
# tdsqlsmith：查看帮助
./bin/tdsqlsmith --help

# 启动 Web 服务
./bin/tdsqlsmith serve --listen :8080
```

辅助脚本（在 `tdsqlsmith/`）：

- `run_parent_child_test.sh <duration>`：长时运行包装脚本
- `run_web_service.sh [start|stop|status|restart] [--daemon]`：Web 服务管理

## 进一步阅读

- `sqlparse/README.md`
- `tdsqlsmith/README.md`
