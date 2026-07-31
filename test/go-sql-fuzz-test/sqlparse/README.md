# sqlparser

Go 版本 SQL 解析器项目，目标是与 `lemon/sql.y` 语法和语义行为保持一致。

## 环境要求

- Go 1.25+
- CMake 3.16+（用于统一测试入口）
- `goyacc`（默认使用 `/tmp/bin/goyacc`，可通过 `GOYACC` 覆盖）

## 目录结构

- `td_sql.y`：GoYacc 语法源文件（主编辑入口）
- `sql.go`：由 `goyacc` 生成的解析器
- `y.output`：GoYacc 分析输出
- `lexer.go`、`keyword.go`：词法与关键字
- `stmt_*.go`、`expr_*.go`：Statement / Expr 实现
- `lemon/`：Lemon 参考语法基线（禁止修改）
- `tool/migrate/`：语法对齐与报告工具
- `reports/`：对齐/覆盖率报告

## 常用命令

### 生成解析器

```bash
/tmp/bin/goyacc -o sql.go -v y.output td_sql.y
```

### 运行全部测试（Go）

```bash
GOCACHE=/tmp/gocache GOMODCACHE=/tmp/gomodcache go test ./... -count=1
```

### 一条命令运行全部测试（CMake）

首次配置：

```bash
cmake -S . -B build
```

之后统一执行：

```bash
cmake --build build --target test-all
```

### 对齐与门禁

```bash
make validate-parity
make query-coverage
make statement-diff
make statement-branch-gate
make statement-roundtrip-gate
```

完整硬门禁：

```bash
make parser-hard-gate
```

## 开发约束

- 不修改 `lemon/` 目录。
- 修改语法后必须重新生成 `sql.go` 和 `y.output`。
- 每次语法变更都应补测试并执行全量回归。

