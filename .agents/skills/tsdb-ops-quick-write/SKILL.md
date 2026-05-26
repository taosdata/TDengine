---
name: tsdb-ops-quick-write
description: "使用 taosBenchmark 快速生成测试数据并写入 TDengine。适用于需要快速造数据、压测写入、验证数据库功能、从源码编译 taosBenchmark 等场景。触发关键词：taosBenchmark, 写入测试数据, 造数据, 压测写入, benchmark, generate test data, insert test data, quick write, 编译 taosBenchmark, build taosBenchmark"
metadata:
  author: yangzy
  version: 1.0.0
  owner_team: engine
---

# taosBenchmark 快速写入数据

## When to use

- 需要快速向 TDengine 创建数据库并写入测试数据
- 开发/测试环境需要造数据验证功能
- 需要模拟持续写入（实时写入场景）
- 压测写入性能
- 需要从源码编译 taosBenchmark（见 `references/build-macos-apple-silicon.md`）

## Input

| 参数 | 说明 | 是否必填 | 默认值 |
|------|------|----------|--------|
| 数据库名 | 写入的目标数据库 | 否 | `test` |
| 子表数量 | 需要创建的子表数 | 否 | `10000` |
| 每表行数 | 每个子表写入的数据行数 | 否 | `10000` |
| 写入线程数 | 并行写入的线程数 | 否 | `8`（taosBenchmark 默认值） |
| 是否追加 | 是否保留已有表，追加写入 | 否 | 否（默认重建） |
| 是否实时模拟 | 是否模拟实时持续写入 | 否 | 否 |

如果用户未指定参数，先询问关键参数（数据库名、数据规模），其余使用默认值。

## Output

- 生成 taosBenchmark 命令并执行
- 输出写入结果（行数、耗时、速率）

## 常用命令模板

### 1. 最简写入（使用全部默认参数）

```bash
taosBenchmark -y
```

创建默认数据库 `test`，10000 子表，每表 10000 行。

### 2. 自定义数据库和数据量

```bash
taosBenchmark -y -d <数据库名> -t <子表数> -n <每表行数> -T <线程数>
```

示例：创建数据库 `abc`，1 张子表，写入 1 万行：

```bash
taosBenchmark -y -T 1 -d abc -n 10000 -t 1 --time-step 1000
```

### 3. 追加写入（不重建表）

使用 `--nodrop` 保留已有表结构和数据，追加写入新数据：

```bash
taosBenchmark -y -T 1 -d abc -n 10000 -t 1 \
  --time-step 1000 --nodrop --start-timestamp 1600000000000
```

注意：追加写入时需指定 `--start-timestamp` 避免时间戳冲突。

### 4. 模拟实时持续写入

从当前时刻开始，每秒写入指定行数：

```bash
taosBenchmark -y -T 1 -d abc -n 10000 -t 1 \
  --time-step 10 --interlace-rows 100 --insert-interval 1000 \
  --start-timestamp $(date +%s%3N) --nodrop
```

### 5. 写入远程 TDengine

使用 `-h` 指定远程地址。如果本地未安装 TDengine 客户端库，需用 `-I rest` 走 REST 接口（通过 6041 端口）：

```bash
taosBenchmark -y -h <远程IP> -d abc -t 1 -n 100 -T 1 -I rest
```

## 关键参数说明

| 参数 | 说明 |
|------|------|
| `-y` | 跳过交互确认 |
| `-d` | 数据库名 |
| `-t` | 子表数量 |
| `-n` | 每个子表的行数 |
| `-T` | 写入线程数 |
| `-h` | 远程 TDengine 服务器地址 |
| `-I` | 连接模式：`taosc`（默认，需本地客户端库）/ `rest`（REST 接口，无需客户端库） |
| `--time-step` | 相邻行时间戳间隔（毫秒） |
| `--interlace-rows` | 一次批量写入的行数 |
| `--insert-interval` | 两次批量写入之间的等待时间（毫秒） |
| `--nodrop` | 不删除已有数据库/表，追加写入 |
| `--start-timestamp` | 起始时间戳（毫秒），追加写入时必须指定 |

## Safety

- 默认行为会 **删除并重建** 同名数据库，执行前应确认目标数据库名称
- 使用 `--nodrop` 可避免误删已有数据
- 本技能仅适用于开发/测试环境，**禁止在生产环境执行**
- 编译时的 `rm -rf debug .externals` 操作仅清理构建缓存，执行前确认当前在 TDengine 源码目录
- 不涉及密钥、凭据等敏感信息

## References

- `references/build-macos-apple-silicon.md` — 在 macOS Apple Silicon 上从源码编译 taosBenchmark 的完整步骤与常见问题

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-ops-quick-write version=1.0.0 author=yangzy`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
