# TDengine 升级兼容性测试工具（CompatCheck）

## 概述

CompatCheck 是一个在单台机器上自动验证 TDengine **冷升级**和**滚动升级**兼容性的测试框架。它会在本机启动一个 3 节点集群，执行写入/查询/订阅负载，完成版本升级，然后验证数据完整性和资源保持情况。

### 入口

```bash
python -m run.main -F <基准版本目录> -T <目标版本目录> [选项]
```

---

## 环境要求

- **操作系统**：Linux（x86_64）
- **Python**：3.8+
- **依赖包**：`pip install taospy pyyaml`
- **版本目录**：两个已解压的 TDengine 安装目录，各自包含 `taosd` 和 `libtaos.so`

版本目录示例：

```
/opt/tdengine/
├── 3.3.8.0/
│   ├── taosd
│   └── libtaos.so
└── 3.4.0.8/
    ├── taosd
    └── libtaos.so
```

---

## 目录结构

```
CompatCheck/
├── run/
│   ├── main.py              # 主入口
│   └── reporter.py          # 结构化输出
├── server/
│   ├── clusterSetup.py      # 集群启动 / 停止
│   └── rollingUpgrade.py    # 滚动升级 / 冷升级执行器
├── resource/
│   ├── resourceManager.py   # DB / 超级表 / Topic / 索引 / TSMA / RSMA / Stream 管理
│   ├── verifier.py          # 升级后资源完整性验证
│   ├── userVerifier.py      # 用户权限验证
│   ├── sysinfo_checker.py   # INFORMATION_SCHEMA 快照与 diff
│   ├── whitelist_loader.py  # 白名单加载与过滤
│   └── priv_compat.py       # 跨版本权限语法兼容层
├── client/
│   ├── writer.py            # 后台写入 Worker
│   ├── querier.py           # 后台查询 Worker
│   └── subscriber.py        # 后台 TMQ 订阅 Worker
├── whitelist/               # INFORMATION_SCHEMA 变更白名单（YAML）
├── config.py                # 全局参数配置
└── config_lib.py            # libtaos.so 动态库加载工具
```

---

## 快速开始

```bash
cd test/tools/CompatCheck

# 冷升级（默认）
python -m run.main -F /opt/td/3.3.8.0 -T /opt/td/3.4.0.8

# 滚动升级（热升级）
python -m run.main -F /opt/td/3.3.8.0 -T /opt/td/3.4.0.8 -r

# 快速模式（CI 冒烟测试）
python -m run.main -F /opt/td/3.3.8.0 -T /opt/td/3.4.0.8 -r -q

# 带 INFORMATION_SCHEMA 检查
python -m run.main -F /opt/td/3.3.8.0 -T /opt/td/3.4.0.8 -r -S
```

---

## 命令行参数

| 参数 | 简写 | 是否必填 | 默认值 | 说明 |
|------|------|----------|--------|------|
| `--from-dir DIR` | `-F` | **必填** | — | 基准版本目录（含 `taosd` 和 `libtaos.so`） |
| `--to-dir DIR` | `-T` | **必填** | — | 目标版本目录 |
| `--path DIR` | `-p` | 可选 | `~/td_rolling_upgrade` | 集群数据和配置文件的工作目录 |
| `--fqdn HOST` | `-f` | 可选 | `socket.gethostname()` | 测试机的 FQDN |
| `--rollupdate` | `-r` | 可选 | 关闭（冷升级） | 开启滚动升级模式 |
| `--quick` | `-q` | 可选 | 关闭 | 快速模式：100 子表 × 1000 行，30 秒验证窗口 |
| `--check-sysinfo` | `-S` | 可选 | 关闭 | 升级后对比 `INFORMATION_SCHEMA` 列定义变化 |
| `--gen-whitelist [FILE]` | `-G` | 可选 | — | 生成白名单文件后退出，不执行负载测试 |
| `--whitelist-dir DIR` | — | 可选 | `<脚本根目录>/whitelist` | 白名单文件目录 |
| `--no-rsma` | — | 可选 | 关闭 | 跳过 RSMA 创建与验证 |
| `--no-tsma` | — | 可选 | 关闭 | 跳过 TSMA 创建与验证 |
| `--no-stream` | — | 可选 | 关闭 | 跳过 Stream 创建与验证 |
| `--no-user` | — | 可选 | 关闭 | 跳过 test_user 权限创建与验证 |

---

## 两种升级模式

### 冷升级（默认）

集群完全停止后再升级，侧重验证数据与资源完整性。

```
Phase 1  启动基准版本集群（3 节点）
Phase 2  创建测试资源（DB / 超级表 / Topic / 索引 / TSMA / RSMA / Stream / 用户权限）
Phase 3  写入初始数据
Phase 4  停止集群 → 逐节点替换二进制 → 重启集群（目标版本）
Phase 5  启动后台写入 / 查询 / 订阅负载
Phase 6  验证资源完整性 + 负载指标
```

冷升级期间集群停机，不验证升级过程中的持续写入/查询/订阅。

### 滚动升级（`-r`）

后台负载在升级全程持续运行，能捕获节点切换瞬间的服务中断。

```
Phase 1  启动基准版本集群（3 节点）
Phase 2  创建测试资源
Phase 3  写入初始数据
Phase 4  启动后台写入 / 查询 / 订阅负载
         逐节点滚动升级（停止 → 替换 → 重启 → 等待 ready）
Phase 5  升级完成后继续观察 30 秒（或 --quick 模式下 30 秒）
Phase 6  验证资源完整性 + 负载指标
```

---

## 检查项

| 检查项 | 冷升级 | 滚动升级 | 备注 |
|--------|:------:|:--------:|------|
| 升级过程本身 | ✅ | ✅ | 所有节点完成版本替换 |
| 升级期间持续写入 | — | ✅ | 冷升级期间集群停机 |
| 升级期间持续查询 | — | ✅ | 同上 |
| 升级期间持续订阅 | — | ✅ | 同上 |
| 升级后写入无失败批次 | ✅ | ✅ | |
| 升级后查询无失败批次 | ✅ | ✅ | |
| 升级后订阅无长时间中断 | ✅ | ✅ | 默认超时 180 秒 |
| 订阅消费行数 == 写入行数 | ✅ | ✅ | |
| test_user 可正常认证 | ✅ | ✅ | |
| test_user 权限集合不变 | ✅ | ✅ | |
| Tag Index 保持存在 | ✅ | ✅ | |
| TSMA 保持存在 | ✅ | ✅ | 基准版本 ≥ 3.3.6.0 时执行 |
| Stream 保持运行 | ✅ | ✅ | 基准版本 ≥ 3.3.7.0 时执行 |
| RSMA 保持存在 | ✅ | ✅ | 基准版本 ≥ 3.3.8.0 时执行 |
| INFORMATION_SCHEMA 无意外变化 | ✅ | ✅ | 需加 `-S` 启用；支持白名单豁免 |

---

## INFORMATION_SCHEMA 检查与白名单

### 开启检查

加 `-S` / `--check-sysinfo` 参数后，工具会在升级前后各拍一次 `INFORMATION_SCHEMA` 快照，对比列定义变化。若存在白名单之外的变化，测试判定失败。

```bash
python -m run.main -F /opt/td/3.3.8.0 -T /opt/td/3.4.0.8 -r -S
```

### 生成白名单

对新版本对首次运行时，先用 `-G` 生成白名单，审核后再正式测试：

```bash
# 生成白名单（不执行负载测试）
python -m run.main -F /opt/td/3.3.8.0 -T /opt/td/3.4.0.8 -G

# 审核生成的 whitelist/3.3.8.0~3.4.0.8.yaml 后，正式运行
python -m run.main -F /opt/td/3.3.8.0 -T /opt/td/3.4.0.8 -r -S
```

> `-G` 和 `-S` 互斥，不可同时使用。

### 白名单文件格式

白名单文件存放在 `whitelist/` 目录，文件名格式为 `{from_ver}~{to_ver}.yaml`，支持版本前缀匹配（如 `3.3~3.4.yaml` 覆盖所有 3.3.x.x → 3.4.x.x 升级）。多个匹配文件取并集。

```yaml
# whitelist/3.4.0.0~3.4.0.8.yaml
modified_tables:
- table: INS_DATABASES
  added_columns:
  - allow_drop
- table: INS_XNODE_AGENTS
  added_columns:
  - token
  - status
  deleted_columns:
  - scope
  changed_position:
  - col: create_time
    before: 3
    after: 4
```

支持的变更类型：`added_columns`、`deleted_columns`、`changed_type`、`changed_position`。

---

## 配置参数（config.py）

主要参数及默认值：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `DNODE_COUNT` | 3 | 集群节点数 |
| `MNODE_COUNT` | 3 | MNode 数量 |
| `BASE_PORT` | 6030 | dnode1 端口；dnode2=6130，dnode3=6230 |
| `SUBTABLE_COUNT` | 100 | 子表数量 |
| `INIT_ROWS_PER_SUBTABLE` | 10000 | 每子表初始行数 |
| `WRITE_BATCH_SIZE` | 5000 | 每次 INSERT 批量行数 |
| `WRITE_THREADS` | 20 | 初始化阶段并发写入线程数 |
| `VERIFY_DURATION_S` | 30 | 升级后观察窗口（秒） |
| `MAX_WRITE_LATENCY_S` | 2.0 | 写入延时上限（秒） |
| `MAX_QUERY_LATENCY_S` | 2.0 | 查询延时上限（秒） |
| `MAX_CONSECUTIVE_RETRIES` | 30 | 连续失败超过此次数计为 1 次真正失败 |
| `NODE_READY_TIMEOUT_S` | 180 | 等待单节点 ready 的最大时间（秒） |

快速模式（`-q`）会将子表数量改为 100、初始行数改为 1000、观察窗口改为 30 秒。

---

## 输出格式

工具使用结构化输出，每个阶段有 START/DONE 标记，最终打印 SUMMARY：

```
════════════════════════════════════════════════════════════════════════
  TDengine Rolling Upgrade Test
────────────────────────────────────────────────────────────────────────
  From version : 3.3.8.0
  To version   : 3.4.0.8
  Host (FQDN)  : myhost
  Cluster      : 3 DNODEs / 3 MNODEs / 3-replica
  Dataset      : 100 subtables × 10,000 rows
  Verify window: 30s
════════════════════════════════════════════════════════════════════════

  [10:01:23] Phase 1: Start base-version cluster
  ────────────────────────────────────────────────────────────────────  START
  ────────────────────────────────────────────────────────────────────  DONE  (12.3s)

  ...

════════════════════════════════════════════════════════════════════════
  TDengine Rolling Upgrade Test  ─  SUMMARY
────────────────────────────────────────────────────────────────────────
  Write latency ≤ 2s ......................................... [passed]
  Query latency ≤ 2s ......................................... [passed]
  Subscribe gap ≤ 2s ......................................... [passed]
  ...
────────────────────────────────────────────────────────────────────────
  Write latency max   : 0.123s
  Query latency max   : 0.087s
  Subscribe gap max   : 0.412s
────────────────────────────────────────────────────────────────────────
  Result  : PASS ✓
  Finished: 2026-03-19 10:08:45  (elapsed 7m 22s)
════════════════════════════════════════════════════════════════════════
```

---

## 退出码

| 退出码 | 含义 |
|--------|------|
| `0` | 全部检查通过 |
| `1` | 至少一项检查失败或发生错误 |
| `2` | 参数错误（缺少必填参数） |

失败时，SUMMARY 中会打印日志目录路径（`<path>/dnode*/log`）以便排查。

---

## 运行前清理

每次运行前需确保上次的集群进程和数据已清理：

```bash
pkill -9 taosd
rm -rf ~/td_rolling_upgrade   # 或 --path 指定的目录
```

---

## 常见问题

**Q: `ModuleNotFoundError: No module named 'taos'`**
A: `pip install taospy`

**Q: `--check-sysinfo` 失败，不知道如何编写白名单**
A: 先用 `-G` 生成候选白名单，审核后放入 `whitelist/` 目录即可。

**Q: `-G` 和 `-S` 可以同时使用吗？**
A: 不可以，两者互斥。`-G` 只做 schema 比对后写文件退出，不执行升级负载测试。

**Q: 白名单文件名版本前缀写多短合适？**
A: 建议先用精确四位版本号（如 `3.3.8.0~3.4.0.8.yaml`），确认无误后若需覆盖整个大版本升级路径，再改为 `3.3~3.4.yaml`。

**Q: 多个白名单文件都匹配时怎么处理？**
A: 所有匹配文件取并集，每个文件中允许的变化项都会被放行。
