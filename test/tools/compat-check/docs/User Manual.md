# TDengine 升级兼容性测试工具 — 用户使用手册

## 目录

1. [工具概述](#1-工具概述)
2. [环境要求](#2-环境要求)
3. [目录结构](#3-目录结构)
4. [快速开始](#4-快速开始)
5. [命令行参数](#5-命令行参数)
6. [测试流程详解](#6-测试流程详解)
   - [冷升级（默认）](#61-冷升级默认)
   - [滚动升级](#62-滚动升级)
7. [INFORMATION_SCHEMA 检查](#7-information_schema-检查)
   - [开启检查](#71-开启检查)
   - [白名单机制](#72-白名单机制)
   - [生成白名单](#73-生成白名单)
   - [白名单文件格式](#74-白名单文件格式)
8. [配置参数说明](#8-配置参数说明)
9. [输出与结果解读](#9-输出与结果解读)
10. [典型使用场景](#10-典型使用场景)
11. [常见问题](#11-常见问题)

---

## 1. 工具概述

本工具用于验证 TDengine 在 **滚动升级（Rolling Upgrade）** 和 **冷升级（Cold Upgrade）** 过程中的兼容性，覆盖以下验证项：

| 验证项 | 说明 |
|--------|------|
| 写入持续性 | 升级期间 + 升级后，INSERT 无连续失败批次 |
| 查询持续性 | 升级期间 + 升级后，SELECT COUNT(\*) 无连续失败批次 |
| 订阅持续性 | TMQ 订阅消费无长时间中断 |
| 数据完整性 | 订阅消费行数与写入行数一致 |
| 用户权限保持 | 升级后 test_user 的权限集合不变 |
| 标签索引保持 | 显式 TAG INDEX 升级后仍存在 |
| TSMA 保持 | TSMA（时序聚合）升级后仍存在（≥ 3.3.6.0）|
| RSMA 保持 | RSMA 升级后仍存在（≥ 3.3.8.0）|
| Stream 保持 | STREAM 升级后仍处于运行状态（≥ 3.3.7.0）|
| 系统表 Schema | `INFORMATION_SCHEMA` 列定义变化均在白名单范围内（可选）|

---

## 2. 环境要求

- **操作系统**：Linux（x86_64）
- **Python**：3.8+，依赖 `pyyaml`（`pip install pyyaml`）
- **TDengine Python 连接器**：`taospy`（`pip install taospy`）
- **单台测试机**：工具在同一台机器上启动多个 taosd 进程（通过不同端口隔离），无需多台机器
- **安装包**：两个已解压的 TDengine 安装目录，分别对应基准版本和目标版本

目录结构示例：

```text
/opt/tdengine/
    TDengine-enterprise-3.3.8.0/    ← from-dir
        taosd
        libtaos.so
        ...
    TDengine-enterprise-3.4.0.8/    ← to-dir
        taosd
        libtaos.so
        ...
```

---

## 3. 目录结构

```text
hot_update/
├── run/
│   └── main.py              # 主入口
├── resource/
│   ├── sysinfo_checker.py   # INFORMATION_SCHEMA 快照与 diff
│   ├── whitelist_loader.py  # 白名单加载与过滤
│   ├── resourceManager.py   # DB / Table / Topic / Index 管理
│   └── userVerifier.py      # 用户权限验证
├── client/
│   ├── writer.py            # 后台写入 Worker
│   ├── querier.py           # 后台查询 Worker
│   └── subscriber.py        # 后台订阅 Worker
├── server/
│   ├── clusterSetup.py      # 集群创建与管理
│   └── rollingUpgrade.py    # 滚动 / 冷升级执行器
├── whitelist/               # 白名单 YAML 文件目录
│   └── 3.4.0.0~3.4.0.8.yaml
├── config.py                # 全局可调配置项
└── doc/
    └── readme.md            # 本文档
```

---

## 4. 快速开始

> 所有命令均在 `hot_update/` 目录下执行。

### 冷升级（最简）

```bash
python3 -m run.main \
  --from-dir /opt/tdengine/TDengine-enterprise-3.3.8.0 \
  --to-dir   /opt/tdengine/TDengine-enterprise-3.4.0.8
```

### 滚动升级

```bash
python3 -m run.main \
  --from-dir /opt/tdengine/TDengine-enterprise-3.3.8.0 \
  --to-dir   /opt/tdengine/TDengine-enterprise-3.4.0.8 \
  --rollupdate
```

### 滚动升级 + 系统表检查（推荐 CI 用法）

```bash
python3 -m run.main \
  --from-dir /opt/tdengine/TDengine-enterprise-3.3.8.0 \
  --to-dir   /opt/tdengine/TDengine-enterprise-3.4.0.8 \
  --rollupdate \
  --check-sysinfo
```

### 快速冒烟测试

```bash
python3 -m run.main \
  --from-dir /opt/tdengine/TDengine-enterprise-3.3.8.0 \
  --to-dir   /opt/tdengine/TDengine-enterprise-3.4.0.8 \
  --rollupdate \
  --quick
```

---

## 5. 命令行参数

| 参数 | 简写 | 是否必填 | 说明 |
|------|------|----------|------|
| `--from-dir DIR` | `-F` | **必填** | 基准版本的 TDengine 安装目录（含 `taosd` 和 `libtaos.so`）|
| `--to-dir DIR` | `-T` | **必填** | 目标版本的 TDengine 安装目录 |
| `--path DIR` | `-p` | 可选 | 集群数据和配置文件存放目录。不存在时自动创建。默认：`~/td_rolling_upgrade` |
| `--fqdn HOST` | `-f` | 可选 | 测试机的 FQDN（主机名）。默认：`socket.gethostname()` 的输出 |
| `--quick` | `-q` | 可选 | 快速模式：100 子表 × 1000 初始行，观察窗口 30 秒 |
| `--rollupdate` | `-r` | 可选 | 执行**滚动升级**。不加此参数则执行**冷升级** |
| `--check-sysinfo` | `-S` | 可选 | 比对升级前后 `INFORMATION_SCHEMA` 的列定义，有意外变化时判定失败 |
| `--gen-whitelist [FILE]` | `-G` | 可选 | 仅生成白名单文件后退出，不执行升级测试。FILE 可省略 |
| `--whitelist-dir DIR` | — | 可选 | 白名单 YAML 文件目录。默认：`<脚本根目录>/whitelist` |
| `--help` | `-h` | — | 打印帮助信息后退出 |

---

## 6. 测试流程详解

### 6.1 冷升级（默认）

冷升级按以下五个 Phase 顺序执行：

```text
Phase 1  集群搭建（基准版本）
         └─ 启动 3 个 taosd 进程 + 创建 3 副本集群
Phase 2  资源准备
         ├─ 创建数据库、超级表、子表，写入初始数据
         ├─ 创建 Topic、TSMA、RSMA、Stream、Tag Index、test_user
         └─ （可选）拍 INFORMATION_SCHEMA 快照
Phase 3  冷升级
         └─ 停所有节点 → 替换二进制 → 重新启动
             （此过程无写入 / 查询 / 订阅在运行）
           ─── 主进程退出，子进程以新版 libtaos.so 接管 Phase 4-5 ───
Phase 4  后台负载（升级完成后启动）
         └─ 写入 / 查询 / 订阅三个后台 Worker 启动
Phase 5  验证窗口（默认 30 秒）
         ├─ 持续监测三路 Worker 的延时和错误数
         └─ 写入停止后等待订阅消费追上
结果汇总（SUMMARY）
```

> **为什么需要子进程？**  
> glibc 在进程启动时读取一次 `LD_LIBRARY_PATH`，无法在运行中切换 `libtaos.so`。  
> 冷升级完成后主进程重新以 `to_dir` 的库路径 spawn 一个子进程，子进程负责 Phase 4-5，确保连接器版本与升级后的服务器版本匹配。

---

### 6.2 滚动升级

```text
Phase 1  集群搭建（基准版本）
Phase 2  资源准备
Phase 3  后台负载（升级前启动）
         └─ 写入 / 查询 / 订阅在升级全程持续运行
Phase 4  滚动升级
         ├─ 随机顺序逐节点升级（STOP → 替换二进制 → START）
         ├─ 每个节点升级前等待其 ready，再升下一个
         └─ 记录升级期间的写入 / 查询 / 订阅成功数
Phase 5  验证窗口（默认 30 秒）
结果汇总（SUMMARY）
```

---

## 7. INFORMATION_SCHEMA 检查

### 7.1 开启检查

在任意升级命令后加 `-S` / `--check-sysinfo`：

```bash
python3 -m run.main -F <from> -T <to> --rollupdate --check-sysinfo
```

工具会在升级前后各拍一次 `INFORMATION_SCHEMA` 快照，比对：

- **新增表 / 删除表**
- **修改表**：新增列、删除列、列类型变化、列位置变化

如有任何**未在白名单中**的变化，该检查项判定为 `[FAILED]`，并在 SUMMARY 中输出差异详情及候选白名单文件路径。

---

### 7.2 白名单机制

白名单文件放在 `whitelist/` 目录（或 `--whitelist-dir` 指定的目录）下，文件名格式：

```text
{from_prefix}~{to_prefix}.yaml
```

**版本前缀匹配规则**（越短覆盖范围越广）：

| 文件名 | 匹配范围 |
|--------|----------|
| `3~3.yaml` | 所有 3.x.x.x → 3.x.x.x 升级 |
| `3.3~3.4.yaml` | 所有 3.3.x.x → 3.4.x.x 升级 |
| `3.3.6~3.4.yaml` | 3.3.6.x → 3.4.x.x 升级 |
| `3.3.6.0~3.4.0.8.yaml` | 仅精确版本对 3.3.6.0 → 3.4.0.8 |

当一个升级路径命中多个白名单文件时，**所有文件合并生效**（取并集）。

---

### 7.3 生成白名单

当遇到一个新的版本对时，先用 `--gen-whitelist` 让工具自动生成白名单，再审核后投入使用：

```bash
# 1. 生成白名单（仅做 schema 比对，不跑升级负载）
python3 -m run.main \
  -F /opt/tdengine/3.3.8.0 \
  -T /opt/tdengine/3.4.0.8 \
  --gen-whitelist

# 2. 审核生成的文件（默认保存在 whitelist/ 目录下）
cat whitelist/3.3.8.0~3.4.0.8.yaml

# 3. 确认无误后，正式运行带检查的升级测试
python3 -m run.main \
  -F /opt/tdengine/3.3.8.0 \
  -T /opt/tdengine/3.4.0.8 \
  --rollupdate \
  --check-sysinfo
```

也可以指定白名单输出路径：

```bash
python3 -m run.main -F <from> -T <to> --gen-whitelist /tmp/my_wl.yaml
```

> **测试失败时自动提示**  
> 若 `--check-sysinfo` 发现意外变化，工具会自动在当前工作目录生成一个候选白名单文件，并在 SUMMARY 的差异内容后打印其绝对路径，方便直接复制使用：
>
> ```text
>   INFORMATION_SCHEMA: no unexpected changes .............. [FAILED]
>     + added    : INS_DATABASES.allow_drop
>     ~ pos      : INS_XNODE_AGENTS.update_time  pos 4 -> 5
>   /root/.../hot_update/3.3.8.0~3.4.0.8.yaml
> ```

---

### 7.4 白名单文件格式

```yaml
# 升级路径注释（自动生成时填写）
# from_version : 3.3.8.0
# to_version   : 3.4.0.8

# 新增的整张表（白名单内 = 预期内，不报错）
added_tables:
  - INS_NEW_TABLE

# 删除的整张表
deleted_tables: []

# 修改了列定义的表
modified_tables:
  - table: INS_DATABASES
    # 新增列
    added_columns:
      - allow_drop
    # 删除列
    deleted_columns: []
    # 列类型变化（写列名即可，任何类型变化都被允许）
    changed_type:
      - config
    # 列位置变化（写列名即可）
    changed_position:
      - update_time
```

---

## 8. 配置参数说明

`config.py` 中的参数控制集群规模和运行时阈值，修改后无需改动命令行：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `DNODE_COUNT` | 3 | 集群节点数 |
| `MNODE_COUNT` | 2 | Mnode 数量 |
| `REPLICA` | 3 | 数据库副本数 |
| `SUBTABLE_COUNT` | 100 | 子表数量 |
| `INIT_ROWS_PER_SUBTABLE` | 10000 | 每张子表初始写入行数（确保 WAL / data / stt 均有文件）|
| `VERIFY_DURATION_S` | 30 | 升级后观测窗口时长（秒）|
| `MAX_CONSECUTIVE_RETRIES` | 30 | 连续失败超过此次数才计为 1 次真正失败 |
| `RETRY_MAX_DURATION_S` | 150 | 单次 SQL 重试超过此秒数计为 1 次失败 |
| `SUBSCRIBE_NO_DATA_TIMEOUT_S` | 180 | 超过此秒未收到订阅数据判为不通过 |
| `NODE_INSTALL_SLEEP_S` | 60 | 节点停止后模拟安装耗时的等待时间（秒）|
| `NODE_READY_TIMEOUT_S` | 180 | 等待单节点 ready 的超时时间（秒）|

`--quick` 参数会将以下三项覆盖为小值用于快速冒烟：

| 参数 | quick 模式值 |
|------|-------------|
| `SUBTABLE_COUNT` | 100 |
| `INIT_ROWS_PER_SUBTABLE` | 1000 |
| `VERIFY_DURATION_S` | 30 |

---

## 9. 输出与结果解读

工具输出分为三层：

1. **步骤日志**（`[HH:MM:SS]` 前缀）：每个 Phase 的进度信息
2. **内联错误**（`[internal-error]` 前缀）：内部模块的错误，输出到 stderr
3. **SUMMARY 框**：测试结束后统一打印所有检查结果

典型 SUMMARY 输出示例：

```text
════════════════════════════════════════════════════════════════════════
  TDengine Rolling Upgrade Test  ─  SUMMARY
  3.3.8.0  ──▶  3.4.0.8   │  host: myserver  │  3 dnodes / 2 mnodes
════════════════════════════════════════════════════════════════════════
  Rolling upgrade completed ............................ [passed]
  Write: no failure batches ............................ [passed]  failures=0  total_rows=1,234  retries=2  max_latency=0.312s
  Query: no failure batches ............................ [passed]  failures=0  max_latency=0.108s
  Subscribe: data received within 180s ................. [passed]  silence=1.2s  total_recv=1,234
  Subscribe: rows received == rows written ............. [passed]  written=1,234  received=1,234  diff=0
  test_user authentication after upgrade ............... [passed]
  test_user privileges unchanged  ...................... [passed]
  Tag indexes preserved after upgrade .................. [passed]
  TSMA preserved after upgrade ......................... [passed]
  RSMA preserved after upgrade ......................... [passed]
  Stream preserved after upgrade ....................... [passed]
  INFORMATION_SCHEMA: no unexpected changes ............ [passed]  (all changes whitelisted)
────────────────────────────────────────────────────────────────────────
  Result  : PASS ✓
  Duration: 4m 32s
  Write   : max_latency=0.312s  Query: max_latency=0.108s  Subscribe: max_gap=0.950s
════════════════════════════════════════════════════════════════════════
```

**退出码**：`0` = 全部通过，`1` = 有检查项失败，`2` = 参数错误。

---

## 10. 典型使用场景

### 场景 A：新版本发布前的完整 CI 验证

```bash
# 先生成白名单（若这是首次测试该版本对）
python3 -m run.main -F /pkg/3.3.8.0 -T /pkg/3.4.0.8 --gen-whitelist

# 审核 whitelist/3.3.8.0~3.4.0.8.yaml 后正式运行
python3 -m run.main -F /pkg/3.3.8.0 -T /pkg/3.4.0.8 --rollupdate --check-sysinfo
```

### 场景 B：小版本补丁回归（快速）

```bash
python3 -m run.main \
  -F /pkg/3.4.0.0 \
  -T /pkg/3.4.0.8 \
  --rollupdate --quick --check-sysinfo
```

### 场景 C：冷升级（停机升级）验证

```bash
python3 -m run.main \
  -F /pkg/3.3.8.0 \
  -T /pkg/3.4.0.8 \
  --check-sysinfo
```

### 场景 D：自定义白名单目录

```bash
python3 -m run.main \
  -F /pkg/3.3.8.0 \
  -T /pkg/3.4.0.8 \
  --rollupdate \
  --check-sysinfo \
  --whitelist-dir /ci/tdengine/whitelists
```

---

## 11. 常见问题

**Q: 运行前需要清理上次的集群数据吗？**  
A: 需要。每次运行前建议执行：

```bash
pkill -9 taosd
rm -rf ~/td_rolling_upgrade   # 或 --path 指定的目录
```

**Q: 报错 `ModuleNotFoundError: No module named 'taos'`**  
A: 安装 TDengine Python 连接器：`pip install taospy`

**Q: `--check-sysinfo` 失败但不知道该如何编写白名单**  
A: 按照 SUMMARY 中打印的候选白名单路径找到自动生成的文件，审核后将其移入 `whitelist/` 目录即可。

**Q: `--gen-whitelist` 和 `--check-sysinfo` 可以同时使用吗？**  
A: 不可以。`--gen-whitelist` 模式只做 schema 比对后写文件退出，不执行升级负载测试，两者互斥。

**Q: 白名单文件的版本前缀应该写多短？**  
A: 建议先用精确四位版本号（如 `3.3.8.0~3.4.0.8.yaml`），确认无误后若希望覆盖整个大版本升级路径，再改为 `3.3~3.4.yaml`。

**Q: 多个白名单文件都匹配时怎么处理？**  
A: 所有匹配到的文件取并集，即每个文件中允许的变化项都会被放行。
