# ASOF JOIN Timerange Pushdown 测试报告

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-05-12 | - | 1.0 | 任新胜 | 新建 ASOF JOIN timerange pushdown 功能与性能测试文档 |

## 2. 测试目标

本测试文档覆盖 ASOF JOIN timerange pushdown 优化的功能正确性验证和性能效果评估。

该优化在 ASOF JOIN 场景下，将 probe 侧（驱动侧）的时间范围条件以及 matched 侧（被匹配侧）的 timestamp 条件下推/拷贝到 scan 算子，缩小 scan 范围，减少无效数据读取和计算，提高查询效率。

具体优化包含两个机制：

1. **Range Derivation（范围推导）**：根据 probe 侧 scan range 和 ASOF 方向，推导 matched 侧 scan 上下界。
2. **COPY Pushdown（条件拷贝下推）**：将 WHERE 中 matched 侧的 `ts` 比较条件拷贝（非移动）到 matched 侧 scan filter，同时保留原始 WHERE 条件以保证 join 语义正确。

## 3. 参考文档

- 开发分支：`enh/join/timerangePushdown`
- 优化代码：`community/source/libs/planner/src/planOptimizer.c`
- 功能测试脚本：`community/test/cases/14-JoinQueries/test_join.py::do_asof_join_right_ts_pushdown`
- 测试 SQL 文件：`community/test/cases/14-JoinQueries/in/test_asof_join_pushdown.in`
- 期望结果文件：`community/test/cases/14-JoinQueries/ans/test_asof_join_pushdown.ans`

## 4. 测试结论

- **功能正确性**：81 条覆盖 SQL 全部通过（含 EXPLAIN 执行计划验证和结果文件对比）。
- **性能效果**：COPY Pushdown 下推完全有效，性能收益取决于裁剪掉的无效数据量。当 matched 侧 `b.ts` 条件精确匹配 probe 时间范围时，可裁剪 99.8% 数据，性能提升 **114~124 倍**；下界越宽松裁剪越少，提升相应降低。建议用户在 ASOF JOIN 查询中显式添加 matched 侧 `b.ts` 时间约束以获得最佳性能（详见 8.3）。

## 5. 测试环境

- OS: Linux x86_64（Ubuntu 22.04）
- TDengine: 企业版 v3.4.1.6.alpha（分支 `enh/join/timerangePushdown`，commit `44979a330b`）
- 测试框架：pytest + new_test_framework
- 数据生成工具：taosBenchmark
- 配置：单节点，2 vgroups

## 6. 功能测试

### 6.1 Range Derivation（范围推导）正确性

#### 6.1.1 测试要点

- LEFT ASOF JOIN（`a.ts >= b.ts`）：probe 侧有 `a.ts <= T` 时，matched 侧 scan 上界应被推导为 `T`。
- LEFT ASOF JOIN（`a.ts >= b.ts`）：probe 侧有 `a.ts >= T` 时，matched 侧 scan 下界应被推导为 `T`。
- RIGHT ASOF JOIN（`b.ts >= a.ts`）：probe 侧有 `b.ts <= T` 时，matched 侧 scan 上界应被推导。
- 不同 ASOF 方向（`>=`, `>`, `<`, `<=`, `=`）下推导逻辑的正确性。
- 推导后 matched 侧 scan range 为空（skey > ekey）时应使用 DESC 初始窗口。
- 查询结果与无优化时完全一致。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | LEFT ASOF `>=` 双边时间范围 | `a.ts >= T1 AND a.ts <= T2` → matched scan `[-inf, T2]`，结果正确 | 通过 |
| 2 | RIGHT ASOF `>=` 双边时间范围 | `b.ts >= T1 AND b.ts <= T2` → matched scan `[-inf, T2]`，结果正确 | 通过 |
| 3 | LEFT ASOF `>` 方向 | `ON a.ts > b.ts` → matched scan 上界推导正确 | 通过 |
| 4 | LEFT ASOF `<` 方向（反向） | `ON a.ts < b.ts` → matched scan 下界推导正确 | 通过 |
| 5 | LEFT ASOF `=` 方向 | `ON a.ts = b.ts` → matched scan 双边推导正确 | 通过 |
| 6 | 范围推导导致空区间 | probe range `[T1, T2]`，matched 条件 `b.ts >= T3 (T3 > T2)` → 空结果 | 通过 |
| 7 | 仅有 probe 下界 | `a.ts >= T` 无上界 → matched scan 无上界推导 | 通过 |

### 6.2 COPY Pushdown（条件拷贝下推）正确性

#### 6.2.1 测试要点

- matched 侧 `b.ts >= T` 条件被 COPY 到 scan filter，同时保留在 WHERE。
- matched 侧 `b.ts <= T` 条件被 COPY 到 scan filter。
- 双边 `b.ts >= T1 AND b.ts <= T2` 均被 COPY。
- OR 条件中含 matched ts 不触发 COPY（安全性检查）。
- 表达式 `b.ts + 0 >= T` 不触发 COPY（非 primary key 列）。
- 函数包裹 `timetruncate(b.ts, 1s)` 不触发 COPY。
- 非 ts 列条件 `b.v >= X` 不触发 COPY。
- COPY 后查询结果与无优化时完全一致。

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | matched `b.ts >= T` COPY 下推 | scan range 下界被缩窄，结果正确 | 通过 |
| 2 | matched `b.ts >= T1 AND b.ts <= T2` COPY | scan range 双边缩窄 | 通过 |
| 3 | OR 条件不触发 COPY | `(b.ts >= T OR b.v >= X)` 留在 WHERE | 通过 |
| 4 | `b.ts + 0 >= T` 不触发 COPY | 非 primary key 表达式 | 通过 |
| 5 | 函数包裹 ts 不触发 COPY | `timetruncate(b.ts, 1s)` 情况 | 通过 |
| 6 | 常量在左边 `T <= b.ts` | 等价于 `b.ts >= T`，正常 COPY | 通过 |
| 7 | RIGHT ASOF matched 侧 COPY | matched 为左表时 COPY 正确 | 通过 |
| 8 | 多个 matched ts 条件 | `b.ts >= T1 AND b.ts >= T2` 均被 COPY | 通过 |

### 6.3 安全性负例

#### 6.3.1 测试要点

- 无 primary key 等值/比较条件的 ASOF JOIN 不触发任何优化。
- FULL JOIN 不触发优化。
- 含 OR 的跨表条件不触发 COPY。
- 混合 probe 和 matched 列的 OR 条件不触发 COPY。

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 跨表 OR `(a.ts >= T OR b.ts >= T)` | 不下推 COPY，WHERE 保留完整 | 通过 |
| 2 | OR 内嵌 `(b.ts >= T AND b.v >= X) OR b.v >= Y` | 不触发 COPY | 通过 |
| 3 | probe-matched 混合 OR | `(a.ts >= T OR b.v >= X)` 不触发 | 通过 |

### 6.4 综合测试

#### 6.4.1 测试要点

- UNION ALL 子查询中 ASOF JOIN 的 pushdown 正确性。
- Range Derivation + COPY Pushdown 组合场景。
- 查询结果通过 `.in/.ans` 文件对比验证。

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | UNION ALL 中 ASOF JOIN pushdown | 两个子查询各自独立 pushdown，结果正确 | 通过 |
| 2 | Range + COPY 组合 | probe `[T1, T2]` + matched `b.ts >= T3` → scan `[T3, T2]` | 通过 |
| 3 | 完整回归（81 条 SQL） | `test_asof_join_pushdown.in/ans` 文件对比全部通过 | 通过 |

### 6.5 验证命令

```bash
cd /root/TDinternal/community/test && \
  python3 -m pytest cases/14-JoinQueries/test_join.py -k "test_join" --skip_stop -v
```

## 7. 性能测试

### 7.1 测试目标

验证 timerange pushdown 优化在典型场景下的性能提升效果。构造 matched 侧有大量无效数据的场景，对比优化前后查询耗时。

### 7.2 测试数据构造

使用 taosBenchmark 生成数据：

**数据分布设计**：
- **probe 表（st_probe）**：1000 行，起始时间 `2025-01-06 00:00:00`，步长 1000ms（覆盖约 16.7 分钟，范围 `[2025-01-06 00:00:00, 2025-01-06 00:16:39]`）。
- **matched 表（st_matched）**：10,000,000 行，起始时间 `2025-01-01 00:00:00`，步长 50ms（覆盖约 5.8 天，范围 `[2025-01-01 00:00:00, 2025-01-06 18:53:20]`）。每行含 BINARY(200) 宽字段。

**场景含义**：probe 查询的 16.7 分钟窗口位于 matched 数据尾部（第 5 天），前 5 天约 8,600,000 行 matched 数据对查询结果无贡献。在 `LEFT ASOF JOIN ON a.ts >= b.ts` 的 merge 算法中，matched 侧必须从头扫描至 probe 范围才能找到匹配。无优化时扫描全部 10M 行（~2.2GB），优化后可跳过无效数据。

**taosBenchmark 配置文件**（`perf_bench.json`）：

```json
{
  "filetype": "insert",
  "cfgdir": "/etc/taos",
  "host": "localhost",
  "port": 6030,
  "user": "root",
  "password": "taosdata",
  "confirm_parameter_prompt": "no",
  "databases": [
    {
      "dbinfo": {
        "name": "db_asof_perf",
        "drop": "yes",
        "vgroups": 2
      },
      "super_tables": [
        {
          "name": "st_probe",
          "childtable_count": 1,
          "childtable_prefix": "probe_",
          "insert_rows": 1000,
          "timestamp_step": 1000,
          "start_timestamp": "2025-01-06 00:00:00.000",
          "columns": [
            {"type": "INT", "name": "v1"},
            {"type": "FLOAT", "name": "v2"},
            {"type": "DOUBLE", "name": "v3"},
            {"type": "BINARY", "name": "info", "len": 200}
          ],
          "tags": [
            {"type": "INT", "name": "tg", "values": [1]}
          ]
        },
        {
          "name": "st_matched",
          "childtable_count": 1,
          "childtable_prefix": "matched_",
          "insert_rows": 10000000,
          "timestamp_step": 50,
          "start_timestamp": "2025-01-01 00:00:00.000",
          "columns": [
            {"type": "INT", "name": "v1"},
            {"type": "FLOAT", "name": "v2"},
            {"type": "DOUBLE", "name": "v3"},
            {"type": "BINARY", "name": "info", "len": 200}
          ],
          "tags": [
            {"type": "INT", "name": "tg", "values": [1]}
          ]
        }
      ]
    }
  ]
}
```

**生成数据命令**：

```bash
taosBenchmark -f perf_bench.json
```

### 7.3 测试场景

#### 场景 1：Range Derivation（范围推导）

**场景描述**：probe 侧有 `[T1, T2]` 时间范围（`2025-01-06 00:00:00` 至 `2025-01-06 00:16:39`），ASOF 方向 `a.ts >= b.ts`。matched 表数据从 `2025-01-01` 开始，merge 算法需从头扫描至 probe 范围才能找到匹配。优化后 matched 侧 scan 上界推导为 `T2`，避免扫描 probe 时间之后的数据。

**测试 SQL**：

```sql
SELECT a.ts, a.v1, a.info, b.ts, b.v1, b.v2, b.v3, b.info
  FROM st_probe a LEFT ASOF JOIN st_matched b
  ON a.ts >= b.ts
  WHERE a.ts >= '2025-01-06 00:00:00' AND a.ts <= '2025-01-06 00:16:39';
```

#### 场景 2：COPY Pushdown + Range Derivation（组合优化）

**场景描述**：在场景 1 基础上，WHERE 中增加 matched 侧条件 `b.ts >= '2025-01-06 00:00:00'`。优化后 COPY 机制将该条件推到 scan filter，叠加 Range Derivation 的上界推导，matched scan 被精确缩窄为 `[2025-01-06 00:00:00, 2025-01-06 00:16:39]`，仅扫描约 20,000 行（占 10M 的 0.2%）。

**测试 SQL**：

```sql
SELECT a.ts, a.v1, a.info, b.ts, b.v1, b.v2, b.v3, b.info
  FROM st_probe a LEFT ASOF JOIN st_matched b
  ON a.ts >= b.ts
  WHERE a.ts >= '2025-01-06 00:00:00' AND a.ts <= '2025-01-06 00:16:39'
    AND b.ts >= '2025-01-06 00:00:00';
```

#### 场景 3：COPY Pushdown 双边 b.ts 条件

**场景描述**：在场景 2 基础上，进一步增加 `b.ts <= '2025-01-06 00:10:00'`，同时限定 matched 侧上下界。两个 b.ts 条件均被 COPY 到 scan filter。matched scan 缩窄为 `[2025-01-06 00:00:00, 2025-01-06 00:16:39]`（scan range 受 Range Derivation 上界与 COPY 下界约束），b.ts 上界条件 `<= 00:10:00` 作为 scan filter 进一步过滤行，结果行数少于场景 2。

**测试 SQL**：

```sql
SELECT a.ts, a.v1, a.info, b.ts, b.v1, b.v2, b.v3, b.info
  FROM st_probe a LEFT ASOF JOIN st_matched b
  ON a.ts >= b.ts
  WHERE a.ts >= '2025-01-06 00:00:00' AND a.ts <= '2025-01-06 00:16:39'
    AND b.ts >= '2025-01-06 00:00:00' AND b.ts <= '2025-01-06 00:10:00';
```

#### 场景 4：COPY Pushdown 宽松下界（部分裁剪）

**场景描述**：matched 侧条件 `b.ts >= '2025-01-03 00:00:00'` 提供较宽松的下界。COPY 下推后 matched scan 缩窄为 `[2025-01-03 00:00:00, 2025-01-06 00:16:39]`，裁掉前 2 天（~3,456,000 行，约 35%），但仍需扫描后 3 天共约 650 万行。展示部分下推的中间效果。

**测试 SQL**：

```sql
SELECT a.ts, a.v1, a.info, b.ts, b.v1, b.v2, b.v3, b.info
  FROM st_probe a LEFT ASOF JOIN st_matched b
  ON a.ts >= b.ts
  WHERE a.ts >= '2025-01-06 00:00:00' AND a.ts <= '2025-01-06 00:16:39'
    AND b.ts >= '2025-01-03 00:00:00';
```

### 7.4 性能测试结果

#### 场景 1：Range Derivation

| 指标 | 优化前（3.0） | 优化后 | 提升倍数 |
| --- | --- | --- | --- |
| Run 1 | 19.992s | 22.599s | - |
| Run 2 | 19.271s | 19.931s | - |
| Run 3 | 18.460s | 19.450s | - |
| **平均耗时** | **19.241s** | **20.660s** | **无提升** |

Range Derivation 仅推导了 matched 侧 scan **上界**，本测试数据的无效数据在左侧，上界裁剪无效。

#### 场景 2：COPY Pushdown + Range Derivation

| 指标 | 优化前（3.0） | 优化后 | 提升倍数 |
| --- | --- | --- | --- |
| Run 1 | 19.430s | 0.177s | 109.8x |
| Run 2 | 20.558s | 0.155s | 132.6x |
| Run 3 | 18.662s | 0.180s | 103.7x |
| **平均耗时** | **19.550s** | **0.171s** | **114.3x** |

#### 场景 3：COPY Pushdown 双边 b.ts 条件

| 指标 | 优化前（3.0） | 优化后 | 提升倍数 |
| --- | --- | --- | --- |
| Run 1 | 25.196s | 0.152s | 165.8x |
| Run 2 | 18.426s | 0.167s | 110.3x |
| Run 3 | 19.100s | 0.184s | 103.8x |
| **平均耗时** | **20.907s** | **0.168s** | **124.4x** |

与场景 2 性能接近，scan 范围相同，双边 b.ts 条件进一步过滤返回行数（601 行 vs 1000 行）。

#### 场景 4：COPY Pushdown 宽松下界（部分裁剪）

| 指标 | 优化前（3.0） | 优化后 | 提升倍数 |
| --- | --- | --- | --- |
| Run 1 | 20.399s | 11.425s | 1.8x |
| Run 2 | 20.041s | 12.775s | 1.6x |
| Run 3 | 21.512s | 10.989s | 2.0x |
| **平均耗时** | **20.651s** | **11.730s** | **1.8x** |

宽松下界 `b.ts >= '2025-01-03'` 裁掉前 2 天约 35% 数据，提升约 **1.8 倍**。

## 8. 总结

### 8.1 正确性

功能正确性已通过 81 条覆盖 SQL 验证（含 EXPLAIN 执行计划对比和结果文件对比），覆盖：

- Range Derivation 各方向（`>=`, `>`, `<`, `<=`, `=`）推导正确性
- COPY Pushdown 条件拷贝及安全性负例（OR、表达式、非 ts 列）
- 组合场景及 UNION ALL 子查询

### 8.2 性能

| 场景 | 描述 | 优化前 | 优化后 | 提升倍数 |
| --- | --- | --- | --- | --- |
| 1 | 仅 Range Derivation（无 b.ts 条件） | 19.241s | 20.660s | 无提升 |
| 2 | COPY 精确下界 `b.ts >= probe 起始` | 19.550s | 0.171s | 114x |
| 3 | COPY 双边 `b.ts >= T1 AND b.ts <= T2` | 20.907s | 0.168s | 124x |
| 4 | COPY 宽松下界 `b.ts >= 中间时间点` | 20.651s | 11.730s | 1.8x |

### 8.3 结论与 SQL 建议

COPY Pushdown 下推机制本身完全有效，性能收益的核心因素是**裁剪掉的无效数据量**：

- 场景 2/3 中 `b.ts >= '2025-01-06'` 精确匹配 probe 起始时间，裁掉 99.8% 无效数据（~998 万行），耗时从 ~20s 降至 ~0.17s，提升 **114~124 倍**。
- 场景 4 中 `b.ts >= '2025-01-03'` 仅裁掉 ~35% 数据（~350 万行），提升 **1.8 倍**。
- 场景 1 无 `b.ts` 条件，无法触发 COPY Pushdown，Range Derivation 仅推导上界，对左侧无效数据无效。

**规律**：无效数据占比越高、裁剪越精确，性能提升越显著。

**SQL 写法建议**：在 ASOF JOIN 查询中，尽量在 WHERE 中为 matched 侧显式添加时间范围约束，使其尽量贴近实际查询的时间窗口：

```sql
-- 推荐：为 matched 侧显式添加 b.ts 下界，触发 COPY Pushdown
SELECT ... FROM probe a LEFT ASOF JOIN matched b ON a.ts >= b.ts
  WHERE a.ts >= T1 AND a.ts <= T2
    AND b.ts >= T1;  -- 关键：matched 侧时间下界

-- 更精确：双边约束进一步缩窄 scan 范围
SELECT ... FROM probe a LEFT ASOF JOIN matched b ON a.ts >= b.ts
  WHERE a.ts >= T1 AND a.ts <= T2
    AND b.ts >= T1 AND b.ts <= T2;
```

不添加 `b.ts` 条件时，matched 侧仍可能全量扫描，在数据量大、无效数据多的场景下性能损失严重。
