# 虚拟表引用性能测试报告

> 分支: `enh/tag-ref`
> 日期: 2026-04-20
> 目标: 量化本分支三个核心功能 (Tag-ref / VChild-ref-VChild / VStable-ref-VStable) 引入的性能开销

---

## 0. 测试环境

| 项目 | 值 |
|------|---|
| 操作系统 | Linux (WSL2, kernel 5.15+) |
| CPU | 多核 x86_64 |
| TDengine 构建 | Debug (`cmake ..`) |
| ASAN | 关闭 |
| debugFlag | `131` |
| 部署 | 单机 1 dnode, 无 replica |
| 隔离 | 每个测试前 `rm -rf sim` 重建环境 |
| 计时 | `time.perf_counter()` 包裹 `tdSql.query()` |
| 重复 | 每条 SQL 连跑 5 次, 取 median (ms) |
| 冷/热 | cold = FLUSH 后首次查询; warm = 第 5 次 |

### 统一查询集

| 代号 | SQL | 用途 |
|------|-----|------|
| Q1 | `SELECT * FROM {tbl}` | 全表扫描 |
| Q2 | `SELECT COUNT(*) FROM {tbl}` | 计数 |
| Q3 | `SELECT SUM(c0), AVG(c1) FROM {tbl}` | 聚合 |
| Q4 | `SELECT * FROM {tbl} WHERE c0 >= X` | 数据过滤 |
| Q5 | `SELECT * FROM {tbl} WHERE t0 = X` | Tag 过滤 |
| Q6 | `SELECT t0, COUNT(*) FROM {tbl} GROUP BY t0` | Tag 分组 |
| Q7 | `SELECT DISTINCT t0 FROM {tbl}` | Tag 去重 |
| Q8 | `SELECT LAST(c0) FROM {tbl}` | 最新值 |

### 测试结果概览

| Feature | 通过 | 总计 | 状态 |
|---------|------|------|------|
| A: Tag-ref | 6/7 | 7 | R4 大数据超时 (Debug 构建限制) |
| B: VChild-ref-VChild | 7/7 | 7 | 全部通过 |
| C: VStable-ref-VStable | 6/7 | 7 | R4 大数据超时 (Debug 构建限制) |

---

## 1. Feature A: Tag-ref 性能测试

### 核心结论

> **Tag-ref 是本分支性能开销最大的特性。** 主要瓶颈在于 TagRefSourceOperator 逐子表扫描源表提取 tag 值。WHERE tag 精确匹配开销可达 +1400%；但纯数据查询开销仅 +6~39%。

### A-R1: 基线对比

**环境:** children=20, rows/child=1000, data_cols=5, tag_cols=3, vgroups=2

**结果:**

| 查询 | Source(ms) | Literal(ms) | TagRef(ms) | TRef/Lit | TRef/Src |
|------|-----------|------------|-----------|---------|---------|
| Q1 SELECT * | 43.28 | 54.47 | 58.04 | **+6.6%** | +34.1% |
| Q2 COUNT | 2.77 | 18.63 | 27.81 | **+49.3%** | +1003% |
| Q3 SUM+AVG | 3.87 | 7.56 | 8.44 | **+11.6%** | +118% |
| Q4 WHERE data | 24.58 | 43.04 | 53.33 | **+23.9%** | +117% |
| Q5 WHERE t0 | 5.18 | 7.03 | 35.25 | **+401.5%** | +580% |
| Q6 GROUP t0 | 2.97 | 26.01 | 31.67 | **+21.8%** | +968% |
| Q7 DISTINCT t0 | 3.08 | 24.71 | 35.50 | **+43.7%** | +1054% |
| Q8 LAST c0 | 4.78 | 7.70 | 10.71 | **+39.1%** | +124% |

**结论:**
- **数据查询 (Q1/Q3/Q4/Q8):** tag-ref 相对 literal-tag 开销 6~39%，可接受
- **Tag 查询 (Q5/Q6/Q7):** tag-ref 开销 22~402%，Q5 (WHERE tag) 最严重
- **固定成本:** tag-ref 的额外开销主要来自 TagRefSourceOperator 扫描源表子表 (每次 3~28ms)

---

### A-R2: Tag 查询类型敏感度

**环境:** children=100, rows/child=1000, data_cols=5, tag_cols=3, vgroups=2

**结果:**

| 查询类型 | Literal(ms) | TagRef(ms) | 开销 |
|---------|------------|-----------|------|
| tag_eq (精确匹配) | 8.79 | 132.05 | **+1402%** |
| tag_in_10 | 33.06 | 153.07 | +363% |
| tag_in_50 | 94.89 | 159.05 | +68% |
| tag_range | 65.06 | 150.76 | +132% |
| tag_group | 110.04 | 126.74 | +15% |
| tag_distinct | 137.12 | 155.88 | +14% |
| tag_order | 145.69 | 155.37 | **+6.6%** |

**结论:**
- **开销与选择性成反比:** tag_eq (选 1/100) 开销最大 +1402%，因为 literal-tag 利用 catalog 快速定位子表，而 tag-ref 必须遍历所有子表的 tag 值
- **低选择性查询开销小:** tag_order/distinct/group 已需遍历所有子表，额外开销仅 +6~15%
- **tag-ref 固定成本约 120ms:** 大部分查询 tag-ref 耗时在 126~159ms 范围内，近似常数

---

### A-R3: 数据量少 + 物理表多

**环境:** rows/child=100, children=50~1000, data_cols=5, tag_cols=3, vgroups=4

**Q5 (WHERE tag) 开销随子表数变化:**

| 子表数 | Literal(ms) | TagRef(ms) | 开销 | 每子表成本(ms) |
|-------|------------|-----------|------|-------------|
| 50 | 7.08 | 67.80 | +858% | 1.21 |
| 100 | 7.26 | 121.09 | +1569% | 1.14 |
| 200 | 9.47 | 238.51 | +2418% | 1.15 |
| 500 | 14.20 | 589.54 | +4052% | 1.15 |
| 1000 | 19.69 | 1254.04 | +6269% | 1.23 |

**结论:**
- **线性增长:** Q5 开销与子表数近似线性增长 (每子表约 1.1~1.2ms)
- **Q2/COUNT 开销:** +61~81%，主要来自虚拟表框架而非 tag-ref 本身
- **Q6/GROUP BY 开销:** +27~46%，增幅低于 Q5，因为 GROUP BY 本身需遍历所有子表

---

### A-R4: 数据量大 + 物理表多

**状态:** Debug 构建下超时，未能完成。参数已调优 (children=100, rows=1K/10K/50K)，但仍因 Debug 构建的插入速度过慢超时。

**建议:** Release 构建下重跑此场景。

---

### A-R5: tag-ref 数量很多

**环境:** children=50, rows/child=1000, data_cols=5, tag_cols=1~50, vgroups=2

**Q5 (WHERE tag) 开销随 tag-ref 列数变化:**

| Tag列数 | Literal(ms) | TagRef(ms) | 开销 | 每列成本(ms) |
|--------|------------|-----------|------|-----------|
| 1 | 9.89 | 78.00 | +688% | 68.1 |
| 3 | 9.57 | 91.20 | +853% | 27.2 |
| 5 | 9.99 | 111.80 | +1019% | 20.4 |
| 10 | 10.78 | 135.01 | +1153% | 12.4 |
| 20 | 17.27 | 208.20 | +1106% | 9.5 |
| 50 | 17.21 | 350.03 | **+1933%** | 6.7 |

**结论:**
- **亚线性增长:** 每列成本从 68ms (1列) 降到 6.7ms (50列)，说明 TagRefSourceOperator 可以批量提取多个 tag 列
- **但绝对开销仍大:** 50 个 tag-ref 列时 Q5 从 17ms 涨到 350ms (+333ms)
- **Q6/GROUP BY:** 增长更温和 (+6~422%)，因为 GROUP BY 本身就需遍历

---

### A-R6: 跨库 tag-ref

**环境:** children=50, rows/child=1000, data_cols=5, tag_cols=3, vgroups=2

**跨库额外开销:**

| 查询 | 同库 TagRef(ms) | 跨库 TagRef(ms) | 跨库额外 | 百分比 |
|------|---------------|---------------|---------|------|
| Q2 COUNT | 70.76 | 82.72 | +11.97ms | +16.9% |
| Q5 WHERE t0 | 77.58 | 86.61 | +9.03ms | +11.6% |
| Q6 GROUP t0 | 63.49 | 81.05 | +17.56ms | +27.7% |

**结论:**
- **跨库额外开销 9~18ms:** 主要是 RPC 跨 vnode 通信成本
- **相对于 tag-ref 本身的开销 (60~80ms)，跨库占比 11~28%**
- **literal-tag 基线对比:** 跨库对 literal-tag 几乎无影响 (delta < 2ms)

---

### A-R7: 混合 tag (部分 literal + 部分 tag-ref)

**环境:** children=50, rows/child=1000, data_cols=5, total_tags=5, vgroups=2

| tag-ref比例 | Q5 t0(ms) | t0 vs基线 | Q5 t4(ms) | t4 vs基线 | t0类型 | t4类型 |
|-----------|----------|----------|----------|----------|-------|-------|
| 0/5 | 9.64 | +0% | 8.31 | +0% | literal | literal |
| 1/5 | 89.42 | **+827%** | 79.41 | +856% | tag-ref | literal |
| 3/5 | 95.96 | +895% | 98.82 | +1090% | tag-ref | literal |
| 5/5 | 91.98 | +854% | 95.96 | +1055% | tag-ref | tag-ref |

**结论:**
- **引入 1 个 tag-ref 列即导致 Q5 全局 +827%:** TagRefSourceOperator 只要有 1 个 tag-ref 就需要扫描源表
- **literal tag 列也被拖慢:** 即使 t4 是 literal，当同一张表有 tag-ref 时 t4 查询也变慢 (+856%)
- **原因:** TagRefSourceOperator 的开启是表级别的，不是列级别的

---

## 2. Feature B: VChild-ref-VChild 性能测试

### 核心结论

> **VChild-ref-VChild 是性能最优的特性。** 得益于 catalog 编译时预解析完整链，运行时 planner/executor 完全无感知。L1-L32 的增量开销在噪声范围内 (平均 <0.3ms/层)。

### B-R1: 基线对比

**环境:** rows=10000, data_cols=5, depths=[0,1,2], vgroups=1

| 查询 | L0(ms) | L1(ms) | L2(ms) | L1-L0 | L2-L1 |
|------|--------|--------|--------|-------|-------|
| SELECT * | 19.09 | 18.36 | 18.65 | -0.73 | +0.30 |
| COUNT | 2.02 | 3.93 | 3.51 | +1.91 | -0.42 |
| SUM+AVG | 2.70 | 2.22 | 2.67 | -0.48 | +0.45 |
| WHERE data | 11.67 | 11.05 | 11.46 | -0.62 | +0.41 |
| WHERE tag | 18.19 | 24.19 | 23.89 | +6.01 | -0.30 |
| LAST | 2.17 | 1.96 | 1.77 | -0.21 | -0.19 |

**结论:**
- **L1-L0 (首层):** 绝大多数查询 delta <1ms；WHERE tag 稍高 (+6ms) 可能是 tag 元数据解析
- **L2-L1 (纯引用层):** 全部 <0.5ms，证实 catalog 预解析后运行时零开销
- **部分 L1 比 L0 还快:** 可能是缓存效应或热路径差异

---

### B-R2: 逐层深度增量

**环境:** rows=10000, data_cols=5, depths=[0,1,2,4,8,16,32], vgroups=1

**边际成本:**

| 查询 | L1-L0(ms) | L2-L1(ms) | Avg L2+(ms) | Max L2+(ms) |
|------|----------|----------|------------|------------|
| SELECT * | -0.67 | +1.92 | **0.28** | 1.92 |
| COUNT | +1.49 | -0.44 | **-0.04** | 0.86 |
| SUM+AVG | -0.40 | -0.21 | **0.13** | 0.72 |
| WHERE data | -0.68 | -0.63 | **0.22** | 1.96 |
| LAST | -0.39 | +0.69 | **0.06** | 0.69 |

**结论:**
- **L2+ 平均增量 0.06~0.28ms:** 完全在噪声范围内
- **L32 无线性增长:** 32 层链与 2 层链性能几乎相同
- **原因:** catalog 在首次查询时递归解析完整链到最终物理表，后续执行器直接读物理表数据

---

### B-R3: 查询类型敏感度

**环境:** rows=1000, data_cols=5, depths=[1,4,16,32], vgroups=1

**L32/L1 比值 (越小越稳定):**

| 查询 | L1(ms) | L32(ms) | L32/L1 | 热点排序 |
|------|--------|---------|--------|---------|
| data filter | 1.91 | 2.50 | 131% | 1 (最高) |
| LAST | 1.21 | 1.42 | 118% | 2 |
| ORDER BY | 2.00 | 2.22 | 111% | 3 |
| SELECT * | 2.84 | 2.87 | 101% | - |
| COUNT+SUM | 1.43 | 1.44 | 101% | - |
| tag filter | 4.36 | 4.32 | 99% | - |

**结论:**
- **所有查询类型 L32/L1 在 99~131%:** 深度不影响查询类型
- **data filter 稍敏感:** 可能涉及更多的元数据检查，但绝对差仅 0.6ms

---

### B-R4: 大数据 + 深链

**环境:** rows=1K/10K/50K, data_cols=5, depths=[1,8,32], vgroups=1

**Warm 中位数 (ms):**

| rows | 查询 | L1 | L8 | L32 | L8/L1 | L32/L1 |
|------|------|-----|-----|------|-------|--------|
| 1K | COUNT | 1.69 | 2.11 | 1.64 | 124% | 97% |
| 10K | COUNT | 2.04 | 1.42 | 1.91 | 70% | 93% |
| 50K | COUNT | 2.46 | 1.44 | 1.99 | 58% | 81% |
| 10K | data filter | 11.28 | 10.45 | 11.41 | 93% | 101% |
| 50K | data filter | 45.25 | 47.25 | 46.60 | 104% | 103% |

**冷/热对比 (50K rows):**
- 冷启动 warm/cold 约 48~105%，大数据量下冷热差异极小 (数据 IO 主导)
- 深链的 catalog 解析成本在大数据量下完全被淹没

**结论:**
- **数据量越大，深度影响越小:** 50K rows 时 L32/L1 在 81~111%
- **冷热差异在大数据下消失:** IO 成本远大于 catalog 解析成本

---

### B-R5: col-ref 数量很多

**环境:** rows=10000, data_cols=1~200, depths=[1,8], vgroups=1

**宽度敏感性:**

| data_cols | SELECT * L1 | SELECT * L8 | L8/L1 | COUNT L1 | COUNT L8 | L8/L1 |
|----------|------------|------------|-------|---------|---------|-------|
| 1 | 13.93 | 12.44 | 89% | 2.35 | 2.18 | 93% |
| 5 | 17.78 | 17.96 | 101% | 4.36 | 4.52 | 104% |
| 10 | 20.88 | 21.03 | 101% | 4.89 | 5.33 | 109% |
| 50 | 92.37 | 90.10 | 98% | 27.86 | 18.05 | 65% |
| 100 | 220.19 | 194.99 | 89% | 31.64 | 34.09 | 108% |
| 200 | 394.13 | 428.98 | 109% | 83.60 | 82.94 | 99% |

**L8/L1 比值始终在 89~109%:** 即使 200 列引用，L8 相对 L1 也没有显著增长。

**绝对值增长:**
- W1-L1 → W200-L8: SELECT * 从 14ms → 429ms (**+3079%**), COUNT 从 2ms → 83ms (**+3537%**)
- 但这是列数增加导致的，不是链深度导致的

**结论:**
- **列数是主要性能因子:** 200 列比 1 列慢 30 倍
- **深度不是因子:** L8/L1 在所有宽度下都接近 100%
- **每列边际成本约 2ms:** (429-14)/(200-1) ≈ 2.1ms/列

---

### B-R6: 跨库引用

**环境:** rows=10000, data_cols=5, depths=[1,2,4], vgroups=1

| Depth | 查询 | 同库(ms) | 跨库(ms) | 跨库/同库 |
|-------|------|---------|---------|---------|
| L1 | SELECT * | 15.60 | 14.96 | 96% |
| L2 | SELECT * | 14.02 | 14.69 | 105% |
| L4 | SELECT * | 14.37 | 14.73 | 103% |
| L1 | COUNT | 3.32 | 2.97 | 90% |
| L4 | LAST | 1.52 | 1.54 | 101% |

**结论:**
- **跨库开销在噪声范围内:** 跨库/同库比值在 90~110%
- **原因:** catalog 预解析时已获取所有远程元数据，运行时无需跨库 RPC
- **VChild 的跨库是"零成本"的:** 与 tag-ref 的 +11~18ms 跨库开销形成鲜明对比

---

### B-R7: 小数据 + 极端深链

**环境:** rows=100, data_cols=5, depths=[0,1,2,4,8,16,32], vgroups=1

| 查询 | L1-L0(ms) | L32-L1(ms) | Avg delta(ms) | L32/L1 |
|------|----------|-----------|-------------|--------|
| SELECT * | +0.02 | +0.44 | 0.09 | 134% |
| COUNT | -0.09 | +0.33 | 0.07 | 133% |
| LAST | +0.02 | +0.54 | 0.11 | 154% |

**结论:**
- **即使数据极少 (100 rows)，深链增量也在 0.1ms/层级别**
- **L32/L1 约 130~154%:** 绝对差仅 0.3~0.5ms，在 1ms 级查询中占比大但绝对值极小
- **纯链路解析成本可忽略:** 证实 catalog 预解析的高效性

---

## 3. Feature C: VStable-ref-VStable 性能测试

### 核心结论

> **VStable-ref-VStable 的开销介于 Tag-ref 和 VChild 之间。** 首层有显著开销 (L1-L0 约 3~30ms, 主要来自 fan-out 的 20 个子表解析)，但后续层增量较小 (1~5ms/层)。跨库和 tag-ref 混合的额外开销都在噪声范围内。

### C-R1: 基线对比

**环境:** children=20, rows/child=1000, data_cols=5, tag_cols=3, vgroups=2

| 查询 | L0(ms) | L1(ms) | L2(ms) | L1-L0 | L2-L1 |
|------|--------|--------|--------|-------|-------|
| SELECT * | 46.10 | 59.40 | 60.22 | **+13.30** | +0.82 |
| COUNT | 2.81 | 22.66 | 25.49 | **+19.85** | +2.84 |
| SUM+AVG | 3.59 | 7.06 | 9.82 | +3.47 | +2.76 |
| WHERE tag | 4.24 | 7.83 | 9.15 | +3.59 | +1.32 |
| GROUP BY t0 | 3.14 | 32.23 | 29.10 | **+29.08** | -3.13 |
| DISTINCT t0 | 3.21 | 26.80 | 31.58 | **+23.59** | +4.79 |
| ORDER BY t0 | 6.71 | 36.42 | 34.15 | **+29.71** | -2.26 |
| LAST(c0) | 3.69 | 7.21 | 9.12 | +3.52 | +1.91 |

**结论:**
- **首层 (L1-L0) 开销 3~30ms:** 取决于查询类型。tag 相关操作 (GROUP/DISTINCT/ORDER) 开销最大，因为需要遍历所有虚拟子表
- **第二层 (L2-L1) 增量 0.8~5ms:** 比首层小一个数量级
- **COUNT 的 L1 开销特别大 (+20ms):** 因为虚拟表框架需要汇总所有子表

---

### C-R2: 逐层深度增量

**环境:** children=20, rows/child=1000, data_cols=5, tag_cols=3, vgroups=2, depths=[0,1,2,4,8]

**边际成本:**

| 查询 | L1-L0(ms) | L2-L1(ms) | Avg L2+(ms) | Max L2+(ms) |
|------|----------|----------|------------|------------|
| SELECT * | +7.58 | +3.46 | **3.96** | 9.07 |
| COUNT | +20.61 | +0.05 | **3.34** | 5.23 |
| SUM+AVG | +2.89 | +1.29 | **3.25** | 6.06 |
| WHERE tag | +3.49 | +0.11 | **1.56** | 2.98 |
| GROUP BY | +25.38 | +4.74 | **4.69** | 10.00 |

**结论:**
- **L2+ 平均增量 1.5~4.7ms:** 比首层小但非零 (VChild 是 <0.3ms)
- **原因:** vstable 每层有 20 个子表的 fan-out，每层需要解析和汇总
- **L8 最大增量约 5~10ms:** 8 层链的总额外开销约 20~40ms

---

### C-R3: 数据量少 + 物理表多

**环境:** rows/child=100, children=50~500, data_cols=5, tag_cols=3, vgroups=4, depths=[1,2,4]

**L4-L1 额外开销 (ms):**

| 子表数 | Q2 COUNT | Q4 WHERE tag | Q5 GROUP BY |
|-------|----------|-------------|-------------|
| 50 | +4.78 | +4.47 | +8.79 |
| 100 | +15.59 | +6.15 | +21.78 |
| 200 | +19.21 | +6.05 | +69.56 |
| 500 | +68.07 | +15.97 | +30.94 |

**结论:**
- **子表数增加时 L4-L1 增量增大:** 但不严格线性
- **Q4 WHERE tag 最稳定:** 500 子表时 L4-L1 仅 +16ms (vs L1 的 14ms)
- **Q5 GROUP BY 波动较大:** 可能受 vgroup 分布和调度影响

---

### C-R4: 数据量大 + 物理表多

**状态:** Debug 构建下超时。需 Release 构建重跑。

---

### C-R5: col-ref 数量很多

**环境:** children=50, rows/child=1000, data_cols=5~200, tag_cols=3, vgroups=2, depths=[1,2,4]

**L4/L1 比值:**

| data_cols | SELECT * L4/L1 | COUNT L4/L1 | WHERE tag L4/L1 |
|----------|---------------|-------------|----------------|
| 5 | 109% | 106% | 95% |
| 10 | 103% | 124% | 139% |
| 50 | 123% | 160% | 174% |
| 100 | 136% | **218%** | 194% |
| 200 | **141%** | **208%** | **217%** |

**绝对开销 (L4-L1):**

| data_cols | SELECT * (ms) | COUNT (ms) | WHERE tag (ms) |
|----------|-------------|-----------|---------------|
| 5 | -5 | +8 | +4 |
| 100 | +258 | +262 | +36 |
| 200 | **+909** | **+658** | +83 |

**结论:**
- **列数是 vstable 最大的性能因子:** 200 列时 L4 比 L1 慢 600~900ms
- **COUNT 对宽度最敏感:** 200 列 L4/L1 = 208% (相比 VChild 的 99%)
- **vstable 的 fan-out 放大了列数影响:** 每层 50 子表 × 200 列 = 10,000 次列引用解析

---

### C-R6: 跨库引用

**环境:** children=20, rows/child=1000, data_cols=5, tag_cols=3, vgroups=2, depths=[1,2,4]

**跨库额外开销 (Delta = Cross - Same, ms):**

| Depth | Q1 SELECT * | Q2 COUNT | Q4 WHERE tag | Q5 GROUP BY |
|-------|------------|---------|-------------|-------------|
| L1 | -4.91 | -1.24 | -1.23 | -0.77 |
| L2 | +0.46 | +1.61 | +2.54 | +0.75 |
| L4 | +0.41 | +0.19 | +0.76 | +4.30 |

**结论:**
- **跨库开销在噪声范围内:** 最大仅 +4.3ms (Q5 GROUP BY L4)
- **L1 同库反而有时更慢:** 可能是缓存预热差异
- **VStable 的跨库成本与 VChild 类似，可忽略**

---

### C-R7: 带 tag-ref 的 vstable 链

**环境:** children=20, rows/child=1000, data_cols=5, tag_cols=3, vgroups=2, depths=[1,2,4]

**Literal-tag vs Tag-ref (三者在 vstable 链上):**

| Depth | 查询 | L0(ms) | Literal(ms) | TagRef(ms) | TR-Lit(ms) | TR/Lit |
|-------|------|--------|-----------|-----------|-----------|--------|
| L1 | COUNT | 2.41 | 30.52 | 25.27 | -5.25 | 83% |
| L2 | COUNT | 2.41 | 25.49 | 25.94 | +0.46 | 102% |
| L4 | WHERE tag | 3.60 | 36.42 | 40.22 | +3.80 | 110% |
| L4 | GROUP BY | 2.47 | 32.72 | 31.83 | -0.89 | 97% |

**结论:**
- **Tag-ref 在 vstable 链上的额外开销出乎意料地小:** 绝大多数场景 TR/Lit 在 81~110%
- **tag-ref 开销被 vstable 的 fan-out 开销淹没:** vstable L1 本身就要遍历所有子表，tag-ref 的逐子表扫描成为其中一部分
- **两种开销近似可加但不严重:** vstable 链主导了性能，tag-ref 是次要因素

---

## 4. 三特性横向对比

### 4.1 首层开销 (L1 vs L0 物理表)

| 指标 | Tag-ref | VChild | VStable |
|------|---------|--------|---------|
| SELECT * | +15ms (+34%) | -1ms (-4%) | +13ms (+29%) |
| COUNT | +25ms (+1003%) | +2ms (+95%) | +20ms (+807%) |
| WHERE tag | +30ms (+580%) | +6ms (+33%) | +4ms (+85%) |
| LAST | +6ms (+124%) | -0.2ms (-10%) | +4ms (+95%) |

### 4.2 深度扩展性 (每层边际成本)

| 指标 | Tag-ref | VChild | VStable |
|------|---------|--------|---------|
| 支持深度 | 单跳 | L1~L32 | L1~L8+ |
| Avg L2+ delta | N/A | **<0.3ms** | **1.5~5ms** |
| Max delta | N/A | <2ms | ~10ms |
| 线性增长? | N/A | **否** | **弱线性** |

### 4.3 跨库额外开销

| 指标 | Tag-ref | VChild | VStable |
|------|---------|--------|---------|
| 额外延迟 | **+9~18ms** | **~0ms** | **~0ms** |
| 占比 | 11~28% | 噪声 | 噪声 |

### 4.4 列/Tag 数量敏感度

| 指标 | Tag-ref (tag列数) | VChild (col-ref) | VStable (col-ref) |
|------|------------------|-----------------|------------------|
| 1→50/200 列增长 | +330ms | +400ms | +900ms |
| 增长模式 | 亚线性 | 线性 | 超线性 (fan-out放大) |
| L8/L1@200列 | N/A | ~100% | **141~208%** |

### 4.5 子表数敏感度

| 子表数 | Tag-ref Q5开销 | VChild N/A | VStable Q5 L4-L1 |
|--------|-------------|-----------|-----------------|
| 50 | +61ms | - | +9ms |
| 100 | +114ms | - | +22ms |
| 200 | +229ms | - | +70ms |
| 500 | +575ms | - | +31ms |

---

## 5. 总体结论与建议

### 5.1 性能排序

```
VChild-ref-VChild (最优)
  - 深度零开销, 跨库零开销, 列数线性增长
  - 推荐用于任何需要列引用的场景

VStable-ref-VStable (中等)
  - 首层有 fan-out 开销, 后续层 ~3ms
  - 跨库零开销, 列数超线性增长
  - 避免超过 100 列的深链

Tag-ref (最大开销)
  - WHERE tag 开销 +1400%, 线性增长
  - 跨库额外 +10~18ms
  - 仅 1 个 tag-ref 就触发全局开销
```

### 5.2 优化建议

1. **Tag-ref 最需要优化:**
   - TagRefSourceOperator 批量扫描改为缓存 tag 值 (当前每次查询重建)
   - 考虑在 catalog 层缓存 resolved tag-ref 映射
   - WHERE tag 精确匹配走索引而非全表扫描

2. **VStable 大量 col-ref 需关注:**
   - 200 列 × 4 层 = 40,000 次引用解析，L4/L1 达 208%
   - 考虑批量解析而非逐列逐层

3. **VChild 无需特殊优化:**
   - 架构设计最优 (catalog 预解析)，运行时零开销

### 5.3 使用建议

| 场景 | 推荐特性 | 原因 |
|------|---------|------|
| 需要跨表列引用 | VChild | 零运行时开销 |
| 需要跨超级表聚合 | VStable | 可接受的 fan-out 开销 |
| 需要 tag 共享 | **慎用 Tag-ref** | 开销大，仅适合 tag 查询不频繁的场景 |
| 跨库引用 | VChild/VStable | 跨库零开销 |
| 跨库 tag-ref | Tag-ref (可接受) | 额外 ~15ms，比例可控 |
| 大量大表 (>100列) | VChild | VStable 200 列开销过大 |
| 极端深链 (>8层) | VChild | 深度零开销 |

---

## 附录: 测试文件清单

| 文件 | Feature | 场景 | 状态 |
|------|---------|------|------|
| `test_perf_tag_ref_r1_baseline.py` | A | 基线对比 | PASS |
| `test_perf_tag_ref_r2_tag_queries.py` | A | Tag 查询类型 | PASS |
| `test_perf_tag_ref_r3_many_tables.py` | A | 表多+数据少 | PASS |
| `test_perf_tag_ref_r4_big_data.py` | A | 表多+数据大 | TIMEOUT |
| `test_perf_tag_ref_r5_many_tagrefs.py` | A | tag-ref 数量多 | PASS |
| `test_perf_tag_ref_r6_cross_db.py` | A | 跨库 tag-ref | PASS |
| `test_perf_tag_ref_r7_mixed_tags.py` | A | 混合 tag | PASS |
| `test_perf_vchild_r1_baseline.py` | B | 基线对比 | PASS |
| `test_perf_vchild_r2_depth.py` | B | 逐层深度 | PASS |
| `test_perf_vchild_r3_shape.py` | B | 查询敏感度 | PASS |
| `test_perf_vchild_r4_big_data.py` | B | 大数据+深链 | PASS |
| `test_perf_vchild_r5_many_colrefs.py` | B | col-ref 数量 | PASS |
| `test_perf_vchild_r6_cross_db.py` | B | 跨库引用 | PASS |
| `test_perf_vchild_r7_small_data_deep.py` | B | 小数据+深链 | PASS |
| `test_perf_vstable_r1_baseline.py` | C | 基线对比 | PASS |
| `test_perf_vstable_r2_depth.py` | C | 逐层深度 | PASS |
| `test_perf_vstable_r3_many_tables.py` | C | 表多+数据少 | PASS |
| `test_perf_vstable_r4_big_data.py` | C | 表多+数据大 | TIMEOUT |
| `test_perf_vstable_r5_many_colrefs.py` | C | col-ref 数量 | PASS |
| `test_perf_vstable_r6_cross_db.py` | C | 跨库引用 | PASS |
| `test_perf_vstable_r7_with_tag_ref.py` | C | 混合 tag-ref | PASS |

**公共框架文件:**
- `perf_test_framework.py` — 统一工具 (bench/median/PerfReport/insert_rows)
- `tag_ref_perf_util.py` — Feature A 拓扑构建
- `vchild_perf_util.py` — Feature B 拓扑构建
- `vstable_perf_util.py` — Feature C 拓扑构建
