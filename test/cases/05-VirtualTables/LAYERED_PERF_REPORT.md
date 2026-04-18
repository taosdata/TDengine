# 虚拟表分层引用性能对比报告

**分支**: `enh/tag-ref`  
**测试环境**: Linux x86-64, Debug 构建, 非 ASAN, debugFlag=131  
**测试日期**: 2026-04-18  
**测试文件**: `test/cases/05-VirtualTables/test_vtable_layered_perf.py`  
**方法**: 每查询重复 5 次取中位数, 20 子表 × 1000 行/子表

---

## 1. 总体结论

### 虚拟表 (vtable) 链: 深度几乎无影响
| 深度 | COUNT(*) | 相对 L0 | 每层增量 |
|------|----------|---------|---------|
| L0 (源表) | 1.89ms | 100% | - |
| L1 | 5.86ms | 310% | +3.97ms |
| L2 | 5.22ms | 276% | -0.63ms |
| L4 | 5.16ms | 273% | -0.06ms |
| L8 | 5.92ms | 313% | +0.76ms |

**结论**: vtable L1 有 ~4ms 固定开销（虚拟表元数据解析），L2-L8 无额外增量。**深度不影响性能**。

### 虚拟超级表 (vstable) 链: L1 有大跳跃，后续层增量小
| 拓扑 | L0 → L1 增量 | L1 → L2 增量 | L2 → L3 增量 | L3 → L4 增量 |
|------|-------------|-------------|-------------|-------------|
| vstb-literal | +22.60ms | -0.07ms | +2.53ms | +3.41ms |
| vstb-tagref | +34.59ms | -3.17ms | +2.41ms | -1.66ms |

**结论**: L0→L1 是主要开销（20-35ms），后续每层仅 0-3ms。tag-ref 比 literal 多 ~12ms 固定开销。

---

## 2. 详细对比

### 2.1 vtable ref 普通表 vs vtable ref vtable

| 查询 | 普通表 L0 | vt-ref-norm L1 | vt-ref-vt L2 | L1 开销 | L2 开销 |
|------|----------|---------------|-------------|---------|---------|
| SELECT * | 31.45ms | 31.98ms | 27.91ms | +2% | -11% |
| COUNT(*) | 2.47ms | 5.13ms | 5.51ms | +107% | +123% |
| SUM+AVG | 3.12ms | 3.48ms | 3.99ms | +11% | +28% |
| filter | 30.62ms | 30.80ms | 31.53ms | +1% | +3% |
| LAST | 2.24ms | 2.07ms | 1.77ms | -8% | -21% |

**结论**: vtable 不论引用普通表还是其他 vtable，性能几乎相同。COUNT(*) 有固定 ~3ms 开销，其余查询 <5% 差异。vtable-ref-vtable 无额外惩罚。

### 2.2 vstable 全拓扑对比（20 子表 × 1000 行）

| 拓扑 | SELECT * | COUNT(*) | SUM+AVG | tag filter | GROUP BY |
|------|----------|----------|---------|------------|----------|
| normal-stable L0 | 40.47ms | 2.63ms | 3.92ms | 3.84ms | 2.67ms |
| vstb-literal L1 | 56.02ms | 26.24ms | 12.31ms | 11.44ms | 27.15ms |
| vstb-tagref L1 | 56.37ms | 29.57ms | 11.76ms | **40.49ms** | 29.93ms |
| vstb-literal L2 | 55.96ms | 28.69ms | 11.44ms | 8.16ms | 29.99ms |
| vstb-tagref L2 | 63.38ms | 37.21ms | 11.81ms | **34.00ms** | 30.62ms |

**关键发现**:
- **SELECT ***: vstb L1/L2 比普通 stable 慢 38-57%，tag-ref 与 literal 差异很小（<2%）
- **COUNT(*)**: vstb 比普通 stable 慢 10-14 倍，tag-ref 比 literal 额外多 10-30%
- **tag filter**: **tag-ref 是最大瓶颈** — literal L1: 11ms, tag-ref L1: 40ms (3.5 倍差距)
- **GROUP BY**: 各拓扑基本一致

### 2.3 vstable 深度链详情

#### vstable + literal tags (L0-L4)
| 深度 | SELECT * | COUNT(*) | SUM+AVG | tag filter | GROUP BY |
|------|----------|----------|---------|------------|----------|
| L0 | 40.38ms | 3.19ms | 2.77ms | 4.04ms | 2.87ms |
| L1 | 59.72ms (+48%) | 28.39ms (+791%) | 10.17ms (+267%) | 7.24ms (+79%) | 29.01ms (+909%) |
| L2 | 62.08ms (+4%) | 34.36ms (+21%) | 11.87ms (+17%) | 8.94ms (+23%) | 29.78ms (+3%) |
| L3 | 55.01ms (-11%) | 26.83ms (-22%) | 13.10ms (+10%) | 8.43ms (-6%) | 33.46ms (+12%) |
| L4 | 61.26ms (+11%) | 27.66ms (+3%) | 17.97ms (+37%) | 9.89ms (+17%) | 34.58ms (+3%) |

#### vstable + tag-ref (L0-L4)
| 深度 | SELECT * | COUNT(*) | SUM+AVG | tag filter | GROUP BY |
|------|----------|----------|---------|------------|----------|
| L0 | 39.57ms | 2.42ms | 3.25ms | 4.48ms | 3.03ms |
| L1 | 59.89ms (+51%) | 30.96ms (+1180%) | 11.08ms (+241%) | 39.76ms (+788%) | 36.10ms (+1092%) |
| L2 | 65.50ms (+9%) | 34.14ms (+10%) | 11.96ms (+8%) | 44.57ms (+12%) | 37.51ms (+4%) |
| L3 | 65.96ms (+1%) | 33.85ms (-1%) | 15.28ms (+28%) | 43.83ms (-2%) | 38.72ms (+3%) |
| L4 | 64.17ms (-3%) | 38.17ms (+13%) | 15.95ms (+4%) | 46.13ms (+5%) | 40.92ms (+6%) |

---

## 3. 性能模型总结

### 3.1 开销模型

```
vtable 查询延迟 ≈ 源表延迟 + 固定开销(~3-5ms) + 0 × 深度
vstable 查询延迟 ≈ 源表延迟 + 固定开销(~20-35ms) + ~2ms × 额外深度
tag-ref 额外开销 ≈ +10-35ms (tag filter 场景最严重)
```

### 3.2 每层边际成本

| 拓扑 | L0→L1 | L1→L2 | L2→L3 | L3→L4 |
|------|-------|-------|-------|-------|
| vtable chain | +4.0ms | -0.6ms | - | -0.1ms |
| vstable-literal | +22.6ms | -0.1ms | +2.5ms | +3.4ms |
| vstable-tagref | +34.6ms | -3.2ms | +2.4ms | -1.7ms |

### 3.3 性能等级

| 场景 | 评估 | 说明 |
|------|------|------|
| vtable ref 普通表/vtable | ✅ 优秀 | 深度无影响，固定开销 <5ms |
| vstable SELECT * | ✅ 可接受 | +38-57% 开销，绝对值 <25ms |
| vstable COUNT/GROUP BY | ⚠️ 需关注 | 比源 stable 慢 10x+，固定开销大 |
| vstable SUM+AVG | ⚠️ 需关注 | 深度每层 +10-37% |
| vstable tag filter (literal) | ✅ 可接受 | L1 +79%，绝对值 <12ms |
| vstable tag filter (tag-ref) | ❌ 严重瓶颈 | 比 literal 慢 3-5 倍，条件下推失效 |

---

## 4. 建议

1. **vtable 链可放心使用** — 即使 L8 深度也无性能退化
2. **vstable 链 L2+ 增量很小** — 主要开销在 L1（元数据解析），后续层仅 ~2ms/层
3. **tag-ref 的 tag filter 仍是核心瓶颈** — 需要在 planner 层实现 tag-ref 值内联以支持条件下推
4. **vstable 的 COUNT/GROUP BY 固定开销偏高** — 需要优化 VirtualTableScan 的子表枚举路径
