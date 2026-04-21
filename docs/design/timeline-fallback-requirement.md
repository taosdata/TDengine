# 时间线回退 (Timeline Fallback) 需求文档

**状态**: 设计中  
**功能**: 当输入 schema 中无主键时间列时，时间线相关函数自动回退到第一个可用的 TIMESTAMP 列  
**版本**: v3.4.1.0

---

## 1. 问题背景

### 1.1 当前限制

TDengine 的时间线函数（`last`、`first`、`diff` 等）和窗口函数（`INTERVAL`、`SESSION` 等）内部隐式依赖主键时间列（`_rowts`）来确定时间顺序。当查询的输入 schema 中没有主键时间列时，这些函数会直接报错，无法使用。

典型的无主键场景包括：

| 场景 | 说明 |
|------|------|
| 子查询只投射非主键时间列 | `SELECT event_time, val FROM t1` — 输出无主键 |
| `UNION ALL` 操作 | 多表合并后的临时表丢失主键信息 |
| 嵌套查询 | 外层查询看到的是内层的投射结果，可能无主键 |
| 临时表 | 没有 schema 元数据的中间结果 |

### 1.2 需求目标

在不增加新语法的前提下，按以下优先级确定时间线：

1. **有主键时间列** → 按当前逻辑处理（行为完全不变）
2. **无主键，有可用 TIMESTAMP 列** → 自动回退到第一个 TIMESTAMP 列作为时间线
3. **无任何 TIMESTAMP 列** → 报错（行为不变）

---

## 2. 受影响函数完整列表

### 2.1 函数分类

根据代码中的 classification 标志，受影响函数分为以下几类：

#### A类：选择函数（IMPLICIT_TS_FUNC + PRIMARY_KEY_FUNC）

这些函数需要隐式时间戳来确定"最后/最先/唯一"的语义，但不需要严格的时间序列顺序。

| 函数 | 说明 | 回退后行为 |
|------|------|-----------|
| `last(col)` | 返回时间线上最后一行的值 | 按回退时间列的最大值确定"最后一行" |
| `first(col)` | 返回时间线上第一行的值 | 按回退时间列的最小值确定"第一行" |
| `last_row(col)` | 返回最后插入的一行 | 按回退时间列确定 |
| `unique(col)` | 返回去重后的值 | 按回退时间列排序 |

#### B类：时间线函数（TIMELINE_FUNC + IMPLICIT_TS_FUNC + PRIMARY_KEY_FUNC）

这些函数逐行处理数据，严格依赖时间序列顺序。回退时**必须**有明确的 ORDER BY 来建立时间线。

| 函数 | 说明 | 回退后行为 |
|------|------|-----------|
| `diff(col)` | 相邻行差值 | 按 ORDER BY 的时间列顺序计算差值 |
| `derivative(col, interval, ignore)` | 导数 | 按 ORDER BY 的时间列顺序计算导数 |
| `irate(col)` | 瞬时速率 | 按 ORDER BY 的时间列顺序计算 |
| `twa(col)` | 时间加权平均 | 按 ORDER BY 的时间列顺序计算 |
| `lag(col [, offset [, default]])` | 前一行的值 | 按 ORDER BY 的时间列确定行顺序 |
| `lead(col [, offset [, default]])` | 后一行的值 | 按 ORDER BY 的时间列确定行顺序 |
| `fill_forward(col)` | 向前填充 NULL | 按 ORDER BY 的时间列确定行顺序 |

#### C类：时间线函数（TIMELINE_FUNC，无 PRIMARY_KEY_FUNC）

这些函数标记为时间线函数但未强制要求主键，理论上受回退影响较小。

| 函数 | 说明 | 回退后行为 |
|------|------|-----------|
| `csum(col)` | 累积和 | 按 ORDER BY 的时间列顺序累加 |
| `mavg(col, k)` | 移动平均 | 按 ORDER BY 的时间列顺序滑动 |
| `statecount(col, op, val)` | 状态计数 | 按 ORDER BY 的时间列顺序 |
| `stateduration(col, op, val)` | 状态持续时间 | 按 ORDER BY 的时间列计算时长 |

#### D类：特殊参数校验

| 函数 | 说明 | 回退后行为 |
|------|------|-----------|
| `elapsed(ts_col)` | 时间跨度计算 | 参数接受任意 TIMESTAMP 列（不仅限主键） |

#### E类：窗口函数

| 窗口类型 | 说明 | 回退后行为 |
|---------|------|-----------|
| `INTERVAL(duration)` | 时间窗口 | 使用回退时间列划分窗口 |
| `SESSION(ts_col, gap)` | 会话窗口 | `ts_col` 接受任意 TIMESTAMP 列 |
| `STATE_WINDOW(col)` | 状态窗口 | 时间线使用回退时间列 |
| `EVENT_WINDOW(...)` | 事件窗口 | 时间线使用回退时间列 |

#### F类：暂不在本次范围内的函数

以下函数虽然有 TIMELINE_FUNC + PRIMARY_KEY_FUNC 标志，但属于高级分析函数，当前版本暂不处理回退：

| 函数 | 原因 |
|------|------|
| `interp` | 插值函数语义复杂，需独立评估 |
| `forecast` | 预测函数，需独立评估 |
| `imputation` | 缺失值填充，需独立评估 |
| `anomalycheck` | 异常检测，需独立评估 |
| `dtw`, `dtw_path`, `tlcc` | 高级时序分析函数，需独立评估 |

---

## 3. 行为变化详细说明

### 3.1 回退规则

```
查找时间线列的优先级：
1. 内部主键列 _rowts → 如存在，直接使用（当前行为不变）
2. 投射列表中第一个 TIMESTAMP 类型列 → 作为回退时间线
3. 找不到任何 TIMESTAMP 列 → 报错（当前行为不变）
```

### 3.2 各场景行为对比

#### 场景1：子查询只投射非主键时间列

```sql
-- 表 t1 有 ts(主键), event_time(TIMESTAMP), val(INT)
-- event_time 与 ts 顺序不同：ts 递增, event_time 递减
--   ts:         00:01, 00:02, 00:03, 00:04, 00:05
--   event_time: 00:05, 00:04, 00:03, 00:02, 00:01
--   val:           10,    20,    30,    40,    50
```

| SQL | 修改前 | 修改后 | 说明 |
|-----|--------|--------|------|
| `select last(val) from (select event_time, val from t1)` | ❌ 报错 0x80002674 | ✅ 返回 10 | event_time 最大值(00:05)对应 val=10 |
| `select first(val) from (select event_time, val from t1)` | ❌ 报错 | ✅ 返回 50 | event_time 最小值(00:01)对应 val=50 |
| `select last_row(val) from (select event_time, val from t1)` | ❌ 报错 | ✅ 返回 50 | 最后插入的行 val=50 |

#### 场景2：子查询 + ORDER BY 非主键时间列 + 时间线函数

```sql
-- 内层 ORDER BY event_time 建立时间线
select diff(val) from (
  select event_time, val from t1 order by event_time
)
```

| SQL | 修改前 | 修改后 | 说明 |
|-----|--------|--------|------|
| `diff(val) FROM (... ORDER BY event_time)` | ❌ 报错 | ✅ 返回4行差值 | 按 event_time 升序计算差值 |
| `csum(val) FROM (... ORDER BY event_time)` | ❌ 报错 | ✅ 返回5行累积和 | 按 event_time 升序累加 |
| `derivative(val, 1s, 0) FROM (... ORDER BY event_time)` | ❌ 报错 | ✅ 返回4行 | 按 event_time 计算导数 |
| `mavg(val, 2) FROM (... ORDER BY event_time)` | ❌ 报错 | ✅ 返回4行 | 按 event_time 顺序滑动平均 |
| `lag(val) FROM (... ORDER BY event_time)` | ❌ 报错 | ✅ 返回5行 | 按 event_time 确定前一行 |
| `lead(val) FROM (... ORDER BY event_time)` | ❌ 报错 | ✅ 返回5行 | 按 event_time 确定后一行 |

#### 场景3：UNION ALL + ORDER BY 非主键时间列

```sql
select csum(val) from (
  select event_time, val from t1
  union all
  select event_time, val from t2
  order by event_time
)
```

| SQL | 修改前 | 修改后 | 说明 |
|-----|--------|--------|------|
| `csum(val) FROM (... UNION ALL ... ORDER BY event_time)` | ❌ 报错 | ✅ 合并后按 event_time 排序计算 | ORDER BY 建立全局时间线 |
| `diff(val) FROM (... UNION ALL ... ORDER BY event_time)` | ❌ 报错 | ✅ 同上 | |
| `diff(val) FROM (... UNION ALL ...)` **无 ORDER BY** | ❌ 报错 | ❌ **仍然报错** | 无时间线，无法计算 |

#### 场景4：窗口函数使用非主键时间列

```sql
select count(val) from (
  select event_time, val from t1 order by event_time
) interval(2s)

select count(val) from (
  select event_time, val from t1 order by event_time
) session(event_time, 5s)
```

| SQL | 修改前 | 修改后 | 说明 |
|-----|--------|--------|------|
| `... INTERVAL(2s)` | ❌ 报错 | ✅ 按 event_time 划分窗口 | |
| `... SESSION(event_time, 5s)` | ❌ 报错 | ✅ 按 event_time 划分会话 | |
| `... STATE_WINDOW(val)` | 取决于时间线 | ✅ 使用回退时间列 | |
| `... EVENT_WINDOW(...)` | 取决于时间线 | ✅ 使用回退时间列 | |

#### 场景5：elapsed() 参数校验

```sql
select elapsed(event_time) from (
  select event_time from t1 order by event_time
)
```

| SQL | 修改前 | 修改后 | 说明 |
|-----|--------|--------|------|
| `elapsed(event_time)` 在子查询中 | ❌ 报错 0x80002812 | ✅ 返回时间跨度 | 参数接受非主键 TIMESTAMP |

#### 场景6：多层嵌套子查询

```sql
select last(val) from (
  select event_time, val from (
    select event_time, val from t1
  )
)
```

| SQL | 修改前 | 修改后 | 说明 |
|-----|--------|--------|------|
| 两层嵌套，无主键 | ❌ 报错 | ✅ 每层都回退到 event_time | 回退在每层独立生效 |

#### 场景7：多个 TIMESTAMP 列

```sql
-- 子查询投射 event_time 和 create_time 两个 TIMESTAMP 列
select last(val) from (
  select event_time, create_time, val from t3
)
```

| SQL | 修改前 | 修改后 | 说明 |
|-----|--------|--------|------|
| 投射中有多个 TIMESTAMP 列 | ❌ 报错 | ✅ 使用**第一个** TIMESTAMP 列 | 按投射顺序确定 |

#### 场景8：仍然报错的情况

以下场景修改后仍然报错，行为不变：

| SQL | 错误原因 |
|-----|---------|
| `select diff(val) from (select val from t1)` | 子查询无任何 TIMESTAMP 列 |
| `select diff(val) from (select ts, val from t1 union all select ts, val from t2)` | UNION ALL 无 ORDER BY，无法建立时间线 |
| `select last(val) from (select cast(1 as int) as v)` | 无 TIMESTAMP 列 |

---

## 4. 实现要点

### 4.1 修改点1：`findAndSetTempTableColumn()` (parTranslater.c)

**目的**: 当子查询投射中找不到 `_rowts` 时，回退到第一个 TIMESTAMP 列  
**触发时机**: 解析器在子查询结果中查找内部主键列  
**修改**: 扫描投射列表时记录 `pFirstTsExpr`（第一个 TIMESTAMP 类型的表达式），若 `_rowts` 查找失败则用 `pFirstTsExpr` 替代  
**影响范围**:
- `last`、`first`、`last_row` 在无主键子查询上的调用
- 窗口函数 `INTERVAL` 的 `pCol` 解析

### 4.2 修改点2：`resetResultTimeline()` (parTranslater.c)

**目的**: ORDER BY 非主键 TIMESTAMP 列时，也能建立有效时间线  
**修改**: 除检查 `isPrimaryKeyImpl(pOrder)` 外，增加 `TSDB_DATA_TYPE_TIMESTAMP == resType.type` 的判断  
**影响范围**: 单表/子查询的 ORDER BY 时间线判定

### 4.3 修改点3：`translateSetOperOrderBy()` (parTranslater.c)

**目的**: UNION ALL 的 ORDER BY 非主键 TIMESTAMP 列时，建立全局时间线  
**修改**: 与修改点2相同的 TIMESTAMP 类型判断  
**影响范围**: `UNION ALL ... ORDER BY event_time` 语句

### 4.4 修改点4：`checkPrimTS()` (builtins.c)

**目的**: `elapsed()` 函数参数校验允许非主键 TIMESTAMP 列  
**修改**: 移除 `!isPrimTs` 条件  
**影响范围**: `elapsed(secondary_ts)` 参数校验

### 4.5 修改点5：`checkSessionWindow()` (parTranslater.c)

**目的**: SESSION 窗口允许使用非主键 TIMESTAMP 列  
**修改**: 列类型校验从 `isPrimaryKeyImpl()` 放宽到 `IS_TIMESTAMP_TYPE()`  
**影响范围**: `SESSION(secondary_ts, gap)` 列校验

---

## 5. 向后兼容性

- 所有修改均为**纯增量**：现有合法查询的行为完全不变
- 有主键的场景走原有代码路径，不受影响
- 回退逻辑仅在主键查找失败时触发
- 回归测试确保现有功能无变化

---

## 6. 已知限制

1. **UNION ALL 无 ORDER BY 仍然报错**：顺序函数需要明确的 ORDER BY 来建立时间线
2. **多 TIMESTAMP 列取第一个**：回退使用投射列表中第一个 TIMESTAMP 列，用户无法指定
3. **不引入新语法**：当前版本不提供显式指定时间线列的语法（作为后续增强）
4. **高级分析函数暂不处理**：`interp`、`forecast`、`imputation`、`anomalycheck`、`dtw` 等需独立评估

---

## 7. 测试覆盖

### 7.1 测试数据

```sql
-- 基础表 t1: ts 递增, event_time 递减
CREATE TABLE t1 (ts TIMESTAMP, event_time TIMESTAMP, val INT);
INSERT INTO t1 VALUES
  ('2022-05-15 00:00:01', '2022-05-15 00:00:05', 10),
  ('2022-05-15 00:00:02', '2022-05-15 00:00:04', 20),
  ('2022-05-15 00:00:03', '2022-05-15 00:00:03', 30),
  ('2022-05-15 00:00:04', '2022-05-15 00:00:02', 40),
  ('2022-05-15 00:00:05', '2022-05-15 00:00:01', 50);

-- 第二张表 t2: 时间范围在 t1 之后
CREATE TABLE t2 (ts TIMESTAMP, event_time TIMESTAMP, val INT);
INSERT INTO t2 VALUES
  ('2022-05-15 00:00:06', '2022-05-15 00:00:10', 60),
  ('2022-05-15 00:00:07', '2022-05-15 00:00:09', 70),
  ('2022-05-15 00:00:08', '2022-05-15 00:00:08', 80);

-- 多 TIMESTAMP 列的表 t3
CREATE TABLE t3 (ts TIMESTAMP, event_time TIMESTAMP, create_time TIMESTAMP, val INT);

-- 带 NULL 的表 t_null
CREATE TABLE t_null (ts TIMESTAMP, event_time TIMESTAMP, val INT);
```

### 7.2 测试分类

| 类别 | 测试项 | 数量 |
|------|--------|------|
| A. 选择函数回退 | last, first, last_row, tail, unique | 5 |
| B. 时间线函数 + ORDER BY 回退 | diff, csum, derivative, mavg, statecount, stateduration, lag, lead, irate, twa, elapsed | 11 |
| C. UNION ALL + ORDER BY 回退 | csum, diff 跨表合并 | 2 |
| D. 窗口函数回退 | INTERVAL, SESSION, STATE_WINDOW, EVENT_WINDOW | 4 |
| E. 边界情况 | 多 TIMESTAMP 列, 无 TIMESTAMP 列, 嵌套子查询, NULL 值 | 4+ |
| F. 向后兼容回归 | last/diff/session 直接查表 | 3+ |
| G. 仍然报错的场景 | UNION ALL 无 ORDER BY, 无 TIMESTAMP 列 | 2+ |

---

**文档版本**: 2.0  
**最后更新**: 2026-04-21
