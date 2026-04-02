# 功能测试报告：interval FILL 现状支持矩阵

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-31 | 2026-03-31 | 0.1 | GitHub Copilot | 新建 interval FILL 现状支持矩阵测试文档 |

## 2. 测试目标

本测试文档用于沉淀当前 interval FILL 的实际行为，为后续 FILL 语义设计、实现评审和差异分析提供可复用的基线。

- 验证 `first(v)`、`avg(v)`、`sum(v)`、`last(v)`、`count(*)` 在 `FILL(NULL)`、`FILL(VALUE)`、`FILL(PREV)`、`FILL(NEXT)` 下的当前结果。
- 验证 `NULL/NULL_F`、`VALUE/VALUE_F` 在“部分空窗”“整段空区间”“PARTITION BY 缺失分组”三类场景中的实际差异。
- 明确 interval 当前语义中的关键边界，尤其是 `count(*)`、force/non-force 和缺失分组的行为。

## 3. 参考文档

- 设计文档：`../05-设计文档/external_window FILL 分析设计.md`
- 测试脚本：`TDinternal/community/test/cases/13-TimeSeriesExt/08-ExternalWindow/test_external_fill.py`

## 4. 测试结论

本轮新增 matrix case 已完成执行，结果稳定通过。interval 当前 FILL 行为可以归纳为以下几点：

- 在非 `PARTITION BY` 且查询范围内存在真实数据的场景，`NULL` 与 `NULL_F` 等价，`VALUE` 与 `VALUE_F` 等价。
- 在整段查询区间完全无数据时，`NULL`、`VALUE` 不产出窗口行，`NULL_F`、`VALUE_F` 会强制产出窗口行。
- 在 `PARTITION BY` 场景下，如果某个分组在查询范围内没有任何数据，该分组不会被物化；`NULL_F`、`VALUE_F` 也不会额外补出该缺失分组。
- 对空窗口的聚合列填充，`PREV`/`NEXT` 会传播相邻真实窗口的聚合结果，`VALUE` 会直接把用户值写入所有可填充聚合列。
- interval 当前实现中，`count(*)` 不是固定为 `0`：在空窗口上可能为 `NULL`、用户填充值，或由 `PREV/NEXT` 传播得到。
- 强制填充场景中，输出窗口的 `_wstart` 按 `INTERVAL(1m)` 的窗口边界对齐，而不是简单从 SQL 的查询起点原样开始。

## 5. 测试环境

- OS: Linux
- Python: 3.10.12
- Test Framework: pytest 8.3.5
- Target Repo: `TDinternal`
- Test Entry: `TDinternal/community/test/cases/13-TimeSeriesExt/08-ExternalWindow/test_external_fill.py`
- 验证命令：`cd TDinternal/community/test && /usr/bin/python3 -m pytest cases/13-TimeSeriesExt/08-ExternalWindow/test_external_fill.py -k "support_matrix" --skip_stop`
- 验证结果：`passed`

## 6. 功能测试

主要测试覆盖内容包括 interval FILL 在非分组、整段空区间以及 `PARTITION BY` 缺失分组等场景下的现状行为矩阵。

### 6.1 interval FILL 现状矩阵

#### 6.1.1 测试要点

- 使用同一组最小数据集覆盖“前导空窗”“中间空窗”“尾部空窗”。
- 同时观察 `first/avg/sum/last/count(*)` 五类结果列，避免只看单一聚合函数造成误判。
- 将“部分空窗”“整段空区间”“分组缺失”拆开验证，分别确认 force/non-force 的边界。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `test_interval_fill_support_matrix_non_partition` | 验证无 `PARTITION BY` 时 `NULL/VALUE/PREV/NEXT` 对 `first/avg/sum/last/count(*)` 的实际填充结果 | 通过 |
| 2 | `test_interval_fill_support_matrix_force_behavior` | 验证无 `PARTITION BY` 时 `NULL/NULL_F`、`VALUE/VALUE_F` 的 force/non-force 差异，以及 `PARTITION BY` 下缺失分组的物化行为 | 通过 |

#### 6.1.3 基础测试数据

无 `PARTITION BY` 测试表：`meters_np(ts, v)`

- 查询范围覆盖 5 个 1 分钟窗口。
- 仅在第 2 个窗口写入 `v=10`。
- 仅在第 4 个窗口写入 `v=30`。
- 第 1、3、5 个窗口均为空窗口。

`PARTITION BY` 测试表：`meters_pt(ts, v) tags(gid int)`

- `gid=1`：查询范围内与无分组用例相同，存在两个真实窗口。
- `gid=2`：表中有数据，但均落在查询范围之外，因此在查询范围内完全无数据。

#### 6.1.4 非分组聚合结果矩阵

查询形态：

```sql
select cast(_wstart as bigint) as ws,
       first(v) as fv,
       avg(v) as av,
       sum(v) as sv,
       last(v) as lv,
       count(*) as cv
from meters_np
where ts >= <start> and ts <= <end>
interval(1m) fill(...)
order by ws;
```

空窗口行为矩阵如下：

| FILL 模式 | 前导空窗口 | 中间空窗口 | 尾部空窗口 | `count(*)` 行为 |
| --- | --- | --- | --- | --- |
| `NULL` | `fv/av/sv/lv/cv` 全为 `NULL` | `fv/av/sv/lv/cv` 全为 `NULL` | `fv/av/sv/lv/cv` 全为 `NULL` | 空窗口为 `NULL` |
| `VALUE(777,777,777,777,777)` | 五列全部填 `777` | 五列全部填 `777` | 五列全部填 `777` | 空窗口被填为 `777` |
| `PREV` | 无前值可用，五列均为 `NULL` | 传播上一真实窗口结果 `10/10/10/10/1` | 传播上一真实窗口结果 `30/30/30/30/1` | 空窗口复制上一窗口结果 |
| `NEXT` | 传播下一真实窗口结果 `10/10/10/10/1` | 传播下一真实窗口结果 `30/30/30/30/1` | 无后值可用，五列均为 `NULL` | 空窗口复制下一窗口结果 |

对应到本次实际断言后的窗口结果如下：

| 窗口位置 | `NULL` | `VALUE(777...)` | `PREV` | `NEXT` |
| --- | --- | --- | --- | --- |
| 第 1 个窗口 | `(NULL, NULL, NULL, NULL, NULL)` | `(777, 777, 777, 777, 777)` | `(NULL, NULL, NULL, NULL, NULL)` | `(10, 10, 10, 10, 1)` |
| 第 2 个窗口 | `(10, 10, 10, 10, 1)` | `(10, 10, 10, 10, 1)` | `(10, 10, 10, 10, 1)` | `(10, 10, 10, 10, 1)` |
| 第 3 个窗口 | `(NULL, NULL, NULL, NULL, NULL)` | `(777, 777, 777, 777, 777)` | `(10, 10, 10, 10, 1)` | `(30, 30, 30, 30, 1)` |
| 第 4 个窗口 | `(30, 30, 30, 30, 1)` | `(30, 30, 30, 30, 1)` | `(30, 30, 30, 30, 1)` | `(30, 30, 30, 30, 1)` |
| 第 5 个窗口 | `(NULL, NULL, NULL, NULL, NULL)` | `(777, 777, 777, 777, 777)` | `(30, 30, 30, 30, 1)` | `(NULL, NULL, NULL, NULL, NULL)` |

#### 6.1.5 force / non-force 结果矩阵

非分组，查询范围内存在部分真实数据：

| 对比项 | 观察结果 | 结论 |
| --- | --- | --- |
| `NULL` vs `NULL_F` | 输出窗口数和各窗口结果完全一致 | 在“部分空窗但整体非空”场景下等价 |
| `VALUE` vs `VALUE_F` | 输出窗口数和各窗口结果完全一致 | 在“部分空窗但整体非空”场景下等价 |

非分组，整个查询范围无任何数据：

查询形态：

```sql
select cast(_wstart as bigint) as ws, avg(v) as av
from meters_np
where ts >= <empty_start> and ts < <empty_end>
interval(1m) fill(...)
order by ws;
```

| FILL 模式 | 是否输出行 | 空窗口值 | 备注 |
| --- | --- | --- | --- |
| `NULL` | 否 | 无 | 非强制模式，不产出窗口 |
| `NULL_F` | 是 | `NULL` | 强制产出 3 个窗口 |
| `VALUE(777)` | 否 | 无 | 非强制模式，不产出窗口 |
| `VALUE_F(777)` | 是 | `777` | 强制产出 3 个窗口 |

补充观察：

- 强制产出时，`_wstart` 按窗口边界对齐。
- 本次空区间 case 中，对齐后的首个 `_wstart` 早于原始 `where ts >= <empty_start>` 的起点，这体现的是窗口边界对齐行为，不是测试误差。

`PARTITION BY`，存在缺失分组：

查询形态：

```sql
select gid, cast(_wstart as bigint) as ws, count(*) as cv
from meters_pt
where ts >= <start> and ts <= <end>
partition by gid
interval(1m) fill(...)
order by gid, ws;
```

| 分组 | `NULL` / `NULL_F` | `VALUE(777)` / `VALUE_F(777)` | 结论 |
| --- | --- | --- | --- |
| `gid=1` | 产出 5 个窗口，空窗口分别为 `NULL` 或真实值 | 产出 5 个窗口，空窗口分别为 `777` 或真实值 | 有数据的分组按正常 partial-empty 行为处理 |
| `gid=2` | 不产出任何窗口 | 不产出任何窗口 | 查询范围内无数据的分组不会被物化，force 模式也不补出该分组 |

#### 6.1.6 对后续设计讨论的直接启示

| 主题 | interval 当前现状 | 说明 |
| --- | --- | --- |
| `count(*)` 在空窗口中的值 | 可能为 `NULL`、用户值、或由 `PREV/NEXT` 传播 | 设计讨论时不能默认其恒为 `0` |
| force/non-force 在非分组部分空窗场景 | `NULL == NULL_F`，`VALUE == VALUE_F` | 可作为后续语义收敛的参考基线 |
| force/non-force 在非分组整段空区间场景 | `NULL_F`、`VALUE_F` 强制产出窗口，`NULL`、`VALUE` 不产出 | 这是 force/non-force 的实际分界点 |
| force/non-force 在分组缺失场景 | 缺失分组不物化，force 模式也不补出 | 分组场景应单独表述，不宜直接套用非分组结论 |

## 7. 易用性测试

不涉及。

## 8. 长期稳定性测试

无。

## 9. 性能测试

无。

## 10. 安全性测试

无。

## 11. 兼容性测试

不涉及兼容性测试。

## 12. 已知问题和限制

- 本文档记录的是 interval 当前实现的实际行为，不代表后续任意新窗口类型的最终产品语义。
- 本轮 matrix case 重点覆盖 `count(*)`，未在同一份矩阵中展开 `count(col)`；如需给设计评审提供更完整基线，后续建议单独补充 `count(col)` 现状矩阵。
- 本次验证聚焦 `NULL`、`VALUE`、`PREV`、`NEXT` 以及 `NULL_F`、`VALUE_F` 的 force/non-force 边界，未纳入 `LINEAR`、`NEAR`、`SURROUND`。
