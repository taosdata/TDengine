# Event Window 多级子事件 FS

## 1. 修订记录

|  编写日期   |   发布日期   | 版本 | 修订人 | 主要修改内容 |
|------------|------------|-----|-------|-------------|
| 2026-04-13 | 2026-04-15 | 1.0 | 邝金清 |     初稿     |
| 2026-04-17 | 2026-04-18 | 1.1 | 邝金清 |     根据评审意见修改     |

## 2. 背景

当前 `EVENT_WINDOW(START WITH ...)` 只支持单个开始条件或一层平铺子事件列表。这个实现把开始事件硬编码成“一层兄弟节点 + 一个扁平索引”，导致以下问题：

- 无法表达嵌套父子关系，例如 `START WITH ((a, b, c), d)` 这种多级子事件结构。
- 事件通知只能给出一层 `conditionIndex`，语义不足以描述当前命中的是哪一个嵌套节点。
- 现有 `windowIndex` 试图表达运行时窗口身份，但它对多级子事件是错的，而且关闭事件里还存在硬编码逻辑，继续保留只会放大歧义。

本功能的目标是为流计算 `EVENT_WINDOW` 提供多级 `START WITH` 子事件能力，并把通知语义从“扁平索引”升级为“静态条件树路径 + 现有 triggerId”模型，使用户、测试和下游通知消费端能够稳定识别开始条件树中的具体节点。

## 3. 定义

- `开始事件树`：`EVENT_WINDOW START WITH` 对应的递归条件结构。叶子节点是普通 `search_condition`，中间节点是由括号包裹的分组。
- `子事件结构`：`START WITH` 中存在括号分组的情况，包括一层子事件和多级嵌套子事件。
- `conditionPath`：开始事件树节点的静态路径字符串，按 SQL 书写顺序生成，例如 `"0"`、`"0.1"`、`"0.1.2"`。
- `conditionIndex`：当前通知节点在父节点下的本地索引，等于 `conditionPath` 最后一段。
- `triggerId`：现有事件通知实例标识。本次不改变其语义，仍用于区分不同运行时窗口实例。
- `_event_condition_path`：event-window 专用占位符，用于在流计算结果中输出当前通知节点的 `conditionPath`，并可与窗口起始时间列一起构成结果表的复合主键语义。

## 4. 行为说明

### 4.1 `START WITH` 支持递归嵌套

`EVENT_WINDOW` 的开始事件从“单条件或一层列表”扩展为“递归条件树”：

```sql
EVENT_WINDOW(
  START WITH start_event_item
  [END WITH end_condition]
)
```

其中 `start_event_item` 可以是：

- 一个普通 `search_condition`
- 一个分组：`(start_event_item, start_event_item, ...)`

示例：

```sql
CREATE STREAM power.s_event_nested
EVENT_WINDOW(
  START WITH ((current >= 12, current >= 10), voltage < 215)
  END WITH voltage < 210
)
FROM power.d1001
INTO power.event_path_result(ts, path, avg_current)
AS SELECT _twstart, _event_condition_path, AVG(current) FROM %%trows;
```

行为约束：

- 现有一层子事件语法继续合法，例如 `START WITH (current >= 12, voltage < 215)`。
- 单个开始条件继续合法，例如 `START WITH current >= 12`。
- `END WITH` 本次保持为单个普通条件，不支持递归嵌套。
- 非法嵌套语法必须在 parser 阶段报错，例如多余逗号、空分组、括号不闭合。

### 4.2 静态条件树路径

开始事件树中的每个节点都分配一个稳定的静态路径，路径由 SQL 书写顺序决定，兄弟节点从 `0` 开始编号。

对如下 SQL：

```sql
START WITH ((current >= 12, current >= 10, current >= 8), voltage < 215)
```

路径定义如下：

- 父分组 `(current >= 12, current >= 10, current >= 8)`：`"0"`
- `current >= 12`：`"0.0"`
- `current >= 10`：`"0.1"`
- `current >= 8`：`"0.2"`
- `voltage < 215`：`"1"`

行为要求：

- `conditionPath` 只表示节点在条件树中的静态位置，不表示运行时窗口实例。
- 没有嵌套的单条件 `START WITH current >= 12` 仍有路径 `"0"`。
- 同一路径在不同窗口实例之间可以重复出现，这是预期行为；运行时实例区分继续依赖 `triggerId`。

### 4.3 事件通知 payload 变更

事件通知继续保留现有 `triggerId`，并调整 `triggerCondition`：

- 新增 `conditionPath`
- 重定义 `conditionIndex`
- 删除 `windowIndex`

示意 payload 如下：

```json
{
  "windowType": "Event",
  "triggerId": "existing-trigger-id",
  "triggerCondition": {
    "conditionIndex": 1,
    "conditionPath": "0.1",
    "fieldValues": {
      "current": 10.3,
      "voltage": 219
    }
  }
}
```

字段语义：

- `conditionPath`：当前通知对应的条件树节点路径。
- `conditionIndex`：当前通知节点在父节点下的本地索引，等于 `conditionPath` 最后一段。
- `fieldValues`：行为保持不变。
  - `WINDOW_OPEN`：仍返回开始条件树整体引用到的所有列值。
  - `WINDOW_CLOSE`：仍返回结束条件引用到的所有列值。
  - 不按 `conditionPath` 裁剪字段。

嵌套分支切换时，通知顺序必须满足：

1. 先关闭当前叶子窗口。
2. 再关闭受影响的祖先分组窗口。
3. 然后打开新分支的祖先分组窗口。
4. 最后打开新叶子窗口。

这条顺序不是装饰，是保证父子事件语义不乱套的底线。

### 4.4 `_event_condition_path` 占位符

新增 `_event_condition_path` 占位符，用户可以在 event-window 流计算结果中直接取到当前通知节点路径。

这个占位符不只是为了“看路径”。它还有一个非常实际的设计目的：让结果表可以用“窗口起始时间 + 条件路径”组成复合主键语义，从而允许祖父窗口、父窗口和子窗口结果共存。

如果结果表里只有一列时间主键，例如：

- `ts = _twstart`

那么父窗口和第一个子窗口很可能拥有相同的起始时间。此时两条结果落到同一个结果表主键上，就会互相覆盖，祖先窗口结果根本留不住。

引入 `_event_condition_path` 后，结果行可以按以下组合唯一标识：

- `ts = _twstart`
- `path = _event_condition_path`

这样即使祖父窗口、父窗口和第一个子窗口共享同一个 `ts`，也仍然可以依靠不同的 `path` 同时保留在结果表中。

正向示例：

```sql
CREATE STREAM power.s_event_path
EVENT_WINDOW(
  START WITH ((current >= 12, current >= 10), voltage < 215)
  END WITH voltage < 210
)
FROM power.d1001
INTO power.event_path_result(ts, condition_path composite key, cnt)
AS
SELECT _twstart ts, _event_condition_path condition_path, COUNT(*) cnt
FROM %%tbname where _c0 >= _twstart and _c0 <= _twend;
```

上例中，`ts + condition_path` 应被当作结果表主键语义来设计和消费。这样当父分组节点 `"0"` 和第一个子节点 `"0.0"` 都以相同 `_twstart` 产出结果时，不会因为只有一个时间键而互相覆盖。

合法性约束：

- 仅在 `EVENT_WINDOW` 场景可用。
- 仅当 `START WITH` 使用了子事件结构时可用。
- 对单个普通开始条件 `START WITH current >= 12`，禁止使用该占位符。
- 在 interval/session/state/count 等非 event-window 场景中使用，必须报错。

错误示例：

```sql
CREATE STREAM power.s_bad_single
EVENT_WINDOW(
  START WITH current >= 12
  END WITH voltage < 210
)
FROM power.d1001
INTO power.bad_result(ts, path, cnt)
AS SELECT _twstart ts, _event_condition_path path, COUNT(*) cnt FROM %%trows;
```

上例必须返回占位符非法错误，而不是悄悄给空值。

### 4.5 保留行为

以下行为保持不变：

- `triggerId` 语义不变。
- 一层子事件语法继续兼容。
- `fieldValues` 收集逻辑不变。
- 不新增 `windowId`、`parentWindowId`、`rootWindowId` 等对外窗口实例字段。
- 不扩展 `END WITH` 为多级结构。

## 5. 性能

本功能不引入新的持久化结构，也不增加网络往返，性能影响主要来自两部分：

- parser/translator 递归遍历开始事件树；
- 运行时在任务初始化时展开一次事件条件元数据表，并在通知阶段做路径查表。

预期影响：

- 对单层或单条件 event-window，开销接近现状。
- 对多级子事件，额外成本随开始事件树节点数线性增长。
- 通知 payload 新增一个短字符串字段 `conditionPath`，体积增长可控。

本次不承诺具体数值型性能收益或损耗，因为源材料里没有性能基线数据。瞎写指标是垃圾行为，这里不装。

## 6. 安全

本功能不引入新的鉴权模型，也不扩大数据可见范围，安全边界沿用现有流计算和通知机制：

- `_event_condition_path` 只暴露条件树路径，不暴露额外业务数据。
- `conditionPath` 与 `conditionIndex` 来自用户自己定义的 `START WITH` 结构，不包含额外敏感信息。
- `fieldValues` 行为保持不变，因此不会因为本功能额外暴露新列。
- 非法使用 `_event_condition_path` 必须被 parser/translator 拦截，避免在错误场景下产生不确定输出。

## 7. 兼容性

兼容性分为“保留”和“破坏”两部分。

保留项：

- 单个开始条件和一层子事件语法继续合法。
- `triggerId`、`fieldValues` 语义保持不变。
- 现有 create-stream request 中 `startCond` 仍通过 AST 序列化表达，不新增新的请求体字段。

破坏项：

- 事件通知 payload 删除 `windowIndex`。
- 事件通知 payload 中 `conditionIndex` 从“扁平子事件索引”改为“当前通知节点在父节点下的本地索引”。

因此，依赖以下旧行为的消费端必须同步调整：

- 直接读取 `windowIndex` 的通知消费逻辑。
- 把 `conditionIndex` 当成全局或一层兄弟索引使用的解析逻辑。

之所以必须破坏，是因为旧模型本来就是错的，继续兼容只会把错误语义永久固化。

## 8. 运维

本功能不新增部署参数、不引入额外组件、不修改安装拓扑。运维侧需要关注的只有行为变更验证：

- 升级后，依赖事件通知的下游服务要确认已经识别 `conditionPath`，并移除对 `windowIndex` 的依赖。
- 如果客户环境启用了 notify 回调，需要在灰度环境先做通知 payload 兼容性回归。
- 对于异常排查，应优先检查 stream 定义 SQL、notify 消费日志以及下游 JSON 解析代码，而不是去怀疑存储层。

## 9. 使用场景

### 场景 1：分层告警

电表告警规则包含一个嵌套电流分组和一个并列电压分支：

- 一级分组：`(current >= 12, current >= 10)`
- 同级另一路：`voltage < 215`

用户希望在流结果中区分“高电流分组打开”还是“电压异常打开”，并且知道命中的是分组父节点还是某个叶子节点。此时可以用 `conditionPath` 和 `_event_condition_path` 直接区分。

### 场景 2：通知消费端精确分流

下游告警平台消费 `notify` 事件时，需要把 `"0"` 路径映射成“高电流父分组”，把 `"0.1"` 映射成“高电流二级阈值”，把 `"1"` 映射成“低电压事件”。多级子事件没有稳定路径根本做不到这件事。

### 场景 3：分支切换追踪

当同一窗口在不同子树之间切换，例如先命中 `current >= 12`，随后切到 `voltage < 215`，用户希望通知顺序能正确反映旧分支关闭、新分支打开，而不是只看到一串毫无层级意义的索引。新的父子通知顺序就是为这个场景服务的。

### 场景 4：流结果回查

用户在结果表中保留 `_event_condition_path`，不仅可以按路径统计哪类开始事件最常触发，还可以避免祖先窗口和子窗口结果因为共享 `_twstart` 而互相覆盖：

```sql
SELECT condition_path, COUNT(*) FROM power.event_path_result GROUP BY condition_path;
```

### 场景 5：兼容现有一层子事件

已有 stream 使用一层 `START WITH (current >= 12, voltage < 215)`。升级后 SQL 仍然合法，通知里会继续返回 `conditionIndex`，但消费端可以进一步读取更明确的 `conditionPath`。

## 10. 约束和限制

约束：

- `_event_condition_path` 只能用于 event-window 且 `START WITH` 使用了子事件结构。
- `conditionIndex` 必须始终等于 `conditionPath` 最后一段。
- 路径编号严格按 SQL 书写顺序生成，不能按运行时命中顺序重排。

限制：

- `END WITH` 仍然只能是单个普通条件，不能写成多级结构。
- 本次不提供对外窗口实例层级标识，运行时实例关联仍需依赖 `triggerId`。
- `fieldValues` 不按具体 `conditionPath` 裁剪；如果用户希望只拿命中叶子的局部字段，这次做不到。
- 历史上依赖 `windowIndex` 的外部逻辑必须改，不存在“零成本兼容”这种童话。

## 11. 常见错误和排查

### 错误 1：嵌套语法写错

表现：

- 创建 stream 时报 parser 语法错误。

常见原因：

- 分组中多写逗号，例如 `((current >= 12,), voltage < 215)`。
- 括号不匹配。
- 把 `END WITH` 也误写成分组。

排查方式：

- 先把 `START WITH` 化简为最小可解析结构，再逐步恢复嵌套层级。
- 确认每一层分组都满足 `(item, item, ...)` 形式。

### 错误 2：`_event_condition_path` 使用场景非法

表现：

- 创建 stream 时报占位符非法错误。

常见原因：

- 在非 event-window 场景使用。
- `START WITH` 只有单个普通条件，没有子事件结构。

排查方式：

- 确认窗口类型是 `EVENT_WINDOW`。
- 确认 `START WITH` 至少包含一层括号分组。

### 错误 3：下游通知解析失败

表现：

- notify 消费端反序列化失败，或逻辑分支不再命中。

常见原因：

- 仍然强依赖 `windowIndex`。
- 仍然把 `conditionIndex` 当成旧的扁平索引语义。

排查方式：

- 检查消费端 JSON schema。
- 使用抓包或服务日志确认 payload 中是否已经切换为 `conditionPath + conditionIndex`。

### 错误 4：父子通知顺序与预期不符

表现：

- 用户认为切换分支时通知顺序不对。

排查方式：

- 核对实际命中的叶子路径。
- 按“旧叶子关闭 -> 旧祖先关闭 -> 新祖先打开 -> 新叶子打开”顺序检查日志。
- 确认用户预期没有把静态路径和运行时实例混为一谈。

## 12. 可观测性

- `taos shell`：可以通过 `_event_condition_path` 在结果表中直接观察当前命中的开始事件路径。
- notify 消费日志：可直接看到 `triggerCondition.conditionPath` 和新的 `conditionIndex` 语义。
- taos Explorer / TDinsight：本次不新增专门 UI 配置项，也不要求 UI 改造；如果这些组件直接展示 notify payload，应按新字段语义渲染。

## 13. 安装和卸载

本功能不要求额外安装或卸载脚本，也不新增独立组件。正常版本安装、升级、回滚流程即可生效。

需要注意的只有两件事：

- 升级前确认 notify 消费端已评估 `windowIndex` 删除的影响。
- 回滚时如果下游已经切换到依赖 `conditionPath` 的逻辑，需要同步回滚消费端适配，否则行为会断裂。

## 14. 文档

需要修改企业版文档：否。本功能位于 `community/source/` 流计算能力，没有企业专属行为差异。

需要修改官网文档：是。至少需要补充以下内容：

- `EVENT_WINDOW START WITH` 递归嵌套语法
- 事件通知 payload 中 `conditionPath` / `conditionIndex` 新语义
- `_event_condition_path` 占位符使用约束与示例
- `windowIndex` 删除说明

发布前应准备对应文档 PR，并同步通知文档评审。

## 15. 参考文档

- `specs/event-window-multilevel-sub-events/design.md`
- `specs/event-window-multilevel-sub-events/implementation.md`
- `community/docs/zh/05-basic/01-model.md`

## 16. 附录

### 16.1 条件路径示例

对：

```sql
START WITH ((current >= 12, current >= 10), voltage < 215)
```

路径映射如下：

| 节点 | `conditionPath` | `conditionIndex` |
|------|-----------------|------------------|
| 父分组 `(current >= 12, current >= 10)` | `0` | `0` |
| `current >= 12` | `0.0` | `0` |
| `current >= 10` | `0.1` | `1` |
| `voltage < 215` | `1` | `1` |

### 16.2 通知兼容性摘要

- 保留：`triggerId`、`fieldValues`
- 新增：`triggerCondition.conditionPath`
- 变更：`triggerCondition.conditionIndex`
- 删除：`windowIndex`

### 16.3 测试覆盖摘要

本功能至少需要覆盖以下验证：

- parser 能正确解析嵌套 `START WITH`，并稳定序列化嵌套开始条件树。
- notify 能正确输出父分组和叶子节点的打开/关闭事件。
- 分支切换时通知顺序正确。
- `conditionIndex` 与 `conditionPath` 最后一段一致。
- `windowIndex` 不再出现在 notify payload。
- `_event_condition_path` 在合法场景可用，在非法场景报错。
- 结果表以 `_twstart + _event_condition_path` 共同标识结果行时，祖先窗口和子窗口结果可以共存，不会因相同起始时间互相覆盖。
