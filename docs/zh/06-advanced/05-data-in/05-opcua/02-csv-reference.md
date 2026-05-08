---
title: "OPC UA CSV 映射文件参考"
sidebar_label: "CSV 映射参考"
---

本页详细说明 taosX **OPC UA Data In** 任务所使用的 CSV 映射文件，重点回答两个高频问题：

1. CSV 中每一列的含义是什么？尤其是 `point_id`、`stable`、`tbname`，以及 CSV 与 OPC 服务器浏览视图中看到的 **path**、**display name** 是什么关系。
2. 当 OPC 节点 ID 中包含 **逗号** 等保留字符时，CSV 上传时应如何处理。

本页面向所有使用 taosX 从 OPC UA 服务器采集数据的用户。

## 1. CSV 是映射，不是地址空间

请务必区分以下两件事：

### OPC 服务器地址空间（浏览得到）

- 通过浏览 OPC UA 服务器获取（Explorer 中的"选择数据点"面板，或 `points` 命令）。
- 每个浏览到的数据点包含：`id`、`name`、`description`、`display_name`、`node_type`、`path`。
- `id` 是规范化的 OPC UA 节点 ID，例如 `ns=3;i=1005` 或 `ns=2;s=Channel1.Device1.Tag1`。
- `display_name` 和 `path` 是服务器提供的 **可读标识**，用于帮助工程师定位节点，但 **不是** 协议订阅时使用的标识。

### taosX OPC UA 映射 CSV（上传文件）

- 一份扁平 CSV，针对每一个要采集的数据点，告诉 taosX：
  - 订阅哪个 OPC 节点（`point_id`）；
  - 写入到 TDengine 的哪个超级表/子表（`stable`、`tbname`）；
  - 使用哪些时间戳/数值/质量列；
  - 附加哪些 TDengine 标签值。
- 与 OPC 服务器有关的列只有一个：**`point_id`**，其余列都是定义 TDengine 侧的写入规则。

:::tip
上传 CSV 中 **没有** `path` 列、也 **没有** `display_name` 列。Path 与 display name 属于 OPC 服务器侧，作用是帮你确定要把哪个 `point_id` 写进 CSV。它们的取值仍会通过专门的 **Tag 列** 由 taosX 自动写入 TDengine（参见 3.2 节）。
:::

## 2. CSV 列说明

当你在 Explorer 中选择数据点并下载 CSV 模板时，taosX 会按照 OPC UA 服务器返回的浏览信息自动生成完整的表头：

```text
No.,point_id,enabled,stable,tbname,value_col,value_transform,type,quality_col,ts_col,ts_transform,request_ts_col,request_ts_transform,received_ts_col,received_ts_transform,tag::VARCHAR(1024)::name,tag::VARCHAR(1024)::BrowseName,tag::VARCHAR(1024)::DisplayName,tag::VARCHAR(1024)::Description,tag::VARCHAR(1024)::Path
```

下载到本地的 CSV 已经把 OPC 服务器返回的 **BrowseName**、**DisplayName**、**Description**、**Path** 自动写入对应的 Tag 列，并把规范化后的可读名称写入 `name` 列。**通常你只需要按需调整 `enabled`、`stable`、`tbname` 等列即可，不需要再手工补充浏览信息**。

一段真实的样例数据（节选自 `taosx-opc` 默认导出）：

```text
1,ns=9;s=/beijing/haidian/humidity,1,opc_{type},t_9_beijing_haidian_humidity,val,,,quality,ts,,qts,,rts,,beijing.haidian.humidity,/beijing/haidian/humidity,/beijing/haidian/humidity,,Objects./beijing./beijing/haidian./beijing/haidian/humidity
2,ns=9;s=/beijing/haidian,1,opc_object,t_9_beijing_haidian,val,,,quality,ts,,qts,,rts,,beijing.haidian,/beijing/haidian,/beijing/haidian,,Objects./beijing./beijing/haidian
3,ns=9;s=beijing,1,opc_object,t_9_beijing,val,,,quality,ts,,qts,,rts,,beijing,/beijing,/beijing,,Objects./beijing
4,ns=9;s=/beijing/haidian/temperature,1,opc_{type},t_9_beijing_haidian_temperature,val,,,quality,ts,,qts,,rts,,beijing.haidian.temperature,/beijing/haidian/temperature,/beijing/haidian/temperature,,Objects./beijing./beijing/haidian./beijing/haidian/temperature
```

可以看到：

- 含 Variable 的叶子节点（如 `humidity`、`temperature`）使用 `stable = opc_{type}`，按数据类型落入不同的超级表。
- 中间对象节点 Node（如 `/beijing`、`/beijing/haidian`）使用 `stable = opc_object`，统一归到 object 超级表。
- `tbname` 默认采用 `t_{ns}_{id}` 模式，且 ID 中的 `/` 等字符已被替换为下划线，保证子表名合法且固定。
- 每个数据点的 `BrowseName`、`DisplayName`、`Path` 都已经被填进 Tag 列，便于下游按可读名称查询。

### 2.1 各列含义

| 列名                                       | 是否必填                                                     | 含义                                                                                                                                                                                                                                                                  |
| ------------------------------------------ | ------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `No.`                                      | 可选                                                         | 行号，仅用于人工查看，taosX 解析时会忽略其取值。                                                                                                                                                                                                                      |
| `point_id`                                 | 必填                                                         | 要订阅的 OPC UA 节点 ID，遵循 OPC UA 规范，例如 `ns=9;s=/beijing/haidian/humidity`（字符串型）、`i=85`（数字型，命名空间默认为 0）、`ns=2;g=09087e75-...`（GUID 型）、`ns=2;b=base64==`（不透明型）。                                                                 |
| `enabled`                                  | 可选（默认 `1`）                                             | `1` 表示采集该点，子表不存在时自动创建；`0` 表示跳过该点，并删除对应的子表。                                                                                                                                                                                          |
| `stable`                                   | 必填                                                         | TDengine **超级表** 名称，支持占位符 `{type}`（运行时按源数据类型替换，例如 `int`、`double`、`varchar`）。导出模板默认对叶子节点使用 `opc_{type}`，对没有数据值的对象节点使用 `opc_object`。                                                                          |
| `tbname`                                   | 必填                                                         | TDengine **子表** 名称，支持占位符 `{ns}`（`point_id` 的命名空间部分）和 `{id}`（`i=` / `s=` / `g=` / `b=` 前缀之后的标识值）。生成时不合法的字符（如 `/`、`.`）会被替换为 `_`，保证每个数据点的子表名固定且合法。                                                    |
| `value_col`                                | 可选（默认 `val`）                                           | 存储 OPC 数值的 TDengine 列名。                                                                                                                                                                                                                                       |
| `value_transform`                          | 可选                                                         | 写入前在 taosX 中执行的数值表达式，例如 `val*1.8 + 32`。详见 transform 表达式相关文档。                                                                                                                                                                               |
| `type`                                     | 可选（默认与源类型一致）                                     | 覆盖数值列类型；同时用于替换 `stable` 中的 `{type}` 占位符。                                                                                                                                                                                                          |
| `quality_col`                              | 可选                                                         | 存储 OPC 质量码的列名。如果不需要保留质量码可留空。                                                                                                                                                                                                                   |
| `ts_col`、`ts_transform`                   | `ts_col` / `request_ts_col` / `received_ts_col` 至少配置一个 | **源时间戳** 列名（OPC 服务器为该数值打上的时间戳）及其可选的转换表达式。                                                                                                                                                                                             |
| `request_ts_col`、`request_ts_transform`   | 同上                                                         | **请求时间戳** 列名（taosX 向服务器发起本次读取/订阅时的时间戳）及其可选的转换表达式。                                                                                                                                                                                |
| `received_ts_col`、`received_ts_transform` | 同上                                                         | **接收时间戳** 列名（taosX 实际收到该数值的时间）及其可选的转换表达式。`ts_transform` 等表达式常用于时区修正，例如 `ts + 8 * 3600 * 1000`。                                                                                                                           |
| `tag::TYPE::name`                          | 可选（可有多列）                                             | 每一列 `tag::TYPE::name` 在 TDengine 中生成一个标签列。`tag` 为保留关键字，`TYPE` 为合法的 TDengine 标签类型，`name` 为标签列名。导出模板默认包含 `name`、`BrowseName`、`DisplayName`、`Description`、`Path` 五个 Tag 列，分别由 OPC 浏览信息自动填充，无需手工录入。 |

:::note
对于一行数据，**`ts_col`、`request_ts_col`、`received_ts_col` 至少要配置一个**。当配置了多个时间戳列时，第一个被配置的列将作为 TDengine 的主键。
:::

### 2.2 自动生成的 Tag 列：name / BrowseName / DisplayName / Description / Path

导出模板中的 5 个 Tag 列直接来自 OPC UA 服务器对每个节点的浏览结果：

| Tag 列        | 含义                                                                                                               |
| ------------- | ------------------------------------------------------------------------------------------------------------------ |
| `name`        | 由 taosX 根据 BrowseName 规范化得到的可读名称（去掉前导分隔符、用 `.` 拼接），便于 SQL 中按可读名称做模糊匹配。    |
| `BrowseName`  | OPC UA 服务器返回的 BrowseName 原始值。                                                                            |
| `DisplayName` | OPC UA 服务器返回的 DisplayName 原始值，通常即工程师在客户端中看到的显示名。                                       |
| `Description` | 节点的描述信息（很多服务器为空）。                                                                                 |
| `Path`        | 由根节点（一般是 `Objects`）到当前节点的浏览路径，使用 `.` 拼接每一级 BrowseName，便于在仪表板中按层级重建资产树。 |

如果你希望在入库后用 `WHERE Path LIKE 'Objects./beijing./beijing/haidian.%'` 这类条件按区域过滤，**保留默认 5 个 Tag 列即可**。如果你想新增自定义维度（例如工厂代号、产线编号），按 `tag::TYPE::name` 的格式追加列，并在每行填写对应的取值。

### 2.3 推荐的模板写法

| 目标                     | 写法                                        | 说明                                                                                                |
| ------------------------ | ------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| 每种数据类型一张超级表   | `stable = opc_{type}`                       | 生成 `opc_int`、`opc_double`、`opc_bool` 等，避免类型冲突。导出模板对叶子节点默认采用此写法。       |
| 对象节点单独一张超级表   | `stable = opc_object`                       | OPC 中的对象节点没有数据值，导出模板将其归到 `opc_object`，方便统一管理。                           |
| 每个数据点对应固定的子表 | `tbname = t_{ns}_{id}`                      | 重启后同一个数据点仍落入同一个子表，非法字符会自动替换为 `_`。                                      |
| 保留 / 扩展 Tag 列       | 默认 5 个 Tag 列 + 自定义 `tag::TYPE::name` | 可读名称由 `name` / `DisplayName` 提供，层级由 `Path` 提供；按需追加业务维度（site、line 等）即可。 |

## 3. OPC 节点 ID 中包含逗号的处理

taosX 的 CSV 解析器使用标准 CSV 格式：字段分隔符为 `,`，引用字符为 `"`，规则与 RFC 4180 一致。OPC UA 节点 ID 中常见的 `;`、`=`、`.` 都不会与 CSV 冲突。**但字符串型节点 ID 允许包含逗号**，例如：

```text
ns=2;s=Site,Plant,Tag-01
```

如果直接把这种节点 ID 原样写入 CSV 单元格，解析器会按每个逗号切分，导致该行列数错乱。

### 3.1 正确写法

**用双引号将整个 `point_id` 单元格包起来**：

```text
point_id,enabled,stable,tbname,value_col,type,quality_col,ts_col,received_ts_col
"ns=2;s=Site,Plant,Tag-01",1,opc_{type},t_{ns},val,double,quality,ts,rts
```

解析器会把外层 `"` 包裹的全部内容视作单个字段，引号内部的逗号被当作普通字符原样传给 taosX。

### 3.2 内部含有双引号的处理

如果节点 ID 自身包含 `"`，按 RFC 4180 规则将其重复一次：

```text
"ns=2;s=He said ""hi""",1,opc_{type},t_{ns},val,double,quality,ts,rts
```

解析器会把 `ns=2;s=He said "hi"` 作为 `point_id` 交给 taosX。

### 3.3 不需要做的事

- **不要** 对节点 ID 做 URL 编码。
- **不要** 把 `,` 替换为 `\,`，解析器并不识别反斜杠转义。
- **不要** 把 CSV 分隔符改成 `;` 或 `\t`，分隔符固定为 `,`。
- **不要** 把整行都用引号包起来，仅对需要的单元格加引号即可。

### 3.4 实用建议

当 `point_id` 是 **字符串型节点 ID**（`s=...`）时，始终加双引号。这样不增加任何成本，却是处理保留字符（今天是逗号，明天可能是别的）的最安全方式。数字型（`i=...`）和 GUID 型（`g=...`）由于不可能包含逗号，不需要加引号。

简单口诀：

:::tip
**凡是 `point_id` 中包含 `s=`，一律加双引号。** 数字型（`i=`）不需要。无论 OPC 服务器侧的命名习惯如何，CSV 都能安全上传。
:::

## 4. 快速检查清单

- 在 Explorer 的"选择数据点"面板（或使用 `points` 命令）浏览 OPC 服务器，勾选要采集的数据点。
- 下载 CSV 模板。模板已经包含正确的表头，并把每个点的 `BrowseName`、`DisplayName`、`Path` 等浏览信息自动写入对应的 Tag 列；通常只需要按需调整：
  - `enabled`（不需要的点改为 `0`）
  - `stable`：默认 `opc_{type}` / `opc_object` 已能覆盖大多数场景
  - `tbname`：默认 `t_{ns}_{id}` 已保证子表名固定且合法
  - 至少保留 `ts_col` / `request_ts_col` / `received_ts_col` 中的一个
  - 如有业务维度需要入库，按 `tag::TYPE::name` 的格式追加列即可
- 凡是字符串型节点 ID（`"ns=2;s=..."`）一律用双引号包裹，特别是可能含逗号时。
- 文件保存为 **UTF-8** 编码（带或不带 BOM 均可），其他编码（如 GBK）会被拒绝。
- 在 Explorer 中上传后，先点击 **检查连接（Check Connection）** 并查看解析后的表头，确认行结构是否正确。
