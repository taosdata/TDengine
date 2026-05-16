# 数据源 DSN 规则优化

## 1. 背景

taosX 在启动同步任务时，会根据`源 DSN`去判断是否有对应名称的 topic 存在，如果存在就会订阅现有的 topic 来同步数据，否则创建一个与源数据库名同名的 topic 来订阅数据。在这个逻辑下，如果现有的 topic 不是基于要同步的数据库创建的，只是同名而已，就会订阅到错误的数据。
基于当前这个问题，将 DSN 的定义做一定的命名规则，以便区分 DSN 是数据库、数据表(含超级表、子表、普通表)或者topic。这样就可以判断订阅的 DSN 所对应 topic 的创建 SQL 是否是 DSN 所期待的，进而做出正确的判断。
**关联jira**:
TD-31841

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024-09-09 | 0.1 | 周营昭 | 初稿 |
| 2024-09-12 | 0.2 | 周营昭 | 根据线下讨论意见修改 1. 补充 DSN 的校验规则和细节行为 1. 补充基于 Explorer 的各种使用场景 |
| 2024-09-12 | 0.3 | Wade | 优化描述、排版、修正（Wade 认为的）错误 |
| 2024-09-13 | 0.4 | Huo Linhe | 修改 DSN 设计部分，添加规则描述 |
| 2024-09-18 | 1.0 |  | 定稿 |

## 3. 定义

**DSN**: DSN 是 Data Source Name 的缩写，是一种标准的定义数据源的方式，主要功能是记录连接信息，如数据库服务器名称、数据库名、用户名、密码等，以及驱动程序名称、数据库类型、端口号等关键参数。

## 4. 行为说明

### 4.1 DSN 设计

重新设计 DSN 对 TDengine 数据源的描述，在DSN 所引用的资源名称前面加上类型，以区分 topic、database 还是数据表，具体定义如下，其中 "topic", "db", "table" 为预留的关键字，分别表示该数据源所引用的资源类型为 topic, database 和表 （含超级表，子表和普通表）。

#### 4.1.1 TDengine 3.x 源 DSN

TDengine 3.x 数据源 DSN 以 `tmq` 开头，原生连接形式如 `tmq://<user>:<pass>@hostname:port/``<``topic``>` ，WebSocket 连接形如 `tmq+ws://<user>:<pass>@hostname:port/``<``topic``>` 。其中 <topic> 允许的表示方式包括如下几种：

| 模式 | 说明 | 规则 | 示例 |
| --- | --- | --- | --- |
| /<topicName> | 以 `topicName` 为名的主题 | `topicName` 必须已创建（可在 show topics 中查询到）。 | tmq://root:taosdata@trd01:6030/tlog |
| tmq+ws://root:taosdata@trd01:6041/topic:tlog |
| tmq://root:taosdata@trd01:6041/topic:tlog |
| tmq+ws://root:taosdata@trd01:6041/db:test |
| tmq://root:taosdata@trd01:6030/db:test |
| /**table:**<dbName.tableName> | 以 `table:` 前缀标识订阅目标为数据库 `dbName` 中的表 `tableName`。taosX 为表 `dbName.tableName` 自动创建 Topic 。 | tmq+ws://root:taosdata@trd01:6041/table:test.meters |
|  |  | tmq://root:taosdata@trd01:6030/table:test.meters |
|  |  | tmq://root:taosdata@trd01:6030/table:test.d1 |
|  |  |  |
| /<source>?use.topic.name=<topicName> | 以 `topicName` 全名为 topic 名建立订阅。source 包含点时视为 `table:source`，不包含点时视为 `db:source`。 | 此参数适用于自动创建自定义主题名。 | tmq://root:taosdata@trd01:6041/tlog?use.topic.name=tlogforcloud |

#### 4.1.2 TDengine 2.x 源 DSN

TDengine 的 2.x 数据源使用 `taos` 标识数据源，其原生连接形式如 `taos://<user>:<pass>@<hostname>:<port>/<source``>` ，WebSocket 连接形如 `taos+ws://<user>:<pass>@<hostname>:<port>/<source>` 。其中 `source` 允许的表示方式包括如下几种：

| 模式 | 说明 | 规则 | 示例 |
| --- | --- | --- | --- |
| /<dbName> | 数据源为 dbName 为名的数据库 | 数据库名 `dbName` 必须存在，不存在则报错。 | taos://root:taosdata@trd01:6041/tlog |
| taos+ws://root:taosdata@trd01:6041/db:test |
| taos://root:taosdata@trd01:6030/db:test |
| /**table:**<dbName.tableName> | 以 `dbName` 中的 `tableName` 为数据源 | 1. 检查 `table:` 后是否包含 `.`，不包含则报错。 1. 检查 `dbName` 数据库是否存在，不存在则报错，存在则继续 1. 检查 `tableName` 表是否存在及表的类型，不存在则报错。 | taos://root:taosdata@trd01:6030/table:test.meters |

#### 4.1.3 Sink DSN

所有数据源的目标端（sink）均使用 `taos` 标识，其原生连接形式如 `taos://<user>:<pass>@<hostname>:<port>/<target>` ，WebSocket 连接形如 `taos+ws://<user>:<pass>@<hostname>:<port>/<target>` 。其中 `target` 允许的表示方式包括如下几种：

| 模式 | 说明 | 规则 | 示例 |
| --- | --- | --- | --- |
| /<dbName> | 数据源为 dbName 为名的数据库 | 数据库名 `dbName` 必须存在，不存在则报错。 | taos://root:taosdata@trd01:6041/tlog |
| taos+ws://root:taosdata@trd01:6041/db:test |
| taos://root:taosdata@trd01:6030/db:test |
| /**table:**<dbName.tableName> | 目标端不支持此用法，将报错 | 报错，不支持的前缀标识符 `table:`。 如果数据库名中确实包含 `table:`，则使用 `db:table:` 避免该错误。 |  |

### 4.2 Explorer 中对 DSN 的使用

#### 4.2.1 展示数据源的 DSN 

1. 数据库查看页面，悬浮展示信息及复制内容，所展示的 DSN 即为符合 4.1 节中规则的 DSN
![](./images/img_HUkAb9e1LohMluxRdfjcoAsVnHd.png)

1. 数据订阅展示页面中，创建的 topic，在查看其 DSN 时，按 4.1 节所示规则展示和复制。
![](./images/img_VWvRbSQ9RoAbZSxIOPucgcKBnF5.png)

#### 4.2.2 数据源 DSN 的使用

1. 数据同步
   - 数据源只能选择数据库（现有行为）
   - 同步任务中记录数据源为 `tmq://``root:taosdata``@trd01:6041/db:k1`
   - 任务启动后，创建 topic，在数据订阅列表中名称为`db:k1`,  DSN 预览为：`tmq://root:taosdata@trd01:6041/topic:db:k1`
![](./images/img_ChpNbKiwpoztptxJVQRcN6x8ntd.png)

1. 数据写入
  下图中的数据源 DSN 可以从上文 [4.3.1](https://taosdata.feishu.cn/wiki/W68Hwt60eiinFTk9xIKcez4LnMe?preview_comment_id=7413679473720967171#share-SERLdUx8UoQT4yxIFXRcwQgunhf) 获取 DSN 填入。
   - 数据库 dsn，见场景1中的截图和说明
   - 数据订阅 topic，将场景2中的截图和说明
  ![](./images/img_ZB1VbhX9poTKAHxLALjc0LT4nSe.png)

数据源 DSN tip 说明为“数据源 DSN 可以从其他 explorer 获取: 1. 查看数据库，复制数据库 DSN; 2. 从数据订阅列表中复制 topic DSN”。

## 5. 性能

无

## 6. 兼容性

为了向后兼容，
1. 对于 TDengine 3.x DSN，无前缀时默认为 Topic，即仍然支持 `tmq://host:6030/<topicName>` ，将没有 <type>: 前缀的情况视为主题名。
2. 对于数据库 DSN，无前缀时默认为数据库名，即兼容原 `taos://root:taosdata@localhost:6030/db` 目标库表示。
3. 特殊的，使用 `use.topic.name` 时（仅云服务使用），`/source` 视为数据库（中间包含点时，视为表，等同于：`table:db.tbname`），`use.topic.name` 的值为订阅主题名。

## 7. 运维

无

## 8. 约束和限制

无

## 9. 常见错误和排查

无

## 10. 可观测性

无

## 11. 文档

需要修改 DSN 显示相关的截图。

## 12. 参考文档

**数据同步中的应用说明**：
