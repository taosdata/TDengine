# 数据订阅 METADATA 类型消息改进 RS

## 1. 引言

### 1.1 术语与缩写名词

无

### 1.2 相关文档资料

JIRA [TD-33798](https://jira.taosdata.com:18080/browse/TD-33798)

### 1.3 优先级要求

高

### 1.4 版本要求

企业版和社区版都支持

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/02/20 | 1.0 | 佘彦杰 | 新建 |

## 3. 需求目标

1. 在 topic 创建时，指定了  only meta，就不返回数据，提高数据订阅性能和接口一致性。连接器作为中间件，无法判断应用只订阅 meta，还会获取 data，影响性能。
2. 在 `msg.enable.batchmeta` 设置情况下，不管是 `TMQ_RES_METADATA` 还是 `TMQ_RES_TABLE_META` 消息，都返回数组形式元数据。避免连接器解析两种格式。

## 4. 功能需求

1. 当订阅语句是 only meta 时，如 `create topic if not exists topic_name only meta as STABLE stb1``* *` ，希望只返回 `TMQ_RES_TABLE_META` 消息。目前会返回 `TMQ_RES_METADATA` 消息。
2. `msg.enable.batchmeta` 在 `TMQ_RES_METADATA` 消息中不生效。希望生效。

## 5. 性能需求

无。

## 6. 其他需求

无
