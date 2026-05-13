# 超级表支持 Keep 参数 RS

## 1. 引言

### 1.1 术语与缩写名词

无

### 1.2 相关文档资料

JIRA [TS-5386](https://jira.taosdata.com:18080/browse/TS-5386)

### 1.3 优先级要求

中

### 1.4 版本要求

企业版和社区版都支持

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/01/23 | 1.0 | 关胜亮 | 新建 |

## 3. 需求目标

允许为超级表设置 Keep 参数，并在 Compact 操作时生效

## 4. 功能需求

1. 创建超级表时，支持 keep 参数，对于企业版，还可以设置  keep1、keep2、keep3 三个值，取值范围和限制条件和 create database 相同
2. 修改超级表时，同样需要支持 keep 参数，与 alter database 相同
3. 数据 compact 时，删除超级表中过期的时序数据
4. 特定说明
   - Compact 语法不调整，即不支持“仅删除过期时序数据的 compact 选项”，具体语法参见[ compact 的 FS](https://taosdata.feishu.cn/wiki/Q5UjwfJoeizl2gkS3iQcAic1nAd)
   - 不支持为子表设置 keep 参数

## 5. 性能需求

无。

## 6. 其他需求

处理好消息兼容和存储兼容，这是本 JIRA 的重点，需要在 TS 中特别说明。
