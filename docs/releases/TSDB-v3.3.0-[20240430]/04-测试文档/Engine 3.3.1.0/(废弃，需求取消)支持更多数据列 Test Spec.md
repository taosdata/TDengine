# (废弃，需求取消)支持更多数据列 Test Spec

## 1. 需求已取消：[2024/6/19 TS-4923 兼容性讨论](https://taosdata.feishu.cn/wiki/Bh3qwDqZ8i12YckjQaZcLkYanpf)

## 

## 2. 测试目标

根据[数据列数量调整](https://taosdata.feishu.cn/wiki/ZTcGwC4lKixmJXkkO2ycrWksnpd)对列数量、行长度、可变列长度等变更进行详细测试，因改动涉及面较广，需要从写入、查询、流计算、订阅、运维等多维度进行覆盖测试
- 列数量从 4096 增加到 32639
- 行长度增加到约 256k（262139）
- 可变列长度做相应调整，从 65519 到 262128
- 标签列数量不变最大仍然为 128，每个标签列长度保持 16k 不变
- tsma 不再依赖总列数，保持 4096 不变 

## 3. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-06-11 | 0.1 | @贾靖斌 | New |
|  |  |  |  |

## 4. 测试范围

### 4.1 写入

#### 4.1.1 连接方式

| **连接方式** | **描述** |
| --- | --- |
| Native |
| Restful |
| Websocket |

#### 4.1.2 写入模式

| **写入模式** | **描述** |
| --- | --- |
| 单条写入 |
| 批量写入 |
| 自动建表写入 |
| 指定列写入 |
| 多表写入 |
| stmt 写入 |
| schemaless 写入 |

## 

### 4.2 查询

在每行长度最大值为 256KB 和最大列数为 30K 的情况下，覆盖常规查询

| 查询覆盖 | 描述 |
| --- | --- |
| 投影 |
| 过滤 |
| 聚合 |
| 分区 |
| 分组 |
| 排序 |
| 函数 |
| limit/slimit |
| union |
| ... |  |

### 4.3 流计算

- 源表包含最长 256KB 的行和最大 30K 的列
- subquery 指定的某列为 30K
- 目的表写满  256 KB的行和 30K 列

### 4.4 订阅

- 源表包含最长 256KB 的行和最大 30K 的列
- subquery 指定的某列为 30K
- 

### 4.5 运维

| 运维 | 描述 |
| --- | --- |
| compact |  |
| redistribute |  |
| split |  |
| Alter replica |  |
| Restart dnode |  |
| Alter schema（length） |  |

## 5. 测试结论

1. 

## 6. 测试数据

1. 

## 7. 已知问题和限制

参考[数据列数量调整](https://taosdata.feishu.cn/wiki/ZTcGwC4lKixmJXkkO2ycrWksnpd)

## 8. 测试环境

- OS：Ubuntu 20.04.2 LTS
- Env：

| **硬件环境** | **IP** | 用途 | **CPU** | **内存** | **硬盘** |
| --- | --- | --- | --- | --- | --- |
| 192.168.1.53 | taosBenchmark |
| 192.168.1.55 | taosd |
| 192.168.1.56 | taosd |
| 192.168.1.57 | taosd |

```shell
软件版本：

```

## 9. 测试用例

### 9.1 功能

| **序号** | **测试项** | **测试点** | **测试步骤** | **期望结果** | **实际结果** |
| --- | --- | --- | --- | --- | --- |
| 1 | 写入 | 单行写入 | 1. 建库建表并写入单行数据，数据最大长度 256KB，最大列数为 30K 1. 查询以确认结果正确性 |  |  |
|  |  | 批量写入 | 1. 建库建表并构建批量 sql 写入多行数据，每行数据最大长度 256KB，最大列数为 30K 1. 查询以确认结果正确性 |  |  |
|  |  | 自动建表写入 | 1. 建库并构建自动建表 sql 写入数据，每行数据最大长度 256KB，最大列数为 30K 1. 查询以确认结果正确性 |  |  |
|  |  | 指定列写入 | 1. 建库建表并指定一列或多列进行写入，每行数据最大长度 256KB，最大列数为 30K 1. 查询以确认结果正确性 |  |  |
|  |  | 多表写入 | 1. 建库建表并指定多表进行写入，每行数据最大长度 256KB，最大列数为 30K 1. 查询以确认结果正确性 |  |  |
|  |  | stmt 写入 | 测试 stmt 写入的情况，应不受影响，每行数据最大长度 256KB，最大列数为 30K |  |  |
|  |  | schemaless 写入 | 测试 schemaless 写入的情况，应不受影响，每行数据最大长度 256KB，最大列数为 30K |  |  |
|  | 查询 | 投影查询 | 1. 建库建表并写入一定量数据，数据最大长度 256KB，最大列数为 30K 1. Select * 查询 |  |  |
|  |  | 过滤 | 1. 建库建表并写入一定量数据，数据最大长度 256KB，最大列数为 30K 1. Select * from stb where 30K_col [condition] |  |  |
|  |  | 聚合 | 1. 建库建表并写入一定量数据，数据最大长度 256KB，最大列数为 30K 1. Select min(c1),max(c2),concat(30K_col)..... from stb interval(1s) |  |  |
|  |  | 分区 | 1. 建库建表并写入一定量数据，数据最大长度 256KB，最大列数为 30K 1. Select min(c1),max(c2),concat(30K_col)..... from stb interval(1s) partition by 30K_col |  |  |
|  |  | 分组 | 1. 建库建表并写入一定量数据，数据最大长度 256KB，最大列数为 30K 1. Select min(c1),max(c2),concat(30K_col)..... from stb partition by 30K_col group by 30K_col |  |  |
|  |  | 排序 | 1. 建库建表并写入一定量数据，数据最大长度 256KB，最大列数为 30K 1. Select min(c1),max(c2),concat(30K_col)..... from stb partition by 30K_col group by 30K_col order by 30K_col [desc]; |  |  |
|  |  | limit/slimit | 1. 建库建表并写入一定量数据，数据最大长度 256KB，最大列数为 30K 1. Select min(c1),max(c2),concat(30K_col)..... from stb partition by 30K_col group by 30K_col order by 30K_col [desc] limit/slimit (10); |  |  |
|  |  | union | 1. 建库建两张表并写入一定量数据，数据最大长度 256KB，最大列数为 30K 1. ...union... |  |  |
|  | 流计算 |  | 1. 建库建表并写入一定量数据，数据最大长度 256KB，最大列数为 30K 1. 建流时 subquery 指定的某列为 30K，目的表写满 256 KB的行和 30K 列 Create stream ... Into target_stb As select 30K_col,...... From source_stb where ... |  |  |
|  | 订阅 |  | 1. 建库建表并写入一定量数据，数据最大长度 256KB，最大列数为 30K 1. 建流时 subquery 指定的某列为 30K |  |  |
|  | 运维 | compact | 1. 建库建表并写入 10 亿数据，数据最大长度 256KB，最大列数为 30K 1. Compact db |  |  |
|  |  | redistribute | 1. 建库建表并写入 10 亿数据，数据最大长度 256KB，最大列数为 30K 1. Redistribute vg 1. redistribute过程中重启dnode |  |  |
|  |  | balance | 1. 建库建表并写入 10 亿数据，数据最大长度 256KB，最大列数为 30K 1. balance |  |  |
|  |  | split | 1. 建库建表并写入 10 亿数据，数据最大长度 256KB，最大列数为 30K 1. Split vg 1. Split 过程中重启dnode |  |  |
|  |  | Alter replica | 1. 建库建表并写入一定量数据，数据最大长度 256KB，最大列数为 30K 1. Alter replica |  |  |
|  |  | Restart dnode | Coverd in redistribute and split |  |  |
|  |  | Alter schema（length） | 1. 建库建表并写入一定量数据，数据最大长度 256KB，最大列数为 30K 1. Alter schema（length） |  |  |
|  | 异常 | Create table时越界 |  |  |  |
|  |  | insert时越界 |  |  |  |
|  |  | Alter schema时越界 |  |  |  |
|  |  | join/union 越界？ |  |  |  |
|  |  | 高并发 insert 时重启taosd |  |  |  |
|  |  |  |  |  |  |


### 9.2 性能

分别测试读写性能，需要 taosBenchmark 同步覆盖相关功能

### 9.3 稳定性

结合各项功能在一定负载下长期运行，需要 taosBenchmark 同步覆盖相关功能

## 10. Jira

| **Jira** | **描述** | **状态** | **备注** |
| --- | --- | --- | --- |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |
