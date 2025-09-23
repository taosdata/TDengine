---
title: 产品路线图
---

TDengine TSDB OSS 之 2025 年年度路线图如下表所示。

|  季度   |  功能  |
| :----- | :----- |
| 2025Q1 | <ol><li>虚拟表</li><li>查询能力：<code>REGEXP</code>、<code>GREATEST</code>、<code>LEAST</code>、<code>CAST</code> 函数支持判断表达式、单行选择函数的其他列值、<code>INTERP</code> 支持插值时间范围</li><li>存储能力：支持将查询结果写入超级表、超级表支持 <code>KEEP</code> 参数、STMT 写入性能提升</li><li>流计算：支持虚拟表、计算资源优化、事件通知机制、创建时间优化</li><li>数据类型：Decimal</li><li>高可用：加快宕机恢复时间、优化客户端 Failover 机制</li><li>稳定性：开始维护新的稳定版本 3.3.6.x</li><li>JDBC：高效写入</li><li>生态工具：对接 Tableau</li><li>生态工具：对接 Excel</li></ol> |
| 2025Q2 | <ol><li>查询能力：大幅放宽关联查询限制、支持 MySQL 所有数学函数、支持积分/积分平均/连续方差函数、<code>CSUM</code> 函数优化、<code>COUNT(DISTINCT)</code> 语法、事件窗口功能增强、提升标签过滤性能、提升 <code>INTERP</code> 查询性能</li><li>存储能力：TSMA 计算资源优化、写入抖动优化</li><li>流计算：节点高可用</li><li>数据类型：BLOB</li><li>数据订阅：支持 MQTT 协议</li><li>高可用：提高副本变更速度、提高集群宕机恢复速度、优化断电数据恢复机制</li><li>可观测性：写入诊断工具</li><li>生态工具：对接帆软 FineBI</li></ol> |
| 2025Q3 | <ol><li>查询能力：支持更多子查询类型、支持 MySQL 运算符、支持 MySQL 所有时间函数、窗口计算逻辑优化、查询性能抖动、计数窗口允许指定列</li><li>存储能力：提高 SQL 模式写入速度</li><li>可观测性：查询诊断工具、优化 <code>EXPLAIN</code> 输出、长任务观测</li></ol> |
| 2025Q4 | <ol><li>查询能力：窗口函数（<code>OVER</code> 子句）、支持 MySQL 所有字符串/聚合/条件函数、Partition 支持组内排序、控制查询资源占用、提高子表聚合查询性能、<code>INTERVAL</code> 窗口支持插值时间范围</li><li>数据类型：支持不定长度字符串数据类型</li><li>数据缓存：提升按行缓存性能</li><li>可观测性：增强运维可观测性</li></ol> |

欲了解更多信息，请参见 [TDengine TSDB Public Roadmap](https://github.com/orgs/taosdata/projects/4) 。
