# InfluxDB 数据源explore页面新增调优参数-FS

## 1. 背景

Influxdb 在遇到大量数据需要同步时，需要进行优化调参，开放性能优化参数，方便交付人员进行客户场景适配。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/08/26 | 0.1 | 张贵川 | 文档撰写 |

## 3. 定义

InfluxDB： 一种时序数据库。
JVM：Java Virtual Machine，Java虚拟机，它是一个抽象的计算机系统，为 Java 字节码提供运行环境。

## 4. 行为说明

1. 数据源同步的 InfluxDB 用户界面增加 4 个参数：
数据源并发读取方式， 每次读取行数，缓存队列大小，JVM 参数 。
中文页面：
![](./images/img_L1imbAp9IoeQiQxGaQOcQpXKnfS.png)

![](./images/img_E8pabGbN1oQzQixrN87cvWR7n1f.png)

![](./images/img_F2XcbRGmcoZi0xxMKl9cz5Zvnzb.png)


英文页面：
![](./images/img_HTBFbpwmooWvbBxgSgscgxlWnwd.png)

![](./images/img_EkUwbzws6obQ4fxzfzTcjdmGnnd.png)

![](./images/img_ER3FbdzDYoHdNwxpSUycLr6Mneb.png)


1. 命令行参数增加参数

| 名称 | 页面名称 | 备注 |
| --- | --- | --- |
| read_concurrency_type | 中文： 数据源并发读取方式 英文：Concurrent Reading Methods | measurement 的并行读取方式。queue: 多线程同时读取一个 measurement，完成后读取下一个。average: 平均方式，多个 measurement 同时被不同线程读取。sequence: 每个 measurement 同时只有一个线程读取。 |
| rows_per_read | 中文：每次读取行数 英文：Rows Per Read | 每次从 InfluxDB 读取数据时的行数。 |
| cache_queue_size | 中文：缓存队列大小 英文：Cache Queue Size | 内部数据缓存队列大小，用于缓存从 influxdb 读取的数据。 |
| jvm_opts | 中文：JVM 参数 英文：JVM Options | 资源限制条件下用于控制内存和 cpu 消耗。 |


## 5. 性能

数据源并发读取方式， 每次读取行数，缓存队列大小 和 jvm 参数 这 4 个参数可以控制内存，cpu消耗等，也可能会影响实际的数据同步性能。

## 6. 兼容性

无

## 7. 运维

无

## 8. 使用场景

无

## 9. 约束和限制

约束：无
限制：无

## 10. 常见错误和排查

无

## 11. 可观测性

这几个参数均在 taos-explorer 产品 UI 页面修改。

## 12. 安装和卸载

无

## 13. 文档

需要修改官网文档。

## 14. 参考文档

## 15. 附录

1. taosx 和 Java plugin 插件同步修改实现
