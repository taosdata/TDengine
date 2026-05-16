# 需求说明：统计指定 DB 的用量信息

JIRA：[TS-4713](https://jira.taosdata.com:18080/browse/TS-4713)

## 一、需求背景

1. 2024-03-12 Simon：在 S3 开发时提出，支持类似 show table distribute 的 SQL ，查看数据压缩比和总大小
   - 可以查看 data 文件在 1/2/3 级的分布、压缩比、大小
   - 可以查看 data 文件在本地与 S3 上的分布、压缩比、大小
   - 允许以 vgroup、数据库名、时间范围作为参数
2. 2024-04-24 李珲：希望有 sql 直接能够得到指定 database 当前占用的磁盘大小。
3. 2024-06-25 Jeff：对于每个 DB，我们要显示它的原始数据大小、压缩后的数据大小、压缩率，考虑在 show databases 命令里增加几列

## 二、使用场景

| 场景 | 查看内容 | 优先级 | 实时要求 |
| --- | --- | --- | --- |
| 按 db 统计 | 可选择全部 db 或者某个 db，查看 1. 原始数据大小 1. 压缩后数据大小 1. 压缩比 | 高 | 能预先存储，并以最快速度返回一个近似结果 |
| 按 vgroup 统计 | 可选择全部 vgroup、某个 db 的 vgroup，或者某个 vgroup，查看 1. 原始数据大小 1. 压缩后数据大小 1. 压缩比 | 中 |  |
| 查看数据分布 | 可选择全部 vgroup、某个 db 的 vgroup，或者某个 vgroup，查看 1. 内存中的数据大小 1. 一级存储上的数据大小 1. 二级存储上的数据大小 1. 三级存储上的数据大小 1. S3 存储上的数据大小 1. 子表的数目 1. 点的数目 1. 总记录条数 | 中 |  |

## 三、建议实现

建议增加一个系统表，例如 ins_usage，包含如下数据列
1. db_name：数据库名称
2. vgroup_id：vgroup 编号
3. raw_data_size：原始数据大小
4. compressed_data_size：压缩后数据大小
5. compress_ratio：压缩比
6. data_in_memory：内存中的数据大小
7. data_in_wal：WAL 上的数据大小
8. data_in_level_1：一级存储上的数据大小
9. data_in_level_2：二级存储上的数据大小
10. data_in_level_3：三级存储上的数据大小
11. data_in_level_4：S3 存储上的数据大小
12. tables：子表数目
13. timeseries：测点数目
14. rows：总记录条数
这些信息考虑单独存储在某个数据文件中，每次落盘时更新。读取数据时，如果仅仅指定了 db_name、compress_ratio，应该能够优化执行计划，不读取 tables、timeseries、rows、data_in_memory、data_in_wal 等无关信息，以最快速度返回一个近似结果。
