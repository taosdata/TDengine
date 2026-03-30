# 单副本增加 wal_level 0 选项

## 1. 背景

TS-4885


TS-4843

TS-4843 POC 测试中，客户磁盘限速导致写入速度无法继续提升，需要不写 WAL 的选项以达到更高的写入速度；这个选项将在 TS-4885 中添加，由于未落盘数据有可能丢失，不建议在生产系统中使用。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/06/04 | 0.1 | 金明磊 | 初稿 |
|  |  |  |  |

## 3. 定义

无。

## 4. 行为说明

创建或修改 DB 时，增加 wal_level  0 的选项，不写 WAL 日志，如下：
```plaintext
taos> create database db wal_level 0;
Create OK, 0 row(s) affected (4.551710s)
taos> alter database db wal_level 1;
Query OK, 0 row(s) affected (0.064257s)
```

非单副本情况下，创建或修改 DB 时，wal_level 选项不能为 0：
```sql
taos> create database db wal_level 0 replica 3;
DB error: Invalid option wal_level 0 should be used with replica 1 (0.000467s)
taos> create database db wal_level 1 replica 3;
Create OK, 0 row(s) affected (4.551710s)
taos> alter database db wal_level 0;
DB error: Invalid option wal_level 0 should be used with replica 1 (0.000467s)
```

当创建 3 副本不写 WAL 时，客户端会报错；改为 3 副本写 WAL，则可以创建成功；然后再把 3 副本的 wal_level 改为 0 时，同样会报错。
wal_level  为 0 的情况下，wal 目录下的 idx, log 和 meta 均为空文件。如果配置为 0 之前已经有 WAL 日志数据，则修改后，WAL 日志数据不会增加。流，订阅，副本变更等功能无法从 WAL 中读取数据，影响正常使用。
备注：主机内存充足，不重启的情况下，可使用 /dev/shm 文件系统替代此方案，降低对系统的影响。

## 5. 性能

当 wal_level 配置为 0 后，可消除写 WAL 日志对数据写入速度的影响，在磁盘 IO 受限环境中，对写入速度应有一定的提升。

## 6. 兼容性

无。

## 7. 运维

数据导入结束后，需要使用 flush database <db_name>; 命令手动落盘，避免出现数据丢失。

## 8. 使用场景

磁盘 IO 受限环境中的测试场景下，如果要提升数据导入速度，可以认真考虑后使用这个选项。

## 9. 约束和限制

约束：单副本情况下使用
限制：未落盘数据有丢失风险，不建议在生产系统中使用。

## 10. 常见错误和排查

无。

## 11. 可观测性

对 taos shell, taos Explorer， TDinsight 等基于 UI 或用户交互界面的产品组件无影响。

## 12. 安装和卸载

对安装和卸载脚本无要求。

## 13. 文档

不需要修改企业版文档
不需要修改官网文档 （已与产品讨论）

## 14. 参考文档

## 15. 附录
