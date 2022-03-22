# 性能优化

因数据行 [update](https://www.taosdata.com/cn/documentation/faq#update)、表删除、数据过期等原因，TDengine 的磁盘存储文件有可能出现数据碎片，影响查询操作的性能表现。从 2.1.3.0 版本开始，新增 SQL 指令 COMPACT 来启动碎片重整过程：

```mysql
COMPACT VNODES IN (vg_id1, vg_id2, ...)
```

COMPACT 命令对指定的一个或多个 VGroup 启动碎片重整，系统会通过任务队列尽快安排重整操作的具体执行。COMPACT 指令所需的 VGroup id，可以通过 `SHOW VGROUPS;` 指令的输出结果获取；而且在 `SHOW VGROUPS;` 中会有一个 compacting 列，值为 2 时表示对应的 VGroup 处于排队等待进行重整的状态，值为 1 时表示正在进行碎片重整，为 0 时则表示并没有处于重整状态（未要求进行重整或已经完成重整）。

需要注意的是，碎片重整操作会大幅消耗磁盘 I/O。因此在重整进行期间，有可能会影响节点的写入和查询性能，甚至在极端情况下导致短时间的阻写。
