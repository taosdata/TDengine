---
title: 数据导出
description: 如何导出 TDengine 中的数据
---

为方便数据导出，TDengine 提供了两种导出方式，分别是按表导出和用 taosdump 导出。

## 按表导出 CSV 文件

如果用户需要导出一个表或一个 STable 中的数据，可在 TDengine CLI 中运行：

```sql
select * from <tb_name> >> data.csv;
```

这样，表 tb_name 中的数据就会按照 CSV 格式导出到文件 data.csv 中。

## 用 taosdump 导出数据

利用 taosdump，用户可以根据需要选择导出所有数据库、一个数据库或者数据库中的一张表，所有数据或一时间段的数据，甚至仅仅表的定义。具体使用方法，请参考 taosdump 的相关文档。