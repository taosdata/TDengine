---
sidebar_label: 快速体验
title: 快速体验写入和查询
toc_max_heading_level: 4
---

## 体验写入

taosBenchmark 是一个专为测试 TDengine 性能而设计的工具，它能够全面评估TDengine 在写入、查询和订阅等方面的功能表现。该工具能够模拟大量设备产生的数据，并允许用户灵活控制数据库、超级表、标签列的数量和类型、数据列的数量和类型、子表数量、每张子表的数据量、写入数据的时间间隔、工作线程数量以及是否写入乱序数据等策略。

启动 TDengine 的服务，在终端中执行如下命令

```shell
taosBenchmark -y
```

系统将自动在数据库 test 下创建一张名为 meters的超级表。这张超级表将包含 10 000 张子表，表名从 d0 到 d9999，每张表包含 10,000条记录。每条记录包含 ts（时间戳）、current（电流）、voltage（电压）和 phase（相位）4个字段。时间戳范围从“2017-07-14 10:40:00 000”到“2017-07-14 10:40:09 999”。每张表还带有 location 和 groupId 两个标签，其中，groupId 设置为 1 到 10，而 location 则设置为 California.Campbell、California.Cupertino 等城市信息。

执行该命令后，系统将迅速完成 1 亿条记录的写入过程。实际所需时间取决于硬件性能，但即便在普通 PC 服务器上，这个过程通常也只需要十几秒。

taosBenchmark 提供了丰富的选项，允许用户自定义测试参数，如表的数目、记录条数等。要查看详细的参数列表，请在终端中输入如下命令
```shell
taosBenchmark --help
```

有关taosBenchmark 的详细使用方法，请参考[taosBenchmark 参考手册](../../reference/components/taosbenchmark)

## 体验查询

使用上述 taosBenchmark 插入数据后，可以在 TDengine CLI（taos）输入查询命令，体验查询速度。

1. 查询超级表 meters 下的记录总条数
```shell
SELECT COUNT(*) FROM test.meters;
```

2. 查询 1 亿条记录的平均值、最大值、最小值
```shell
SELECT AVG(current), MAX(voltage), MIN(phase) FROM test.meters;
```

3. 查询 location = "California.SanFrancisco" 的记录总条数
```shell
SELECT COUNT(*) FROM test.meters WHERE location = "California.SanFrancisco";
```

4. 查询 groupId = 10 的所有记录的平均值、最大值、最小值
```shell
SELECT AVG(current), MAX(voltage), MIN(phase) FROM test.meters WHERE groupId = 10;
```

5. 对表 d1001 按每 10 秒进行平均值、最大值和最小值聚合统计
```shell
SELECT _wstart, AVG(current), MAX(voltage), MIN(phase) FROM test.d1001 INTERVAL(10s);
```

在上面的查询中，使用系统提供的伪列_wstart 来给出每个窗口的开始时间。