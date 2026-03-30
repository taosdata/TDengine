# [Test Report] TS-3812： license授权中的测点数，与实际运行中使用的测点数相差有点大

## 1. 测试背景

TS-3812

给客户授权了5000测点，使用taosBenchmark批量建表的时候，实际使用到了230万测点。

## 2. 测试场景

定时控制：每秒更新，所以每个增加测点数，减少测点数的操作都会更新已有测点数
定量控制：timeseriesThreshold 控制 （0-2000），每个 vnode 测点数变化超过 timeseriesThreshold 都会上报

## 3. 数据集

1. 限制测点数为 5000
2. 测点数计算：tables * numofColumns (ts column is not included), 所以测试过程中不用写入数据，所以可以直接是用 taosBenchmark 命令行方式直接指定建表数量，每个表写入的记录（设置到最小值1），只需要修改表的数量即可测试出要写入的测点数
3. 当前测点数可以通过 show grants\G 查看

## 4. 测试用例

| 用例 | 期待结果 | 实际结果 | 测试通过 |
| --- | --- | --- | --- |
| 1. 5000 测点以下，建表，添加列, 减少列，删除数据库 | 测点数及时更新 | 测点数及时更新 | pass |
| 2. 单个 vnode 写入 5049 测点超 5000 限制 （taosBenchmark -t 1683 -n 1 -v 1 -y ） | 小于 1 秒并且小于timeseriesThreshold 不会上报，写入成功 | 建表成功 | pass |
| 3. 单个 vnode 写入 5052 测点超 5000 + 50 限制 （taosBenchmark -t 1684 -n 1 -v 1 -y ） | 超过timeseriesThreshold ，建表失败 | 建表失败 | pass |
| 4. Add column, 在用例2基础上添加列 | 报错，测点超上限 | 报错，测点超上限 | pass |
| 5. Drop column,在用例2基础上删除列 | 测点数及时更新 | 测点数及时更新 | pass |
| 6. 多个 vnode 的建表 | 测点数远超 timeseriesThreshold | 测点数远超 timeseriesThreshold | pass |
| 7. 测点数不统计系统表 | 测点数限制不统计系统表 | log，audit 数据库中的测点数会被统计到 | fail |

## 5. 测试结论

这个 feature 在测试用例覆盖范围内功能基本没有问题。唯一的小问题，有些系统表的测点数会被统计在当前测点里面，有些没有，这个地方还有优化的空间。
问题：如果每秒都去扫描测点是否超过限制，这个在数据持续写入过程中会不会影响写入性能？（待测试）
