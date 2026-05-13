# last()/last_row()同时查询时的性能优化+last遗留问题

## 一、测试功能简介

  在database的cache mode 配置为 both时， 一条查询语句中如果同时使用last 和 last_row函数时，性能较差。
需求文档：[last()/last_row()同时查询时的性能优化](https://taosdata.feishu.cn/wiki/ZynKwyNeVi2I3vk648Cc1689nag) 

jira:https://jira.taosdata.com:18080/browse/TD-27003?filter=23428

TS-4177


TS-4178


## 二、测试结论

  该功能测试通过，（功能测试通过，该场景性能提升明显），具体见测试用例结果。

## 三、测试资源及环境

   测试平台：Linux x64
   测试资源：192.168.1.43、64

## 四、测试重点及难点

   本节中描述功能测试的重点及容易出问题的地方、不好测试的地方及描述测试的方法
   本节中把主要的测试过程说清楚，在第四节中的表格就不用再详细描述，简述即可

## 五、功能测试用例

| 分类 | 测试场景 | 测试用例编号 | 测试内容/步骤 | 预期 | 结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| cachemodel=both | 1 | select last(*) from meters ; select last_row(*) from meters ; select last(*) from meters group/partition by tbname; select last_row(*) from meters group/partition by tbname; | 1、同现在查询执行计划 2、同现在查询性能 | 通过 | 备注或报BUG 的JIRA号 |
|  | 2 | select last(*), last_row(*) from meters ; select last(*), last_row(*) from meters group/partition by tbname; | 1、Explain 查询计划变更，从table scan到last row scan 2、性能提升明显 | 通过 |  |
|  | 3 | select last(id+1) from meters ; select last_row(id+1) from meters ; select last(id) +1 from meters ; select last_row(id)+1 from meters ; select last(id+1) from meters group/partition by tbname; ; select last_row(id+1) from meters group/partition by tbname;; select last(id) +1 from meters group/partition by tbname;; select last_row(id)+1 from meters group/partition by tbname;; | 1、查询不再报错 2、查询性能同测试用例1 | 通过 |  |
|  |  |  |  |  |  |
|  |  |  |  |  |  |
| cachemodel=none、last_row,last_value | 1 | select last(*) from meters ; select last_row(*) from meters ; select last(*) from meters group/partition by tbname; select last_row(*) from meters group/partition by tbname; | 1、同现在查询执行计划 2、同现在查询性能 | 通过 | [TD-28676](https://jira.taosdata.com:18080/browse/TD-28676?filter=-4) |
|  | 2 | select last(*), last_row(*) from meters ; select last(*), last_row(*) from meters group/partition by tbname; | 1、同现在查询执行计划 2、同现在查询性能 | 通过 |  |
|  | 3 | select last(id+1) from meters ; select last_row(id+1) from meters ; select last(id) +1 from meters ; select last_row(id)+1 from meters ; select last(id+1) from meters group/partition by tbname; ; select last_row(id+1) from meters group/partition by tbname;; select last(id) +1 from meters group/partition by tbname;; select last_row(id)+1 from meters group/partition by tbname;; | 1、查询不再报错 2、查询性能同测试用例1 | 通过 | [TD-28698](https://jira.taosdata.com:18080/browse/TD-28698?filter=-2) |
|  |  |  |  |  |  |
|  |  |  |  |  |  |
| 性能测试 |  | 1 | 记录下不同版本cachemode=both时 last(*), last_row(*)的性能对比 |  |  |  |
|  |  | 2 | 记录下相同版本cachemode=both-》none时 last(*), last_row(*)的性能对比 |  |  |  |


## 六、性能测试结果

旧版本：3220、新版本：3230
场景1:
10w子表，每个子表1000数据，一共1亿数据量（10w子表*1000数据量）。
2个vgroup，建库时开启cachemodel=both。
直接查询last_row（*）两个版本差别不大，都在0.7s左右，再次查询约0.3s。
然后在新建10w张子表，是空表不写数据。
旧版本，再进行查询，飙升到6300s左右，再次查询稳定在0.3s上下浮动，未超过0.5s。
新版本，一直稳定在0.3s上下浮动，未超过0.5s。

场景2:
10w子表，每个子表1000数据，一共1亿数据量。
2个vgroup，建库时开启cachemodel=both。
直接查询last_row（*）两个版本差别不大，都在0.7s左右。新增一列为NULL后，
旧版本，多次查询时间越来越快，从6217s、1619s、57s、0.9s上下浮动
新版本，一直稳定在0.8s上下浮动，未超过1s。

场景3:
10w子表，每个子表1000数据，一共1亿数据量。
2个vgroup，建库时开启cachemodel=both。
旧版本：直接查询last（*） 0.13s、直接查询last_row（*）0.11s，直接查询last（*）、last_row（*）约0.677s.
如果在叠加场景1或者2，上述结果会飙升到6300s。
新版本：直接查询last（*） 0.11s、直接查询last_row（*）0.10s，直接查询last（*）、last_row（*）约0.14s.
如果在叠加场景1或者2，查询last（*） 0.13s、直接查询last_row（*）0.11s，直接查询last（*）、last_row（*）约0.18s。

## 七、CASE 覆盖

    在 CI 或全量测试中加入的覆盖此功能的自动化测试 CASE 的名称
https://github.com/taosdata/TDengine/blob/main/tests/system-test/2-query/last_and_last_row.py

##
