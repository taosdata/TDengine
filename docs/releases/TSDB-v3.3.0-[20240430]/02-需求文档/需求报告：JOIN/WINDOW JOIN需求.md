# WINDOW JOIN需求

### 1. 背景

目前多个客户场景(金融、工业)要求，TDengine支持WINDOW JOIN查询
典型场景有：通力电梯、宽睿科技、发那科

### 2. 客户功能需求

1. 从左表符合筛选条件记录，获得时间基准，然后查找右表在指定时间段内(基于时间基准)，且符合JOIN条件的所有记录，示例如下
```sql {wrap}
子表示例
select a.ts ts1, a.event event1, a.val val1, ts ts2, event event2, val val2 
from alarm1 
where event = 6 
window join (
    select ts, ts + 60s, event, val 
    from fault1 
    where event = 1
    ) a 
having count(*) > 0；
```

```sql {wrap}
超级表示例
select a.ts ts1, a.event event1, a.val val1, ts ts2, event event2, val val2 
from alarm 
where event = 6 
paritition by tbname 
window join (
    select tbname tbname1, ts, ts + 60s, event, val 
    from fault 
    where event = 1
    ) a 
on tbname = a.tbname1 
having count(*) > 0；
```

1. ~~如右表在时间段内未找到对应值，支持向前查找前值并返回~~
2. WINDOW JOIN无需对齐时间戳，允许多个条件：tbname/标签列/普通列？（普通列如果实现难度大可以舍弃）
3. WINDOW JOIN允许附带筛选条件，左表、右表均可独立设定筛选条件：时间戳、普通列、标签列均可
4. 左表一条记录，对应右表一条或多条记录。如右表未匹配到结果，其结果列返回NULL （相当于 WINDOW LEFT)

### 3. 引申需求

1. 企业版专属功能
2. 针对时序结果列，支持算术运算、聚合类、选择类、时序特有函数，需正确返回对所有符合条件的记录的计算结果 （对 Join 透明）
3. 支持UDF （对 Join 透明）
4. 支持窗口子句：interval, session, state_window, event_window；支持fill子句 （对 Join 透明）
5. ~~支持流计算。用户可通过该类WINDOW JOIN查询结果来创建流计算~~
6. ~~支持数据订阅。用户可通过该类WINDOW JOIN查询结果来创建topic~~

### 4. 性能需求

查询性能，与基准版本相同

附：
[电梯场景](https://taosdata.feishu.cn/wiki/ZTDcwcAsZitvVVkadnZcTV50n1b) 
[Window JOIN需求场景分析](https://taosdata.feishu.cn/wiki/KUGNwC5ggiIJd1k4BZec6C5xnHe)
