# 流计算 Event Window

## 1. 背景

流计算引擎按照用户可以指定的开始条件和结束条件来划分窗口，并进行聚合计算。流计算支持与批量查询相同的事件窗口切分逻辑，主要语法也与批量查询保持一致。

## 2. 定义

请参见官方用户手册中对 Event Window 的[定义和解释](https://docs.taosdata.com/taos-sql/distinguished/#%E4%BA%8B%E4%BB%B6%E7%AA%97%E5%8F%A3)。

## 3. 变更历史

| 日期 | 版本 | 负责人 |
| --- | --- | --- |
| 2023/12/21 | 1.0.0 | 刘垚 |

## 4. 行为说明

流计算中事件窗口的定义与批量查询的事件窗口定义完全一致，并且其计算的方式与批查询中也完全一致。在流计算中使用事件窗口的方式与使用其他类型的窗口一致，在创建流计算的 SQL 语句中使用关键词 event_window 并搭配窗口开始的定义和窗口关闭的定义使用。例如：
```sql
CREATE STREAM sample_stream INTO stream_dst_table 
    AS 
    SELECT COUNT(*) AS val
    FROM queried_table
    EVENT_WINDOW START WITH c1 > 0 END WITH c2 < 10 
```

在第五行的 event_window 定义了事件窗口的打开和关闭逻辑。由此可见事件窗口与其他类型的窗口使用相同。

## 5. 性能因素

影响流计算针对事件窗口计算性能的因素有以下几个方面：
- 事件判断逻辑复杂度。针对每条记录，均需要判断其是否是触发该事件，即窗口开始和结束的判断逻辑会应用在每一条记录上，过于复杂的计算逻辑会直接影响计算效率。窗口开始条件和窗口结束条件不能太过复杂，否则会增加流计算的开销。例如包含几百或几千个过滤条件。
- 乱序数据。大量的乱序数据，会导致窗口重算，浪费计算资源。例如乱序数据落在窗口中间，且满足窗口结束条件的数据，会导致窗口重算。
- 事件窗口数量。触发事件窗口开始（结束）条件的记录数直接影响流计算推送的结果规模，（触发对比行数的记录与最终推送的结果的关联对比见下表）。如果能够数据集（事件窗口个数与原始数据量比例），可以避免推送过多的事件窗口，从而节省大量的IO开销。
  | 事件窗口数量测试场景 | 事件窗口包含数据行数 | 窗口个数 | 流计算结果的IO量 |
| --- | --- | --- | --- |
| 用taosBenchmark写入1000万行数据。 | 1 | 1000万 | 458.93MiB |
| 用taosBenchmark写入1000万行数据。 | 100 | 10万 | 9.97MiB |

## 6. 兼容性

无

## 7. 运维

无

## 8. 使用场景

在计算过程中需要依赖于特殊事件来进行事件窗口划分的流计算场景均可使用事件窗口。以智能电表为例（其schema 定义见官网 https://docs.taosdata.com/taos-sql/distinguished/#%E7%A4%BA%E4%BE%8B）
| ts | current | voltage | phase | location | groupId |
| --- | --- | --- | --- | --- | --- |
| 1500000000 | 1.2 | 220 | 1 | haidian | 12 |
| 1500015000 | 2 | 220 | 1 | haidian | 12 |
| 1500030000 | 1.7 | 220 | 1 | haidian | 12 |
| 1500045000 | 4.4 | 219 | 1 | haidian | 12 |
| 1500060000 | 0.4 | 219 | 2 | haidian | 12 |
| 1500075000 | 1.3 | 219 | 2 | haidian | 12 |
| 1500090000 | 2.2 | 220 | 1 | haidian | 12 |
| 1500105000 | 1.9 | 220 | 2 | haidian | 12 |


| 举例说明 | SQL 语句 |
| --- | --- |
| 按电压值划分窗口(单列值划分窗口) | CREATE STREAM streams1 INTO streamt AS SELECT _wstart as start, _wend as end, count(*) c1 from meters 
event_window start with voltage < 220 end with voltage >= 220; |
| 按电压和电流划分事件窗口（多个单列的值的变化来划分窗口，一个列标识窗口开始，一个列标识窗口结束） | create stream streams1 into streamt as select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 
event_window start with voltage < 220 end with current >= 2.0 |
| 分别计算不同区域事件（在超级表上独立计算每个时间线的窗口） | create stream streams1 into streamt as select  _wstart as s, count(*) c1,  sum(b), max(c) from st
PARTITION BY tbname 
event_window start with colA = 0 end with colB = 9; |


## 9. 约束和限制

逻辑上来看，事件只针对单个时间线（单个设备）才有意义。因此，使用事件窗口从语义进行了限制，流计算的源只能是：
1. 子表
2. 如果要应用在超级表上，需要搭配 `partition by tbname``,tag, column`。分组条件中可以包含其他的标签（tag）或普通列（column），但是必须包含 tbname（`partition by tbname`）来强制流计算引擎将事件限制在每个时间线独立处理。见示例：
```sql {wrap}
taos> CREATE STREAM streams3 
                    TRIGGER at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 
                    INTO streamt3                              
                    AS 
                       SELECT _wstart AS s, count(*) c1, sum(b), max(c) 
                       FROM   st 
                       EVENT_WINDOW START WITH a = 0 END WITH b = 9;

DB error: Event window for stream on super table must patitioned by table name (0.004874s)

taos> CREATE STREAM streams3 
                    TRIGGER at_once IGNORE EXPIRED 0 IGNORE UPDATE 0 
                    INTO streamt3 
                    AS
                       SELECT _wstart AS s, count(*) c1, sum(b), max(c) 
                       FROM st 
                       PARTITION BY tbname, ta, a 
                       EVENT_WINDOW START WITH a = 0 END WITH b = 9;
Create OK, 0 row(s) affected (0.174491s)

```


## 10. 常见错误和排查

在建流的时候，如果不符合上述限制，会报错，按错误修改语句即可。
