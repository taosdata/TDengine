# 通过sort方式进行分组性能优化

@王加明请补充语法变化，在测试完成后添加xinn报告 请补充语法变化，在测试完成后添加xinn报告

### 1. 场景说明

目前sql: partition by col 默认使用执行算子的Partition Node. 如sql:
```sql
taos> explain verbose true select ts, c0, c1,c2,cc from meters partition by c0 slimit 1 limit 1;
                                                                  QUERY_PLAN                                                                  |
===============================================================================================================================================
 -> Projection (columns=5 width=43 input_order=asc )                                                                                          |
       Output: columns=5 width=43 limit=1 slimit=1                                                                                            |
       Output: Ignore Group Id: false                                                                                                         |
       Merge ResBlocks: True                                                                                                                  |
    -> SortMerge (columns=5 width=43 input_order=asc output_order=asc)                                                                        |
          Output: columns=5 width=43                                                                                                          |
          Output: Ignore Group Id: false                                                                                                      |
          Merge Key: _group_id asc                                                                                                            |
       -> Data Exchange 1:1 (width=43)                                                                                                        |
             Output: columns=5 width=43                                                                                                       |
          -> Partition on Column c0 (width=43)                                                                                                |
                Output: columns=5 width=43                                                                                                    |
                Partition Key: partitions=1                                                                                                   |
             -> Table Scan on meters (columns=4 pseudo_columns=1 width=43 order=[asc|1 desc|0])                                               |
                   Output: columns=5 width=43                                                                                                 |
                   Time Range: [-9223372036854775808, 9223372036854775807]                                                                    |
....      
Query OK, 40 row(s) in set (0.002471s)
```

其中Partition by为 某一列或几列, 其中包含至少一列普通列.
Partition node的实现在某些场景下性能较低, 如以下场景:
1000表, 10万行每张表, c0为bigint类型, select distinct(c0) from meters;一共1000行左右.
分析原因, partition node内部由于大量的磁盘读写操作导致性能降低比较明显.

### 2. 优化后逻辑

我们通过将Partition Node替换为Sort Node以减少随机的磁盘访问次数.
语法如下所示:
```sql
taos> explain verbose true select /*+ sort_for_group() */ ts, c0, c1,c2,cc from meters partition by c0 slimit 1 limit 1;
                                                                  QUERY_PLAN                                                                  |
===============================================================================================================================================
 -> Projection (columns=5 width=43 input_order=asc )                                                                                          |
       Output: columns=5 width=43 limit=1 slimit=1                                                                                            |
       Output: Ignore Group Id: false                                                                                                         |
       Merge ResBlocks: True                                                                                                                  |
    -> SortMerge (columns=5 width=43 input_order=unknown output_order=unknown)                                                                |
          Output: columns=5 width=43                                                                                                          |
          Output: Ignore Group Id: false                                                                                                      |
          Merge Key: c0 asc                                                                                                                   |
       -> Data Exchange 1:1 (width=43)                                                                                                        |
             Output: columns=5 width=43                                                                                                       |
          -> Sort input_order=unknown output_order=unknown  (columns=5 width=43)                                                              |
                Output: columns=5 width=43                                                                                                    |
             -> Table Scan on meters (columns=4 pseudo_columns=1 width=43 order=[asc|1 desc|0])                                               |
                   Output: columns=5 width=43                                                                                                 |
                   Time Range: [-9223372036854775808, 9223372036854775807]                                                                    |
       -> Data Exchange 1:1 (width=43)                                      
       ...   
Query OK, 36 row(s) in set (0.001714s)
```

唯一区别是select之后添加了 sort_for_group() hint.

### 3. 性能测试

将partition node替换为sort node之后partition 列分组个数较多时有明显性能提升. 最好情况下性能提升70%.
但是某些场景还有明显的性能下降, 如partition的分组个数较少时, partition node并不需要太多的磁盘随机操作, sort 性能相比partition node性能差.
具体测试数据见文档: [Partition by + Slimit/Limit相关性能优化](https://taosdata.feishu.cn/docx/Ka8OdSOSpo4OuXxsveicDBrDnPb) 
根据性能测试结果得出结论: 在分组个数较大时可以考虑使用添加hint方式以提高性能.
