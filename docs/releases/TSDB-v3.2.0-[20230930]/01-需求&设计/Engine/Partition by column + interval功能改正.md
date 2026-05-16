# Partition by column + interval功能改正

@王加明请提供一下测试验证的场景 请提供一下测试验证的场景

### 1. 场景说明

```sql
select count(*), c0 from meters partition by c0 interval(1s);
taos> explain verbose true select count(*), c0 from meters partition by c0 interval(1s);
                                                                  QUERY_PLAN                                                                  |
===============================================================================================================================================
 -> Merge Aligned Interval on Column  (functions=2 width=16 input_order=asc output_order=asc)                                                 |
       Output: columns=2 width=16                                                                                                             |
       Time Window: interval=1s offset=0a sliding=1s                                                                                          |
       Merge ResBlocks: True                                                                                                                  |
    -> SortMerge (columns=3 width=24 input_order=asc output_order=asc)                                                                        |
          Output: columns=3 width=24                                                                                                          |
          Output: Ignore Group Id: false                                                                                                      |
          Merge Key: _group_id asc,  asc                                                                                                      |
       -> Data Exchange 1:1 (width=24)                                                                                                        |
             Output: columns=3 width=24                                                                                                       |
          -> Interval on Column ts (functions=3 width=24 input_order=asc output_order=asc )                                                   |
                Output: columns=3 width=24                                                                                                    |
                Time Window: interval=1s offset=0a sliding=1s                                                                                 |
                Merge ResBlocks: False                                                                                                        |
             -> Partition on Column c0 (width=16)                                                                                             |
                   Output: columns=2 width=16                                                                                                 |
                   Partition Key: partitions=1                                                                                                |
                -> Table Scan on meters (columns=2 width=16 order=[asc|1 desc|0])                                                             |
                      Output: columns=2 width=16                                                                                              |
                      Time Range: [-9223372036854775808, 9223372036854775807]        
                      ...                                                         |
```



Partition by列包含某一列普通列, 由于interval算子要求每个分组内的数据按照ts列排序, partition by col之后, 此条件无法得到满足. 导致计算结果有误.
若使用sort算子进行分组操作, 那么在在排序时一并和ts列排序, 输出的结果直接满足条件.

### 2. 解决

在partition node或sort node输出的数据每个分组内按照ts排序即可.

###
