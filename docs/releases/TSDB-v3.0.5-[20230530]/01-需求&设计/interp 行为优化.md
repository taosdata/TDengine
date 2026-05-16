# interp 行为优化

TD-19801


TD-19803


TD-21187


TD-21182

@Gavin Please prepare user manual first to describe the new behavior clearly.

### 1. INTERP 支持超级表

1. INTERP支持直接对超级表进行插值，所有属于该超级表的子表会按照时间戳排序规约到一条时间线上。类似其他依赖时间线的函数(如diff, csum等)，当子表中有相同时间戳时，会报错提示"duplicate timestamp"
```sql
SELECT INTERP(c0) FROM stable RANGE(ts1, ts2) EVERY(time_unit) FILL(fill_type);
```

1. INTERP支持对超级表查询并使用partition by 划分出的分片分别进行查询，但是保持对普通表查询行为一样，分片后不能用于对时间窗口查询以及group by查询中。
```sql
SELECT INTERP(c0) FROM stable PARTITION BY tbname RANGE(ts1, ts2) EVERY(time_unit) FILL(fill_type) ;
```

### 2. INTERP 支持输入列NULL值进行忽略

INTERP支持使用第二个配置参数指定是否对NULL值进行忽略。如果ignore_null_value设置为1，进行NULL值忽略，否则不忽略。如果不指定该参数，默认行为不对NULL值进行忽略。
```plaintext
INTERP(expr [, ignore_null_value])

ignore_null_value: {
    0
  | 1
}

```

当对表中的多个列同时使用INTERP时，如果不忽略NULL值，每个列插值后输出行数为固定值。但是如果其中某些列忽略NULL值，可能会导致对不同列插值后的数据输出行数不一致。在这种情况下，为保证所有列的输出行数相同， 每列的输出会对没有产生插值点的时间戳使用null值进行填充。比如以下标红数据为每列插值产生的值，NULL值为对每列的填充。

| _irowts | INTERP(c0, 1) | INTERP(c1, 1) | INTERP(c2, 1) |
| --- | --- | --- | --- |
| 2020-01-01 00:00:01 | 1 | NULL | 1 |
| 2020-01-01 00:00:02 | NULL | 1 | NULL |
| 2020-01-01 00:00:03 | NULL | NULL | 1 |
| 2020-01-01 00:00:04 | 1 | NULL | NULL |
