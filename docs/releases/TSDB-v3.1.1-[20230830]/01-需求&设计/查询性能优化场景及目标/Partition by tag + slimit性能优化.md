# Partition by tag + slimit性能优化

@王加明请补充一下本次优化scope（测试验证范围）及性能优化结果 请补充一下本次优化scope（测试验证范围）及性能优化结果

### 1. 场景

- 仅限于agg + partition by tag + slimit的场景, 没有order by, 没有窗口函数, select列表需要聚合函数.
如sql: 
```sql
select count(*), tg1 from meters partition by tg1 slimit 10;
```

典型执行计划是: 
project(slimit) + aggregate(slimit) + sort merge + [exchange + agg(slimit) + scan(partition), ...]

### 2. 性能测试

在slimit较小时,性能有明显提升.
优化后由于扫表方式为tag顺序扫表, 较文件顺序扫表慢, 因此当slimit较大时, 性能有所下降.
见文档: [Partition by + Slimit/Limit相关性能优化](https://taosdata.feishu.cn/docx/Ka8OdSOSpo4OuXxsveicDBrDnPb) 6.3
