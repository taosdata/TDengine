# Partition by column + slimit/limit性能优化

@王加明请在性能测试完成后添加性能测试报告 请在性能测试完成后添加性能测试报告

### 1. 场景

```sql
select count(*), c0 from meters partition by c0 slimit 10;
select /*+ sort_for_group()*/ count(*), c0 from meters partition by c0 slimit 10;
```

Agg + partition by col + slimit.  Partition 列表内包含普通列, 不能包含tbname或者tag.

### 2. 性能测试

见 [文档](https://taosdata.feishu.cn/docx/Ka8OdSOSpo4OuXxsveicDBrDnPb#SVQydGJPVo6FTNx184FchE2En5g).
