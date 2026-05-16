# [Test Reort] TS-4463 [宁德新能源] 3.1.1.20 tmp 空间占满

### 1. 测试总结：

1. 在相同的schema，子表数，每个子表包含相同行数的前提下，不使用hints查询磁盘空间占用上升，内存占用上升较小；使用hints查询磁盘空间占用零增长，内存占用上升较大
2. 在相同的schema，每个子表包含相同行数的前提下，增加子表数，不使用hints查询磁盘空间占用上升，内存占用上升较小；使用hints查询磁盘空间占用零增长，内存上升较大
3. 在相同的schema，相同子表数的前提下，增加每个子表的行数，不使用hints查询磁盘空间占用上升较大，内存占用上升较小；使用hints查询磁盘空间零增长，内存上升较大
综上，修改符合预期需求

### 2. 测试目标

需求说明：[Table Merge Scan优化磁盘占用](https://taosdata.feishu.cn/wiki/NqV7w2J8Hi0BJbkgB4Rcc9ZQnhg)
[TS-4463](https://jira.taosdata.com:18080/browse/TS-4463?src=confmacro) [[宁德新能源] 3.1.1.20 tmp 空间占满](https%3A%2F%2Fjira.taosdata.com%3A18080%2Fbrowse%2FTS-4463%3Fsrc%3Dconfmacro)
此次修改通过在查询中增加提示 para_tables_sort(), 选用排序时不占用临时磁盘空间的算法. 此方法使用大量内存, 减少磁盘占用，本次测试目标验证相同查询语句在使用与不使用para_tables_sort()提示时，内存、磁盘及时间的消耗对比。

### 3. 变更历史

| 日期 | 版本 | 负责人 | 修改记录 |
| --- | --- | --- | --- |
| 2024-02-26 | 0.1 | Charles | Init |

### 4. 测试范围

测试范围主要验证在使用和不使用para_tables_sort()进行查询时时间排序正确性，查询中磁盘、内存及时间的对比，对不同数据规模下，查询性能暂未覆盖。

### 5. 已知问题

无

### 6. 测试环境

测试平台：Linux x64
测试资源：
- 192.168.1.35

### 7. 测试用例

数据集采用10w子表，每个子表写入1000行数据，总计1亿记录 （)
> ⚠ 嵌入文件，需在飞书中查看 (token: LjsjbrfcBoRDaaxWRrgczUlznAe)

sql：select * from st order by ts; *select /*+* para_tables_sort()*/ * from st order by ts;

|  | 子表数 | 查询时间 | 磁盘 | 内存 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 不带para_tables_sort()查询 | 1w(子表行数1000) | 51.1s | 129GB | 20.5GB |  |
| 带para_tables_sort()查询 | 1w(子表行数1000) | 56.48s | 128GB | 37.4GB |  |
| 不带para_tables_sort()查询 | 1w(子表行数10w) | 3611.39s | 336GB | 25.3GB |  |
| 带para_tables_sort()查询 | 1w(子表行数10w) | 1392.31s | 224GB | 63.7GB |  |
| 不带para_tables_sort()查询 | 10w(子表行数1000) | 209.47s | 159GB | 45.2GB |  |
| 带para_tables_sort()查询 | 10w(子表行数1000) | N/A | 147GB | 129GB | taosd退出 |

### 8. 开始结束时间

2024-02-23 - 2024-02-27
