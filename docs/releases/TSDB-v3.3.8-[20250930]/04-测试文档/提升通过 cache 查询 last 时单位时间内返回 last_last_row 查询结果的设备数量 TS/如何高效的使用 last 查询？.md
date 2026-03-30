# 如何高效的使用 last 查询？

last 查询的性能被很多因素影响着，除了我们耳熟能详的 cachemodel , cachesize 之外，使用不同的查询语句最终得到的 QPS （这里的 QPS 可能不准确，更准确的应该是返回的设备数）也是天差地别。

| 查询方式 | 示例 | 推荐\不推荐 | 备注 |
| --- | --- | --- | --- |
| 查询超级表使用 tbname in 过滤 | select tbname,last(*) from test.meters where tbname in ('d1','d2','d3','d4','d5','d6','d7','d8','d9','d10') partition by tbname; | **最推荐** | 开发分支上优化了 tbname in 的执行计划，是目前测试中性能最好的查询方式，建议一次查询可以查询 500 到 2000 张子表。 |
| 查询超级表使用 tag in 过滤 | select channel_id,last(*) from meters where channel_id in (1,2,3,4,5,6,7,8,9,10)partition by channel_id; | **推荐** | 开发分支上优化了 tag in 的执行计划，确保通过 tag 过滤是可以通过已存在的索引提升查询速度。但是整体性能大幅低于 tbname in ，并且使用索引过滤同时使用索引需要很苛刻的条件，**如果可以的话更推荐客户使用 tbname in 。** 可以走优化路径的条件： 1. **用来过滤的 tag 存在索引，第一个 tag 列默认存在索引。** 1. **不可以使用 or 。** 1. **类型要求：** **Tag == varchar 并且 value == varchar** **Tag == 有符号数 并且 value == 有符号数 并且 Tag 的类型 >= value 的类型** 例如： tag bigint value int 那么就可以，如果 tag 是int value 是 bigint 则不可以。还需要格外注意 channel_id in (1) 可以使用索引，但是 channel_id in ('1') 不可以使用索引，因为value 会被识别为 varchar 。 |
| 查询子表 | SELECT last(ts,r32) FROM test.d1; | **不推荐** | 无法批量获取结果，性能较差，不推荐使用。 |

**二进制：**
<view type="1">

  > ⚠ 嵌入文件，需在飞书中查看 (token: E12lb8yhOoSQ0vxrfkcc3GwNnQg)

</view>

**测试程序：**
<view type="1">

  > ⚠ 嵌入文件，需在飞书中查看 (token: V6dMbjaOLo3dvOxicg1caPulnjb)

</view>

```plaintext
./taos_query_a_simple 2

一共有五个模式：
0 查询子表
1 tbname in 查询100张子表
2 tbname in 查询 1 w 张子表（每个线程625个子表）
3 查询超级表
4 tag 过滤
```
