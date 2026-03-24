# last()/last_row()同时查询时的性能优化

## 1. 背景

基于用户需求（TS-4206）, 在database的cache mode 配置为 both时， 一条查询语句中如果同时使用last 和 last_row函数时，性能较差，在查询数据量比较大的表时此问题非常突出，原因为同时使用last 和 last_row函数时使用的是表扫描，而不是像单独使用last或者last_row时使用缓存扫描，用户希望提升该操作的性能。

## 2. 变更历史

| 日期 | 版本 | 负责人 |
| --- | --- | --- |
| 2023.12.27 | 0.1 | 符鸿 |

## 3. 定义

无。

## 4. 行为说明

1. Database cache mode为both时，本需求将select last, last_row的扫描模式改为缓存扫描，此时性能最佳。
2. 本需求将支持原来不支持的操作：database的cache model 配置为both或last_value时，last函数输入参数为表达式的情况下可以正确执行,但该last函数使用的为表扫描而不是缓存扫描。
<callout emoji="chestnut" background-color="light-orange" border-color="light-orange">
select last(id + 1) from testlast;
       last(id + 1)        |
============================
         0.000000000000000 |
</callout>


## 5. 性能

数据量较小的情况下，使用表扫描与使用缓存扫描性能区别并不大，在此只描述数据量较大的情况下的性能（以查询taosBenchmask为例，表数：10000，每张表10000条数据，共1亿条记录）
Database cachemodel 为both时，本需求将显著提升select last, last_row的查询性能。

| 缓存未加载 | both | last_value | last_row | none |
| --- | --- | --- | --- | --- |
| Last, last_row | 差 | 差 | 差 | 较好 |
| last | 差 | 差 | 差 | 较好 |
| last_row | 差 | 差 | 差 | 较好 |

由于缓存在使用中需要预热，所以对于缓存未加载的数据，在第一次使用缓存扫描时性能会比使用表扫描慢，但之后再次使用缓存扫描该数据，则性能优化很明显。

| 缓存已加载数据 | both | last_value | last_row | none |
| --- | --- | --- | --- | --- |
| Last, last_row | 优 | 较好 | 较好 | 较好 |
| last | 优 | 优 | 较好 | 较好 |
| last_row | 优 | 较好 | 优 | 较好 |

## 6. 兼容性

无。

## 7. 运维

无。

## 8. 使用场景

无

## 9. 约束和限制

无

## 10. 常见错误和排查

无。

## 11. 参考文档

**Note: 用户手册中尽量不出现设计方案或实现相关的内容。**
