# NCHAR 匹配性能优化

## 1. 背景

      测试人员使用TDengine与竞品timescale的比较测试发现，TDengine的nchar类型的字段在使用match/nmatch函数时性能与竞品timescale差距很大，相同数据量的情况下，TDengine查询耗时193秒，timescale耗时不到10秒，严重影响用户体验(TD-26789)。

## 2. 变更历史

| 日期 | 版本 | 负责人 |
| --- | --- | --- |
| 2024/01/08 | 0.1 | 符鸿 |

## 3. 定义

      无。

## 4. 行为说明

      无。

## 5. 性能


| 字段类型 | 字段长度 | 表类型 | 数据量(万） | 操作函数 | 参数 | Timesacle RT | TDengine RT | 优化的目标性能 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nchar | 1 | 超级表（子表数1000） | 2000 | nmatch | ^[0-9] | <=10s | 193s | 20倍（193/20） |
| nchar | 1 | 超级表子表数1000） | 2000 | match | ^[0-9] | <=10s | 192s | 20倍（192/20） |

sql如下：
<callout emoji="pushpin" background-color="light-orange" border-color="light-orange">
select count(*) from st_common where v_nchar nmatch '^[0-9]';

       count(*)        |

========================

              16070699 |

Query OK, 1 row(s) in set (10.0s)
</callout>

<callout emoji="pushpin" background-color="light-orange" border-color="light-orange">
select count(*) from st_common where v_nchar match '^[0-9]';

       count(*)        |

========================

               3028982 |

Query OK, 1 row(s) in set (10.0s)
</callout>


## 6. 兼容性

      无。

## 7. 运维

      无。

## 8. 使用场景

      无。

## 9. 约束和限制

      无

## 10. 常见错误和排查

      无。

## 11. 参考文档

      无。
