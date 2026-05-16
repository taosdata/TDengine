# 简版 LAG 函数 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-19 | 2025-12-03 | 1.0 | 金明磊 | 新建 |

## 2. 测试目标

简版 LAG 函数常规测试及性能测试，验证正确性，测试性能情况，包括
1. 有 null 存在情况下的正确性
2. 空字符串输入情况下的正确性
3. 单字符串输入情况下的正确性
4. 异常情况下的正确性
5. 基础性能测试，所有函数循环执行，统计查询性能并分析

## 3. 参考文档

[简版 Lag 函数 FS](https://taosdata.feishu.cn/wiki/UzTfwDGd7itJ8tkRkfScN4YNnYb)
[简版 Lag 函数 RS](https://taosdata.feishu.cn/wiki/Iq3KwD9DKifFKVkg40lcLGEynD2)

## 4. 测试结论

1. 功能测试正确
2. 性能测试符合预期

## 5. 测试环境

1. 测试环境：Linux：ubuntu 20.4
2. 测试数据：多种类型的超级表和子表

## 6. 功能测试

### 6.1 功能

测试脚本：test_fun_win_lag.py
函数列表：
1. lag
覆盖的测试场景及用例：

| 测试场景 | 用例名称 | 预期结果 | 测试结果 |
| --- | --- | --- | --- |
| 基本用例 | smoke | 每个函数在常见输入情况下，查询语句中使用，检查获得的结果符合预期 | 通过 |
| 支持类型 | input_types | 各种支持的数据类型情况下获得正确输出，包括正反序情况 | 通过 |
| 普通表/超级表 | lag_with_stable | 普通表/超级表情况下获得正确输出，包括正反序情况 | 通过 |
| 虚拟普通表/虚拟超级表 | lag_with_vtable | 虚拟普通表/虚拟超级表情况下获得正确输出，包括正反序情况 | 通过 |
| 异常情况 | error | 能够得到预期错误信息，包括不支持类型的错误信息 | 通过 |

### 6.2 可用性

无

### 6.3 可靠性

重复测试无异常

## 7. 易用性测试（可选）

## 8. 长期稳定性测试（可选）

## 9. 性能

### 9.1 测试方法

使用 taosBenchmark -f tools/taos-tools/case/insertMix.json 生成 10 万行的随机空值数据(太多 interp 无法计算)，然后分别对下列语句并发 10，进行 100 次查询：
```sql
select fc,dc,diff(fc), diff(dc),diff(fc)*diff(dc) from test.d0
select fc,dc,lag(fc), lag(dc),lag(fc)*lag(dc) from test.d0
select f1l, f2l, f1l*f2l from (select _irowts ts, interp(fc, 1) f1l from mix.d0 RANGE("2017-07-14 10:40:00.000", "2017-07-17 22:01:19.000") every(1s) fill(prev)) as tb1 join (select _irowts ts, interp(dc, 1
) f2l from db.tb RANGE("2017-07-14 10:40:00.000", "2017-07-17 22:01:19.000") every(1s) fill(prev)) as tb2 on tb1.ts=tb2.ts
select f1l, f2l, f1l*f2l from (select _irowts ts, interp(fc, 1) f1l from mix.d0 RANGE("2017-07-14 10:40:00.000", "2017-07-17 22:01:19.000") every(1s) fill(linear)) as tb1 join (select _irowts ts, interp(dc, 1
) f2l from db.tb RANGE("2017-07-14 10:40:00.000", "2017-07-17 22:01:19.000") every(1s) fill(linear)) as tb2 on tb1.ts=tb2.ts
```

查询使用以下 json 文件：
```plaintext {wrap}
{
        "filetype": "query",
        "cfgdir": "/etc/taos",
        "host": "127.0.0.1",
        "port": 6030,
        "user": "root",
        "password": "taosdata",
        "confirm_parameter_prompt": "no",
        "continue_if_fail": "yes",
        "databases": "test",
        "query_times": 100,
        "query_mode": "taosc",
        "specified_table_query": {
                "query_interval": 1,
                "threads": 10,
                "sqls": [
                        {
                                "sql": "select fc,dc,lag(fc), lag(dc),lag(fc)*lag(dc) from test.d0",
                                "result": "./query_res12.txt"
                        }
                ]
        }
}
```

### 9.2 测试结果

| # | 耗时 | lag | diff | interp-prev | interp-linear | unit |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 查询平均耗时 | 2.151074s | 2.306944 | 5.418385 | 5.516000 | s |
| 2 | 查询最小耗时 | 2.442046s | 2.473583 | 5.452115 | 5.602989 | s |
| 3 | 查询最大耗时 | 2.168115s | 2.252471 | 5.407788 | 5.480007 | s |

### 9.3 结果说明

Lag 为新函数，没有历史性能数据供对比，仅比对同类或功能相似函数。符合预期

## 10. 安全性

不涉及

## 11. 兼容性

1. 与现有 SQL 语法兼容，不影响未使用简化 LAG 函数的查询逻辑
2. 后续扩展参数配置时，需与窗口函数体系兼容，无功能冲突
3. 新增功能，原来不支持的语法改为支持。

## 12. 已知问题和限制

已知的限制
1. LAG 是窗口函数，这个简版 LAG 函数仅支持 null 填充
2. 支持数值类型及字符串类型，不支持其它输入类型
