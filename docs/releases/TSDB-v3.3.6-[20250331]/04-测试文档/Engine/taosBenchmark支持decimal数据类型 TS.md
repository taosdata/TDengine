# taosBenchmark支持decimal数据类型 TS

## 1. 测试目标

本次测试的主要目标是验证 taosBenchmark 工具对 TDengine 数据库中 decimal 数据类型的支持情况，确保其能够正确地进行数据的写入和查询操作。具体目标如下：
- 验证Native方式写入功能：确保 taosBenchmark 可以使用原生方式将 decimal 类型的数值高效、准确地写入到TDengine 数据库中。
- 验证查询功能：确保用户可以通过 taosBenchmark 对已存储的 decimal 类型数据进行查询，并且返回的结果与预期一致。

## 2. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025/03/19 | 1.0 | 裴亚明 | 初始版本 |
|  |  |  |  |

## 3. 测试范围

- 数据写入测试
  - 数据准备：生成不同精度和尺度（precision and scale）的 decimal 类型数据集。
  - 写入操作：使用 taosBenchmark 工具通过原生方式将上述数据集写入到 TDengine 数据库中。
  - 准确性验证：检查写入的数据是否完整无误地保存在数据库中。
- 数据查询测试
  - 基础查询：执行基本的选择查询，验证能否正确返回 decimal 类型的数据。
  - 边界条件测试：测试极端情况下（如最大值、最小值、空值等）的数据查询能力。
- 性能测试
  - 写入性能：测量在不同数据量下，taosBenchmark 将 decimal 类型数据写入到 TDengine 的吞吐量和延迟。
  - 查询性能：测量在不同查询条件下，taosBenchmark 从 TDengine 检索 decimal 类型数据的速度和响应时间。

## 4. 测试结论

taosBenchmark 已成功实现了对 TDengine 数据库中 decimal 数据类型的全面支持。不仅在功能上达到了预期目标，确保了数据的准确写入和查询，而且在性能方面也展示了优异的表现。

## 5. 已知问题和限制

这里用于记录产品使用上的一些限制，包括不支持的场景等，以及在发版时没有解决的minor issues.
- 受 TDengine 侧限制，taosBenchmark 目前仅支持 Native 方式写入；

## 6. 测试环境

1. 硬件环境：
   - CPU：8C
   - 内存：8GB
2. 软件环境：
   - 操作系统：ubuntu 24.04

## 7. 测试数据

1. decimal64
- 建表语句
```plaintext
CREATE TABLE IF NOT EXISTS test.meters (ts TIMESTAMP,c0 decimal(18,10)) TAGS (t0 int,t1 binary(24))
```

- 1亿条随机数据
1. decimal128
- 建表语句
```plaintext
CREATE TABLE IF NOT EXISTS test.meters (ts TIMESTAMP,c0 decimal(38,20)) TAGS (t0 int,t1 binary(24))
```

- 1亿条随机数据

## 8. 测试用例

### 8.1 功能测试

| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | JIRA问题 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 命令行参数写入decimal数据测试 | 测试生成命令参数指定decimal类型和精度的数据，并写入到TDengine | 通过参数运行taosBenchmark：-b 'int,decimal(10,6),decimal(24,10)' -t 10 -y | 生成命令参数指定decimal数据类型的精度，符合最大值最小值约束，并成功写入到TDengine | Y | Pass |  |  |
| 命令行参数写入decimal数据边界检测 | 测试命令行参数指定precision为0时是否报错 | 通过参数运行taosBenchmark：-b 'int,decimal(10,6),decimal(0,0)' -t 10 -y | 报错：Invalid precision value of decimal type in args | Y | Pass |  |  |
|  | 测试命令行参数指定precision为负数时是否报错 | 通过参数运行taosBenchmark：-b 'int,decimal(10,6),decimal(-3,0)' -t 10 -y | 报错：Invalid precision value of decimal type in args | Y | Pass |  |  |
|  | 测试命令行参数指定precision超过最大精度时是否报错 | 通过参数运行taosBenchmark：-b 'int,decimal(10,6),decimal(39,0)' -t 10 -y | 报错：Invalid precision value of decimal type in args | Y | Pass |  |  |
|  | 测试命令行参数指定precision为负数时是否报错 | 通过参数运行taosBenchmark：-b 'int,decimal(10,6),decimal(10,-3)' -t 10 -y | 报错：Invalid scale value of decimal type in args | Y | Pass |  |  |
|  | 测试命令行参数指定scale超过precision时是否报错 | 通过参数运行taosBenchmark：-b 'int,decimal(10,6),decimal(10,11)' -t 10 -y | 报错：Invalid scale value of decimal type in args | Y | Pass |  |  |
| json文件写入decimal数据测试 | 测试生成json文件中指定各种decimal类型和精度的数据，并写入到TDengine | 运行文件tests\army\tools\benchmark\basic\[insert-decimal.py](http://insert-decimal.py)中测试用例：check_json_normal | 生成json文件中指定decimal数据类型的精度，符合最大值最小值约束，并成功写入到TDengine | Y | Pass |  |  |
| json文件写入decimal数据边界检测 | 测试json文件指定precision为0时是否报错 | 运行测试用例check_json_others中：new_json_file = self.genNewJson(json_file, self.func_precision_zero)
self.exec_benchmark_and_check(benchmark, new_json_file, "Invalid precision value of decimal type in json", options) | 报错：Invalid precision value of decimal type in json | Y | Pass |  |  |
|  | 测试json文件指定precision为负数时是否报错 | 运行测试用例check_json_others中：new_json_file = self.genNewJson(json_file, self.func_precision_negative)
self.exec_benchmark_and_check(benchmark, new_json_file, "Invalid precision value of decimal type in json", options) | 报错：Invalid precision value of decimal type in json | Y | Pass |  |  |
|  | 测试json文件指定precision超过最大精度时是否报错 | 运行测试用例check_json_others中：new_json_file = self.genNewJson(json_file, self.func_precision_exceed_max)
self.exec_benchmark_and_check(benchmark, new_json_file, "Invalid precision value of decimal type in json", options) | 报错：Invalid precision value of decimal type in json | Y | Pass |  |  |
|  | 测试json文件指定precision为负数时是否报错 | 运行测试用例check_json_others中：new_json_file = self.genNewJson(json_file, self.func_scale_negative)
self.exec_benchmark_and_check(benchmark, new_json_file, "Invalid scale value of decimal type in json", options) | 报错：Invalid scale value of decimal type in json | Y | Pass |  |  |
|  | 测试json文件指定scale超过precision时是否报错 | 运行测试用例check_json_others中：new_json_file = self.genNewJson(json_file, self.func_scale_exceed_precision)
self.exec_benchmark_and_check(benchmark, new_json_file, "Invalid scale value of decimal type in json", options) | 报错：Invalid scale value of decimal type in json | Y | Pass |  |  |
| dec_min/dec_max约束条件测试 | 测试Decimal64类型dec_min/dec_max满足约束条件dec_min<dec_max | 运行测试用例：new_json_file = self.genNewJson(json_file, self.func_dec64_min_max)
self.exec_benchmark_and_check(benchmark, new_json_file, "Invalid dec_min/dec_max value of decimal type in json", options) | 报错：Invalid dec_min/dec_max value of decimal type in json | Y | Pass |  |  |
|  | 测试Decimal类型dec_min/dec_max满足约束条件dec_min<dec_max | 运行测试用例：new_json_file = self.genNewJson(json_file, self.func_dec128_min_max)
self.exec_benchmark_and_check(benchmark, new_json_file, "Invalid dec_min/dec_max value of decimal type in json", options) | 报错：Invalid dec_min/dec_max value of decimal type in json | Y | Pass |  |  |


### 8.2 性能测试

1. decimal 随机数生成性能测试
   - 函数 randInt64 生成速度
  生成1亿条数据：Time elapsed: 9.574 seconds, pps: 10.444 * 10⁶ row/s
   - 函数 randUint64 生成速度
  生成1亿条数据：Time elapsed: 9.667 seconds, pps: 10.344 * 10⁶ row/s
   - 函数 decimal64Rand 生成速度
  生成1亿条数据：Time elapsed: 12.252 seconds, pps: 8.1610 * 10⁶ row/s
   - 函数 decimal128Rand precision 18 生成速度
  生成1亿条数据：Time elapsed: 13.839 seconds, pps: 7.2250 * 10⁶ row/s
   - 函数 decimal128Rand precision 38 生成速度
  生成1亿条数据：Time elapsed: 16.761 seconds, pps: 5.9660 * 10⁶ row/s
1. decimal64 写入性能测试
```plaintext
8线程写入1亿条数据：
    Spent 67.447103 (real 64.318223) seconds to insert rows: 100000000 with 8 thread(s) into test 1482643.37 (real 1554769.32) records/second

## 9. 单列 Decimal64 类型，precision：18，scale：10

## 10. ./build/bin/taosBenchmark -b 'decimal(18, 10)' -y

CREATE TABLE IF NOT EXISTS test.meters (ts TIMESTAMP,c0 decimal(18,10)) TAGS (t0 int,t1 binary(24))
[03/25 15:55:09.993029] SUCC: Spent 67.447103 (real 64.318223) seconds to insert rows: 100000000 with 8 thread(s) into test 1482643.37 (real 1554769.32) records/second
[03/25 15:55:09.993161] SUCC: insert delay, min: 16.3050ms, avg: 51.4546ms, p90: 74.3260ms, p95: 84.7320ms, p99: 110.7110ms, max: 1302.9080ms

taos> select min(c0), max(c0) from test.meters;
       min(c0)        |       max(c0)        |
==============================================
 -99957459.1455948643 |  99965647.9326320577 |
Query OK, 1 row(s) in set (0.307984s)

taos> 
```

1. decimal128 写入性能测试
```plaintext
8线程写入1亿条数据：
    Spent 67.447103 (real 64.318223) seconds to insert rows: 100000000 with 8 thread(s) into test 1482643.37 (real 1554769.32) records/second

## 11. 单列 Decimal128 类型，precision：38，scale：20

## 12. ./build/bin/taosBenchmark -b 'decimal(38, 20)' -y

CREATE TABLE IF NOT EXISTS test.meters (ts TIMESTAMP,c0 decimal(38,20)) TAGS (t0 int,t1 binary(24))
[03/25 16:02:56.627002] SUCC: Spent 232.920627 (real 228.391787) seconds to insert rows: 100000000 with 8 thread(s) into test 429330.80 (real 437844.12) records/second
[03/25 16:02:56.627114] SUCC: insert delay, min: 38.7280ms, avg: 182.7134ms, p90: 171.8790ms, p95: 256.2570ms, p99: 1224.6760ms, max: 6649.5590ms


taos> select min(c0), max(c0) from test.meters;
                 min(c0)                  |                 max(c0)                  |
======================================================================================
 -999255237940088942.81657511898211418112 |  999828776077941729.45626107451038236672 |
Query OK, 1 row(s) in set (1.052080s)

taos> 
```


1. decimal64 查询性能测试
查询配置文件内容如下，与 decimal128 相同。
```plaintext
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
        "query_times": 1000,
        "query_mode": "taosc",
        "specified_table_query": {
                "query_interval": 0,
                "concurrent": 10,
                "sqls": [
                               {"sql": "select count(*) from test.d0"},
                               {"sql": "select count(*) from test.d1"},
                               {"sql": "select count(*) from test.d2"},
                               {"sql": "select count(*) from test.d3"},
                               {"sql": "select count(*) from test.d4"},
                               {"sql": "select count(*) from test.d5"},
                               {"sql": "select count(*) from test.d6"},
                               {"sql": "select count(*) from test.d7"},
                               {"sql": "select count(*) from test.d8"},
                               {"sql": "select count(*) from test.d9"}
                ]
        }
}
```

```plaintext
complete query with 10 threads and 10000 sql 10 spend 3.820942s QPS: 2617.156 query delay avg: 0.003817s min: 0.001417s max: 0.013785s p90: 0.005284s p95: 0.005879s p99: 0.007442s SQL command: select count(*) from test.d9 
[03/25 16:44:13.542728] INFO: Spend 38.3830 second completed total queries: 100000, the QPS of all threads:   2605.320 ,error 0 (rate:0.000%)
```

1. decimal128 查询性能测试
```plaintext
complete query with 10 threads and 10000 sql 10 spend 4.406252s QPS: 2269.503 query delay avg: 0.004386s min: 0.001268s max: 0.195260s p90: 0.005625s p95: 0.006405s p99: 0.010488s SQL command: select count(*) from test.d9 
[03/25 16:14:23.537824] INFO: Spend 40.0090 second completed total queries: 100000, the QPS of all threads:   2499.438 ,error 0 (rate:0.000%)

```

## 13. 测试计划

2025/03/24-2025/03/25 两天

## 14. 参考文档

- 无
