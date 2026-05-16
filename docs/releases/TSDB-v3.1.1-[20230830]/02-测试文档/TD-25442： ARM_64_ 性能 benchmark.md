# TD-25442： ARM/64: 性能 benchmark

## 1. 测试结论：

ARM 64 机器上之前没有做过专门的 Benchmark 测试，没有对比的对象，所以用 Linux X64 的测试结果作为一个参考的对象，从下面的测试结果可以看出没有明显的异常，测试结果符合预期。

## 2. 测试环境

硬件：CPU 8核 32G
软件：TDengine main branch (ec8472290d)

## 3. 测试结果

1. 写入：tables: 10000, recors per talbe: 10000, stt_trigger: 1，cachemodel: both
|  | ARM |  | Linux X64 |  |
| --- | --- | --- | --- | --- |
| interlace_rows | 0 | 1 | 0 | 1 |
| time (s) | 54.173093 | 430.935821 | 23.777702 | 196.924633 |
| speed | 1845934.84 | 232053.12 | 4205620.88 | 507808.49 |
| min delay (s) | 2.228 | 14.909 | 0.949 | 6.257 |
| avg delay (s) | 5.1593 | 42.0376 | 2.1788 | 19.2178 |
| max delay (s) | 64.978 | 165.709 | 200.5 | 336.391 |

1. 查询
|  | ARM64 | Linux X64 |
| --- | --- | --- |
| select last_row(*) from meters | 0.017834 | 0.013698 |
| select count(*) from meters | 0.213992 | 0.089822 |
| select count(*) from d0 | 0.003197 | 0.007439 |
| select avg(current), max(voltage), min(phase) from meters | 0.381506 | 0.214151 |
| select avg(current), max(voltage), min(phase) from meters interval(10s) | 1.785054 | 0.674498 |
| select avg(current), max(voltage), min(phase) from meters group by tbname limit 10000 | 0.492275 | 0.23748 |
| select avg(current), max(voltage), min(phase) from meters partition by tbname limit 10000 | 2.740292 | 1.115141 |
| select last(*) from meters group by tbname slimit 10000 | 0.097231 | 0.048224 |
| select last_row(*) from meters group by tbname slimit 10000 | 0.100045 | 0.047155 |
| select last(*) from meters partition by tbname slimit 10000 | 0.902599 | 0.408556 |
| select last_row(*) from meters partition by tbname slimit 10000 | 0.897398 | 0.379957 |
| select count(*) from meters where location = 'San Francisco' | 0.043996 | 0.021208 |
| select avg(current), max(voltage), min(phase) from meters where groupid = 1 | 0.056194 | 0.040781 |
| select * from meters limit 10000 | 0.064611 | 0.037616 |
| select * from d100 limit 10000 | 0.051238 | 0.03668 |
| select spread(phase) from meters | 0.273166 | 0.124194 |
| select * from meters order by ts desc limit 1000 | 3.176284 | 1.333675 |
| select last(*) from meters | 0.012388 | 0.009022 |
| select count(*) from information_schema.ins_tables | 0.007907 | 0.008758 |
| select count(*) from information_schema.ins_tables where db_name = 'test' | 0.004476 | 0.009667 |
| select count(*) from information_schema.ins_tables where db_name = 'test' and stable_name = 
'meters' | 0.009561 | 0.006949 |
| select db_name, count(*) from information_schema.ins_tables group by db_name | 0.004563 | 0.006973 |
| select db_name, stable_name, count(*) from information_schema.ins_tables group by db_name, s
table_name | 0.007284 | 0.002004 |
