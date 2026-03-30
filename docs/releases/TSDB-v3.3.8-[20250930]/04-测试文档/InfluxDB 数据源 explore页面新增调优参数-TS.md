# InfluxDB 数据源 explore页面新增调优参数-TS

## 1. 测试目标

1. 测试中文版本配置参数后 1.x，2.x 版本是否正常工作
2. 测试英文页面配置参数后 1.x，2.x 版本是否正常工作
3. 测试命令行配置参数后 1.x，2.x 版本是否正常工作

## 2. 参考文档

https://jira.taosdata.com:18080/browse/TD-37596
https://jira.taosdata.com:18080/browse/TS-7096

## 3. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025/08/26 | 1.0 | 张贵川 | 初始版本 |

## 4. 测试结论

1. 中文版本页面配置参数后 1.x，2.x 版本均正常工作
2. 英文页面 配置参数后1.x，2.x 版本均正常工作
3. 命令行配置参数后 1.x，2.x 版本均正常工作
4. 代理方式配置参数后测试 1.x 版本是否正常工作

## 5. 测试环境

- OS: Linux
- Browser: Chrome

## 6. 功能测试

涉及参数名：

| 名称 | 页面名称 | 备注 |
| --- | --- | --- |
| read_concurrency_type | 中文： 数据源并发读取方式 英文：Concurrent Reading Methods | measurement 的并行读取方式。queue: 多线程同时读取一个 measurement，完成后读取下一个。average: 平均方式，多个 measurement 同时被不同线程读取。sequence: 每个 measurement 同时只有一个线程读取。 |
| rows_per_read | 中文：每次读取行数 英文：Rows Per Read | 每次从 InfluxDB 读取数据时的行数。 |
| cache_queue_size | 中文：缓存队列大小 英文：Cache Queue Size | 内部数据缓存队列大小，用于缓存从 influxdb 读取的数据。 |
| jvm_opts | 中文：JVM 参数 英文：JVM Options | 资源限制条件下用于控制内存和 cpu 消耗。 |


Influxdb 1.x 测试：

| # | 测试用例 | 测试描述 | 预期行为 | 测试结果 |
| --- | --- | --- | --- | --- |
| 1 | 中文页面上配置参数： 数据源并发读取方式： average 每次读取行数： 1003 缓存队列大小： 200003 JVM 参数： -Xms4g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 | 中文页面配置参数: ![](./images/img_McqdbvYLwo1ZwHxHZeacjo16nHH.png) 执行结果： ![](./images/img_H3aAbAyQSoiFHYxmpSZcBF6nnDd.png) | 1. 数据导入正常 1. JVM 限制加上 | 通过 |
| 2 | 英文页面上配置参数： 数据源并发读取方式： sequence 每次读取行数： 1003 缓存队列大小： 200003 JVM 参数： -Xms4g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 | 英文页面配置参数: ![](./images/img_RdXsbvQtnoRSooxc0t7cbn3snFh.png) 执行结果： ![](./images/img_Avp3btN81o33iPxum7ycFaXQn3d.png) ![](./images/img_Gt0zbYRejo9ukGxMwzcc2pgznVf.png) | 1. 数据导入正常 1. JVM 限制加上 | 通过 |
| 3 | 命令行测试： 数据源并发读取方式： sequence 每次读取行数： 1003 缓存队列大小： 200003 JVM 参数： -Xms4g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 | 执行命令： ```plaintext {wrap} taosx run -f "influxdb+http://root:taosdata@172.19.0.3:8086?addDbrp=false&batch_size=5004&batch_timeout=1000&beginTime=2025-08-04T00:00:00+08:00&bucket=mydb&busy_threshold=100%&cache_queue_size=200004&delay=10&endTime=2025-08-05T00:00:00+08:00&health_check_window_in_second=0s&jvm_opts=-Xms4g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2&log_level=info&max_errors_in_window=10&max_queue_length=1000&measurements=type_614d3a6cb5289800500e81f2_1j76Rb20VWg&readWindow=60&read_concurrency=50&read_concurrency_type=sequence&rows_per_read=1004&version=1.7&write_concurrency=50" -t "taos+http://root:taosdata@buildkitsandbox:6041/testdb" ``` JVM 限制参数加上： ![](./images/img_B4Tlby3PPoFXY6xs5mrc8kk5n0d.png) | 1. 数据导入正常 1. JVM 限制加上 | 通过 |
| 4 | 代理测试： 数据源并发读取方式： sequence 每次读取行数： 1003 缓存队列大小： 200003 JVM 参数： -Xms4g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 | 参数配置： ![](./images/img_C95wbpqseovrMUxJ7lMcvlcTnwd.png) 运行结果： ![](./images/img_OI8jbCd61oRjLdx7ofOcFV6unHe.png) JVM 限制参数加上： ![](./images/img_NWeAbzdrVoQ7gzxI8uycxKzYndd.png) | 1. 数据导入正常 1. JVM 限制加上 | 通过 |

Influxdb 2.x 测试：

| # | 测试用例 | 测试描述 | 预期行为 | 测试结果 |
| --- | --- | --- | --- | --- |
| 1 | 中文页面上配置参数： 数据源并发读取方式： queue 每次读取行数： 1007 缓存队列大小： 200007 JVM 参数： -Xms4g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 | 参数配置： ![](./images/img_ClqSbwRNGo31nuxTMDzcpIxznYZ.png) ![](./images/img_AuXlbY20goRanYxt8Y6cToUWn1d.png) 执行结果： ![](./images/img_LQ51br5XIowHZaxgChMc7Ytin6b.png) ![](./images/img_FIFGbJlLyoANqFxGnmDckEWRnjc.png) | 1. 数据导入正常 1. JVM 限制加上 | 通过 |
| 2 | 英文页面上配置参数： 数据源并发读取方式： average 每次读取行数： 1007 缓存队列大小： 200007 JVM 参数： -Xms4g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 | 参数配置： ![](./images/img_AewSb4tBOoZERbxQla0c1P6fnce.png) ![](./images/img_Iy8sb6hNmojLzZx6YgGcZs9bnXh.png) 执行结果： ![](./images/img_RgTIb0ygaoiCpdxDYnwcHPxfnIg.png) ![](./images/img_VwRZbIP78o3ZPFxb6Cmci7UAnFe.png) | 1. 数据导入正常 1. JVM 限制加上 | 通过 |
| 3 | 英文页面上配置参数： 数据源并发读取方式： queue 每次读取行数： 1007 缓存队列大小： 200007 JVM 参数： -Xms4g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 | 执行命令： ```plaintext {wrap} taosx run -f "influxdb+http://172.19.0.5:8086?addDbrp=false&batch_size=5007&batch_timeout=1000&beginTime=2025-08-04T00:00:00+08:00&bucket=mydb&busy_threshold=100%&cache_queue_size=200007&delay=10&endTime=2025-08-05T00:00:00+08:00&health_check_window_in_second=0s&jvm_opts=-Xms4g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2&log_level=info&max_errors_in_window=10&max_queue_length=1000&measurements=type_614d3a6cb5289800500e81f2_1j76Rb20VWg&only-choose-one$=2~x&orgId=1d7744fe5f06c5c7&readWindow=60&read_concurrency=50&read_concurrency_type=queue&rows_per_read=1007&token=C_EmHMyFx9o3fUimXDlvcgoMK1zbT6DHZGdXu_aXHgx10OlaBuOnrbOUqzW2CI_STFI1HoLbc9wxUpe1mo-k3A%3D%3D&version=2.7&write_concurrency=50" -t "taos+http://root:taosdata@buildkitsandbox:6041/testdb" ``` 执行结果： ![](./images/img_PcbSbIMYvouxpJxgKVBc4R8qn2d.png) | 1. 数据导入正常 1. JVM 限制加上 | 通过 |


## 7. 易用性测试（可选）

无

## 8. 长期稳定性测试（可选）

无

## 9. 性能测试

无

## 10. 安全测试

无

## 11. 兼容性测试

无

## 12. 已知问题和限制（可选）

无
