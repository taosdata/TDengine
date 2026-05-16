# taosgen 支持发布到 Kafka TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-01 |  | 1.0 | 裴亚明 | 编写文档 |

## 2. 测试目标

- 验证 taosgen 向 Kafka 发布数据的功能正确性、性能达标性

## 3. 参考文档

[taosgen 支持发布到 Kafka FS](https://taosdata.feishu.cn/wiki/Lor0www3PiPQPbk5rVKc698knsO)

## 4. 测试结论

- taosgen 向 Kafka 发布数据的功能正确、相比 Kafka 官方工具，性能略有提升，taosgen 性能已达标

## 5. 测试环境

| 客户端 | 192.168.1.54 |
| --- | --- |
| 服务端 | 192.168.1.43 |
| 操作系统 | Ubuntu 20.04.6 LTS (64-bit) |
| CPU和内存 | 40C 251G |
| 存储 | 447G SSD * 2、1.76T SSD |
| Kafka 版本 | 4.1.0 |
| taosgen commit ID | 3eadb2be6aa52899b818e91a67089237ad0f05cb |

## 6. 功能测试

### 6.1 功能单元测试

#### 6.1.1 测试要点

使用单元测试方式测试发布到kafka涉及到的各子模块的基础功能，包括：kafka配置解析、kafka格式化、kafka Key生成器、kafka数据写入、kafka客户端等。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | TestConfigParser/test_KafkaConfig | 测试kafka连接相关配置解析 | 通过 |
| 2 | TestConfigParser/test_KafkaConfig_unknown_key | 测试kafka配置中未知项的识别 | 通过 |
| 3 | TestConfigParser/test_InsertDataConfig_kafka | 测试kafka数据写入相关配置解析 | 通过 |
| 4 | TestKafkaInsertDataFormatter/test_kafka_format_json_single_record | 测试kafka json格式化单条记录 | 通过 |
| 5 | TestKafkaInsertDataFormatter/test_kafka_format_json_multiple_records | 测试kafka json格式化多条记录 | 通过 |
| 6 | TestKafkaInsertDataFormatter/test_kafka_format_influx_single_record | 测试kafka influx行协议格式化单条记录 | 通过 |
| 7 | TestKafkaInsertDataFormatter/void test_kafka_format_influx_multiple_records | 测试kafka influx行协议格式化多条记录 | 通过 |
| 8 | TestKafkaInsertDataFormatter/test_kafka_format_empty_batch | 测试kafka 格式化空数据的处理 | 通过 |
| 9 | TestKafkaInsertDataFormatter/test_kafka_format_invalid_serializer | 测试kafka无效格式化类型的处理 | 通过 |
| 10 | TestKeyGenerator/test_string_serializer | 测试字符串类型key序列化 | 通过 |
| 11 | TestKeyGenerator/test_integer_serializer | 测试整数类型key序列化 | 通过 |
| 12 | TestKeyGenerator/test_invalid_pattern_for_integer_serializer | 测试整数类型无效格式的处理 | 通过 |
| 13 | TestKeyGenerator/test_unsupported_serializer | 测试不支持序列化类型的处理 | 通过 |
| 14 | TestKeyGenerator/test_integer_parse_error | 测试整数类型key数值转换错误的处理 | 通过 |
| 15 | TestKafkaWriter/test_constructor | 测试kafka写入器的构造函数 | 通过 |
| 16 | TestKafkaWriter/test_connection | 测试kafka写入器的连接功能 | 通过 |
| 17 | TestKafkaWriter/test_connection_failure | 测试kafka写入器的连接失败的处理 | 通过 |
| 18 | TestKafkaWriter/test_write_operations | 测试kafka写入器的写入操作的功能（包括异常处理） | 通过 |
| 19 | TestKafkaWriter/test_write_with_retry | 测试kafka写入器的写入重试功能 | 通过 |
| 20 | TestKafkaWriter/test_write_without_connection | 测试kafka写入器对未连接时写入的处理 | 通过 |
| 21 | TestKafkaClient/test_connect_and_close | 测试kafka客户端的连接和关闭功能 | 通过 |
| 22 | TestKafkaClient/test_connect_failure | 测试kafka客户端对连接失败的处理 | 通过 |
| 23 | TestKafkaClient/test_execute_and_produce | 测试kafka客户端的执行发布功能 | 通过 |
| 24 | TestKafkaClient/test_execute_not_connected | 测试kafka客户端对未连接时执行发布的处理 | 通过 |
| 25 | TestKafkaClient/test_produce_failure | 测试kafka客户端对发布失败的处理 | 通过 |

### 6.2 功能集成测试

#### 6.2.1 测试要点

本集成测试旨在全面验证 taosgen 工具向 Kafka 发布数据的核心功能、关键性能行为的正确性。测试覆盖 Kafka 客户端连接配置（包括安全协议认证）、消息生产行为（如序列化格式、消息 Key 生成、时间戳精度）、多并发写入能力以及不同可靠性与吞吐量策略组合下的运行表现。重点验证在复杂配置场景下，数据能否按预期格式和语义准确发布到指定 Topic，并确保高并发、批量打包、压缩、ACK 等机制正常工作。

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | IT_KAFKA_CONNECT_PLAINTEXT | 验证 taosgen 能否通过 plaintext 协议成功连接 Kafka 集群并发送数据 | 通过 |
| 2 | IT_KAFKA_CONNECT_SASL_PLAINTEXT_PLAIN | 验证使用 SASL_PLAINTEXT 和 PLAIN 机制的身份认证正常连接并发布数据 | 通过 |
| 3 | IT_KAFKA_TOPIC_WRITE_DEFAULT | 验证数据能正确写入配置中指定的 Kafka Topic，且消费者可正常读取 | 通过 |
| 4 | IT_KAFKA_KEY_PATTERN_TABLE | 验证 key_pattern 设置为 `{table}` 时，消息 Key 正确生成为表名 | 通过 |
| 5 | IT_KAFKA_KEY_PATTERN_FIELD | 验证 key_pattern 使用字段占位符（如 `{ts}`）时，Key 值正确替换 | 通过 |
| 6 | IT_KAFKA_KEY_SERIALIZER_STRING_UTF8 | 验证 key_serializer="string-utf8" 时，字符串 Key 能正确序列化发送 | 通过 |
| 7 | IT_KAFKA_KEY_SERIALIZER_INT32 | 验证 key_serializer="int32" 等整数选项时，数值字段（如 `{ts}`）能以大端序整型序列化 Key | 通过 |
| 8 | IT_KAFKA_KEY_SERIALIZER_UNKNOW | 验证 key_serializer 为未知项时，程序运行报错，并给出提示信息 | 通过 |
| 9 | IT_KAFKA_VALUE_SERIALIZER_JSON | 验证 value_serializer="json" 时，消息体以标准 JSON 格式输出，包含 ts、fields 和 table 字段 | 通过 |
| 10 | IT_KAFKA_VALUE_SERIALIZER_INFLUX | 验证 value_serializer="influx" 时，消息体采用 InfluxDB Line Protocol 格式编码 | 通过 |
| 11 | IT_KAFKA_VALUE_SERIALIZER_UNKNOW | 验证 value_serializer 为未知项时，程序运行报错，并给出提示信息 | 通过 |
| 12 | IT_KAFKA_TBNAME_KEY_CUSTOM | 验证设置 tbname_key 为自定义字段名（如 "measurement"）后，JSON 输出中正确体现该字段 | 通过 |
| 13 | IT_KAFKA_TBNAME_KEY_EMPTY | 验证当 tbname_key 设置为空字符串时，JSON 消息中不包含表名字段 | 通过 |
| 14 | IT_KAFKA_ACKS_0 | 验证 acks="0" 模式下消息快速发出，无确认等待，适用于高吞吐压测场景 | 通过 |
| 15 | IT_KAFKA_ACKS_1 | 验证 acks="1" 模式下生产者收到 Leader 写入确认后返回，具备基本可靠性 | 通过 |
| 16 | IT_KAFKA_ACKS_ALL | 验证 acks="all" 模式下需所有 ISR 副本确认，确保强持久性 | 通过 |
| 17 | IT_KAFKA_COMPRESSION_NONE | 验证 compression="none" 时消息未压缩，内容可直接解析 | 通过 |
| 18 | IT_KAFKA_COMPRESSION_GZIP | 验证启用 gzip 压缩后消息被正确压缩，消费者可解压并解析内容 | 通过 |
| 19 | IT_KAFKA_COMPRESSION_SNAPPY | 验证 Snappy 压缩模式下消息正常收发 | 通过 |
| 20 | IT_KAFKA_COMPRESSION_LZ4 | 验证 lz4 压缩模式下消息正常收发 | 通过 |
| 21 | IT_KAFKA_COMPRESSION_ZSTD | 验证 zstd 压缩模式下消息正常收发 | 通过 |
| 22 | IT_KAFKA_RECORDS_PER_MESSAGE_1 | 验证 records_per_message=1 时每条消息仅包含一条记录 | 通过 |
| 23 | IT_KAFKA_RECORDS_PER_MESSAGE_100 | 验证批量模式下每条消息打包 100 条记录，提升吞吐量 | 通过 |
| 24 | IT_KAFKA_TIMESTAMP_PRECISION_MS | 验证 timestamp_precision="ms" 时时间戳以毫秒精度生成 | 通过 |
| 25 | IT_KAFKA_TIMESTAMP_PRECISION_US | 验证 microsecond 级时间戳生成准确性 | 通过 |
| 26 | IT_KAFKA_TIMESTAMP_PRECISION_NS | 验证 nanosecond 级时间戳生成准确性 | 通过 |
| 27 | IT_KAFKA_CONCURRENCY_8 | 验证 concurrency=8 时多线程并发生产数据，系统资源利用合理，无冲突 | 通过 |
| 28 | IT_KAFKA_SCHEMA_INTERLACE_MODE | 验证 interlace 模式下多表数据交错生成并发布，符合模拟设备并发上报逻辑 | 通过 |
| 29 | IT_KAFKA_FAILURE_HANDLING_RETRY | 验证网络抖动或 Broker 暂不可用时，生产者具备重试机制，避免数据丢失 | 通过 |
| 30 | IT_KAFKA_RDKAFKA_CUSTOM_OPTS | 验证 rdkafka_options 可传递 librdkafka 参数（如 linger.ms），影响底层行为 | 通过 |

## 7. 易用性测试（可选）

无

## 8. 长期稳定性测试（可选）

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | IT_KAFKA_LONG_TERM_STABILITY | 连续运行 4 小时并发写入，验证内存稳定、数据完整性一致、未发现内存泄漏 | 通过 |

## 9. 性能测试

### 9.1 taosgen VS Kafka  官方自带脚本

#### 9.1.1 Kafka 官方自带脚本

提前生成：1亿条、100万子表数据的meters数据，json 格式，预加载，然后测试，性能：91.27 万 rps
```bash
root@u1-54:~/crispei/kafka/own$ /usr/local/kafka/bin/kafka-producer-perf-test.sh   --topic tsbench-topic   --num-records 100000000   --throughput -1   --producer-props     bootstrap.servers=172.16.1.43:9092     acks=0     buffer.memory=157286400     batch.size=2097152     linger.ms=5     compression.type=none   --payload-file /root/crispei/kafka/own/kafka_perf_test_data.json
Reading payloads from: /root/crispei/kafka/own/kafka_perf_test_data.json
Number of messages read: 100000000
3415862 records sent, 682762.7 records/sec (62.50 MB/sec), 4.2 ms avg latency, 524.0 ms max latency.
4378405 records sent, 875681.0 records/sec (80.15 MB/sec), 4.1 ms avg latency, 12.0 ms max latency.
4656940 records sent, 930829.5 records/sec (85.20 MB/sec), 4.2 ms avg latency, 11.0 ms max latency.
4534160 records sent, 906469.4 records/sec (82.97 MB/sec), 4.1 ms avg latency, 12.0 ms max latency.
4633306 records sent, 926661.2 records/sec (84.82 MB/sec), 4.2 ms avg latency, 33.0 ms max latency.
4615476 records sent, 922910.6 records/sec (84.47 MB/sec), 4.1 ms avg latency, 11.0 ms max latency.
4681296 records sent, 936259.2 records/sec (85.70 MB/sec), 4.1 ms avg latency, 11.0 ms max latency.
4526441 records sent, 904383.8 records/sec (82.78 MB/sec), 4.0 ms avg latency, 11.0 ms max latency.
4639826 records sent, 927779.6 records/sec (84.92 MB/sec), 4.1 ms avg latency, 11.0 ms max latency.
4664050 records sent, 932623.5 records/sec (85.37 MB/sec), 4.1 ms avg latency, 31.0 ms max latency.
4648680 records sent, 929178.5 records/sec (85.05 MB/sec), 4.1 ms avg latency, 12.0 ms max latency.
4686814 records sent, 936800.7 records/sec (85.75 MB/sec), 4.1 ms avg latency, 11.0 ms max latency.
4640099 records sent, 927834.2 records/sec (84.93 MB/sec), 4.1 ms avg latency, 11.0 ms max latency.
4754999 records sent, 950429.5 records/sec (86.99 MB/sec), 4.1 ms avg latency, 28.0 ms max latency.
4659936 records sent, 931428.3 records/sec (85.25 MB/sec), 4.1 ms avg latency, 10.0 ms max latency.
4676072 records sent, 935214.4 records/sec (85.60 MB/sec), 4.0 ms avg latency, 11.0 ms max latency.
4598153 records sent, 919630.6 records/sec (84.18 MB/sec), 4.0 ms avg latency, 10.0 ms max latency.
4613603 records sent, 922351.7 records/sec (84.42 MB/sec), 4.1 ms avg latency, 11.0 ms max latency.
4571177 records sent, 913504.6 records/sec (83.61 MB/sec), 4.2 ms avg latency, 27.0 ms max latency.
4660449 records sent, 931903.4 records/sec (85.30 MB/sec), 4.1 ms avg latency, 11.0 ms max latency.
4673382 records sent, 934302.7 records/sec (85.52 MB/sec), 4.1 ms avg latency, 11.0 ms max latency.
100000000 records sent, 912700.2 records/sec (83.54 MB/sec), 4.10 ms avg latency, 524.00 ms max latency, 4 ms 50th, 6 ms 95th, 7 ms 99th, 10 ms 99.9th.
```


#### 9.1.2 taosgen 

由于 Kafka 官方自带脚本为单线程，因此将 taosgen 的并发度改成 1，行为：96.89 万 rps，相比官方脚本写入性能提升：+6.16%
```yaml
2025-12-02 17:18:14.587151 2850999 INFO  =================================================== Insert Summary Statistics ========================================================
2025-12-02 17:18:14.587154 2850999 INFO  Insert Threads: 1
2025-12-02 17:18:14.587156 2850999 INFO  Total Rows: 100000000
2025-12-02 17:18:14.587162 2850999 INFO  Total Duration: 103.21 seconds
2025-12-02 17:18:14.587166 2850999 INFO  Average Rate: 968925.32 rows/second
2025-12-02 17:18:14.587167 2850999 INFO  =======================================================================================================================================
2025-12-02 17:18:14.589021 2850999 INFO  
2025-12-02 17:18:14.589029 2850999 INFO  =================================================== Insert Latency & Efficiency Metrics ==============================================
2025-12-02 17:18:14.589031 2850999 INFO  Total Operations: 12000
2025-12-02 17:18:14.589033 2850999 INFO  Total Duration: 103.21 seconds
2025-12-02 17:18:14.589034 2850999 INFO  Pure Insert Latency: 103.13 seconds
2025-12-02 17:18:14.589036 2850999 INFO  Effective Time Ratio: 99.92%
2025-12-02 17:18:14.589038 2850999 INFO  Framework Overhead: 0.08%
2025-12-02 17:18:14.589039 2850999 INFO  Idle Time After Finish: 0.00 seconds
2025-12-02 17:18:14.589062 2850999 INFO  Write Latency Distribution: min: 3.2512ms, avg: 8.5939ms, p90: 10.7084ms, p95: 10.8514ms, p99: 11.5502ms, max: 17.2914ms
2025-12-02 17:18:14.589063 2850999 INFO  ======================================================================================================================================
2025-12-02 17:18:14.589064 2850999 INFO  
2025-12-02 17:18:14.590340 2850999 INFO  InsertDataAction completed successfully
2025-12-02 17:18:15.328458 2850999 INFO  Step completed: kafka/produce
2025-12-02 17:18:15.397633 2850996 INFO  All jobs completed successfully!
```

### 9.2 taosgen VS Kafka 官方脚本 20 进程并发

#### 9.2.1 Kafka 官方自带脚本 20 进程并发

提前生成：1亿条、100万子表数据的meters数据，拆分成 20 个等量文件，然后测试，性能：277.16 万 rps
```yaml
===============================================
Kafka 多进程并发压测启动
目标: 100000000 条记录, 20 个并发进程
每个进程: 5000000 条记录
开始时间: 2025-12-02 19:44:18
===============================================
19:44:18 - 进程 00 已启动 (PID: 2867138)
19:44:18 - 进程 01 已启动 (PID: 2867142)
19:44:18 - 进程 02 已启动 (PID: 2867146)
19:44:18 - 进程 03 已启动 (PID: 2867153)
19:44:18 - 进程 04 已启动 (PID: 2867160)
19:44:18 - 进程 05 已启动 (PID: 2867172)
19:44:18 - 进程 06 已启动 (PID: 2867185)
19:44:18 - 进程 07 已启动 (PID: 2867200)
19:44:18 - 进程 08 已启动 (PID: 2867216)
19:44:18 - 进程 09 已启动 (PID: 2867236)
19:44:19 - 进程 10 已启动 (PID: 2867257)
19:44:19 - 进程 11 已启动 (PID: 2867285)
19:44:19 - 进程 12 已启动 (PID: 2867307)
19:44:19 - 进程 13 已启动 (PID: 2867334)
19:44:19 - 进程 14 已启动 (PID: 2867363)
19:44:19 - 进程 15 已启动 (PID: 2867397)
19:44:19 - 进程 16 已启动 (PID: 2867426)
19:44:19 - 进程 17 已启动 (PID: 2867466)
19:44:19 - 进程 18 已启动 (PID: 2867510)
19:44:19 - 进程 19 已启动 (PID: 2867554)
-----------------------------------------------
所有 20 个进程已启动，等待完成...
-----------------------------------------------
进程 00 (PID: 2867138) 成功发送 5000000 条记录
进程 01 (PID: 2867142) 成功发送 5000000 条记录
进程 02 (PID: 2867146) 成功发送 5000000 条记录
进程 03 (PID: 2867153) 成功发送 5000000 条记录
进程 04 (PID: 2867160) 成功发送 5000000 条记录
进程 05 (PID: 2867172) 成功发送 5000000 条记录
进程 06 (PID: 2867185) 成功发送 5000000 条记录
进程 07 (PID: 2867200) 成功发送 5000000 条记录
进程 08 (PID: 2867216) 成功发送 5000000 条记录
进程 09 (PID: 2867236) 成功发送 5000000 条记录
进程 10 (PID: 2867257) 成功发送 5000000 条记录
进程 11 (PID: 2867285) 成功发送 5000000 条记录
进程 12 (PID: 2867307) 成功发送 5000000 条记录
进程 13 (PID: 2867334) 成功发送 5000000 条记录
进程 14 (PID: 2867363) 成功发送 5000000 条记录
进程 15 (PID: 2867397) 成功发送 5000000 条记录
进程 16 (PID: 2867426) 成功发送 5000000 条记录
进程 17 (PID: 2867466) 成功发送 5000000 条记录
进程 18 (PID: 2867510) 成功发送 5000000 条记录
进程 19 (PID: 2867554) 成功发送 5000000 条记录
===============================================
并发压测执行完毕！
结束时间: 2025-12-02 19:44:55
-----------------------------------------------
最终汇总统计:
目标总记录数: 100000000 条
成功进程数: 20 / 20
实际发送总记录数: 100000000 条
总耗时: 36.08 秒
整体吞吐率: 2771618.62 条/秒
整体吞吐率: 396.48 MB/秒 (按每条150字节估算)
===============================================
各进程性能摘要:
进程 00: 5000000 records sent, 250100.0 records/sec (22.92 MB/sec), 825.32 ms avg latency, 6907.00 ms max latency, 532 ms 50th, 2723 ms 95th, 4786 ms 99th, 6858 ms 99.9th.
进程 01: 5000000 records sent, 249389.0 records/sec (22.85 MB/sec), 664.56 ms avg latency, 4279.00 ms max latency, 375 ms 50th, 2432 ms 95th, 3517 ms 99th, 4237 ms 99.9th.
进程 02: 5000000 records sent, 242612.5 records/sec (22.00 MB/sec), 782.83 ms avg latency, 7077.00 ms max latency, 399 ms 50th, 3103 ms 95th, 5862 ms 99th, 7039 ms 99.9th.
进程 03: 5000000 records sent, 256858.1 records/sec (23.54 MB/sec), 663.81 ms avg latency, 6220.00 ms max latency, 350 ms 50th, 2522 ms 95th, 4426 ms 99th, 6201 ms 99.9th.
进程 04: 5000000 records sent, 267852.4 records/sec (24.55 MB/sec), 522.38 ms avg latency, 5933.00 ms max latency, 234 ms 50th, 2318 ms 95th, 3242 ms 99th, 5917 ms 99.9th.
进程 05: 5000000 records sent, 249053.6 records/sec (22.82 MB/sec), 849.35 ms avg latency, 11796.00 ms max latency, 506 ms 50th, 2579 ms 95th, 7171 ms 99th, 11787 ms 99.9th.
进程 06: 5000000 records sent, 250237.7 records/sec (22.93 MB/sec), 631.23 ms avg latency, 5044.00 ms max latency, 262 ms 50th, 2600 ms 95th, 4531 ms 99th, 5012 ms 99.9th.
进程 07: 5000000 records sent, 248422.5 records/sec (22.77 MB/sec), 704.88 ms avg latency, 6720.00 ms max latency, 328 ms 50th, 2664 ms 95th, 6021 ms 99th, 6696 ms 99.9th.
进程 08: 5000000 records sent, 244594.5 records/sec (22.41 MB/sec), 722.73 ms avg latency, 5203.00 ms max latency, 482 ms 50th, 2286 ms 95th, 4075 ms 99th, 5180 ms 99.9th.
进程 09: 5000000 records sent, 245110.1 records/sec (22.46 MB/sec), 581.83 ms avg latency, 5198.00 ms max latency, 282 ms 50th, 2257 ms 95th, 3887 ms 99th, 5161 ms 99.9th.
进程 10: 5000000 records sent, 254090.9 records/sec (23.28 MB/sec), 544.32 ms avg latency, 5780.00 ms max latency, 270 ms 50th, 1823 ms 95th, 5435 ms 99th, 5768 ms 99.9th.
进程 11: 5000000 records sent, 248040.5 records/sec (22.73 MB/sec), 673.05 ms avg latency, 9844.00 ms max latency, 359 ms 50th, 2447 ms 95th, 4255 ms 99th, 9797 ms 99.9th.
进程 12: 5000000 records sent, 254181.3 records/sec (23.29 MB/sec), 685.61 ms avg latency, 5391.00 ms max latency, 402 ms 50th, 2294 ms 95th, 4464 ms 99th, 5381 ms 99.9th.
进程 13: 5000000 records sent, 275938.2 records/sec (25.29 MB/sec), 731.17 ms avg latency, 6333.00 ms max latency, 419 ms 50th, 2688 ms 95th, 3812 ms 99th, 6319 ms 99.9th.
进程 14: 5000000 records sent, 252525.3 records/sec (23.14 MB/sec), 505.96 ms avg latency, 5870.00 ms max latency, 170 ms 50th, 2695 ms 95th, 4678 ms 99th, 5836 ms 99.9th.
进程 15: 5000000 records sent, 249950.0 records/sec (22.61 MB/sec), 679.90 ms avg latency, 5348.00 ms max latency, 387 ms 50th, 2283 ms 95th, 4360 ms 99th, 5318 ms 99.9th.
进程 16: 5000000 records sent, 256081.9 records/sec (23.47 MB/sec), 617.84 ms avg latency, 6539.00 ms max latency, 333 ms 50th, 2093 ms 95th, 5172 ms 99th, 6527 ms 99.9th.
进程 17: 5000000 records sent, 264480.3 records/sec (24.24 MB/sec), 523.00 ms avg latency, 7004.00 ms max latency, 198 ms 50th, 2080 ms 95th, 4000 ms 99th, 6985 ms 99.9th.
进程 18: 5000000 records sent, 252499.7 records/sec (23.14 MB/sec), 602.12 ms avg latency, 4882.00 ms max latency, 262 ms 50th, 2167 ms 95th, 4557 ms 99th, 4832 ms 99.9th.
进程 19: 5000000 records sent, 252934.0 records/sec (23.18 MB/sec), 626.19 ms avg latency, 5938.00 ms max latency, 291 ms 50th, 2753 ms 95th, 5658 ms 99th, 5900 ms 99.9th.
```


#### 9.2.2 taosgen

taosgen 20 线程并发性能：547.29 万 rps，相比官方脚本写入性能提升：+97.46%
```yaml
2025-12-02 19:50:07.169706 2876342 INFO  =================================================== Insert Summary Statistics ========================================================
2025-12-02 19:50:07.169709 2876342 INFO  Insert Threads: 20
2025-12-02 19:50:07.169711 2876342 INFO  Total Rows: 100000000
2025-12-02 19:50:07.169717 2876342 INFO  Total Duration: 18.27 seconds
2025-12-02 19:50:07.169718 2876342 INFO  Average Rate: 5472919.51 rows/second
2025-12-02 19:50:07.169719 2876342 INFO  =======================================================================================================================================
2025-12-02 19:50:07.171651 2876342 INFO  
2025-12-02 19:50:07.171659 2876342 INFO  =================================================== Insert Latency & Efficiency Metrics ==============================================
2025-12-02 19:50:07.171661 2876342 INFO  Total Operations: 12000
2025-12-02 19:50:07.171663 2876342 INFO  Total Duration: 18.27 seconds
2025-12-02 19:50:07.171665 2876342 INFO  Pure Insert Latency: 8.36 seconds
2025-12-02 19:50:07.171666 2876342 INFO  Effective Time Ratio: 45.77%
2025-12-02 19:50:07.171667 2876342 INFO  Framework Overhead: 54.23%
2025-12-02 19:50:07.171669 2876342 INFO  Idle Time After Finish: 0.49 seconds
2025-12-02 19:50:07.171703 2876342 INFO  Write Latency Distribution: min: 2.3782ms, avg: 13.9396ms, p90: 28.2630ms, p95: 30.3573ms, p99: 43.2702ms, max: 136.9496ms
2025-12-02 19:50:07.171718 2876342 INFO  ======================================================================================================================================
```


### 9.3 taosgen 20 进程并发+500 条记录打包成一个消息

#### 9.3.1 json 格式

1亿条meters表时序数据、20并发、500条记录打包、Kafka Leader 确认的性能：1280.38 万 rps
```yaml
2025-12-05 11:25:55.845775 3011187 INFO  =================================================== Insert Summary Statistics ========================================================
2025-12-05 11:25:55.845779 3011187 INFO  Insert Threads: 20
2025-12-05 11:25:55.845780 3011187 INFO  Total Rows: 100000000
2025-12-05 11:25:55.845786 3011187 INFO  Total Duration: 7.81 seconds
2025-12-05 11:25:55.845787 3011187 INFO  Average Rate: 12803839.40 rows/second
2025-12-05 11:25:55.845788 3011187 INFO  =======================================================================================================================================
2025-12-05 11:25:55.847674 3011187 INFO  
2025-12-05 11:25:55.847680 3011187 INFO  =================================================== Insert Latency & Efficiency Metrics ==============================================
2025-12-05 11:25:55.847681 3011187 INFO  Total Operations: 12000
2025-12-05 11:25:55.847683 3011187 INFO  Total Duration: 7.81 seconds
2025-12-05 11:25:55.847686 3011187 INFO  Pure Insert Latency: 0.50 seconds
2025-12-05 11:25:55.847687 3011187 INFO  Effective Time Ratio: 6.40%
2025-12-05 11:25:55.847689 3011187 INFO  Framework Overhead: 93.60%
2025-12-05 11:25:55.847690 3011187 INFO  Idle Time After Finish: 1.19 seconds
2025-12-05 11:25:55.847727 3011187 INFO  Write Latency Distribution: min: 0.1328ms, avg: 0.8336ms, p90: 1.4515ms, p95: 1.8373ms, p99: 5.3297ms, max: 33.6260ms
2025-12-05 11:25:55.847730 3011187 INFO  ======================================================================================================================================
```


#### 9.3.2 influx 格式

1亿条meters表时序数据、20并发、500条记录打包、Kafka Leader 确认的性能：1673.93 万 rps
```yaml
2025-12-05 11:17:01.502143 3011012 INFO  =================================================== Insert Summary Statistics ========================================================
2025-12-05 11:17:01.502147 3011012 INFO  Insert Threads: 20
2025-12-05 11:17:01.502150 3011012 INFO  Total Rows: 100000000
2025-12-05 11:17:01.502160 3011012 INFO  Total Duration: 5.97 seconds
2025-12-05 11:17:01.502163 3011012 INFO  Average Rate: 16739330.82 rows/second
2025-12-05 11:17:01.502165 3011012 INFO  =======================================================================================================================================
2025-12-05 11:17:01.504100 3011012 INFO  
2025-12-05 11:17:01.504110 3011012 INFO  =================================================== Insert Latency & Efficiency Metrics ==============================================
2025-12-05 11:17:01.504113 3011012 INFO  Total Operations: 12000
2025-12-05 11:17:01.504117 3011012 INFO  Total Duration: 5.97 seconds
2025-12-05 11:17:01.504121 3011012 INFO  Pure Insert Latency: 0.63 seconds
2025-12-05 11:17:01.504122 3011012 INFO  Effective Time Ratio: 10.53%
2025-12-05 11:17:01.504124 3011012 INFO  Framework Overhead: 89.47%
2025-12-05 11:17:01.504125 3011012 INFO  Idle Time After Finish: 0.59 seconds
2025-12-05 11:17:01.504158 3011012 INFO  Write Latency Distribution: min: 0.0784ms, avg: 1.0489ms, p90: 1.6015ms, p95: 2.0248ms, p99: 8.4782ms, max: 41.8706ms
2025-12-05 11:17:01.504160 3011012 INFO  ======================================================================================================================================
```

## 10. 安全测试

无

## 11. 兼容性测试

无

## 12. 已知问题和限制（可选）

1. 不支持 Avro、Protobuf 等二进制序列化：
  - 目前 `value_serializer` 仅支持 `"json"` 和 `"influx"` 格式。
  - 不支持将数据序列化为 Avro、Protocol Buffers (Protobuf) 或 MessagePack 等二进制格式。
1. 不支持复杂的消息 Header 操作：
  - 当前接口不提供自定义 Kafka 消息 Header 的能力。
  - 所有消息均以默认的、空的 Header 发送。
1. 不支持事务性写入 (Transactional Writes)：
  - `taosgen` 使用的是标准的 Kafka Producer API，不支持开启事务模式（即设置 `enable.idempotence=true` 并配合 `transactional.id`）。
  - 因此，无法保证跨多条消息的原子性提交（All-or-Nothing），也无法完全避免消息的重复。
1. 不支持精确一次 (Exactly-Once) 语义：
  - 由于不支持事务，`taosgen` 本身不能保证端到端的精确一次投递。在极端情况下（如生产者崩溃并重启），可能会产生重复消息。
1. Key 的 "int" 系列序列化方式限制：
  - 当 `key_serializer="int"` 时，`key_pattern` 只能包含一个整数类型的占位符（例如 `{device_id}`）。
  - 不支持任何形式的组合或表达式，包括：
    - 多个占位符：`{table}_{id}` ❌
    - 字符串拼接：`prefix_{id}` ❌
    - 数学运算：`{id + 1}`, `{current * 100}` ❌
  - 如果尝试使用，将导致运行时错误。
1. 不支持动态 schema 变更：
  - 在 `taosgen` 运行期间，`schema` 配置是静态的。程序启动后，无法动态修改正在生成的数据结构。
1. 不支持压缩算法的自动协商：
  - `compression` 参数指定了生产者使用的压缩算法，但不保证Broker 会接受或使用该算法。最终的压缩方式由 Broker 的 `compression.type` 配置和主题级别设置决定。
  - 生产者只是“建议”使用某种压缩，实际生效情况需在 Broker 端确认。
1. 不直接管理 Topic 生命周期：
  - `taosgen` 不会自动创建、删除或修改 Kafka Topic。
  - 用户必须确保目标 `topic` 已经存在，并且具有足够的分区数来满足并发写入的需求。如果 Topic 不存在，生产者将报错。
