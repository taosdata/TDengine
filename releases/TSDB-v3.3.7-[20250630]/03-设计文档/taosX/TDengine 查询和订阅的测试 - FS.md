# TDengine 查询和订阅的测试 - FS

## 1. 背景

根据 [taosX Data Migration User Manual](https://taosdata.feishu.cn/wiki/wikcnlXBGv4UKBOGld94f6leHre) 进行数据迁移和同步功能和性能测试。建立 Test Spec 和自动化测试流程。
1. 收集和整理使用场景，增加各种网络条件测试。
2. 针对目前支持的所有数据类型，增加不同数据类型及其组合的相关测试用例。
3. 增加长期稳定性测试。
4. 增加性能测试用例，进行性能监控。
相关的 JIRA：

TD-34842


TD-32960

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/5/15 | 0.1 | @杨志宇 | 初稿 |
|  |  |  |  |

## 3. 定义

无

## 4. 行为说明

### 4.1 单元测试结果生成测试报告

通过优化当前的 pr-ci 的 workflow，将单元测试的结果，添加到 allure report 中。详见：附录 15.1

### 4.2 测试用例支持环境变量

taosX 的单元测试用例中，所有以`_with_taos`结尾的用例，需要依赖 TDengine 实例。这些用例可以配置以下环境变量：

| **环境变量** | **用处** | **默认值** | **示例** |
| --- | --- | --- | --- |
| HOST | 指定要连接的数据库地址 | 127.0.0.1 | `HOST=192.168.2.13 ``*cargo nextest run CASE_NAME*` |
| WS_ENABLE | 是否使用 websocket | false | `WS_ENABLE=true HOST=192.168.2.13 ``*cargo nextest run CASE_NAME*` |

### 4.3 测试用例的注释

所有 taosX 的单元测试用例，建议添加以下注释。后续通过工具，将注释内容加入到测试报告中。

| **标题** | **描述** |
| --- | --- |
| description | 用例的英文描述 |
| description_cn | 用例的中文描述 |
| jira | 关联的 jira，可以为空 |
| example | 如何运行当前测试用例 |

示例：
```rust
/// # description
/// This case test synchronize database with specified time range
/// # description_cn
/// 同步数据库，指定时间区间：[strat, ∞), (∞, end), [start, end)
/// 1. 创建数据库 DB_SRC 和 DB_DST
/// 2. 在 DB_SRC 中创建 1 个超级表，写入 30 天的数据，每天 N 条；
/// 3. 创建数据同步任务，分别指定时间区间为：[strat, ∞), (∞, end), [start, end)
/// 4. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致为用例通过，否则用例失败。
/// # jira
/// close https://jira.taosdata.com:18080/browse/TD-34842
/// # example
/// ```shell
/// cargo nextest run test_sync_time_range_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_sync_time_range_with_taos() -> anyhow::Result<()> {
    ...
}
```

### 4.4 TDengine Query 的用例

| **TestCase** | **Description** | **JIRA** |
| --- | --- | --- |
| test_sync_with_taos | 同步数据库，包括 stable 和普通表。 1. 创建数据库：`x-sync-2`和`x-sync`，在`x-sync`中写入数据； 1. 运行 legacy_to_taos 任务，同步`x-sync`到`x-sync-2`； 1. 检查`x-sync`和`x-sync-2`，同步成功，用例通过，否则用例失败。 | 无 |
| test_sync_large_table_with_taos | 同步 schema，超级表包含多列和多种数据类型 1. 创建数据库 DB1 和 DB2； 1. 在 DB1 中创建超级表，超级表包含 3600，12 种不同的数据类型，并创建 1000 张子表； 1. 运行 legacy_to_taos 任务，schema=only； 1. 检查 DB2 的 schema，schema 同步成功，用例通过，否则失败。 | TS-4323 |
| test_sync_large_normal_table_with_taos | 同步 schema，普通表包含多列和多种数据类型 1. 创建数据库 DB1 和 DB2； 1. 在 DB1 中创建普通表，超级表包含 3600，12 种不同的数据类型，并创建 1000 张子表； 1. 运行 legacy_to_taos 任务，schema=only； 1. 检查 DB2 的 schema，schema 同步成功，用例通过，否则失败。 | TS-4323 |
| test_ts5124_with_taos | 数据同步时，带特殊字符的表名 1. 创建数据库 DB1，创建表名包含特殊字符的表：`>♑1`和`nTb1`，并各自写入一条数据； 1. 创建数据库 DB2； 1. 运行 legacy_to_taos 任务，actions=["rename-table:map:nTb1,nTb2"]; 1. 检查 SINK，`>♑1`和`nTb1` 同步成功，用例通过，否则失败。 | TS-5124 |
| test_ts6449_with_taos | 表结构不一致时，通过 actions 可以写入成功 1. 创建数据库 SOURCE，创建普通表 nTb1，val 为 double，写入 1 条数据； 1. 创建数据库 SINK，创建普通表 nTb2，valu 为 float； 1. 运行 legacy_to_taos 任务，actions=["rename-table:map:nTb1,nTb2"]; 1. 检查 SINK，在 schema 不一致的情况写，写入成功，用例通过，否则失败。 | TS-6449 |
| test_ts6499_with_taos | 同步 stream 创建的表 schema 1. 创建数据库：DB_SRC 和 DB_DST，在 DB_SRC 建超级表，建 stream； 1. 写入数据到 DB_SRC； 1. 创建数据同步任务，schema=only； 1. 写入数据到 DB_SRC，stream 会产生新的表和数据； 1. 再次执行数据同步任务，schema=only； 1. 检查 DB_SRC 和 DB_DST 的表，schema 一致，用例通过，否则失败。 | TS-6499 |
| test_ts6402_with_taos | Realtime 模式支持从断点开始同步 1. 建2个数据库：DB_SRC 和 DB_DST，在 DB_SRC 内建表； 1. 创建同步任务，mode=realtime，sparse=true，运行 60 秒后，停止； 1. 写入数据到 DB_SRC； 1. 重启同步任务，运行 60 秒后，停止 1. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致为用例通过，否则用例失败。 | TS-6402 |
| test_td_33256_with_taos | 密码中带特殊字符 1. 创建数据库 SOURCE，向 SOURCE 中写入 1 万行； 1. 创建 USER，密码带特殊字符，grant all on SOURCE to USER； 1. 创建数据库 SINK； 1. 创建数据同步任务，mode=history 1. 任务成功后，检查 SOURCE 和 SINK 的数据是否一致，一致为用例通过，否则用例失败。 | TD-33256 |
| test_sync_several_stables_with_taos | 同步 1～N 个超级表 1. 创建数据库 DB_SRC 和 DB_DST 1. 在 DB_SRC 中创建 1～N 个超级表，每个超级表中写入 M 行数据； 1. 创建数据同步任务 1. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致为用例通过，否则用例失败。 |
| test_sync_specified_tables_with_taos | 同步 N 个子表和普通表 1. 创建数据库 DB_SRC 和 DB_DST； 1. 在 DB_SRC 中创建 1 个超级表，向 N 个子表中写入数据； 1. 创建数据同步任务，指定表和普通表； 1. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致为用例通过，否则用例失败。 |
| test_sync_time_range_with_taos | 同步数据库，指定时间区间：[strat, ∞), (∞, end), [start, end) 1. 创建数据库 DB_SRC 和 DB_DST; 1. 在 DB_SRC 中创建 1 个超级表，写入 30 天的数据，每天 N 条； 1. 创建数据同步任务，分别指定时间区间为：[strat, ∞), (∞, end), [start, end)； 1. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致为用例通过，否则用例失败。 |
| test_sync_realtime_with_taos | 同步，mode=realtime，restro=5m, interval=1s,excursion=500ms 1. 创建数据库 DB_SRC 和 DB_DST； 1. 在 DB_SRC 中创建 1 个超级表，向 N 个子表中写入数据； 1. 创建数据同步任务，mode=realtime，restro=5m, interval=1s,excursion=500ms 1. 向 DB_SRC 中写入数据；运行 60 秒后，停止； 1. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致为用例通过，否则用例失败。 |
| test_sync_all_with_taos | 同步，mode=all 1. 创建数据库 DB_SRC 和 DB_DST； 1. 在 DB_SRC 中创建 1 个超级表，向 N 个子表中写入数据； 1. 创建数据同步任务，mode=all； 1. 向 DB_SRC 中写入数据；运行 60 秒后，停止； 1. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致为用例通过，否则用例失败。 |
| test_sync_select_from_stable_with_taos | 同步指定子表的数据，且从超级表取数据 1. *创建数据库 DB_SRC 和 DB_DST；* 1. *在 DB_SRC 中创建 1 个超级表，向 N 个子表中写入数据；* 1. *创建数据同步任务，指定 3 个子表，从超级表中取数据；* 1. *检查 DB_SRC 和 DB_DST 中的数据是否一致，一致为用例通过，否则用例失败。* |

### 4.5 TDengine Data Subscription 的用例

| **TestCase** | **Description** | **JIRA** |
| --- | --- | --- |
| test_td34829_with_taos | tmq 同步数据库中写入的数据以及 stream 产生的数据 1. 创建数据库 DB_SRC 和 DB_DST，在 DB_SRC 中创建超级表和 stream； 1. 创建数据复制任务，timeout=never 1. 向 DB_SRC 中写入数据，同时 stream 会产生新表和新数据； 1. 运行 20 秒后，停止数据复制任务； 1. 检查 DB_SRC 和 DB_DST 中的数据，表和 stream 的数据都完成了同步，则用例通过，否则失败。 | TD-34829 |
| test_td33080_with_taos | 目标端表不存在可自动建表 1. 创建数据库 DB_SRC 和 DB_DST，在 DB_SRC 中创建超级表 meters； 1. 创建数据复制任务，timeout=10s； 1. 向 DB_SRC 中写入数据，写 100 个表，每个表写 100 行数据； 1. 写入到一半时，删除 DB_DST 中的 t1, t2, t3 三个表； 1. 等待数据复制任务结束，检查 DB_SRC 和 DB_DST 中的表，数量一致，则用例通过，否则失败。 | TD-33080 |
| test_timestamp_out_of_range_with_taos | 目标数据库的 keep 值小于源数据库的 keep，写入 timestamp out of range 的数据 1. 创建数据库 DB_SRC 和 DB_DST，DB_SRC 的 keep 为 10d，DB_DST 的 keep 为 7d； 1. 向 DB_SRC 中写入 10 行数据，每天一条； 1. 运行 tmq_to_td 任务； 1. 检查 DB_SRC 和 DB_DST 中的数据，DB_SRC 最早为 10d 前，DB_DST 最早为 7d 前，正确则用例通过，否则失败。 | 无 |
| test_sync_database_with_taos | 同步数据库 1. 创建数据库 DB_SRC 和 DB_DST；创建 topic，订阅 DB_SRC 1. 在 DB_SRC 中创建超级表 stb1 ~ stb10，并插入数据 1. 创建同步任务，将 DB_SRC 同步到 DB_DST 1. 检查 DB_DST 中的超级表 stb1 ~ stb10 是否与 DB_SRC 中的数据，一致则用例通过，否则失败 |
| test_sync_stable_with_taos | 同步超级表 1. 创建数据库 DB_SRC 和 DB_DST；在 DB_SRC 中创建超级表 stb1 ~ stb10，并插入数据 1. 创建 topic，随机订阅 DB_SRC 中的一个超级表 1. 创建同步任务，将 TOPIC 同步到 DB_DST 1. 检查 DB_DST 中的超级表是否与 DB_SRC 中的数据一致，且 DB_DST 中只有一个超级表，否则用例失败 |
| test_sync_query_with_taos | 同步一个 SELECT 查询 1. 创建数据库 DB_SRC 和 DB_DST；在 DB_SRC 中创建超级表 stb，并插入数据 1. 创建 topic，订阅 DB_SRC 中的一个 SELECT 查询结果；同时，在 DB_DST 中创建一个普通表，表结构与查询结果一致 1. 创建同步任务，将 TOPIC 同步到 DB_DST 1. 检查 DB_DST 中的表数据是否与 DB_SRC 中的查询结果一致，否则用例失败 |
| test_realtime_sync_with_taos | 实时同步数据库，指定写入模式 1. 创建数据库 DB_SRC 和 DB_DST；创建 topic，订阅 DB_SRC 1. 创建一个线程，启动同步任务，将 DB_SRC 同步到 DB_DST 1. 创建 N 个线程，每个线程写入 T 张表，每张表写入 BATCH_NUM 次，每次写入 BATCH_SIZE 条数据 1. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致则用例通过，否则用例失败 |
| test_add_tag_action_with_taos | 同步数据库，并且为 DB_SRC 的超级表添加一个 TAG 1. 创建数据库 DB_SRC 和 DB_DST；在 DB_SRC 中创建超级表 stb1 和 stb2，并插入数据 1. 创建 topic，订阅 DB_SRC 1. 创建同步任务，将 DB_SRC 同步到 DB_DST，并且指定 Action: `add-tag:location=beijing` 1. 检查 DB_DST 中的超级表 stb1 和 stb2 是否与 DB_SRC 中的数据一致，且每行数据都添加了 location=beijing 的 TAG，否则用例失败 |

## 5. 性能

无

## 6. 兼容性

无

## 7. 运维

无

## 8. 使用场景

无

## 9. 约束和限制

无

## 10. 常见错误和排查

无

## 11. 可观测性

无

## 12. 安装和卸载

无

## 13. 文档

无

## 14. 参考文档

无

## 15. 附录

### 15.1 测试用例自动化

#### 15.1.1 cargo-nextest + allure report

通过 cargo-nextest 生成 junit 形式的测试报告，通过 allure generate 生成 allure report。步骤如下：
1. 在 ./config/nextes.toml 中添加
```shell
[profile.ci.junit]  # this can be some other profile, too
path = "taosx-unit-test.xml"
store-success-output = false
store-failure-output = true
```

默认，成功不输出；失败，输出system.out 和 system.error。
1. 生成 junit.xml
```shell

## 16. 运行单元测试，末尾加 --profile ci

cargo nextest run test_ts6499_with_taos --profile ci
```

1. 结果默认生成在 `./target/nextest/ci/junit.xml`。
2. 生成 allure report
```shell
allure generate ./target/nextest/ci -o ./allure-report --clean
```

报告生成到 ./allure-report
1. 打开报告
```shell
allure generate ./target/nextest/ci -o ./allure-report --clean
```

#### 16.0.1 为 allure report 添加 descrption 属性

在 allure_report/data/test-cases/4867236f9bcb92a6.json 中添加 descriptionHtml 属性。
```json
{
    "uid": "4867236f9bcb92a6",
    "name": "test_ts6499_with_taos",
    "descriptionHtml": "test_ts6499_with_taos",
    "historyId": "taosx::td2td:taosx::td2td#test_ts6499_with_taos",
    "time": {
        "duration": 18400
    },
    ...
}
```
