# taosX replica 功能测试报告

## 1. Objectives

- 测试 taosX 在 TDengine 双活场景下的数据复制功能（新增 replica 参数）

## 2. Revision History

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 20240221 | 0.1 | @王旭 | Initial draft |
|  |  |  |  |

## 3. Scope

这里用于描述本需求的覆盖范围：
- taosX 同步数据的完整性
- taosX 同步数据的逻辑正确性，即机器 A 同步到机器 B 的数据不会被反向同步回来，形成死循环

## 4. 测试结论

## 5. Limitations and Known Issues

- failover 功能是通过 client driver 实现的，尚未完成，不在本次测试的范围内

## 6. Environment

- 机器A: 192.168.2.14
- 机器B: 192.168.1.42

## 7. Test Data (Optional)

选用 taosBenchmark 生成的 1 亿条智能电表数据作为测试数据。

## 8. Test Cases

### 8.1 Functional

在提测时，开发应保证 basic 类型的用例全部通过。
| Type | Purpose | Description | Expected Result | Result | Jira | Memo |
| --- | --- | --- | --- | --- | --- | --- |
| basic | 验证数据同步的完整性 | 使用 replica 参数启动 taosx，将 A 上的 test DB 同步至 B 上的 test DB | test DB on A 与 test DB on B 完全一致 | Pass |  |  |
| basic | 验证数据同步的逻辑正确性 | 使用 replica 参数启动 taosx，将 A 上的 test DB 同步至 B 上的 test DB
使用 replica 参数，将 B 上的 test DB 同步至 B 上的 test2 DB | test2 DB on A 应未被同步任何数据 | Fail | [TD-28711](https://jira.taosdata.com:18080/browse/TD-28711) |  |
| basic | 验证能够同步schema变更 | 使用 replica 参数启动 taosx，修改 A 上的 test DB 的 schema, 增加 1 列 | schema 变更可同步至 B | Fail | [TD-28718](https://jira.taosdata.com:18080/browse/TD-28718) |  |
| command | 验证 show subscription 命令的修改 | 执行 show subscription, 观察 offset 字段的输出 | 格式修改为：消费位置/WAL最新数据位置，例如：wal:11195/11195 | Pass |  |  |
| script | 验证运维脚本的功能 | deploy.sh start |  |  |  | not ready |
|  |  | deploy.sh stop |  |  |  | not ready |
|  |  | deploy.sh restart |  |  |  | not ready |
|  |  | checkdiff.sh |  |  |  | not ready |
| installer | 验证企业版的安装包中是否包含了以上两个运维脚本且有可执行权限 | 使用installer安装后，执行以上运行脚本 | 运维脚本可以正确执行 |  |  | not ready |

### 8.2 Usability

n/a

### 8.3 Reliability

同时启动机器 A 和 B 上的两套系统，创建一个 OPC UA 的任务，使机器 A 的 TDengine 有实时数据写入，观察系统是否可以稳定运行 24 小时以上，并记录两个机器上 taosX 的资源占用情况。

### 8.4 Performance

### 8.5 Security

n/a

### 8.6 Compatibility

配置为双活的两个机器，必须同时使用支持 replica 参数的 taosX.

### 8.7 Localization

n/a

## 9. Questions (Optional)

这里用于记录需要讨论的问题：
- aaa
- bbb

## 10. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: taosx-replica

## 11. Schedule (Optional)

这里用于计划此 feature 测试的开始和结束时间。

## 12. Notes (Optional)

在 A/B 两个机器上，执行以下命令，启动 taosX:
```bash {wrap}
taosx run -f "tmq:///test?replica&timeout=never" -t "taos://192.168.1.42:6030/test?assert"

taosx run -f "tmq:///test?replica&timeout=never" -t "taos://192.168.2.14:6030/test2?assert"
```

## 13. Test Reuslt


## 14. Reference (Optional)

- [TDengine 双活 ](https://taosdata.feishu.cn/wiki/E9NmwBfIbiTA5bkq8kScFX0yn8c)
