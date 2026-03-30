# 数据源获取示例数据 Test spec

## 1. 测试目标

- 验证explorer上，kafka Datain 任务和MQTT Datain 任务从数据源获取示例数据的功能

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.06.06 | 0.1 | 聂敏慧 | Initial Draft |

## 3. 测试范围

本需求的覆盖范围：
- Kafka Datain 任务从数据源获取示例数据
- MQTT Datain 任务从数据源获取示例数据
- 本次的测试在 Explorer 上进行

## 4. 测试结论

- 在explorer 上验证， kafka Datain 任务和 MQTT Datain 任务，可以从数据源获取示例数据，验证通过。
超时时间为 3s,
1. 在 timeout 内，拉取示例数据达到配置的示例数据行数上限，直接返回，提示：Kafka 追加了n条示例数据 或 Mqtt 追加了n条示例数据
2. 检索示例数据达到 timeout，且示例数据不为空，则返回数据，提示：Kafka 追加了n条示例数据 或 Mqtt 追加了n条示例数据
3. 检索示例数据达到 timeout，且示例数据为空，则提示：未获取到示例数据
4. 预览数据的上限固定为 100 条

## 5. 开发质量报告

结论：本特性/优化的开发质量是 良

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 8 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

- 无

## 7. 测试环境

- OS: Windows, Linux
- Browser: Chrome

## 8. 测试用例

### 8.1 功能

在提测时，开发应保证 basic 类型的用例全部通过。
| Description | Expected Results | Result | Jira | Automated | Memo |
| --- | --- | --- | --- | --- | --- |
| [basic] kafka数据源，配置bootstrap_servers, topics, offset=earliest/latest,示例数据行数使用默认值 | 能成功拉取示例数据并解析 | Pass |  |  |  |
| [basic] mqtt数据源，配置address, version=3.1/3.1.1/5.0, topics，示例数据行数使用默认值 | 能成功拉取示例数据并解析 | Pass |  |  |  |
| kafka数据源，配置bootstrap_servers, topics, offset=earliest/latest，示例数据行数配置为最小值 1 | 能成功拉取1条示例数据并解析 | Pass |  |  |  |
| kafka数据源，配置bootstrap_servers, topics, offset=earliest/latest，示例数据行数配置为最大值 100 | 能成功拉取100条示例数据并解析 | Pass |  |  |  |
| kafka数据源，配置bootstrap_servers, topics, offset=earliest/latest，topics消息数（如果是latest，则是30s内往topics生产的消息数）<示例数据行数 | 能成功拉取topics中的所有消息 | Pass | [TD-30705](https://jira.taosdata.com:18080/browse/TD-30705) |  |  |
| kafka数据源，配置bootstrap_servers, topics, offset=earliest/latest, 配置2个 topics， 两个topics中消息总数< 示例数据行数 | 能成功拉取topics中的所有消息 | Pass | [TD-30705](https://jira.taosdata.com:18080/browse/TD-30705) |  |  |
| kafka数据源，配置bootstrap_servers, topics, offset=earliest/latest，配置2个 topics， 两个topics中消息总数>= 示例数据行数 | 能成功拉取示例数据并解析 | Pass |  |  |  |
| kafka数据源，配置bootstrap_servers, topics offset=earliest/latest，示例数据行数使用默认值，topics中消息为空 | 返回超时信息 | Pass | [TD-30707](https://jira.taosdata.com:18080/browse/TD-30707) |  |  |
| kafka数据源，配置bootstrap_servers, topics，offset=earliest/latest，示例数据行数使用默认值，配置的topics不存在 | 返回错误信息 | Pass |  |  |  |
| kafka数据源，配置bootstrap_servers, topics，offset=earliest/latest，示例数据行数使用默认值，配置的2个topics， 其中有一个topics不存在 | 提示topic不存在的错误信息 | Pass |  |  |  |
| kafka数据源，配置bootstrap_servers, topics，offset=earliest/latest，示例数据行数使用默认值，配置的bootstrap_servers错误 | 返回错误信息 | Pass |  |  |  |
| kafka数据源，配置bootstrap_servers, topics, 示例数据行数配置为默认值，使用SSL认证 | 能成功拉取示例数据并解析 | Pass | [TD-30706](https://jira.taosdata.com:18080/browse/TD-30706) |  |  |
| mqtt数据源，配置address, topics, 示例数据行数配置为最小值 1 | 能成功拉取1条示例数据并解析 | Pass |  |  |  |
| mqtt数据源，配置address, topics, 示例数据行数配置为最大值 100 | 能成功拉取100条示例数据并解析 | Pass |  |  |  |
| mqtt数据源，配置address, topics, 在30s内数据源发送消息数<示例数据行数 | 能成功拉取topics中的所有消息 | Pass | [TD-30701](https://jira.taosdata.com:18080/browse/TD-30701) |  |  |
| mqtt数据源，配置address, topics, 配置2个 topics， 在30s内数据源发送到两个topics中消息总数< 示例数据行数 | 能成功拉取topics中的所有消息 | Pass | [TD-30701](https://jira.taosdata.com:18080/browse/TD-30701) |  |  |
| mqtt数据源，配置address, topics, 配置2个 topics， 在30s内数据源发送到两个topics中消息总数>= 示例数据行数 | 能成功拉取示例数据并解析 | Pass |  |  |  |
| mqtt数据源，配置address, topics, 示例数据行数使用默认值，在30s内数据源发送消息数为0 | 返回超时信息 | Fail | [TD-30744](https://jira.taosdata.com:18080/browse/TD-30744) |  |  |
| mqtt数据源，配置address, topics, 示例数据行数使用默认值，配置的address错误 | 返回错误信息 | Pass | [TD-30703](https://jira.taosdata.com:18080/browse/TD-30703) |  |  |
| mqtt数据源，配置address, topics, 示例数据行数配置为默认值，使用用户名密码认证 | 能成功拉取示例数据 | Pass |  |  |  |
| mqtt数据源，配置address, topics, 示例数据行数配置为默认值，使用SSL认证 | 能成功拉取示例数据 | Pass | [TD-30700](https://jira.taosdata.com:18080/browse/TD-30700) |  |  |

### 8.2 可用性

- UI 是否美观
- 交互是否合理
- 是否存在错别字

### 8.3 可靠性

无

### 8.4 性能

无

### 8.5 安全性

无

### 8.6 兼容性

无

### 8.7 本地化

测试用例包括但不局限于：
- 点击切换语言按钮后，UI上的所有元素是否按照选择的语言，正确展示

## 9. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: [mqtt/kafka 示例数据], epic：taosx1.7.0
<!-- Unsupported block type: 999 -->

## 10. 测试计划 (Optional)

| 测试输出 | 计划完成时间 |
| --- | --- |
| 测试用例设计（本文档 9 章节） | 2024-06-11 |
| 测试执行 | 2024-06-21 |
| 测试结论及开发质量报告（本文档 4,5 章节） | 2024-06-25 |


## 11. 参考文档 

[数据源获取示例数据](https://taosdata.feishu.cn/wiki/We1WwrSB3iorkAkXiQUco4nsnsb)
