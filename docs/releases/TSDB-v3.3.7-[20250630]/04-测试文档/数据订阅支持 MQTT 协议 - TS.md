# 数据订阅支持 MQTT 协议 - TS

## 1. 测试目标

数据订阅支持 MQTT 的功能测试，性能测试及稳定性测试，包括 bnode 创建、删除、查看操作，订阅 QoS，共享订阅，退订，登录验证等。

## 2. 相关资料

### 2.1 相关文档

1. [https://jira.taosdata.com:18080/browse/TS-5842](https://jira.taosdata.com:18080/browse/TS-6100)
2. [数据订阅支持 MQTT 协议 RS](https://taosdata.feishu.cn/wiki/Ij7cwQFasiRNalkPEopc0V4jnue)
3. [数据订阅支持 MQTT - FS](https://taosdata.feishu.cn/wiki/AvNswNnyNihPi5koZCccRWYwnpf)

### 2.2 用新测试框架测试

1. 用户手册：TDinternal/community/test/README.md
2. 样例文件：TDinternal/community/test/cases/12-DataSubscription/04-MQTT/test_mqtt_smoking.py
3. 运行方法：
  ```bash
  cd /root/TDinternal/community/test
  ../tests/script/sh/stop_dnodes.sh
  rm -rf ~/TDinternal/sim/*
  pytest cases/12-DataSubscription/04-MQTT/test_mqtt_smoking.py
  ```

## 3. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025-5-16 | 0.1 | 金明磊 |  |

## 4. 测试结论

通过

## 5. 测试环境

1. 功能测试: 开发机、Linux 系统
2. 稳定性测试：物理机: 192.168.1.58

## 6. 功能测试

### 6.1 订阅节点

#### 6.1.1 测试要点

1. 基础功能
   - 创建、删除`bnode`基本操作
   - 语法校验、边界
   - `ins_bnodes`系统表
   - `show bnode`命令
   - 特殊情况
      - 无效、离线的`dnode`
      - 重复创建与删除`bnode`
      - 删除最后一个`bnode`
2. 订阅任务
   - 删除有订阅任务的`bnode`
   - 同一个节点上反复创建`bnode`，检查订阅任务
   - `bnode`所在`dnode`启停对订阅任务的影响

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | test_mqtt_bnodes.py | 1. 创建 bnode 1. 删除 bnode 1. 查询 information_schema.ins_bnodes 1. 执行 show bnodes 命令 1. 在同一个 dnode 上重复创建和删除 1. 重启并测试持久化状态 1. 删除非法的 bnode、最后一个 bnode 1. 删除存在 bnode 的 dnode | 通过 |

### 6.2 订阅主题

#### 6.2.1 测试要点

1. 基础功能
   - 覆盖查询，超级表，库三种类型的主题
   - 语法校验、边界
   - 异常主题名，长度边界
   - 特殊情况
      - 离线，重启的`dnode`
      - 删除最后一个`bnode`

#### 6.2.2 用例列表

各类型主题的订阅已包含于 test_mqtt_soak.py

| # | 测试用例 | 测试描 | 测试结果 |
| --- | --- | --- | --- |
| 1 | topic_query | 查询订阅 1. 创建 topic 1. 删除 topic 1. 查询 information_schema.ins_topics 1. 执行 show 命令 1. 重启并测试数据持久化状态 1. 暂停、恢复订阅 1. dnode/bnode 离线时的创建、删除 1. dnode/bnode 启停时的订阅 | 通过 |
| 3 | topic_stb | 根据官网文档 6.1 节内容，超级表和库订阅容易出错，需要多覆盖各种异常情况 1. 语法合法性检查、语法错误 1. 命名、边界等 |  |
| 2 | topic_db | 库订阅各种情况检查 1. 语法合法性检查、语法错误 1. 命名、边界等 |  |

### 6.3 订阅 QoS

#### 6.3.1 测试要点

1. 基础功能
   - QoS 0，1 基本订阅
   - 语法校验、边界
   - 特殊情况
      - 无效、离线的`dnode`
      - 删除最后一个`bnode`

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | test_mqtt_qos.py | 1. QoS 0 1. QoS 1 1. QoS 2 1. 负值及其他非法值 （paho 库检测了，需要自定义客户端测试） | 通过 |

### 6.4 共享订阅

#### 6.4.1 测试要点

1. 基础功能
   - 共享订阅基本操作
   - 客户端与订阅组的各种组合场景
   - 特殊情况
      - 无效、离线的`dnode`
      - 删除订阅进行中`bnode`
      - 删除最后一个`bnode`

#### 6.4.2 用例列表

已包含于长稳用例： test_mqtt_soak.py

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | mqtt_shared | 1. 单客户端单 group 1. 单客户端多 group 1. 多客户端多 group 1. 多客户端单 group 1. 在同一个 dnode 上重复创建和删除 1. 重启并测试持久化状态 1. 删除非法的 bnode、最后一个 bnode 1. 删除订阅进行中存在 bnode 的 dnode | 通过 |

## 7. 特殊场景测试

用例文件：test_mqtt_special.py

### 7.1 数据库时间精度

#### 7.1.1 测试要点

#### 7.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | special_precision | 覆盖各种精度下的数据订阅: 'ms' | 'us' | 'ns' 1. ms 1. us 1. ns 1. 多 DB 同时订阅 | 通过 |

### 7.2 数据乱序、更新、删除

#### 7.2.1 测试要点

#### 7.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | special_data | 1. 乱序 1. 更新 1. 删除 1. 上述情况混合测试 | 通过 |

### 7.3 数据库表等元数据的更新、删除

#### 7.3.1 测试要点

#### 7.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | special_meta | 1. 元数据更新 1. 元数据删除 1. 上述情况混合测试 | 通过 |

## 8. 用户场景测试

按照用户场景构建数据模型，写入模式和订阅模式，用较小规模的数据集验证功能可用性。

### 8.1 山东能源 

长稳用例基于此场景，不单独提供用例。
~~这个原始场景中通过订阅把数据消费到 Flink 中使用；这里编写一个 MQTT 测试场景：test_mqtt_sdenergy.py~~
~~64 vgroups, 10 topics，29 consumers，select * from db.stb;~~

## 9. 长期稳定性测试

1. 扩大用户场景的数据集，进行至少 72 小时的长稳测试
2. 6 dnodes, 6 bnodes, 4 query topics, 4 stable topics, 4 db topics
3. 5 sub clients per bnode, 30 consumers totally
4. 编写一个典型的测试场景: test_mqtt_soak.py，涵盖连接，登录，订阅，退订，qos 0, qos 1，断链，共享订阅等的混合情况

## 10. 性能测试

### 10.1 测试步骤

使用 TDengine/source/libs/tmqtt/tools/perf.py 及 topic-producer 工具测试如下：
1. 使用 taosBenchmark -n 100 -y 创建一万子表，每表 100 行数据
2. 创建主题：create topic as select * from test.meters;
3. 创建订阅节点：create bnode on dnode 1;
4. 分别使用 paho，native tmq earliest 订阅。

### 10.2 测试结果

100 万行数据结果如下：
Native tmq: 1.894s
Native tmq with JSON: 9.894s
Paho: 10.547s

### 10.3 测试结论

1. 主要新能损耗在 json 的组装及解析，且文本协议对网络传输也有较大影响；
2. 故下一步性能方面工作应采用二进制协议，降低额外性能代价；
3. 系统负载方面 taosmqtt 子进程 cpu 20%～50%，资源消耗符合预期。
以下内容已过期，可在二进制协议后继续：
1. ~~按照 ~~[数据订阅支持 MQTT - FS](https://taosdata.feishu.cn/wiki/AvNswNnyNihPi5koZCccRWYwnpf)~~文档中第 5 节的场景：test_mqtt_perf.py，开展一轮性能测试~~
2. ~~对于每种不同的订阅模式，分别查看火焰图，查看是否存在明显的性能瓶颈或可优化的性能关键点~~
3. ~~分别比对 SSD，HDD 情况下订阅性能指标~~
4. ~~最终需要使用 taosBenchmark 订阅功能进行测试，测试重点为以下两个方面：~~
   - ~~常见性能指标：吞吐量，延迟，扩展性，系统负载~~
   - ~~与原有订阅对比，性能及负载变化~~

## 11. 安全测试

在`6.1`和`6.2`的场景中，相关场景已得到覆盖，因此本节无需再进行额外的安全测试。

## 12. 兼容性测试

1. 新功能，不存在与之前功能的兼容问题
2. Mnode 新加 Bnode, 不支持回退到 v3.3.5.0 以前的版本

## 13. 参考文档

[如何评判数据订阅的性能？——抓住关键，简化问题](https://taosdata.feishu.cn/wiki/WgdrwlVFQidX59kQdECcUJt5nyd)
[taosBenchmark 重构 FS](https://taosdata.feishu.cn/wiki/KNDKwZJTIiJk7fkCeCwc0rmMnic)
