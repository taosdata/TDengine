# 新增 Pulsar 数据源-FS

## 背景

Apache Pulsar 是一款开源的分布式发布-订阅消息系统，专为高性能、强一致性和弹性伸缩而设计。目前是 Kafka 的有力竞争者，也具有一定量的用户群体和生态，taosx 支持 Pulsar 数据同步可以扩展 tsdb 的生态系统。
本需求源自 UmeTea 项目支持，目的是支持 Pulsar 数据源，以及客户涂鸦基于 Pulsar 定制的 Pulsar-Tuya 数据源。

## 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025-12-04 | 0.1 | 张贵川 | 文档撰写 |

## 定义

**Pulsar**：Apache Pulsar 是一款开源的分布式发布-订阅消息系统，专为高性能、强一致性和弹性伸缩而设计
**RecordBatch**：Apach Arrow 的核心数据结构，是一种高效的数据组织和交换格式。
**GCM 加密**：全称 Galois Counter Mode，是一种认证加密模式，它在提供数据机密性的同时，还能确保数据完整性和真实性。

## 行为说明

采集整体流程从 Pulsar/Pulsar-Tuya 数据源采集数据，进行数据组装为 RecordBatch 发送到 taosx 下游进行数据转换和写入。数据转换，写入模块复用原有模块功能，保持不变。
主要涉及 taos-explorer 页面修改，taosx 后端支持。

### 4.1 taos-explorer 页面支持

#### 4.1.1 在新增数据源页面的增加 Pulsar 和 Pulsar-Tuya 两种数据类型

![](./images/img_V0sGb2Jq3o4achxW7L3cUCMBn4c.png)

#### 4.1.2 Pulsar 数据源链接配置支持多种认证

认证机制支持常见的 Basic Auth，JWT，mTLS 以及自定义认证方式。
![](./images/img_V2iLbrBPfonP4yxsKeDc9lvznZc.png)


#### 4.1.3 Pulsar 数据源采集配置

采集配置基本和 kafka 数据源页面一致，除了主题格式是 Pulsar 自己的格式。
![](./images/img_ZjMxbthcyolZ3bxOjadc2N75ngf.png)


#### 4.1.4 Pulsar-Tuya 数据源链接配置支持

Tuya 数据源认证需要按照涂鸦自定的方式进行，需要填写对方提供的 Access Id  和 Access Key
![](./images/img_F4nzbH53Co5p4lxZ19FcCVYsnRb.png)

#### 4.1.5 Pulsar-Tuya  数据源采集配置

Tuya 数据源采集配置的消费者和订阅者是根据 Access Id 变化的，是固定格式，页面不开放填写。
![](./images/img_UKgBbYrZdoS47JxWkMgcPOd4nme.png)

#### 4.1.6 运行指标

数据流指标页面新增 Pulsar 数据源相关运行指标。
![](./images/img_BALxbcZLTofRKQxNr3WcFZp3nWg.png)


### 4.2 命令行支持

命令支持支持 pulsar 和 pulsarTuya 协议，命令执行样例如下：
```http {wrap}
taosx run -f "pulsar://192.168.2.131:6650?batch_size=1000&busy_threshold=100%&char_encoding=UTF_8&consumer_name=c1&initial_position=Earliest&subscription=zgc&timeout=0ms&topics=persistent://public/default/pt-zgc" -t "taos+http://root:taosdata@192.168.2.131:6041/zgc" -p "@./docs/taosx/pulsar-parser.json"
```


### 4.3 agent 模式支持

Agent 模式直接在页面上按正常流程操作即可，后端修改支持，用户行为上无变化。

### 4.4 涂鸦特定数据源说明

涂鸦的 Pulsar 数据源有多种加密方式，主要是 AES-GCM，其解密秘钥在对方提供的 Access Key 的部分字段中，并且需要按照对方提供的 Access Id 动态产生对应的订阅名。
这部分逻辑在后端的处理逻辑中，并不体现在与用户的行为交互上。

## 性能

仅做简单测试，优先满足功能需求，后续有性能瓶颈再优化。目前复用 kafka 采集框架，这样一定程度上保证性能与 kafka 数据流靠齐。

## 兼容性

无

## 运维

无

## 使用场景

无

## 约束和限制

约束：无
限制：无

## 常见错误和排查

无

## 可观测性

参见4.1节页面支持

## 安装和卸载

无

## 文档

需要修改企业版文档
需要修改官网文档

## 参考文档

## 附录

具体实现方案：新增 source-pulsar crate，复用 kafka 数据源采集框架，保证性能上尽量与 kafka 数据源靠齐。
