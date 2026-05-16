# PCS一级部署项目-taosX相关改进需求

## 1. 需求描述

PCS一级部署项目是当前所有项目中使用taosX同步功能比较重的客户之一，给我们完善taosX功能提供一个很好的机会。为了完善taosX在边云协同场景、分公司实时同步数据到中心同步场景的使用功能，基于与@张玮绚 @霍琳贺 @李珲 针对于[PCS项目-taosX现状与问题](https://taosdata.feishu.cn/wiki/RvdSws3sliakV3kQpChcEb5CnLg)的讨论后提出本次改进需求（客户环境背景、以及现场环境中遇到的问题在上述文档有详细介绍）

### 1.1 具体需求点

计划把当前taosX命令行方式改进为在taos-Explorer里配置taosX任务，所以需要进行以下改进：
1. taos-Explorer里的taosX同步任务配置页面增加更多的参数配置项，当前配置参数项较少，很多参数无法配置选择，比如group.id、client.id、experimental.snapshot.enable、~~stables、tables、unit~~等参数。具体增加哪些参数合适，可以继续沟通讨论确定。
2. 当前taosX的监控项不够完善，希望增加完善监控指标
   - taos-Explorer里增加更详细的metrics，比如源端、目标端的last_row值、count值等信息，考虑报错报警有显性提示，具体增加指标项后面继续沟通讨论确定
   - TDinsight里面增加taosX的监控指标页面（目标端的last_row值、count值），监控指标至少包括taos-Explorer里的指标内容，而且可以配置告警，比如发邮件通知，同时需要给出一个告警规则的配置模版（佘），方便交付给客户时部署配置。

## 2. 意向用户

列出本需求完成后，可以向前推动的意向用户列表。

| 序号 | 经手人 | 项目名称 | 推动策略 |
| --- | --- | --- | --- |
| 1 |  | PCS一级部署项目 | 当前taosX命令行运行方式不够稳定，需要改进为taos-Explorer部署配置 |


## 3. 用户场景

1. 用户及所在行业的简要说明
中石化9家分公司本地TDengine（每家各一台taosx机器跑taosX命令行任务）---taosX-->呼和浩特集群---taosX-->北京EPDC集群

1. 用户遇到的技术问题，在本需求实现前用户所采取的解决方案
主要遇到的问题、当前使用的解决方案见[PCS项目-taosX现状与问题](https://taosdata.feishu.cn/wiki/RvdSws3sliakV3kQpChcEb5CnLg)

1. 本需求实现后用户采取的解决方案
需求开发完成后，推动用户改为在总部部署1个taos-Explorer，统一管理taosX同步任务，而不是使用当前的命令行方式

1. 本需求给用户带来的主要商业价值
在边云协同场景、分公司实时同步数据到中心同步场景下，taosX数据同步的完整性、时效性都有所提高，可以及时告警同步失败、报错。也可以增加客户满意度。
