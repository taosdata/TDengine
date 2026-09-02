---
title: License Center 参考手册
sidebar_label: License Center
toc_max_heading_level: 4
---

License Center 用于管理 TDengine TSDB / IDMP 的授权，由两侧组成：

| 组件 | 全称 | 部署位置 | 作用 |
| ---- | ---- | -------- | ---- |
| ELS | Enterprise License Server | TDengine 服务商侧 | 签发、续期、吊销许可证，管理客户整体额度与审计；设定许可证允许的最大槽位数 |
| CLS | Customer License Server | 客户本地（或托管环境） | 承接许可证，向本地下属的 TSDB / IDMP 实例发放配额并汇总用量；在许可证限额内调整各槽位配额 |

典型链路：ELS 管理客户的整体授权与最大槽位数 → 客户环境部署 CLS 并导入/同步许可证 → 多个 TSDB、IDMP 实例连接本地 CLS，按配额槽位（Slot）申请授权并上报心跳与用量。

:::note
本文及同目录示例中的许可证 ID、配额 ID、地址等均为演示数据，无实际业务意义。
:::

## 与激活码授权的关系

TDengine TSDB 企业版另可通过激活码直接激活集群，见 [激活 TDengine TSDB 企业版](../../02-operations/03-deployment/04-activate.md)。

| 方式 | 适用场景 |
| ---- | -------- |
| 激活码 | 单集群或少量集群、由服务商按机器信息签发激活码 |
| License Center（ELS + CLS） | 需要统一管理多实例配额、在线续期/吊销、在本地查看实例用量；例如托管方在一套环境中服务多个下游系统 |

若计划由本地 CLS 统一承接多个 TSDB / IDMP 实例，请先阅读 [配额与槽位](./02-quota-and-slots.md)，再按 [部署与授权](./01-deploy-and-activate.md) 完成安装与接入。

## 文档导航

| 文档 | 说明 |
| ---- | ---- |
| [部署与授权](./01-deploy-and-activate.md) | 安装 CLS、配置、在线/离线导入许可证、TSDB/IDMP 连接 CLS |
| [配额与槽位](./02-quota-and-slots.md) | 槽位数量与槽位配额的分工、多实例与多许可类型 |
| [用量查看与可用性](./03-usage-and-availability.md) | 实例级用量、与 ELS 同步、CLS 部署形态 |

## 模型要点

- 一个 CLS 可对接多个独立的 TSDB / IDMP 实例；一份许可证下可有多个槽位，各槽位配额之和不超过该许可证总限额。
- TSDB 与 IDMP 配额相互独立；同一客户可持有多张不同类型或不同有效期的许可证。
- 槽位数量（最大槽位数）在 ELS 侧设定；各槽位的具体配额在 CLS 侧调整，且不得超过许可证总限额。
- CLS 可查看实例连接与用量，联网时可向 ELS 同步；当前 CLS 为单点服务。

细节见 [配额与槽位](./02-quota-and-slots.md) 与 [用量查看与可用性](./03-usage-and-availability.md)。
