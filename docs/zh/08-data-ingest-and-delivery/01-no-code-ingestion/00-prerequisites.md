---
title: 数据接入前置条件
sidebar_label: 前置条件
---

本页列出通过 taosExplorer 创建数据写入任务前需要确认的通用前置条件，适用于 OPC DA、OPC UA、PI、MQTT 等所有数据源。各数据源的专属前置条件（软件依赖、认证与权限等）见对应数据源文档。

## 环境就绪

- TDengine 集群已部署并正常运行。
- taosX 已安装并可通过 taosExplorer 访问。
- 目标数据库已创建，或准备好在 taosExplorer 中创建。

## 确认网络拓扑

根据 taosX 与数据源之间的网络关系，选择直连模式或代理模式：

| 拓扑 | 适用场景 | 网络要求 |
| --- | --- | --- |
| 直连模式 | taosX 能直接访问数据源 | taosX → 数据源端口连通 |
| 代理模式 | taosX 无法直接访问数据源，如数据源在隔离的工控网段、或 taosX 部署在公有云无法访问内网 | 在数据源所在网络部署 taosX-Agent，taosX ↔ taosX-Agent 与 taosX-Agent ↔ 数据源两段网络分别连通 |

## 网络与防火墙要求

| 源 | 目标 | 端口 | 说明 |
| --- | --- | --- | --- |
| taosX-Agent | taosX | taosX gRPC 端口，默认 `6055` | 代理模式必须放通；Agent 连接多个 taosX 时，需能访问每一个 taosX |
| taosX-Agent | 数据源 | 按数据源要求，如 PI 为 `5450`/`5457` | 代理模式必须放通，见各数据源文档 |
| taosX | 数据源 | 按数据源要求 | 直连模式必须放通 |

以上端口均为默认值，可通过配置文件修改（taosX 的 gRPC 监听地址由 `serve.grpc` 配置）。完整的端口列表与规划建议见 [网络端口要求](../../12-operations-and-tooling/02-operations/01-planning.md#网络端口要求)。

## 检查端口连通性

部署前，建议先验证关键端口的连通性。在源主机上执行：

```bash
# Linux
nc -zv <目标主机> <端口>
```

```powershell
# Windows PowerShell
Test-NetConnection -ComputerName <目标主机> -Port <端口>
```

例如在 taosX-Agent 所在主机上，检查 Agent 到 taosX 的连通性：

```bash
nc -zv <taosX_host> 6055
```

```powershell
Test-NetConnection -ComputerName <taosX_host> -Port 6055
```

## 创建任务前检查清单

- [ ] TDengine 集群、taosX、taosExplorer 均正常运行
- [ ] 已确定直连模式或代理模式
- [ ] 直连模式：taosX → 数据源端口已放通
- [ ] 代理模式：taosX-Agent 已安装，taosExplorer 中代理状态显示为 **正常**
- [ ] 代理模式：taosX-Agent → taosX（默认 `6055`）端口已放通
- [ ] 代理模式：taosX-Agent → 数据源端口已放通

## 相关文档

- [安装 taosX-Agent](./01-install-agent.md)：代理的下载、安装与上线
- [taosX-Agent 参考手册](../../12-operations-and-tooling/03-components/07-taosx-agent/index.md)：组件功能、配置与排障
- [PI 数据接入前置条件](./04-pi/01-prerequisites.md)：按数据源展开的完整前置条件范例
