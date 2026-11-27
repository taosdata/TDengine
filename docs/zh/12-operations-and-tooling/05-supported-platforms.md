---
title: 支持平台列表
description: "TDengine 服务端、客户端和连接器支持的平台列表"
---

import PlatformSupported from "../10-developer-guide/08-connectors-reference/resources/_platform_supported.mdx";

`v3.0.7.1` 之后的社区版所支持的操作系统范围，可参考本文档。如果使用国产操作系统，请使用企业版本，或者使用 Docker 方式安装。

## 服务端支持的平台列表

注：

1. ● 表示经过官方测试验证，○ 表示非官方测试验证，E 表示仅企业版支持。
2. 社区版仅支持主流操作系统较新版本（Ubuntu 18+ / CentOS 7+ / CentOS Stream / Rocky(v3.4.3.0+) / AlmaLinux(v3.4.3.0+) / RedHat / Debian / Fedora / FreeBSD / OpenSUSE / SUSE Linux / macOS 等）。若有其他操作系统及版本需求，请联系企业版支持。

| | **版本** | **X64 64bit** | **ARM64** |
|---|---|---|---|
| **CentOS** | **7.9 以上** | ● | ● |
| **Ubuntu** | **18 以上** | ● | ● |
| **RedHat** | **RHEL 7 以上** | ● | ● |
| **Debian** | **6.0 以上** | ● | ● |
| **FreeBSD** | **12 以上** | ● | ● |
| **OpenSUSE** | **全部版本** | ● | ● |
| **SUSE Linux** | **11 以上** | ● | ● |
| **Fedora** | **21 以上** | ● | ● |
| **Windows Server** | **2016 以上** | ● / E | |
| **Windows** | **10/11** | ● / E | |
| **银河麒麟** | **V10 以上** | ● / E | ● / E |
| **中标麒麟** | **V7.0 以上** | ● / E | ● / E |
| **统信 UOS** | **V20 以上** | ● / E | |
| **凝思磐石** | **V8.0 以上** | ● / E | |
| **国能磐石 CEOS** | **全部发行版本** | ● / E | ● / E |
| **华为欧拉 openEuler** | **V20.03 以上** | ● / E | ● / E |
| **龙蜥 Anolis OS** | **V8.6 以上** | ● / E | |
| **macOS** | **14.0 以上** | | ● |

## 支持的 CPU 架构

企业版服务端支持以下 CPU 架构。同一操作系统在不同架构上使用对应架构的安装包，功能一致。

| 架构 | 典型处理器 | 安装包架构 | 支持情况 |
| --- | --- | --- | --- |
| X64 | Intel、AMD 64 位处理器 | `x64` | ● |
| 海光 C86 | 海光 C86 系列（如 7280、7390） | `x64` | ● / E |
| ARM64 | 64 位 ARM 处理器 | `arm64` | ● / E |

**说明**

- 海光 C86 处理器兼容 X64 指令集，直接使用 `x64` 安装包，无需单独编译。官方已在海光 C86 搭配华为欧拉 openEuler、龙蜥 Anolis OS 的环境中完成完整测试用例验证。
- 在 ARM64 服务器上部署时，请选择操作系统对应的 `arm64` 安装包。

## 信创环境与私有云部署

TDengine 企业版遵循国产信创安全适配要求，可部署在国产操作系统与国产 CPU 组合的环境中，包括国能磐石 CEOS、银河麒麟、华为欧拉 openEuler、凝思磐石等操作系统，以及 X64、ARM64、海光 C86 架构。上表列出了各操作系统支持的版本和架构；未列出的组合请联系企业版技术支持确认。

在企业私有云中，TDengine 可以运行在物理机、虚拟机（如 KVM、VMware）和容器（Docker、Kubernetes）中，部署方式与物理机相同。在虚拟化环境中部署时请注意：

- 为每个 dnode 分配独占的 CPU、内存和数据盘，避免与高负载业务混部导致写入和查询延迟抖动。
- 数据目录应位于持久化存储上，不要使用虚拟机或容器的临时盘。
- 启用透明数据加密时，密钥与机器码绑定。部分虚拟化或容器环境无法提供稳定的机器码，部署前请先验证，详见 [静态数据保护](../11-security-guide/06-data-security.md)。
- 容器部署方式见 [Docker 部署](./02-operations/03-deployment/02-docker.md) 和 [Kubernetes 部署](./02-operations/03-deployment/03-kubernetes.md)。

## 客户端和连接器支持的平台列表

<PlatformSupported />

## 支持的网络环境

TDengine 支持 IPv4 和 IPv6 两种通信方式，其中 IPv6 内容参见 [IPv6 配置](./02-operations/09-network.md)。
