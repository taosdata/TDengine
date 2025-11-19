---
sidebar_label: 支持平台
title: 支持平台列表
description: "TDengine TSDB 服务端、客户端和连接器支持的平台列表"
---

import PlatformSupported from "./05-connector/_platform_supported.mdx";

3.0.7.1 之后的社区版所支持的操作系统范围，可参考本文档，如果使用国产操作系统，请使用企业版本，或者使用 Docker 方式安装。

## TDengine TSDB 服务端支持的平台列表

注：1) ● 表示经过官方测试验证， ○ 表示非官方测试验证，E 表示仅企业版支持。
   2) 社区版仅支持主流操作系统的较新版本，包括 Ubuntu 18+/CentOS 7+/CentOS Stream/RedHat/Debian/CoreOS/FreeBSD/OpenSUSE/SUSE Linux/Fedora/macOS 等。如果有其他操作系统及版本的需求，请联系企业版支持。

|                       | **版本**        | **X64 64bit** | **ARM64** |
| ----------------------|----------------| ------------- | --------- |
| **CentOS**            | **7.9 以上**    | ●             | ●         |
| **Ubuntu**            | **18 以上**     | ●             | ●         |
| **RedHat**            | **RHEL 7 以上** | ●             | ●         |
| **Debian**            | **6.0 以上**    | ●             | ●         |
| **FreeBSD**           | **12 以上**     | ●             | ●         |
| **OpenSUSE**          | **全部版本**     | ●             | ●         |
| **SUSE Linux**        | **11 以上**     | ●             | ●         |
| **Fedora**            | **21 以上**     | ●             | ●         |
| **Windows Server**    | **2016 以上**  | ●/E           |           |
| **Windows**           | **10/11**      | ●/E           |           |
| **银河麒麟**           | **V10 以上**     | ●/E           | ●/E      |
| **中标麒麟**           | **V7.0 以上**    | ●/E           | ●/E      |
| **统信 UOS**          | **V20 以上**     | ●/E           |           |
| **凝思磐石**           | **V8.0 以上**    | ●/E           |           |
| **华为欧拉 openEuler** | **V20.03 以上**  | ●/E           |           |
| **龙蜥 Anolis OS**     | **V8.6 以上**   | ●/E           |           |
| **macOS**             | **11.0 以上**   |                | ●         |

## TDengine TSDB 客户端和连接器支持的平台列表

<PlatformSupported /> 

## TDengine TSDB 支持的网络环境

TDengine 支持 IPv4 和 IPv6 两种通信方式, 其中 IPv6 内容参见 [IPv6 配置](../08-operation/13-network.md)
