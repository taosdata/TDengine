---
title: Supported Platforms
---

import PlatformSupported from "../10-developer-guide/08-connectors-reference/resources/_platform_supported.mdx";

For Community Edition releases after v3.0.7.1, use this page to check operating-system support. For domestic Chinese operating systems, use TDengine Enterprise or deploy with Docker.

## List of Platforms Supported by TDengine Server

|                       | **Version**        | **X64 64bit** | **ARM64** |
| ----------------------|----------------| ------------- | --------- |
| **CentOS**            | **7.9 and above**    | ●             | ●         |
| **Ubuntu**            | **18 and above**     | ●             | ●         |
| **RedHat**            | **RHEL 7 and above** | ●             | ●         |
| **Debian**            | **6.0 and above**    | ●             | ●         |
| **FreeBSD**           | **12 and above**     | ●             | ●         |
| **OpenSUSE**          | **All versions**     | ●             | ●         |
| **SUSE Linux**        | **11 and above**     | ●             | ●         |
| **Fedora**            | **21 and above**     | ●             | ●         |
| **Windows Server**    | **2019-2022**     | ●/E           |           |
| **Windows**           | **10/11**      | ●/E           |           |
| **Galaxy Kirin**      | **V10 and above**     | ●/E           | ●/E      |
| **NeoKylin**          | **V7.0 and above**    | ●/E           | ●/E      |
| **UnionTech UOS**     | **V20 and above**     | ●/E           |           |
| **Inspur K-UX**       | **V8.0 and above**    | ●/E           |           |
| **Huawei Euler openEuler** | **V20.03 and above**  | ●/E           |           |
| **Anolis OS**         | **V8.6 and above**   | ●/E           |           |
| **macOS**             | **14.0 and above**   |                | ●         |

Note: 1) ● indicates officially tested and verified, ○ indicates unofficially tested, E indicates only supported by the TDengine TSDB-Enterprise.

   1) The TDengine TSDB-OSS only supports newer versions of mainstream operating systems, including Ubuntu 18+/CentOS 7+/CentOS Stream/RedHat/Debian/CoreOS/FreeBSD/OpenSUSE/SUSE Linux/Fedora/macOS, etc. For other operating systems and versions, please contact enterprise support.

## List of Platforms Supported by TDengine Client and Connectors

<PlatformSupported />

## Supported Network Environments

TDengine supports both IPv4 and IPv6. For IPv6 configuration, see [Network and FQDN Configuration](./02-operations/08-network.md).
