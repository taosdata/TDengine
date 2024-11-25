---
title: Supported Platforms
description: "List of supported platforms for TDengine server, client, and connectors"
slug: /tdengine-reference/supported-platforms
---

## Supported Platforms for TDengine Server

|              | **Windows Server 2016/2019** | **Windows 10/11** | **CentOS 7.9/8** | **Ubuntu 18+** | **UnionTech UOS** | **Galaxy/NeoKylin** | **Ningsi V60/V80** | **macOS** |
| ------------ | ---------------------------- | ----------------- | ---------------- | -------------- | ---------------- | ------------------ | ----------------- | --------- |
| X64          | ●/E                          | ●/E               | ●                | ●              | ●/E              | ●/E                | ●/E               | ●         |
| Raspberry Pi ARM64 |                        |                   | ●                |                |                  |                    |                   |           |
| Huawei Cloud ARM64 |                        |                   |                  | ●              |                  |                    |                   |           |
| M1           |                              |                   |                  |                |                  |                    |                   | ●         |

:::note

1. ● indicates official testing and verification, ○ indicates unofficial testing, and E indicates support for enterprise edition only.  
2. The community edition supports only recent versions of mainstream operating systems, such as Ubuntu 18+/CentOS 7+/RedHat/Debian/CoreOS/FreeBSD/OpenSUSE/SUSE Linux/Fedora/macOS, among others. For other operating systems or versions, please contact enterprise support.

:::

For Linux systems, the minimum environment requirements are as follows:

- Linux kernel version: 3.10.0-1160.83.1.el7.x86_64
- glibc version: 2.17

If building from source code, you also need:

- cmake version: 3.26.4 or higher
- gcc version: 9.3.1 or higher

## Supported Platforms for TDengine Client and Connectors

TDengine connectors support a wide range of platforms, including X64/X86/ARM64/ARM32/MIPS/LoongArch64 hardware platforms, and development environments such as Linux/Win64/Win32/macOS.

The compatibility matrix is as follows:

| **CPU**     | **X64 64bit** | **X64 64bit** | **ARM64** | **X64 64bit** | **ARM64** |
| ----------- | ------------- | ------------- | --------- | ------------- | --------- |
| **OS**      | **Linux**     | **Win64**     | **Linux** | **macOS**     | **macOS** |
| **C/C++**   | ●             | ●             | ●         | ●             | ●         |
| **JDBC**    | ●             | ●             | ●         | ○             | ○         |
| **Python**  | ●             | ●             | ●         | ●             | ●         |
| **Go**      | ●             | ●             | ●         | ●             | ●         |
| **NodeJs**  | ●             | ●             | ●         | ○             | ○         |
| **C#**      | ●             | ●             | ○         | ○             | ○         |
| **Rust**    | ●             | ●             | ○         | ●             | ●         |
| **RESTful** | ●             | ●             | ●         | ●             | ●         |

:::note

● indicates official testing and verification, ○ indicates unofficial testing, and -- indicates untested.

:::
