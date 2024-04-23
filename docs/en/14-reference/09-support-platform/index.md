---
title: List of supported platforms
description: This document describes the supported platforms for the TDengine server, client, and client libraries.
---

## List of supported platforms for TDengine server

|              | **Windows Server 2016/2019** | **Windows 10/11** | **CentOS 7.9/8** | **Ubuntu 18 or later** | **macOS** |
| ------------ | ---------------------------- | ----------------- | ---------------- | ---------------- | --------- |
| X64          | ●/E                          | ●/E               | ●                | ●                | ●         |
| ARM64        |                              |                   | ●                |                  | ●         |

Note: 1) ● means officially tested and verified, ○ means unofficially tested and verified, E means only supported by the enterprise edition. 2) The community edition only supports newer versions of mainstream operating systems, including Ubuntu 18+/CentOS 7+/RetHat/Debian/CoreOS/FreeBSD/OpenSUSE/SUSE Linux/Fedora/macOS, etc. If you have requirements for other operating systems and editions, please contact support of the enterprise edition.

## List of supported platforms for TDengine clients and client libraries

TDengine's client libraries can support a wide range of platforms, including X64/X86/ARM64/ARM32/MIPS/Alpha/LoongArch64 hardware platforms and Linux/Win64/Win32/macOS development environments.

The comparison matrix is as follows.

| **CPU**     | **X64 64bit** | **X64 64bit** | **ARM64** | **X64 64bit** | **ARM64** |
| ----------- | ------------- | ------------- | --------- | ------------- | --------- |
| **OS**      | **Linux**     | **Win64**     | **Linux** | **macOS**     | **macOS** |
| **C/C++**   | ●             | ●             | ●         | ●             | ●         |
| **JDBC**    | ●             | ●             | ●         | ○             | ○         |
| **Python**  | ●             | ●             | ●         | ●             | ●         |
| **Go**      | ●             | ●             | ●         | ●             | ●         |
| **NodeJs**  | ●             | ●             | ●         | ○             | ○         |
| **C#**      | ●             | ●             | ○         | ○             | ○         |
| **RESTful** | ●             | ●             | ●         | ●             | ●         |

Note: ● means the official test is verified, ○ means the unofficial test is verified, -- means not verified.
