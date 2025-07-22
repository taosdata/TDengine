---
title: Supported Platforms
slug: /tdengine-reference/supported-platforms
---

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
| **Windows Server**    | **2016 and above**  | ●/E           |           |
| **Windows**           | **10/11**      | ●/E           |           |
| **Galaxy Kirin**      | **V10 and above**     | ●/E           | ●/E      |
| **NeoKylin**          | **V7.0 and above**    | ●/E           | ●/E      |
| **UnionTech UOS**     | **V20 and above**     | ●/E           |           |
| **Inspur K-UX**       | **V8.0 and above**    | ●/E           |           |
| **Huawei Euler openEuler** | **V20.03 and above**  | ●/E           |           |
| **Anolis OS**         | **V8.6 and above**   | ●/E           |           |
| **macOS**             | **11.0 and above**   |                | ●         |

Note: 1) ● indicates officially tested and verified, ○ indicates unofficially tested, E indicates only supported by the TDengine TSDB-Enterprise.
   1) The TDengine TSDB-OSS only supports newer versions of mainstream operating systems, including Ubuntu 18+/CentOS 7+/CentOS Stream/RedHat/Debian/CoreOS/FreeBSD/OpenSUSE/SUSE Linux/Fedora/macOS, etc. For other operating systems and versions, please contact enterprise support.

## List of Platforms Supported by TDengine Client and Connectors

Currently, TDengine connectors support a wide range of platforms, including hardware platforms such as X64/X86/ARM64/ARM32/MIPS/LoongArch64, and development environments such as Linux/Win64/Win32/macOS.

The compatibility matrix is as follows:

| **CPU**     | **X64 64bit** | **X64 64bit** | **X64 64bit** | **ARM64** | **ARM64** |
| ----------- | ------------- | ------------- | ------------- | --------- | --------- |
| **OS**      | **Linux**     | **Win64**     | **macOS**     | **Linux** | **macOS** |
| **C/C++**   | ●             | ●             | ●             | ●         | ●         |
| **JDBC**    | ●             | ●             | ●             | ●         | ●         |
| **Python**  | ●             | ●             | ●             | ●         | ●         |
| **Go**      | ●             | ●             | ●             | ●         | ●         |
| **NodeJs**  | ●             | ●             | ●             | ●         | ●         |
| **C#**      | ●             | ●             | ○             | ●         | ○         |
| **Rust**    | ●             | ●             | ●             | ○         | ●         |
| **RESTful** | ●             | ●             | ●             | ●         | ●         |

Note: ● indicates official testing and verification passed, ○ indicates non-official testing and verification passed, -- indicates not verified.
