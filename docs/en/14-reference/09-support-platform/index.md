---
title: List of supported platforms
description: "List of platforms supported by TDengine server, client, and connector"
---

## List of supported platforms for TDengine server

|              | **CentOS 7/8** | **Ubuntu 16/18/20** | **Other Linux** |
| ------------ | -------------- | ------------------- | --------------- |
| X64          | ●              | ●                   |                 |
| MIPS64       |                |                     | ●               |
| ARM64        |                | ○                   | ○               |
| Alpha64      |                |                     | ○               |

Note: ● means officially tested and verified, ○ means unofficially tested and verified.

## List of supported platforms for TDengine clients and connectors

TDengine's connector can support a wide range of platforms, including X64/X86/ARM64/ARM32/MIPS/Alpha hardware platforms and Linux/Win64/Win32 development environments.

The comparison matrix is as follows.

| **CPU**     | **X64 64bit** |           |           | **X86 32bit** | **ARM64** | **ARM32** | **MIPS**  | **Alpha** |
| ----------- | ------------- | --------- | --------- | ------------- | --------- | --------- | --------- | --------- |
| **OS**      | **Linux**     | **Win64** | **Win32** | **Win32**     | **Linux** | **Linux** | **Linux** | **Linux** |
| **C/C++**   | ●             | ●         | ●         | ○             | ●         | ●         | ●         | ●         |
| **JDBC**    | ●             | ●         | ●         | ○             | ●         | ●         | ●         | ●         |
| **Python**  | ●             | ●         | ●         | ○             | ●         | ●         | ●         | --        |
| **Go**      | ●             | ●         | ●         | ○             | ●         | ●         | ○         | --        |
| **NodeJs**  | ●             | ●         | ○         | ○             | ●         | ●         | ○         | --        |
| **C#**      | ●             | ●         | ○         | ○             | ○         | ○         | ○         | --        |
| **RESTful** | ●             | ●         | ●         | ●             | ●         | ●         | ●         | ●         |

Note: ● means the official test is verified, ○ means the unofficial test is verified, -- means not verified.
