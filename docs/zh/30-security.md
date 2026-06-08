---
sidebar_label: 安全公告
title: TDengine 安全公告
description: TDengine 产品安全漏洞公告与修复信息
toc_max_heading_level: 4
---

TDengine 产品安全漏洞公告与修复信息会统一发布在本页。

## 报告安全漏洞

如果您在 TDengine 中发现了安全漏洞，请通过以下渠道私密报告给我们。我们会在确认后尽快修复，并在修复完成后公开披露。请勿在公开论坛或 Issue 中讨论未修复的安全漏洞。

- 邮件上报：[TDengine 安全团队](mailto:security@taosdata.com)
- GitHub 私密上报：[GitHub Security Advisory](https://github.com/taosdata/TDengine/security/advisories/new)

## CVE-2026-42542

1. 基本信息

    - 严重级别：高危
    - CVSS：7.5
    - GHSA：`GHSA-vg95-j2hf-hvjx`
    - 标题：`uvConnMayGetUserInfo()` 整数下溢导致未认证远程拒绝服务（DoS）
    - 发布日期：2026-06-04
    - CWE：`CWE-191`
    - 发现者：Yan @ Ridge Security

2. 受影响版本

    - `>= 3.4.0.0, <= 3.4.1.5`

3. 已修复版本

    - `3.4.1.6`

4. 漏洞摘要

    `source/libs/transport/src/transSvr.c` 中的 `uvConnMayGetUserInfo()` 函数存在整数下溢漏洞。未认证的远程攻击者可通过发送单个精心构造的 RPC 数据包导致 `taosd` 服务进程崩溃，无需任何凭据或预先建立的会话状态。
