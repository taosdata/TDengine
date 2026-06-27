---
sidebar_label: 安全公告
title: 安全公告
description: TDengine 产品安全漏洞公告与修复信息
toc_max_heading_level: 4
---

TDengine 产品安全漏洞公告与修复信息会统一发布在本页。

## 报告安全漏洞

如果您在 TDengine 中发现了安全漏洞，请通过以下渠道私密报告给我们。我们会在确认后尽快修复，并在修复完成后公开披露。请勿在公开论坛或 Issue 中讨论未修复的安全漏洞。

- 邮件上报：[TDengine 安全团队](mailto:security@taosdata.com)
- GitHub 私密上报：[GitHub Security Advisory](https://github.com/taosdata/TDengine/security/advisories/new)

## CVE-2026-42542 (TD-SEC-2026-001)

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

## GHSA-67g2-ffwr-7x9h (TD-SEC-2026-002)

1. 基本信息

    - 严重级别：中危
    - CVSS：5.0
    - GHSA：`GHSA-67g2-ffwr-7x9h`
    - 标题：`KILL SSMIGRATE` 缺少权限校验导致低权限用户可中断共享存储迁移
    - 发布日期：2026-06-16
    - CWE：`CWE-862` Missing Authorization
    - 发现者：DavidCarliez

2. 受影响版本

    - `>= 3.3.8.0, <= 3.4.1.14` Enterprise

3. 已修复版本

    - `3.4.1.15`

4. 漏洞摘要

    MNode 的 `KILL SSMIGRATE` 处理路径中缺失 `MND_OPER_SSMIGRATE_DB` 权限校验，低权限 SQL 用户可绕过权限检查中断共享存储迁移操作，影响企业版 SSMIGRATE 功能的正常运行。

## GHSA-8pc4-p252-f5m7 (TD-SEC-2026-003)

1. 基本信息

    - 严重级别：高危
    - CVSS：7.5
    - GHSA：`GHSA-8pc4-p252-f5m7`
    - 标题：`transDecompressMsg()` 越界读取（OOB Read）导致未认证远程拒绝服务
    - 发布日期：2026-06-16
    - CWE：`CWE-125` Out-of-bounds Read
    - 发现者：ghaithabdulreda

2. 受影响版本

    - `<= 3.4.1.6`

3. 已修复版本

    - `3.4.1.15`

4. 漏洞摘要

    RPC 传输层 `transDecompressMsg()` 函数（`source/libs/transport/src/transComm.c`）在认证检查之前被调用。未认证的远程攻击者可构造压缩 RPC 数据包，使数据包长度小于 `sizeof(STransCompMsg)`，触发 4 字节越界读取（OOB Read），导致 `taosd` 服务进程崩溃。

## GHSA-4v5h-fxjw-vrmq (TD-SEC-2026-004)

1. 基本信息

    - 严重级别：高危
    - CVSS：8.1
    - GHSA：`GHSA-4v5h-fxjw-vrmq`
    - 标题：`trimString()` Off-by-One 栈缓冲区溢出
    - 发布日期：2026-06-08
    - CWE：`CWE-121` Stack-based Buffer Overflow / `CWE-787` Out-of-bounds Write
    - 发现者：ghaithabdulreda

2. 受影响版本

    - `<= 3.4.1.6`

3. 已修复版本

    - `3.4.1.14`

4. 漏洞摘要

    SQL 解析器 `trimString()` 函数（`source/libs/parser/src/parUtil.c`）处理转义序列时存在 Off-by-One 错误。攻击者可通过构造包含特殊转义序列的 SQL 查询，使输入恰好填满目标栈缓冲区，触发 1 字节越界写入，导致栈损坏和潜在远程代码执行（RCE）。

## GHSA-5r9p-3j4f-gmgp (TD-SEC-2026-005)

1. 基本信息

    - 严重级别：中危
    - CVSS：5.3
    - GHSA：`GHSA-5r9p-3j4f-gmgp`
    - 标题：SQL 词法分析器 `tGetToken()` 越界读取（需认证）
    - 发布日期：2026-06-08
    - CWE：`CWE-125` Out-of-bounds Read / `CWE-126` Buffer Over-read
    - 发现者：ghaithabdulreda

2. 受影响版本

    - `<= 3.4.1.13`

3. 已修复版本

    - `3.4.1.14`

4. 漏洞摘要

    SQL 词法分析器 `tGetToken()` 函数（`source/libs/parser/src/parTokenizer.c`）在处理尾部反斜杠时，未正确检查缓冲区边界，导致 null 终止符后 1 字节越界读取。已认证攻击者可发送包含尾部反斜杠的特殊 SQL 语句触发越界读取，导致 `taosd` 进程崩溃或信息泄露。

## GHSA-gm53-hjh6-pjg9 (TD-SEC-2026-006)

1. 基本信息

    - 严重级别：低危
    - CVSS：3.1
    - GHSA：`GHSA-gm53-hjh6-pjg9`
    - 标题：TDengine OSS 存储型 XSS
    - 发布日期：2026-06-16
    - CWE：Stored XSS
    - 发现者：外部安全研究人员

2. 受影响版本

    - `latest`（TDengine OSS Web 控制台）

3. 已修复版本

    - `3.4.1.14`

4. 漏洞摘要

    TDengine OSS（开源版）Web 控制台存在存储型跨站脚本（Stored XSS）漏洞。攻击者可将恶意脚本注入特定字段，当其他用户访问相关页面时触发脚本执行。
\n

## GHSA-f7wh-p233-87xv (TD-SEC-2026-008)

1. 基本信息

    - 严重级别：严重
    - CVSS：9.8
    - GHSA：`GHSA-f7wh-p233-87xv`
    - 标题：UDF 导致远程代码执行（RCE）
    - 发布日期：2026-06-20
    - CWE：RCE via UDF
    - 发现者：Luca C.

2. 受影响版本

    - `3.0.5.0`

3. 已修复版本

    - `3.4.1.15`

4. 漏洞摘要

    TDengine 用户自定义函数（UDF）接口存在远程代码执行漏洞。攻击者可通过特制的 UDF 调用在 `taosd` 进程上下文中执行任意代码，获取服务器控制权。

## GHSA-fmp7-rf4r-8q7p (TD-SEC-2026-009)

1. 基本信息

    - 严重级别：中危
    - CVSS：5.0
    - GHSA：`GHSA-fmp7-rf4r-8q7p`
    - 标题：标准用户权限异常
    - 发布日期：2026-06-20
    - CWE：权限管理缺陷
    - 发现者：Luca C.

2. 受影响版本

    - `3.0.5.0`

3. 已修复版本

    - `3.4.1.15`（3.4.0.0 起已细分权限体系）

4. 漏洞摘要

    旧版本（3.0.5.0）中标准用户存在未预期的额外权限。在 3.4.0.0 版本中权限系统已重构为三员权限 + 强制访问控制（MAC）体系，细化了管理员、安全审计、系统管理员等角色权限。

## CVE-2023-38502 (TD-SEC-2026-010)

1. 基本信息

    - 严重级别：中危
    - CVSS：5.5
    - GHSA：`GHSA-w23f-r2fm-27hf`
    - 标题：TDengine 数据库拒绝服务
    - 发布日期：2026-06-20
    - CWE：DoS
    - 发现者：[security@huntr.dev](mailto:security@huntr.dev)

2. 受影响版本

    - `<= 3.0.5.0`

3. 已修复版本

    - `>= 3.0.7.1` / `3.4.1.14`

4. 漏洞摘要

    TDengine 数据库存在拒绝服务漏洞，攻击者可通过特定操作使服务不可用。该漏洞已在 3.0.7.1 版本修复，并在 3.4.1.14 版本中全面确认修复。

## GHSA-v8cj-fw82-9jjf (TD-SEC-2026-018)

1. 基本信息

    - 严重级别：高危
    - CVSS：7.5
    - GHSA：`GHSA-v8cj-fw82-9jjf`
    - 标题：Parser 模块 createSimpleSubQStmt Double-Free / UAF
    - 发布日期：2026-06-27
    - CWE：`CWE-415` Double Free / `CWE-416` Use After Free
    - 发现者：RigelYoung

2. 受影响版本

    - `>= 3.4.1.0`

3. 已修复版本

    - `3.4.1.14`

4. 漏洞摘要

    `source/libs/parser/src/parTranslater.c` 的 `createSimpleSubQStmt()` 函数在错误检查时错误地评估了 `pCxt->errCode` 而非本地 `code` 变量，导致已释放的 AST 节点 `pSelect` 被上层调用者继续使用，触发 Double-Free 和 Use-After-Free，可造成 taosd 进程崩溃或内存损坏。

## GHSA-f8pf-77fh-53wv (TD-SEC-2026-019)

1. 基本信息

    - 严重级别：低危
    - CVSS：3.1
    - GHSA：`GHSA-f8pf-77fh-53wv`
    - 标题：projectApplyFunction 错误处理路径 Double Free
    - 发布日期：2026-06-27
    - CWE：`CWE-415` Double Free
    - 发现者：RigelYoung

2. 受影响版本

    - `>= 3.3.7.0`

3. 已修复版本

    - `3.4.1.14`

4. 漏洞摘要

    `source/libs/parser/src/parTranslater.c` 的 `projectApplyFunction()` 中，`nodesListMakeStrictAppend` 失败时释放了 `pCol` 节点但未将其置空，上层 `_return` 清理路径再次释放同一指针，导致 Double-Free 内存损坏。

## GHSA-998r-264c-5jcv (TD-SEC-2026-021)

1. 基本信息

    - 严重级别：低危
    - CVSS：3.1
    - GHSA：`GHSA-998r-264c-5jcv`
    - 标题：createStreamReqBuildTriggerSelect 错误处理路径 Double Free
    - 发布日期：2026-06-27
    - CWE：`CWE-415` Double Free
    - 发现者：RigelYoung

2. 受影响版本

    - `>= 3.3.7.0`

3. 已修复版本

    - `3.4.1.14`

4. 漏洞摘要

    `source/libs/parser/src/parTranslater.c` 的 `createStreamReqBuildTriggerSelect()` 中 `nodesListMakeStrictAppend` 失败时内部释放 `pFunc` 但仍返回非零错误码，`PAR_ERR_JRET` 宏导致跳转到 `_return` 标签再次释放同一指针。

## GHSA-vqj6-pwq9-qc5j (TD-SEC-2026-022)

1. 基本信息

    - 严重级别：高危
    - CVSS：7.5
    - GHSA：`GHSA-vqj6-pwq9-qc5j`
    - 标题：monGenDnodeStatusInfoTable 悬垂 gauge 指针 UAF
    - 发布日期：2026-06-27
    - CWE：`CWE-416` Use After Free
    - 发现者：RigelYoung

2. 受影响版本

    - `>= 3.3.3.0`

3. 已修复版本

    - `3.4.1.14`

4. 漏洞摘要

    `source/libs/monitor/src/monFramework.c` 的 `monGenDnodeStatusInfoTable()` 中，指标注册失败时销毁 gauge 后未将其置 `NULL`，后续循环中通过悬垂指针调用 `taos_gauge_set()` 写入已释放内存，导致 Use-After-Free 和 taosd 崩溃。

## GHSA-c97w-rp4j-2jc9 (TD-SEC-2026-023)

1. 基本信息

    - 严重级别：低危
    - CVSS：3.1
    - GHSA：`GHSA-c97w-rp4j-2jc9`
    - 标题：buildTriggerPartitionForCreateStream 错误处理路径 Double Free / UAF
    - 发布日期：2026-06-27
    - CWE：`CWE-415` Double Free / `CWE-416` Use After Free
    - 发现者：RigelYoung

2. 受影响版本

    - `>= 3.3.8.0`

3. 已修复版本

    - `3.4.1.14`

4. 漏洞摘要

    `source/libs/parser/src/parTranslater.c` 的 `buildTriggerPartitionForCreateStream()` 中，`nodesListMakeStrictAppend` 在列表初始化失败时释放了 `pTagCol` 节点，但 `PAR_ERR_JRET` 跳回 `_return` 标签后再次释放同一指针。

## GHSA-4h6w-f7vf-96xj (TD-SEC-2026-024)

1. 基本信息

    - 严重级别：高危
    - CVSS：7.5
    - GHSA：`GHSA-4h6w-f7vf-96xj`
    - 标题：tableMetaCommit 错误处理路径 UAF / Double Free
    - 发布日期：2026-06-27
    - CWE：`CWE-415` Double Free / `CWE-416` Use After Free
    - 发现者：RigelYoung

2. 受影响版本

    - `>= 3.3.7.0`

3. 已修复版本

    - `3.4.1.14`

4. 漏洞摘要

    `source/dnode/mnode/impl/src/mndVgroup.c` 的 `tableMetaCommit()` 错误处理路径中，资源清理顺序不当导致已释放的哈希表对象被再次使用和释放。
