---
sidebar_label: 安全公告
title: 安全公告
description: TDengine 产品安全漏洞公告与修复信息
toc_max_heading_level: 4
---

TDengine 产品安全漏洞公告与修复信息统一发布在本页。如何配置认证、传输加密、审计与部署加固，见 [安全指南](./index.md)；本页仅收录漏洞披露与修复版本，不替代配置手册。

## 报告安全漏洞

如果在 TDengine 中发现安全漏洞，请通过以下渠道私密报告。确认后会尽快修复，并在修复完成后公开披露。请勿在公开论坛或 Issue 中讨论未修复的安全漏洞。

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

    - `>= v3.4.0.0, <= v3.4.1.5`

3. 已修复版本

    - `v3.4.1.6`

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

    - `>= v3.3.8.0, <= v3.4.1.14`（Enterprise）

3. 已修复版本

    - `v3.4.1.15`

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

    - `<= v3.4.1.6`

3. 已修复版本

    - `v3.4.1.15`

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

    - `<= v3.4.1.6`

3. 已修复版本

    - `v3.4.1.14`

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

    - `<= v3.4.1.13`

3. 已修复版本

    - `v3.4.1.14`

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

    - `v3.4.1.14`

4. 漏洞摘要

    TDengine OSS（开源版）Web 控制台存在存储型跨站脚本（Stored XSS）漏洞。攻击者可将恶意脚本注入特定字段，当其他用户访问相关页面时触发脚本执行。

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

    - `v3.0.5.0`

3. 已修复版本

    - `v3.4.1.15`

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

    - `v3.0.5.0`

3. 已修复版本

    - `v3.4.1.15`（`v3.4.0.0` 起已细分权限体系）

4. 漏洞摘要

    旧版本（`v3.0.5.0`）中标准用户存在未预期的额外权限。在 `v3.4.0.0` 中权限系统已重构为三权分立（SYSDBA / SYSSEC / SYSAUDIT）与强制访问控制（MAC），细化了系统管理、安全策略与审计等职责。权限语法见 [权限管理](../05-tdengine-sql/07-user-and-privilege/02-grant.md)。

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

    - `<= v3.0.5.0`

3. 已修复版本

    - `>= v3.0.7.1` / `v3.4.1.14`

4. 漏洞摘要

    TDengine 数据库存在拒绝服务漏洞，攻击者可通过特定操作使服务不可用。该漏洞已在 `v3.0.7.1` 版本修复，并在 `v3.4.1.14` 版本中全面确认修复。

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

    - `>= v3.4.1.0`

3. 已修复版本

    - `v3.4.1.14`

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

    - `>= v3.3.7.0`

3. 已修复版本

    - `v3.4.1.14`

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

    - `>= v3.3.7.0`

3. 已修复版本

    - `v3.4.1.14`

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

    - `>= v3.3.3.0`

3. 已修复版本

    - `v3.4.1.14`

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

    - `>= v3.3.8.0`

3. 已修复版本

    - `v3.4.1.14`

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

    - `>= v3.3.7.0`

3. 已修复版本

    - `v3.4.1.14`

4. 漏洞摘要

    `source/dnode/mnode/impl/src/mndVgroup.c` 的 `tableMetaCommit()` 错误处理路径中，资源清理顺序不当导致已释放的哈希表对象被再次使用和释放。

## GHSA-crfh-3p34-23cw (TD-SEC-2026-030)

1. 基本信息

    - 严重级别：中危
    - GHSA：`GHSA-crfh-3p34-23cw`
    - 标题：`mndProcessAskEpReq` 缺少属主校验导致 IDOR，任意已认证用户可获取他人 consumer 的 VNode 端点
    - 发布日期：2026-09-28
    - CWE：`CWE-285` Improper Authorization / `CWE-639` IDOR
    - 发现者：外部安全研究人员

2. 受影响版本

    - `>= v3.4.0.0, <= v3.4.2.12`

3. 已修复版本

    - `v3.4.2.13`

4. 漏洞摘要

    MNode 的 `mndProcessAskEpReq`（`TDMT_MND_TMQ_ASK_EP`）只校验 consumerId/cgroup，不验证请求用户是否为该 TMQ consumer 的属主。已认证用户可利用不安全的直接对象引用（IDOR）查询其他用户 consumer 的 VNode 端点分配信息。修复后在处理请求前校验 consumer 属主或授权。

## GHSA-gmhq-7r4w-7rp5 (TD-SEC-2026-031)

1. 基本信息

    - 严重级别：中危
    - GHSA：`GHSA-gmhq-7r4w-7rp5`
    - 标题：INSERT 反序列化整数溢出与边界检查绕过（`tDecodeColDataVersion0` / `tDecodeBinaryWithSize`）
    - 发布日期：2026-09-28
    - CWE：`CWE-190` Integer Overflow / `CWE-125` Out-of-bounds Read
    - 发现者：外部安全研究人员

2. 受影响版本

    - `>= v3.4.0.0, <= v3.4.2.12`

3. 已修复版本

    - `v3.4.2.13`

4. 漏洞摘要

    INSERT 反序列化路径 `tDecodeColDataVersion0()` 与 `tDecodeBinaryWithSize()` 中存在整数溢出与边界检查绕过，攻击者可构造恶意 INSERT 数据触发越界访问，导致 `taosd` 崩溃或数据损坏。修复后对长度字段进行溢出安全校验。

## GHSA-pc9g-w85r-7c6x (TD-SEC-2026-035)

1. 基本信息

    - 严重级别：中危
    - GHSA：`GHSA-pc9g-w85r-7c6x`
    - 标题：`msgToDatum()` DECIMAL 分支经由构造的查询计划触发堆缓冲区溢出
    - 发布日期：2026-09-28
    - CWE：`CWE-122` Heap-based Buffer Overflow / `CWE-787` Out-of-bounds Write
    - 发现者：外部安全研究人员

2. 受影响版本

    - `>= v3.0.0.0, <= v3.4.2.12`

3. 已修复版本

    - `v3.4.2.13`

4. 漏洞摘要

    `msgToDatum()` 的 DECIMAL 分支经由精心构造的 `TDMT_SCH_QUERY` 查询计划 TLV 可触发堆缓冲区溢出，已认证远程攻击者可使 `taosd` 崩溃（DoS）。修复后对 DECIMAL 字段长度与目标缓冲区进行一致性校验。

## GHSA-382r-9pxq-7vg2 (TD-SEC-2026-036)

1. 基本信息

    - 严重级别：高危
    - GHSA：`GHSA-382r-9pxq-7vg2`
    - 标题：`mndProcessStreamHb` 缺少长度校验导致整数下溢与堆越界读取
    - 发布日期：2026-09-28
    - CWE：`CWE-191` Integer Underflow / `CWE-125` Out-of-bounds Read
    - 发现者：外部安全研究人员

2. 受影响版本

    - `>= v3.4.2.8, <= v3.4.2.12`

3. 已修复版本

    - `v3.4.2.13`

4. 漏洞摘要

    `mndProcessStreamHb` 处理空消息体的 `TDMT_MND_STREAM_HEARTBEAT` 消息时缺少长度校验，导致整数下溢并引发堆越界读取，造成 mnode 崩溃（DoS）。修复后在反序列化前校验消息体长度。

## GHSA-c483-rvf9-p53q (TD-SEC-2026-038)

1. 基本信息

    - 严重级别：中危
    - GHSA：`GHSA-c483-rvf9-p53q`
    - 标题：`CREATE ANODE` URL 处理中的服务端请求伪造（SSRF）
    - 发布日期：2026-09-28
    - CWE：`CWE-918` Server-Side Request Forgery
    - 发现者：外部安全研究人员

2. 受影响版本

    - `>= v3.0.0.0, <= v3.4.2.12`

3. 已修复版本

    - `v3.4.2.13`

4. 漏洞摘要

    `CREATE ANODE` 语句的 URL 处理缺少目标校验，存在服务端请求伪造（SSRF）漏洞，已认证用户可诱使服务端向任意内部或外部地址发起请求，探测内网或访问受限服务。修复后对 URL 进行白名单与目标地址校验。

## GHSA-j63p-9gcr-5fv8 (TD-SEC-2026-040)

1. 基本信息

    - 严重级别：高危
    - GHSA：`GHSA-j63p-9gcr-5fv8`
    - 标题：SQL Hint 参数超过 10 个时 `createHintNodeList()` 栈缓冲区溢出
    - 发布日期：2026-09-28
    - CWE：`CWE-121` Stack-based Buffer Overflow / `CWE-787` Out-of-bounds Write
    - 发现者：外部安全研究人员

2. 受影响版本

    - `>= v3.0.0.0, <= v3.4.2.12`

3. 已修复版本

    - `v3.4.2.13`

4. 漏洞摘要

    `createHintNodeList()` 处理 SQL Hint 时使用固定大小的栈缓冲区，Hint 参数超过 10 个时发生栈溢出，导致 `taosd` 崩溃（DoS）。修复后对 Hint 参数数量增加上限校验并改用动态分配。

## GHSA-f939-x5h3-cg4f (TD-SEC-2026-041)

1. 基本信息

    - 严重级别：高危
    - GHSA：`GHSA-f939-x5h3-cg4f`
    - 标题：`translateRepeat` 整数溢出导致 `repeatFunction` 堆缓冲区溢出
    - 发布日期：2026-09-28
    - CWE：`CWE-190` Integer Overflow / `CWE-680` Integer Overflow to Buffer Overflow / `CWE-787` Out-of-bounds Write
    - 发现者：外部安全研究人员

2. 受影响版本

    - `>= v3.4.2.0, <= v3.4.2.12`

3. 已修复版本

    - `v3.4.2.13`

4. 漏洞摘要

    `translateRepeat` 在计算 REPEAT 函数输出长度时存在整数溢出，导致 `repeatFunction` 按溢出后的长度分配内存并写入，造成堆缓冲区溢出和远程拒绝服务（服务崩溃）。修复后使用溢出安全的长度计算。

## GHSA-qh9p-wprq-p2hf (TD-SEC-2026-046)

1. 基本信息

    - 严重级别：高危
    - GHSA：`GHSA-qh9p-wprq-p2hf`
    - 标题：`tDeserializeSCreateFuncReq()` pCode 字段堆缓冲区溢出
    - 发布日期：2026-09-28
    - CWE：`CWE-122` Heap-based Buffer Overflow
    - 发现者：外部安全研究人员

2. 受影响版本

    - 所有版本（`<= v3.3.6.64`，`>= v3.4.0.0, <= v3.4.2.12`）

3. 已修复版本

    - `v3.3.6.65` / `v3.4.2.13`

4. 漏洞摘要

    `tDeserializeSCreateFuncReq()` 反序列化 pCode 字段时，字段长度与内存分配大小不一致，可触发堆缓冲区溢出，未认证远程攻击者可使 `taosd` 崩溃（DoS）。修复后按实际长度分配并校验。

## GHSA-8c53-8w6m-w28r (TD-SEC-2026-047)

1. 基本信息

    - 严重级别：严重
    - GHSA：`GHSA-8c53-8w6m-w28r`
    - 标题：`tDeserializeSCreateFuncReq()` 栈缓冲区溢出可导致远程代码执行
    - 发布日期：2026-09-28
    - CWE：`CWE-121` Stack-based Buffer Overflow
    - 发现者：外部安全研究人员

2. 受影响版本

    - `>= v3.0.0.0, <= v3.3.6.64`，`>= v3.4.0.0, <= v3.4.2.12`

3. 已修复版本

    - `v3.3.6.65` / `v3.4.2.13`

4. 漏洞摘要

    `tDeserializeSCreateFuncReq()` 反序列化 name 字段时缺少边界校验，可触发栈缓冲区溢出。未认证远程攻击者可利用该漏洞使 `taosd` 崩溃，甚至可能实现远程代码执行（RCE）。修复后对 name 字段进行长度校验。

## GHSA-4w45-hf5j-95m3 (TD-SEC-2026-048)

1. 基本信息

    - 严重级别：高危
    - CVSS：8.8
    - GHSA：`GHSA-4w45-hf5j-95m3`
    - 标题：TDGpt 安装器 `tarfile.extractall` 路径穿越导致任意文件写入
    - 发布日期：2026-09-28
    - CWE：`CWE-22` Path Traversal
    - 发现者：外部安全研究人员

2. 受影响版本

    - `>= v3.4.2.9, < v3.4.2.12`（TDGpt 组件）

3. 已修复版本

    - `v3.4.2.12`（TDGpt 组件）

4. 漏洞摘要

    TDGpt 安装脚本 `tools/tdgpt/script/install.py` 对模型资源包直接调用 `tarfile.extractall()` / `zipfile.extractall()`，未校验成员路径。攻击者可通过包含 `../` 等穿越路径的恶意归档向解压目录外写入任意文件；安装通常以 root 运行，可覆盖系统文件。修复后解压前校验所有成员路径并拒绝穿越。

## GHSA-x8q3-55fg-4pwh (TD-SEC-2026-049)

1. 基本信息

    - 严重级别：中危
    - GHSA：`GHSA-x8q3-55fg-4pwh`
    - 标题：`tDecodeCStrTo` 栈缓冲区溢出
    - 发布日期：2026-09-28
    - CWE：`CWE-121` Stack-based Buffer Overflow
    - 发现者：外部安全研究人员

2. 受影响版本

    - 所有版本（`<= v3.3.6.64`，`>= v3.4.0.0, <= v3.4.2.12`）

3. 已修复版本

    - `v3.3.6.65` / `v3.4.2.13`

4. 漏洞摘要

    `tDecodeCStrTo` 在解码字符串时未校验目标缓冲区长度，可触发栈缓冲区溢出，已认证远程攻击者可使 `taosd` 崩溃或执行任意代码。修复后增加目标缓冲区长度校验。

## GHSA-9m72-77jp-x5f2 (TD-SEC-2026-050)

1. 基本信息

    - 严重级别：高危
    - GHSA：`GHSA-9m72-77jp-x5f2`
    - 标题：taosKeeper 审计端点未授权 SQL 注入可篡改审计日志
    - 发布日期：2026-09-28
    - CWE：`CWE-89` SQL Injection
    - 发现者：外部安全研究人员

2. 受影响版本

    - `>= v3.3.6.0, <= v3.4.2.12`（taosKeeper）

3. 已修复版本

    - `v3.4.2.13`

4. 漏洞摘要

    taosKeeper 审计端点缺少身份认证，且指标/审计写入处理中存在 SQL 注入。未授权远程攻击者可对后端 TDengine 实例执行任意 SQL 语句并篡改审计日志。修复后为审计端点增加认证，写入路径改用参数化查询。

## GHSA-2hff-w2r4-f7g5 (TD-SEC-2026-051)

1. 基本信息

    - 严重级别：中危
    - GHSA：`GHSA-2hff-w2r4-f7g5`
    - 标题：contrib/TSZ 内嵌 zstd 1.3.5 落在 CVE-2019-11922 影响范围
    - 发布日期：2026-09-28
    - CWE：`CWE-362` Concurrent Execution using Shared Resource with Improper Synchronization
    - 发现者：TDengine 安全团队

2. 受影响版本

    - `<= v3.4.2.12`（contrib/TSZ/zstd 1.3.5）

3. 已修复版本

    - `v3.4.2.13`（zstd 升级至 1.3.8 及以上）

4. 漏洞摘要

    contrib/TSZ 内嵌的 zstd 库版本为 1.3.5，落在 CVE-2019-11922（压缩/解压竞态条件）影响范围（`< 1.3.8`）。该问题为库级别缺陷，利用依赖特定并发时序。修复方式为升级 vendored zstd 至 1.3.8 及以上。
