---
sidebar_label: Security Advisories
title: Security Advisories
description: TDengine product security vulnerability advisories and fix information
toc_max_heading_level: 4
---

TDengine product vulnerability advisories and fix information are published on this page. For authentication, transport encryption, auditing, and deployment hardening, see the [Security Guide](./index.md). This page documents disclosures and fixed versions; it does not replace configuration guidance.

## Report a Security Vulnerability

If you discover a vulnerability in TDengine, report it privately through one of the following channels. After confirmation, we will address it and publish details after a fix is available. Do not discuss unfixed vulnerabilities in public forums or issues.

- Email: [TDengine Security Team](mailto:security@taosdata.com)
- Private GitHub report: [GitHub Security Advisory](https://github.com/taosdata/TDengine/security/advisories/new)

## CVE-2026-42542 (TD-SEC-2026-001)

1. Basic information

    - Severity: High
    - CVSS: 7.5
    - GHSA: `GHSA-vg95-j2hf-hvjx`
    - Title: Integer underflow in `uvConnMayGetUserInfo()` causes unauthenticated remote denial of service (DoS)
    - Published: 2026-06-04
    - CWE: `CWE-191`
    - Reporter: Yan @ Ridge Security

2. Affected versions

    - `>= v3.4.0.0, <= v3.4.1.5`

3. Fixed version

    - `v3.4.1.6`

4. Summary

    An integer underflow in `uvConnMayGetUserInfo()` in `source/libs/transport/src/transSvr.c` allows an unauthenticated remote attacker to crash `taosd` with one crafted RPC packet, without credentials or an established session.

## GHSA-67g2-ffwr-7x9h (TD-SEC-2026-002)

1. Basic information

    - Severity: Medium
    - CVSS: 5.0
    - GHSA: `GHSA-67g2-ffwr-7x9h`
    - Title: Missing authorization for `KILL SSMIGRATE` lets a low-privilege user interrupt shared-storage migration
    - Published: 2026-06-16
    - CWE: `CWE-862` Missing Authorization
    - Reporter: DavidCarliez

2. Affected versions

    - `>= v3.3.8.0, <= v3.4.1.14` (Enterprise)

3. Fixed version

    - `v3.4.1.15`

4. Summary

    The MNode `KILL SSMIGRATE` path omitted the `MND_OPER_SSMIGRATE_DB` privilege check, allowing a low-privilege SQL user to interrupt Enterprise shared-storage migration.

## GHSA-8pc4-p252-f5m7 (TD-SEC-2026-003)

1. Basic information

    - Severity: High
    - CVSS: 7.5
    - GHSA: `GHSA-8pc4-p252-f5m7`
    - Title: Out-of-bounds read in `transDecompressMsg()` causes unauthenticated remote denial of service
    - Published: 2026-06-16
    - CWE: `CWE-125` Out-of-bounds Read
    - Reporter: ghaithabdulreda

2. Affected versions

    - `<= v3.4.1.6`

3. Fixed version

    - `v3.4.1.15`

4. Summary

    `transDecompressMsg()` in `source/libs/transport/src/transComm.c` runs before authentication. A crafted compressed RPC packet shorter than `sizeof(STransCompMsg)` triggers a four-byte out-of-bounds read and can crash `taosd`.

## GHSA-4v5h-fxjw-vrmq (TD-SEC-2026-004)

1. Basic information

    - Severity: High
    - CVSS: 8.1
    - GHSA: `GHSA-4v5h-fxjw-vrmq`
    - Title: Off-by-one stack buffer overflow in `trimString()`
    - Published: 2026-06-08
    - CWE: `CWE-121` Stack-based Buffer Overflow / `CWE-787` Out-of-bounds Write
    - Reporter: ghaithabdulreda

2. Affected versions

    - `<= v3.4.1.6`

3. Fixed version

    - `v3.4.1.14`

4. Summary

    An off-by-one error in SQL parser function `trimString()` in `source/libs/parser/src/parUtil.c` can write one byte beyond a full stack buffer when processing a crafted escape sequence, causing stack corruption and potential remote code execution (RCE).

## GHSA-5r9p-3j4f-gmgp (TD-SEC-2026-005)

1. Basic information

    - Severity: Medium
    - CVSS: 5.3
    - GHSA: `GHSA-5r9p-3j4f-gmgp`
    - Title: Authenticated out-of-bounds read in SQL lexer `tGetToken()`
    - Published: 2026-06-08
    - CWE: `CWE-125` Out-of-bounds Read / `CWE-126` Buffer Over-read
    - Reporter: ghaithabdulreda

2. Affected versions

    - `<= v3.4.1.13`

3. Fixed version

    - `v3.4.1.14`

4. Summary

    SQL lexer function `tGetToken()` in `source/libs/parser/src/parTokenizer.c` does not correctly check the buffer boundary for a trailing backslash. An authenticated attacker can cause a one-byte read beyond the null terminator, potentially crashing `taosd` or disclosing information.

## GHSA-gm53-hjh6-pjg9 (TD-SEC-2026-006)

1. Basic information

    - Severity: Low
    - CVSS: 3.1
    - GHSA: `GHSA-gm53-hjh6-pjg9`
    - Title: Stored XSS in the TDengine OSS web console
    - Published: 2026-06-16
    - CWE: Stored XSS
    - Reporter: External security researcher

2. Affected versions

    - `latest` (TDengine OSS web console)

3. Fixed version

    - `v3.4.1.14`

4. Summary

    The TDengine OSS web console allowed malicious script injection into specific fields. The script could execute when another user viewed the affected page.

## GHSA-f7wh-p233-87xv (TD-SEC-2026-008)

1. Basic information

    - Severity: Critical
    - CVSS: 9.8
    - GHSA: `GHSA-f7wh-p233-87xv`
    - Title: Remote code execution (RCE) through UDF
    - Published: 2026-06-20
    - CWE: RCE via UDF
    - Reporter: Luca C.

2. Affected versions

    - `v3.0.5.0`

3. Fixed version

    - `v3.4.1.15`

4. Summary

    The TDengine user-defined function (UDF) interface allowed crafted UDF calls to execute arbitrary code in the `taosd` process context.

## GHSA-fmp7-rf4r-8q7p (TD-SEC-2026-009)

1. Basic information

    - Severity: Medium
    - CVSS: 5.0
    - GHSA: `GHSA-fmp7-rf4r-8q7p`
    - Title: Unexpected privileges for standard users
    - Published: 2026-06-20
    - CWE: Privilege management flaw
    - Reporter: Luca C.

2. Affected versions

    - `v3.0.5.0`

3. Fixed version

    - `v3.4.1.15` (the privilege model was made more granular starting with `v3.4.0.0`)

4. Summary

    Standard users had unexpected additional privileges in `v3.0.5.0`. The privilege system was redesigned in `v3.4.0.0` with SYSDBA / SYSSEC / SYSAUDIT separation of duties and mandatory access control (MAC). See [Privileges](../05-tdengine-sql/07-user-and-privilege/02-grant.md).

## CVE-2023-38502 (TD-SEC-2026-010)

1. Basic information

    - Severity: Medium
    - CVSS: 5.5
    - GHSA: `GHSA-w23f-r2fm-27hf`
    - Title: TDengine database denial of service
    - Published: 2026-06-20
    - CWE: DoS
    - Reporter: [security@huntr.dev](mailto:security@huntr.dev)

2. Affected versions

    - `<= v3.0.5.0`

3. Fixed versions

    - `>= v3.0.7.1` / `v3.4.1.14`

4. Summary

    Specific operations could make the TDengine database unavailable. The issue was fixed in `v3.0.7.1` and confirmed fixed in `v3.4.1.14`.

## GHSA-v8cj-fw82-9jjf (TD-SEC-2026-018)

1. Basic information

    - Severity: High
    - CVSS: 7.5
    - GHSA: `GHSA-v8cj-fw82-9jjf`
    - Title: Double-free / UAF in parser function `createSimpleSubQStmt`
    - Published: 2026-06-27
    - CWE: `CWE-415` Double Free / `CWE-416` Use After Free
    - Reporter: RigelYoung

2. Affected versions

    - `>= v3.4.1.0`

3. Fixed version

    - `v3.4.1.14`

4. Summary

    During error handling, `createSimpleSubQStmt()` in `source/libs/parser/src/parTranslater.c` evaluated `pCxt->errCode` instead of local variable `code`. The caller could continue using freed AST node `pSelect`, causing a double-free or use-after-free and potentially crashing `taosd`.

## GHSA-f8pf-77fh-53wv (TD-SEC-2026-019)

1. Basic information

    - Severity: Low
    - CVSS: 3.1
    - GHSA: `GHSA-f8pf-77fh-53wv`
    - Title: Double-free in the `projectApplyFunction` error path
    - Published: 2026-06-27
    - CWE: `CWE-415` Double Free
    - Reporter: RigelYoung

2. Affected versions

    - `>= v3.3.7.0`

3. Fixed version

    - `v3.4.1.14`

4. Summary

    In `projectApplyFunction()` in `source/libs/parser/src/parTranslater.c`, a failed `nodesListMakeStrictAppend` call freed `pCol` without setting it to null. The upper `_return` cleanup path then freed the pointer again.

## GHSA-998r-264c-5jcv (TD-SEC-2026-021)

1. Basic information

    - Severity: Low
    - CVSS: 3.1
    - GHSA: `GHSA-998r-264c-5jcv`
    - Title: Double-free in the `createStreamReqBuildTriggerSelect` error path
    - Published: 2026-06-27
    - CWE: `CWE-415` Double Free
    - Reporter: RigelYoung

2. Affected versions

    - `>= v3.3.7.0`

3. Fixed version

    - `v3.4.1.14`

4. Summary

    In `createStreamReqBuildTriggerSelect()` in `source/libs/parser/src/parTranslater.c`, `nodesListMakeStrictAppend` freed `pFunc` on list-initialization failure but returned a nonzero error code. `PAR_ERR_JRET` then jumped to `_return`, where the pointer was freed again.

## GHSA-vqj6-pwq9-qc5j (TD-SEC-2026-022)

1. Basic information

    - Severity: High
    - CVSS: 7.5
    - GHSA: `GHSA-vqj6-pwq9-qc5j`
    - Title: UAF through a dangling gauge pointer in `monGenDnodeStatusInfoTable`
    - Published: 2026-06-27
    - CWE: `CWE-416` Use After Free
    - Reporter: RigelYoung

2. Affected versions

    - `>= v3.3.3.0`

3. Fixed version

    - `v3.4.1.14`

4. Summary

    In `monGenDnodeStatusInfoTable()` in `source/libs/monitor/src/monFramework.c`, a gauge destroyed after registration failure was not reset to `NULL`. A later loop called `taos_gauge_set()` through the dangling pointer, causing a use-after-free and `taosd` crash.

## GHSA-c97w-rp4j-2jc9 (TD-SEC-2026-023)

1. Basic information

    - Severity: Low
    - CVSS: 3.1
    - GHSA: `GHSA-c97w-rp4j-2jc9`
    - Title: Double-free / UAF in the `buildTriggerPartitionForCreateStream` error path
    - Published: 2026-06-27
    - CWE: `CWE-415` Double Free / `CWE-416` Use After Free
    - Reporter: RigelYoung

2. Affected versions

    - `>= v3.3.8.0`

3. Fixed version

    - `v3.4.1.14`

4. Summary

    In `buildTriggerPartitionForCreateStream()` in `source/libs/parser/src/parTranslater.c`, `nodesListMakeStrictAppend` freed `pTagCol` after list-initialization failure, and `PAR_ERR_JRET` caused `_return` to free the pointer again.

## GHSA-4h6w-f7vf-96xj (TD-SEC-2026-024)

1. Basic information

    - Severity: High
    - CVSS: 7.5
    - GHSA: `GHSA-4h6w-f7vf-96xj`
    - Title: UAF / double-free in the `tableMetaCommit` error path
    - Published: 2026-06-27
    - CWE: `CWE-415` Double Free / `CWE-416` Use After Free
    - Reporter: RigelYoung

2. Affected versions

    - `>= v3.3.7.0`

3. Fixed version

    - `v3.4.1.14`

4. Summary

    In the `tableMetaCommit()` error path in `source/dnode/mnode/impl/src/mndVgroup.c`, incorrect cleanup ordering caused a freed hash-table object to be used and freed again.
