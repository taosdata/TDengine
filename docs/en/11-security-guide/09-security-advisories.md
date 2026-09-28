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

## GHSA-crfh-3p34-23cw (TD-SEC-2026-030)

1. Basic information

    - Severity: Medium
    - GHSA: `GHSA-crfh-3p34-23cw`
    - Title: Missing ownership verification in `mndProcessAskEpReq` (IDOR) allows any authenticated user to fetch another consumer's VNode endpoints
    - Published: 2026-09-28
    - CWE: `CWE-285` Improper Authorization / `CWE-639` IDOR
    - Reporter: External security researcher

2. Affected versions

    - `>= v3.4.0.0, <= v3.4.2.12`

3. Fixed version

    - `v3.4.2.13`

4. Summary

    MNode handler `mndProcessAskEpReq` (`TDMT_MND_TMQ_ASK_EP`) only validated the consumerId/cgroup and never checked that the requesting user owned the TMQ consumer. An authenticated user could exploit this insecure direct object reference (IDOR) to retrieve VNode endpoint assignments for other users' consumers. The fix verifies consumer ownership or authorization before handling the request.

## GHSA-gmhq-7r4w-7rp5 (TD-SEC-2026-031)

1. Basic information

    - Severity: Medium
    - GHSA: `GHSA-gmhq-7r4w-7rp5`
    - Title: Integer overflow and boundary-check bypass in INSERT deserialization (`tDecodeColDataVersion0` / `tDecodeBinaryWithSize`)
    - Published: 2026-09-28
    - CWE: `CWE-190` Integer Overflow / `CWE-125` Out-of-bounds Read
    - Reporter: External security researcher

2. Affected versions

    - `>= v3.4.0.0, <= v3.4.2.12`

3. Fixed version

    - `v3.4.2.13`

4. Summary

    Integer overflows and a boundary-check bypass in the INSERT deserialization routines `tDecodeColDataVersion0()` and `tDecodeBinaryWithSize()` allow crafted INSERT payloads to trigger out-of-bounds access, crashing `taosd` or corrupting data. Length fields are now validated with overflow-safe checks.

## GHSA-pc9g-w85r-7c6x (TD-SEC-2026-035)

1. Basic information

    - Severity: Medium
    - GHSA: `GHSA-pc9g-w85r-7c6x`
    - Title: Heap buffer overflow in the `msgToDatum()` DECIMAL branch via a crafted query plan
    - Published: 2026-09-28
    - CWE: `CWE-122` Heap-based Buffer Overflow / `CWE-787` Out-of-bounds Write
    - Reporter: External security researcher

2. Affected versions

    - `>= v3.0.0.0, <= v3.4.2.12`

3. Fixed version

    - `v3.4.2.13`

4. Summary

    The DECIMAL branch of `msgToDatum()` can be driven into a heap buffer overflow through a crafted `TDMT_SCH_QUERY` query plan TLV. An authenticated remote attacker can crash `taosd` (DoS). The fix consistency-checks DECIMAL field lengths against the target buffer.

## GHSA-382r-9pxq-7vg2 (TD-SEC-2026-036)

1. Basic information

    - Severity: High
    - GHSA: `GHSA-382r-9pxq-7vg2`
    - Title: Missing length validation in `mndProcessStreamHb` causes integer underflow and heap out-of-bounds read
    - Published: 2026-09-28
    - CWE: `CWE-191` Integer Underflow / `CWE-125` Out-of-bounds Read
    - Reporter: External security researcher

2. Affected versions

    - `>= v3.4.2.8, <= v3.4.2.12`

3. Fixed version

    - `v3.4.2.13`

4. Summary

    `mndProcessStreamHb` did not validate the message length when processing `TDMT_MND_STREAM_HEARTBEAT` messages with an empty body, causing an integer underflow that leads to a heap out-of-bounds read and an mnode crash (DoS). The fix validates the message body length before deserialization.

## GHSA-c483-rvf9-p53q (TD-SEC-2026-038)

1. Basic information

    - Severity: Medium
    - GHSA: `GHSA-c483-rvf9-p53q`
    - Title: Server-side request forgery (SSRF) in `CREATE ANODE` URL handling
    - Published: 2026-09-28
    - CWE: `CWE-918` Server-Side Request Forgery
    - Reporter: External security researcher

2. Affected versions

    - `>= v3.0.0.0, <= v3.4.2.12`

3. Fixed version

    - `v3.4.2.13`

4. Summary

    The URL handling of the `CREATE ANODE` statement lacked destination validation, allowing server-side request forgery (SSRF). An authenticated user could make the server issue requests to arbitrary internal or external addresses, probing the internal network or reaching restricted services. The fix applies allow-list and destination checks to the URL.

## GHSA-j63p-9gcr-5fv8 (TD-SEC-2026-040)

1. Basic information

    - Severity: High
    - GHSA: `GHSA-j63p-9gcr-5fv8`
    - Title: Stack buffer overflow in `createHintNodeList()` via SQL hint with more than 10 parameters
    - Published: 2026-09-28
    - CWE: `CWE-121` Stack-based Buffer Overflow / `CWE-787` Out-of-bounds Write
    - Reporter: External security researcher

2. Affected versions

    - `>= v3.0.0.0, <= v3.4.2.12`

3. Fixed version

    - `v3.4.2.13`

4. Summary

    `createHintNodeList()` uses a fixed-size stack buffer when processing SQL hints; a hint with more than 10 parameters overflows the buffer and crashes `taosd` (DoS). The fix caps the number of hint parameters and uses dynamic allocation.

## GHSA-f939-x5h3-cg4f (TD-SEC-2026-041)

1. Basic information

    - Severity: High
    - GHSA: `GHSA-f939-x5h3-cg4f`
    - Title: Integer overflow in `translateRepeat` leads to heap buffer overflow in `repeatFunction`
    - Published: 2026-09-28
    - CWE: `CWE-190` Integer Overflow / `CWE-680` Integer Overflow to Buffer Overflow / `CWE-787` Out-of-bounds Write
    - Reporter: External security researcher

2. Affected versions

    - `>= v3.4.2.0, <= v3.4.2.12`

3. Fixed version

    - `v3.4.2.13`

4. Summary

    `translateRepeat` miscalculates the output length of the REPEAT function with an integer overflow; `repeatFunction` then allocates the wrapped length and writes past the heap buffer, causing remote denial of service (server crash). The fix uses overflow-safe length computation.

## GHSA-qh9p-wprq-p2hf (TD-SEC-2026-046)

1. Basic information

    - Severity: High
    - GHSA: `GHSA-qh9p-wprq-p2hf`
    - Title: Heap buffer overflow in the `tDeserializeSCreateFuncReq()` pCode field
    - Published: 2026-09-28
    - CWE: `CWE-122` Heap-based Buffer Overflow
    - Reporter: External security researcher

2. Affected versions

    - All versions (`<= v3.3.6.64`, `>= v3.4.0.0, <= v3.4.2.12`)

3. Fixed versions

    - `v3.3.6.65` / `v3.4.2.13`

4. Summary

    In `tDeserializeSCreateFuncReq()` the pCode field length is inconsistent with the allocated memory size, triggering a heap buffer overflow. An unauthenticated remote attacker can crash `taosd` (DoS). The fix allocates based on the actual length and validates it.

## GHSA-8c53-8w6m-w28r (TD-SEC-2026-047)

1. Basic information

    - Severity: Critical
    - GHSA: `GHSA-8c53-8w6m-w28r`
    - Title: Stack buffer overflow in `tDeserializeSCreateFuncReq()` allows unauthenticated remote code execution
    - Published: 2026-09-28
    - CWE: `CWE-121` Stack-based Buffer Overflow
    - Reporter: External security researcher

2. Affected versions

    - `>= v3.0.0.0, <= v3.3.6.64`, `>= v3.4.0.0, <= v3.4.2.12`

3. Fixed versions

    - `v3.3.6.65` / `v3.4.2.13`

4. Summary

    `tDeserializeSCreateFuncReq()` does not bound-check the deserialized name field, allowing a stack buffer overflow. An unauthenticated remote attacker can crash `taosd` and potentially achieve remote code execution (RCE). The fix validates the name field length.

## GHSA-4w45-hf5j-95m3 (TD-SEC-2026-048)

1. Basic information

    - Severity: High
    - CVSS: 8.8
    - GHSA: `GHSA-4w45-hf5j-95m3`
    - Title: Path traversal in the TDGpt installer (`tarfile.extractall`) allows arbitrary file write
    - Published: 2026-09-28
    - CWE: `CWE-22` Path Traversal
    - Reporter: External security researcher

2. Affected versions

    - `>= v3.4.2.9, < v3.4.2.12` (TDGpt component)

3. Fixed version

    - `v3.4.2.12` (TDGpt component)

4. Summary

    The TDGpt installer script `tools/tdgpt/script/install.py` calls `tarfile.extractall()` / `zipfile.extractall()` on model resource archives without validating member paths. An attacker can write arbitrary files outside the extraction directory using malicious archives containing `../` traversal paths; installation usually runs as root, so system files can be overwritten. The fix validates all member paths before extraction and rejects traversal.

## GHSA-x8q3-55fg-4pwh (TD-SEC-2026-049)

1. Basic information

    - Severity: Medium
    - GHSA: `GHSA-x8q3-55fg-4pwh`
    - Title: Stack buffer overflow in `tDecodeCStrTo`
    - Published: 2026-09-28
    - CWE: `CWE-121` Stack-based Buffer Overflow
    - Reporter: External security researcher

2. Affected versions

    - All versions (`<= v3.3.6.64`, `>= v3.4.0.0, <= v3.4.2.12`)

3. Fixed versions

    - `v3.3.6.65` / `v3.4.2.13`

4. Summary

    `tDecodeCStrTo` decodes strings without checking the destination buffer length, triggering a stack buffer overflow. An authenticated remote attacker can crash `taosd` or execute arbitrary code. The fix adds destination-buffer length validation.

## GHSA-9m72-77jp-x5f2 (TD-SEC-2026-050)

1. Basic information

    - Severity: High
    - GHSA: `GHSA-9m72-77jp-x5f2`
    - Title: Unauthenticated SQL injection in the taosKeeper audit endpoint allows audit log tampering
    - Published: 2026-09-28
    - CWE: `CWE-89` SQL Injection
    - Reporter: External security researcher

2. Affected versions

    - `>= v3.3.6.0, <= v3.4.2.12` (taosKeeper)

3. Fixed version

    - `v3.4.2.13`

4. Summary

    The taosKeeper audit endpoint lacked authentication and the metric/audit ingestion handlers were vulnerable to SQL injection. An unauthenticated remote attacker could execute arbitrary SQL against the backend TDengine instance and tamper with audit logs. The fix adds authentication to the audit endpoint and uses parameterized queries in the ingestion path.

## GHSA-2hff-w2r4-f7g5 (TD-SEC-2026-051)

1. Basic information

    - Severity: Medium
    - GHSA: `GHSA-2hff-w2r4-f7g5`
    - Title: Vendored zstd 1.3.5 in contrib/TSZ falls within the affected range of CVE-2019-11922
    - Published: 2026-09-28
    - CWE: `CWE-362` Concurrent Execution using Shared Resource with Improper Synchronization
    - Reporter: TDengine security team

2. Affected versions

    - `<= v3.4.2.12` (contrib/TSZ/zstd 1.3.5)

3. Fixed version

    - `v3.4.2.13` (zstd upgraded to 1.3.8 or later)

4. Summary

    The zstd library vendored in contrib/TSZ is version 1.3.5, which falls within the affected range of CVE-2019-11922 (a compression/decompression race condition, `< 1.3.8`). This is a library-level flaw whose exploitation depends on specific concurrency timing. The fix upgrades the vendored zstd to 1.3.8 or later.
