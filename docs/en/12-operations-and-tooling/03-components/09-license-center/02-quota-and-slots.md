---
title: Quotas and Slots
sidebar_label: Quotas and Slots
toc_max_heading_level: 4
---

This page describes entitlement, slots, and multi-instance behavior under License Center. For procedures, see [Deploy and Activate](./01-deploy-and-activate.md).

## Concepts

| Concept | Description |
| ------- | ----------- |
| License | An entitlement credential issued by ELS and held by CLS; usually maps to one order, with a type and validity period |
| License limit | The overall entitlement cap on one license; the sum of per-slot quotas under that license must not exceed this cap |
| Slot | A quota unit under a license. A license has a fixed number of slots (or a maximum slot count). Each slot can carry its own quota for TSDB / IDMP instances |
| Slot quota | The authorization-item limits on a slot (for example TSDB Quota and IDMP Quota in the UI); follow ELS/CLS configuration and the Quota page |

TSDB and IDMP quotas are independent and accumulate separately. With multiple instances, CLS aggregates consumption per instance. Utilization can be read as current usage / corresponding limit.

## One CLS and many instances

One software license can include multiple slots. Each slot has its own quota. An instance selects a license and slot through `clsLicenseId` and `clsQuotaSlotId`. The sum of slot quotas must stay within the license limit. Instances must communicate with CLS.

Instances may run on different hosts, or belong to different customers or clusters. Licensing enforces license and slot quotas; it does not model deployment topology.

## What CLS adjusts vs what ELS adjusts

| What changes | Where | Notes |
| ------------ | ----- | ----- |
| Slot count (maximum number of slots) | ELS | Only ELS can change how many slots CLS is allowed to use |
| Quota on each slot | CLS | Within the license limit, adjust each slot’s quota on CLS |
| Adding slots beyond the current maximum | ELS | CLS does not increase the slot count by itself; raise the maximum on ELS when more slots are required |

When CLS is online, structural changes such as the maximum slot count can sync from ELS. Offline environments follow the import flow for updated license material.

## Instance teardown and quota reuse

On the product license page, users typically enter:

1. License Key / ID (required)
2. Slot (used under a multi-slot license to select which slot this instance uses)

After an instance stops or is deleted, it no longer contributes runtime usage. A new instance can use a free slot, or you can rebalance quotas across slots on CLS so the sum remains within the license limit.

If the current maximum slot count is not enough, raise the maximum on ELS, then set quotas for the new slots on CLS and connect instances.

## Multiple license types

One license has one type and one validity window, usually one order. The same customer may hold multiple licenses (for example trial and paid, or different product lines), imported or synced separately in CLS. Which license and slot an instance uses depends on the instance configuration.

## Planning notes

- On ELS, provision a maximum slot count that covers the expected concurrent instances.
- On CLS, assign quotas to slots so the sum stays within the license limit.
- When downstream instances are added, removed, or moved, prefer rebalancing slot quotas or using free slots on CLS; go back to ELS when the maximum slot count must increase.

For usage views, see [Usage and Availability](./03-usage-and-availability.md).
