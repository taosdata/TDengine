---
title: License Center Reference
sidebar_label: License Center
toc_max_heading_level: 4
---

License Center manages licensing for TDengine TSDB / IDMP. It has two sides:

| Component | Full name | Where it runs | Role |
| --------- | --------- | ------------- | ---- |
| ELS | Enterprise License Server | TDengine service provider | Issues, renews, and revokes licenses; manages overall entitlement and audit; sets the maximum slot count for a license |
| CLS | Customer License Server | Customer premises (or hosted environment) | Holds licenses; grants quotas to local TSDB / IDMP instances and aggregates usage; adjusts per-slot quotas within the license limit |

Typical path: ELS manages overall entitlement and the maximum slot count → the customer deploys CLS and imports or syncs the license → multiple TSDB and IDMP instances connect to the local CLS, request authorization by quota slot, and report heartbeat and usage.

:::note
License IDs, quota IDs, and addresses in this directory are examples only and have no real business meaning.
:::

## Relationship to activation-code licensing

TDengine TSDB Enterprise can also activate a cluster with an activation code. See [Activate TDengine TSDB-Enterprise](../../02-operations/03-deployment/04-activate.md).

| Method | Typical use |
| ------ | ----------- |
| Activation code | One or a few clusters; the provider issues a code from machine information |
| License Center (ELS + CLS) | Unified multi-instance quotas, online renew/revoke, and local usage views—for example a hoster serving multiple downstream systems from one environment |

If a local CLS will serve multiple TSDB / IDMP instances, read [Quotas and Slots](./02-quota-and-slots.md), then follow [Deploy and Activate](./01-deploy-and-activate.md).

## Document map

| Page | Contents |
| ---- | -------- |
| [Deploy and Activate](./01-deploy-and-activate.md) | Install CLS, configure, online/offline import, connect TSDB/IDMP |
| [Quotas and Slots](./02-quota-and-slots.md) | Slot count vs per-slot quota, multi-instance and license types |
| [Usage and Availability](./03-usage-and-availability.md) | Instance usage views, sync to ELS, CLS deployment shape |

## Model summary

- One CLS can serve multiple independent TSDB / IDMP instances. One license can have multiple slots; the sum of slot quotas must stay within the license limit.
- TSDB and IDMP quotas are independent. The same customer may hold multiple licenses of different types or validity periods.
- The maximum slot count is set on ELS. Per-slot quotas are adjusted on CLS and must stay within the license limit.
- CLS can show instance connectivity and usage and sync to ELS when online. CLS currently runs as a single-node service.

Details: [Quotas and Slots](./02-quota-and-slots.md) and [Usage and Availability](./03-usage-and-availability.md).
