---
title: FAQ
sidebar_label: FAQ
toc_max_heading_level: 4
---

This page collects common questions about using License Center. For the licensing flow and basic concepts, see [Deploy and Activate](./01-deploy-and-activate.md) and [Quotas and Slots](./02-quota-and-slots.md).

## 1. License Expiry

### 1.1 Why did my all-in-one free license expire after 30 days while TSDB still works?

Two questions here:

1. **Early license expiry**: The free all-in-one license is normally valid for one year. If it expires after 30 days, the expiration time was usually adjusted on the backend (for example, for testing). If a license expires unexpectedly early, contact your TDengine service provider to verify the license information.
2. **TSDB still works after expiry**: IDMP and TSDB are essentially two separate systems. After a license expires, TSDB enters a grace period, while IDMP does not implement this feature yet. It is therefore possible to see IDMP expired while TSDB is still usable. Grace-period behavior depends on the specific product version and license type.

## 2. Frontend Errors

### 2.1 What does the IDMP frontend error `Request quota slot conflict` mean?

This error means the slot is already occupied: the same license can be used by multiple clusters, and reusing a slot already occupied by another cluster triggers this error. It is common during version upgrades or abnormal installation and usage scenarios.

Resolution: Log in to CLS, delete the occupied slot on the **Cluster** page, then retry. See [Deploy and Activate](./01-deploy-and-activate.md) for cluster management.

## 3. License Code Import

### 3.1 What should I do when CLS fails to parse the license code?

CLS failures to parse a license code are mostly caused by paste errors when entering the license code manually (for example, incomplete copy, stray spaces, or newlines). Use file upload instead of manual entry to avoid such errors.

## 4. Offline Licensing

### 4.1 How do I activate a free license in an offline environment?

Generate an offline license file on ELS and import it into CLS to complete activation. For steps, see [Deploy and Activate](./01-deploy-and-activate.md), "Offline Licensing".

### 4.2 How do I import a license when the customer network is restricted and cannot reach ELS?

Use offline mode: ELS generates an offline license file and delivers it to the customer, who completes authorization by clicking **Offline Import** on the CLS **License** page. For steps, see [Deploy and Activate](./01-deploy-and-activate.md).

## 5. Licensing Flow and Rules

### 5.1 How does ELS issue a license? How is CLS handled in offline scenarios?

License issuance, renewal, and revocation on the ELS side are performed by the TDengine service provider. For the overall flow, see [License Center Reference](./index.md). For offline licensing in CLS, see [Deploy and Activate](./01-deploy-and-activate.md), "Offline Licensing".

## 6. Version Query

### 6.1 How do I check the CLS version?

Run `taoscls -V` on the host where CLS is deployed:

```bash
taoscls -V
taoscls version: 1.0.5 (core-1.0.5)
git: 2befd19ee17194c8aa460eb30ee98a3508030d95
build: linux-x86_64 2026-07-31 10:24:41 +08:00
```

The `version` field in the output is the CLS version.

## Related Pages

- [License Center Reference](./index.md)
- [Deploy and Activate](./01-deploy-and-activate.md)
- [Quotas and Slots](./02-quota-and-slots.md)
- [Usage and Availability](./03-usage-and-availability.md)
