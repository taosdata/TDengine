---
sidebar_label: TDengine
title: TDengine Release History and Download Links
slug: /release-history/tdengine
---

## TDengine Versioning Rules

The TDengine version number consists of four digits separated by dots, defined as follows:

- `[Major+].[Major].[Feature].[Maintenance]`
- `Major+`: Significant restructuring of the product, direct upgrade is not possible, contact TDengine customer support team if upgrade is needed
- `Major`: Major new features, does not support rolling upgrades, and is not reversible after upgrade, e.g., cannot revert back after upgrading from 3.2.3.0 to 3.3.0.0
- `Feature`: New features, does not support rolling upgrades, but can revert back after upgrading from the same Major Release to a different Feature Release, e.g., can revert back to 3.3.0.0 after upgrading to 3.3.1.0. Client drivers (libtaos.so) and servers need to be upgraded simultaneously.
- `Maintenance`: No new features, only bug fixes, supports rolling upgrades, and is reversible after upgrade
- Rolling upgrade: For clusters composed of three or more nodes using three replicas, each node is stopped, upgraded, and restarted one at a time, repeating this process until all nodes in the cluster are upgraded. The cluster can still provide services during the upgrade. For versions that do not support rolling upgrades, the entire cluster must be stopped, all nodes upgraded, and then the entire cluster restarted. The cluster cannot provide services during the upgrade.

## TDengine 2.x Downloads

For TDengine 2.x version installation packages, see [Download Historical Versions](https://tdengine.com/downloads/historical/).

## TDengine 3.x Downloads

For TDengine 3.3.7.0 and later versions, see [Download Center](https://www.tdengine.com/download-center).

Download links for TDengine 3.x version installation packages are as follows:

import Release from "/components/ReleaseV3";

## 3.3.6.13

<Release type="tdengine" version="3.3.6.13" />

## 3.3.6.9

<Release type="tdengine" version="3.3.6.9" />

## 3.3.6.6

<Release type="tdengine" version="3.3.6.6" />

## 3.3.6.3

<Release type="tdengine" version="3.3.6.3" />

## 3.3.6.0

<Release type="tdengine" version="3.3.6.0" />

## 3.3.5.8

<Release type="tdengine" version="3.3.5.8" />

## 3.3.5.2

<Release type="tdengine" version="3.3.5.2" />

## 3.3.5.0

<Release type="tdengine" version="3.3.5.0" />

## 3.3.4.8

<Release type="tdengine" version="3.3.4.8" />

## 3.3.4.3

<Release type="tdengine" version="3.3.4.3" />

## 3.3.3.0

<Release type="tdengine" version="3.3.3.0" />

## 3.3.2.0

<Release type="tdengine" version="3.3.2.0" />

## 3.3.1.0

<Release type="tdengine" version="3.3.1.0" />

## 3.3.0.3

<Release type="tdengine" version="3.3.0.3" />

## 3.3.0.0

<Release type="tdengine" version="3.3.0.0" />

## 3.2.3.0

<Release type="tdengine" version="3.2.3.0" />

## 3.2.2.0

<Release type="tdengine" version="3.2.2.0" />

## 3.2.1.0

<Release type="tdengine" version="3.2.1.0" />

## 3.2.0.0

<Release type="tdengine" version="3.2.0.0" />

## 3.1.1.0

<Release type="tdengine" version="3.1.1.0" />

## 3.1.0.3

<Release type="tdengine" version="3.1.0.3" />

## 3.1.0.2

<Release type="tdengine" version="3.1.0.2" />

## 3.1.0.0

<Release type="tdengine" version="3.1.0.0" />

## 3.0.7.1

<Release type="tdengine" version="3.0.7.1" />

## 3.0.7.0

<Release type="tdengine" version="3.0.7.0" />

## 3.0.6.0

<Release type="tdengine" version="3.0.6.0" />

## 3.0.5.1

<Release type="tdengine" version="3.0.5.1" />

## 3.0.5.0

<Release type="tdengine" version="3.0.5.0" />

## 3.0.4.2

<Release type="tdengine" version="3.0.4.2" />

## 3.0.4.1

<Release type="tdengine" version="3.0.4.1" />

## 3.0.4.0

<Release type="tdengine" version="3.0.4.0" />

## 3.0.3.2

<Release type="tdengine" version="3.0.3.2" />

## 3.0.3.1

<Release type="tdengine" version="3.0.3.1" />

## 3.0.3.0

<Release type="tdengine" version="3.0.3.0" />

## 3.0.2.6

<Release type="tdengine" version="3.0.2.6" />

## 3.0.2.5

<Release type="tdengine" version="3.0.2.5" />

## 3.0.2.4

<Release type="tdengine" version="3.0.2.4" />

## 3.0.2.3

<Release type="tdengine" version="3.0.2.3" />

## 3.0.2.2

<Release type="tdengine" version="3.0.2.2" />

## 3.0.2.1

<Release type="tdengine" version="3.0.2.1" />

## 3.0.2.0

<Release type="tdengine" version="3.0.2.0" />

## 3.0.1.8

<Release type="tdengine" version="3.0.1.8" />

## 3.0.1.7

<Release type="tdengine" version="3.0.1.7" />

## 3.0.1.6

<Release type="tdengine" version="3.0.1.6" />

## 3.0.1.5

<Release type="tdengine" version="3.0.1.5" />

## 3.0.1.4

<Release type="tdengine" version="3.0.1.4" />

## 3.0.1.3

<Release type="tdengine" version="3.0.1.3" />

## 3.0.1.2

<Release type="tdengine" version="3.0.1.2" />

## 3.0.1.1

<Release type="tdengine" version="3.0.1.1" />

## 3.0.1.0

<Release type="tdengine" version="3.0.1.0" />
