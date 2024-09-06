---
title: TDengine Release History and Download Links
sidebar_label: TDengine
description: This document provides download links for all released versions of TDengine 3.0.
---

## TDengine Version Rules

TDengine version number consists of 4 numbers separated by `.`, defined as below:
- `[Major+].[Major].[Feature].[Maintenance]`
- `Major+`: Significant rearchitecture release, can't be upgraded from an old version with different `Major+` number. If you have such a need, please contact TDengine support team.
- `Major`: Important new feature release, can't be rolling upgraded from old version with different `Major` number, and can't be rolled back after upgrading. For example, after upgrading from `3.2.3.0` to `3.3.0.0`, you can't roll back to `3.2.3.0`. 
- `Feature`：New feature release, can't be rolling upgraded from an old version with different `Feature` number, but can be rolled back after upgrading. For example, after upgrading from `3.3.0.0` to `3.3.1.0`, you can roll back to `3.3.0.0`. The client driver (libtaos.so) must be upgraded to same version as the server side (taosd).
- `Maintenance`: Maintenance release, no new features but only bug fixings, can be rolling upgraded from an old version with only `Maintenance` number different, and can be rolled back after upgrading. 
- `Rolling Upgrade`: For a cluster consisting of three or more dnodes with three replica enabled, you can upgrade one dnode each time by stopping it, upgrading it, and then restarting it, repeat this process to upgrade the whole cluster. During this period, the cluster is still in service. If rolling upgrade is not supported based on the above version number rules, you need to first stop the whole cluster, upgrade all dndoes, and restart all dnodes after upgrading. During this period, the cluster is out of service.

TDengine 3.x installation packages can be downloaded at the following links:

For TDengine 2.x installation packages by version, please visit [here](https://tdengine.com/downloads/historical/).

import Release from "/components/ReleaseV3";

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

:::note IMPORTANT
- Once you upgrade to TDengine 3.1.0.0, you cannot roll back to any previous version of TDengine. Upgrading to 3.1.0.0 will alter your data such that it cannot be read by previous versions.
- You must remove all streams before upgrading to TDengine 3.1.0.0. If you upgrade a deployment that contains streams, the upgrade will fail and your deployment will become nonoperational.
:::

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
