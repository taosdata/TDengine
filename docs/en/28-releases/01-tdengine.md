---
sidebar_label: TDengine
title: TDengine Release History and Download Links
description: TDengine Release History, Release Notes, and Download Links
slug: /release-history/tdengine
---

## TDengine Versioning Rules

The TDengine version number consists of four digits separated by dots, defined as follows:

- `[Major+].[Major].[Feature].[Maintenance]`
- `Major+`: Indicates a major overhaul of the product that cannot be upgraded directly. If an upgrade is needed, please contact the TDengine customer support team.
- `Major`: Indicates significant new features. Upgrading does not support rolling upgrades, and rolling back is not possible after the upgrade (for example, once upgraded from 3.2.3.0 to 3.3.0.0, it cannot be rolled back).
- `Feature`: Indicates new features. Upgrading does not support rolling upgrades, but it is possible to roll back after upgrading from different Feature Releases of the same Major Release (for example, after upgrading from 3.3.0.0 to 3.3.1.0, it can be rolled back to 3.3.0.0). The client driver (libtaos.so) must be upgraded in sync with the server.
- `Maintenance`: Indicates that there are no new features, only defect fixes, and supports rolling upgrades. It can be rolled back after the upgrade.
- Rolling upgrade: In a cluster composed of three or more nodes using triple replication, one node is stopped for upgrading, then restarted, and this process is repeated until all nodes in the cluster are upgraded. During the upgrade, the cluster can continue to provide services. For version changes that do not support rolling upgrades, the entire cluster must be stopped, all nodes in the cluster must be upgraded, and then the entire cluster must be started. During the upgrade, the cluster cannot provide external services.

## TDengine 2.x Download

For installation packages of TDengine 2.x versions, please visit [here](https://tdengine.com/downloads/historical/).

## TDengine 3.x Download

The download links for installation packages of TDengine 3.x versions are as follows:

import Release from "/components/ReleaseV3";

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
