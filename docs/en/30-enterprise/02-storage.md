---
title: Tiered Storage
sidebar_label: Tiered Storage 
toc_max_heading_level: 4
---

## Introduction

This article describes how to use tiered storage in TDengine. With tiered storage, you can balance data accessibility and storage costs by moving older data to less expensive storage media automatically.

## Configuration

TDengine supports three tiers of storage. Each tier can include 16 mount points.

To configure tiered storage, modify your `taos.cfg` file as follows:

```
dataDir [path] <level> <primary>
```

- path: The path to a mount point
- level: The tier for the specified mount point. Enter 0, 1, or 2.
  Tier 0 contains the latest data, tier 1 contains older data, and tier 2 contains the oldest data. The default value is 0.
  As data ages, it is moved from tier 0 to tier 1 and then to tier 2.
  You can mount multiple disks in a single tier. The data stored on each tier is distributed among all disks associated with the tier.
  Note that TDengine moves between tiers automatically.
- primary: Whether the specified mount point is the primary mount point. Enter 0 for false or 1 for true. The default value is 1.

A TDengine cluster can have only one primary mount point, which must be on tier 0. An example configuration is as follows:

```
dataDir /mnt/data1 0 1
dataDir /mnt/data2 0 0
dataDir /mnt/data3 1 0
dataDir /mnt/data4 1 0
dataDir /mnt/data5 2 0
dataDir /mnt/data6 2 0
```

:::note

1. Skipping tiers is not allowed. Your configuration can have tier 0 storage only, tier 0 and tier 1 storage, or tier 0, 1, and 2 storage. You cannot configure tier 1 storage without tier 0 storage or tier 2 storage without tier 0 and tier 1 storage.
2. You cannot manually remove mount points that are in use. You cannot mount network disks.
3. You cannot remove disks that have been mounted.

:::

## Load Balancing

System metadata is stored on the primary mount point. The root directory of each vnode is stored on the primary mount point of the associated dnode. For this reason, data ingestion performance on each dnode is limited by the I/O throughput of the primary mount point.

In TDengine 3.1.0.0 and later, the root directories of the vnodes on a dnode are distributed among all tier 0 storage. There is a direct correlation between the number of tier 0 storage devices and the ingestion performance of the system. To improve write performance, add tier 0 storage devices to your dnodes.

## Disk Selection Within Tiers

TDengine uses a round robin policy to select the mount point that it uses to store new data files. This policy can become a problem if certain disks on a tier have less remaining space than others, as TDengine may select a disk that is almost full. You can specify the `minDiskFreeSize` parameter to set a minimum threshold for remaining disk space, after which TDengine will no longer store new data files on disks that do not meet the threshold. Specify a value in bytes. It is recommended that you set this value to 2 GB or higher.