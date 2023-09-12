---
title: Tiered Storage
sidebar_label: Tiered Storage 
toc_max_heading_level: 4
---

## Introduction

This section describes TDengine Enterprise's unique Multi-Level Storage feature, which is designed to store more recent, hot data on high-speed media, and older, less hot data on low-cost media, thus achieving multiple, seemingly contradictory goals at the same time: improving access efficiency, expanding storage space, and controlling costs.

## Configuration

Multi-level storage supports 3 levels with up to 16 mount points per level.

TDengine multi-level storage is configured in the following way (in the configuration file /etc/taos/taos.cfg):

```
dataDir [path] <level> <primary>
```

- path: Folder path of the mount point
- level: The media storage level, which takes the values 0, 1, and 2.
  Level 0 stores the newest data, level 1 stores the next newest data, level 2 stores the oldest data, and omitted defaults to 0.
  Data flow between levels of storage: level 0 storage -> level 1 storage -> level 2 storage.
  Multiple hard disks can be mounted on the same storage level, and data files on the same storage level are distributed across all hard disks in that storage level.
  It should be noted that the movement of data across different levels of storage media is done automatically by the system without user intervention.
- primary: Whether or not it is the primary mount point, 0 (no) or 1 (yes), omitted defaults to 1.

In the configuration, only one primary mount point is allowed (level=0, primary=1), e.g. using the following configuration:

```
dataDir /mnt/data1 0 1
dataDir /mnt/data2 0 0
dataDir /mnt/data3 1 0
dataDir /mnt/data4 1 0
dataDir /mnt/data5 2 0
dataDir /mnt/data6 2 0
```

:::note

1. Multi-level storage does not allow cross-level configurations, and the legal configuration options are: level 0 only, level 0 + level 1 only, and level 0 + level 1 + level 2. Instead, it is not allowed to configure only level=0 and level=2 without level=1.
2. Prohibit manual removal of active mount disks, which currently do not support non-local network disks.
3. Multi-level storage does not currently support the ability to delete mounted hard disks.

:::

## Load Balancing

In multilevel storage, there is only one primary mount point, which holds the most important metadata in the system, and the home directory of each vnode exists on the current dnode's primary mount point, which results in the dnode's write performance being limited by the IO throughput capacity of a single disk.

Starting from TDengine 3.1.0.0, if a dnode is configured with more than one level 0 mount point, we distribute the home directories of all the vnodes on the dnode to all level 0 mount points in a balanced way, and these level 0 mount points share the write load. Under the condition that the network I/O and other processing resources do not become bottlenecks, through the optimization of cluster configuration, the test results prove that the write capacity of the whole system and the number of level 0 mount points show a linear relationship, that is, with the increase of the number of level 0 mount points, the write capacity of the whole system also increases exponentially.

## Sibling mount point selection strategy

In general, when TDengine wants to select one of the sibling mount points for generating a new data file, the round robin policy is used for selection. However, in reality, the capacity of each disk may not be the same, or the capacity is the same but the amount of data written to it is not the same, which will lead to an imbalance in the available space on each disk, and in the actual selection, it is possible to select a disk that has very little space left. To address this issue, a new configuration `minDiskFreeSize` has been introduced since 3.1.1.0, whereby when the free space on a disk is less than or equal to this threshold, that disk will no longer be selected for generating new data files. This configuration item is in bytes and should have a value greater than 2GB, i.e. it will skip mount points with less than 2GB of free space.