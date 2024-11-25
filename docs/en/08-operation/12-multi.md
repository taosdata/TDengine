---
title: Advanced Storage Options
slug: /operations-and-maintenance/advanced-storage-options
---

This section introduces the unique multi-level storage feature of TDengine Enterprise, which aims to store frequently accessed hot data on high-speed media while placing older, less frequently accessed cold data on low-cost media, achieving the following goals:

- **Reduce Storage Costs** – By tiering data storage, vast amounts of extremely cold data can be stored on inexpensive media, resulting in significant economic benefits.
- **Enhance Write Performance** – Each level of storage supports multiple mount points, and the WAL (Write-Ahead Logging) mechanism also supports parallel writing with multiple mount points at level 0, significantly improving write performance (in real scenarios, it supports continuous writing of over 300 million data points per second), achieving high disk I/O throughput on mechanical hard drives (measured up to 2GB/s).
- **Ease of Maintenance** – After configuring the mount points for each storage level, tasks such as system data migration can be performed without manual intervention; storage expansion is more flexible and convenient.
- **Transparent to SQL** – Regardless of whether the queried data spans levels, a single SQL statement can return all data simply and efficiently.

All storage media involved in multi-level storage are local storage devices. In addition to local storage devices, TDengine Enterprise also supports using object storage (S3) to keep the coldest batch of data on the cheapest media to further reduce storage costs while still allowing queries when necessary, with the data storage location being transparent to SQL. Support for object storage was first released in version 3.3.0.0, and using the latest version is recommended.

## Multi-Level Storage

### Configuration Method

Multi-level storage supports three levels, with a maximum of 128 mount points configurable for each level.

:::tip

Typical configuration schemes include: level 0 configured with multiple mount points, each corresponding to a single SAS hard drive; level 1 configured with multiple mount points, each corresponding to a single or multiple SATA hard drives; level 2 can be configured with S3 storage or other inexpensive network storage.

:::

TDengine multi-level storage configuration is as follows (in the configuration file `/etc/taos/taos.cfg`):

```shell
dataDir [path] <level> <primary>
```

- **path**: The folder path of the mount point.
- **level**: The storage level of the media, with values of 0, 1, or 2. Level 0 stores the newest data, level 1 stores the next newest data, and level 2 stores the oldest data, defaulting to 0 if omitted. The data flow direction between storage levels is: level 0 storage -> level 1 storage -> level 2 storage. Multiple hard drives can be mounted at the same storage level, and data files at the same storage level are distributed across all hard drives of that level. It is important to note that the movement of data between different levels of storage media is done automatically by the system, and user intervention is not required.
- **primary**: Indicates whether it is the primary mount point, with values of 0 (no) or 1 (yes), defaulting to 1 if omitted. Only one primary mount point is allowed in the configuration (level=0, primary=1). For example, the following configuration can be used:

```shell
dataDir /mnt/data1 0 1
dataDir /mnt/data2 0 0
dataDir /mnt/data3 1 0
dataDir /mnt/data4 1 0
dataDir /mnt/data5 2 0
dataDir /mnt/data6 2 0
```

:::note

1. Multi-level storage does not allow cross-level configuration; valid configurations include: only level 0, only level 0 + level 1, and level 0 + level 1 + level 2. Configurations that only include level 0 and level 2 without level 1 are not allowed.
2. Manually removing in-use mounted disks is prohibited; currently, mounted disks do not support non-local network disks.

:::

### Load Balancing

In multi-level storage, there is only one primary mount point, which is responsible for the most important metadata storage in the system. At the same time, the main directories of all vnodes exist on the current dnode's primary mount point, which limits the writing performance of that dnode to the I/O throughput capability of a single disk.

Starting from TDengine 3.1.0.0, if a dnode is configured with multiple level 0 mount points, the main directories of all vnodes on that dnode will be evenly distributed across all level 0 mount points, allowing these level 0 mount points to collectively bear the write load.

In the absence of network I/O and other processing resource bottlenecks, optimizing cluster configuration has proven through testing that the overall write capacity of the system has a linear relationship with the number of level 0 mount points; that is, as the number of level 0 mount points increases, the overall write capacity of the system also increases exponentially.

### Same-Level Mount Point Selection Strategy

Generally, when TDengine needs to select one from the same-level mount points to generate a new data file, a round-robin strategy is employed. However, in reality, each disk may have different capacities, or the same capacity but different amounts of written data, which can lead to imbalances in available space on each disk. This may result in selecting a disk with very little remaining space during the actual selection process.

To address this issue, starting from version 3.1.1.0, a new configuration option `minDiskFreeSize` was introduced. When the available space on a disk falls below or is equal to this threshold, that disk will no longer be selected for generating new data files. The unit of this configuration item is in bytes, and its value should be greater than 2GB, which means it will skip mount points with available space less than 2GB.

Starting from version 3.3.2.0, a new configuration option `disable_create_new_file` was introduced to control whether to prohibit the creation of new files on a specific mount point. The default value is false, meaning new files can be created on each mount point by default.

## Object Storage

This section describes how to use S3 object storage in TDengine Enterprise. This feature is implemented based on the general S3 SDK, with compatibility adjustments made to the access parameters for various S3 platforms, allowing access to object storage services such as Minio, Tencent Cloud COS, and Amazon S3. With appropriate parameter configuration, most cold time-series data can be stored in S3 services.

:::note

When used in conjunction with multi-level storage, data stored on each level of storage media may be backed up to remote object storage according to specified rules, with local data files deleted.

:::

### Configuration Method

In the configuration file `/etc/taos/taos.cfg`, add the parameters for S3 access:

| Parameter Name       | Parameter Meaning                                            |
| :------------------- | :----------------------------------------------------------- |
| s3EndPoint           | The COS service domain name for the user's region, supporting http and https; the bucket's region must match the endpoint, or access will be denied. |
| s3AccessKey          | User SecretId:SecretKey separated by a colon. For example: AKIDsQmwsfKxTo2A6nGVXZN0UlofKn6JRRSJ:lIdoy99ygEacU7iHfogaN2Xq0yumSm1E |
| s3BucketName         | The bucket name, with the user registered COS service's AppId following a hyphen. The AppId is unique to COS and does not exist in AWS or Aliyun; it must be included as part of the bucket name, separated by a hyphen. Parameter values are all string types and do not require quotes. For example: test0711-1309024725 |
| s3UploadDelaySec     | How long the data file must remain unchanged before being uploaded to S3, in seconds. Minimum value: 1; Maximum value: 2592000 (30 days); Default value: 60 seconds. |
| s3PageCacheSize      | The number of pages in the S3 page cache, in pages. Minimum value: 4; Maximum value: 1024*1024*1024; Default value: 4096. |
| s3MigrateIntervalSec | The triggering cycle for automatically uploading local data files to S3, in seconds. Minimum value: 600; Maximum value: 100000; Default value: 3600. |
| s3MigrateEnabled     | Whether to automatically perform S3 migration; default value is 1, indicating that automatic S3 migration is enabled and can be set to 0. |

### Check Configuration Parameter Availability

After completing the S3 configuration in `taos.cfg`, you can check whether the configured S3 service is available using the `checks3` parameter of the `taosd` command:

```shell
taosd --checks3
```

If the configured S3 service is inaccessible, this command will output the corresponding error message during execution.

### Create a Database Using S3

After completing the configuration, you can start the TDengine cluster and create a database using S3, for example:

```sql
create database demo_db duration 1d s3_keeplocal 3d;
```

Time-series data written to the database `demo_db` will automatically be stored in S3 storage in chunks after 3 days.

By default, the mnode will issue commands to check for S3 data migration every hour. If there is time-series data that needs to be uploaded, it will automatically be stored in S3 storage in chunks. You can also manually trigger this operation using SQL commands, with the syntax as follows:

```sql
s3migrate database <db_name>;
```

Detailed DB parameters are as follows:

| #    | Parameter    | Default Value | Minimum Value | Maximum Value | Description                                                  |
| :--- | :----------- | :------------ | :------------ | :------------ | :----------------------------------------------------------- |
| 1    | s3_keeplocal | 3650          | 1             | 365000        | The number of days data is retained locally, i.e., how long the data file can remain on local disks before it can be uploaded to S3. Default unit: days; supports m (minutes), h (hours), and d (days) units. |
| 2    | s3_chunksize | 262144        | 131072        | 1048576       | The size threshold for uploaded objects, which cannot be modified, and is measured in TSDB pages. |
| 3    | s3_compact   | 0             | 0             | 1             | Whether to automatically perform a compact operation when the TSDB file group is first uploaded to S3. |

## Azure Blob Storage

This section describes how to use Microsoft Azure Blob object storage in TDengine Enterprise. This feature is an extension of the previous section on 'Object Storage' and relies on the S3 gateway provided by Flexify services. With appropriate parameter configuration, most cold time-series data can be stored in Azure Blob services.

### Flexify Service

Flexify is an application in the Azure Marketplace that allows S3-compatible applications to store data in Azure Blob Storage via the standard S3 API. Multiple Flexify services can be used to establish several S3 gateways for the same Blob storage.

For deployment instructions, please refer to the application page of [Flexify](https://azuremarketplace.microsoft.com/en-us/marketplace/apps/flexify.azure-s3-api?tab=Overview).

### Configuration Method

In the configuration file `/etc/taos/taos.cfg`, add the parameters for S3 access:

```text
s3EndPoint   http //20.191.157.23,http://20.191.157.24,http://20.191.157.25
s3AccessKey  FLIOMMNL0:uhRNdeZMLD4wo,ABCIOMMN:uhRNdeZMD4wog,DEFOMMNL049ba:uhRNdeZMLD4wogXd
s3BucketName td-test
```

- You can configure multiple items for `s3EndPoint` and `s3AccessKey`, but the number of items must be the same. Separate multiple configuration items with a comma. Only one item is allowed for `s3BucketName`.
- Each group of `{s3EndPoint, s3AccessKey}` configuration corresponds to one S3 service, and a random service will be selected each time an S3 request is made.
- All S3 services are assumed to point to the same data source, and operations on various S3 services are completely equivalent.
- If an operation fails on one S3 service, it will switch to another service; if all services fail, the last generated error code will be returned.
- The maximum number of supported S3 service configurations is 10.
