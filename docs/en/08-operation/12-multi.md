---
title: Advanced Storage Options
slug: /operations-and-maintenance/advanced-storage-options
---

import Enterprise from '../assets/resources/_enterprise.mdx';

<Enterprise/>

This section introduces the multi-tier storage feature unique to TDengine Enterprise, which stores recent, frequently accessed data on high-speed media and old, infrequently accessed data on low-cost media, achieving the following objectives:

- Reduce storage costs -- By tiering data, storing massive amounts of extremely cold data on cheap storage media brings significant economic benefits
- Improve write performance -- Thanks to the support for multiple mount points at each storage level, and WAL pre-write logs also supporting parallel writing on multiple mount points at level 0, greatly enhancing write performance (tested to support continuous writing of over 300 million data points per second), achieving extremely high disk IO throughput on mechanical hard drives (tested up to 2GB/s)
- Easy maintenance -- After configuring the storage mount points for each level, system data migration and other tasks do not require manual intervention; storage expansion is more flexible and convenient
- Transparent to SQL -- Regardless of whether the queried data spans multiple levels, a single SQL query can return all data, simple and efficient

The storage media involved in multi-tier storage are all local storage devices. In addition to local storage devices, TDengine also supports using object storage as storage device. However, since most object storage systems already have multiple replicas, enabling TDengine's multi-replica feature on top would lead to wasted storage space and increased costs. To address this, TDengine has further improved object storage into shared storage—where the storage device logically maintains only one copy of data that is shared across all nodes in the TDengine cluster. This approach significantly reduces both storage space requirements and costs, and still allows querying when necessary, and where the data is stored is also transparent to SQL.

Shared storage was first released in version 3.3.7.0, it is an enhancement of the object storage (S3) support released in version 3.3.0.0. However, version 3.3.7.0 is not compatible with previous versions, if you have object storage (S3) feature enabled before, upgrading to version 3.3.7.0 and above will require manual operations.

## Multi-Tier Storage

### Configuration Method

Multi-tier storage supports 3 levels, with up to 128 mount points per level.

**Tips** Typical configuration schemes include: Level 0 configured with multiple mount points, each corresponding to a single SAS hard drive; Level 1 configured with multiple mount points, each corresponding to a single or multiple SATA hard drives.

The configuration method for TDengine multi-tier storage is as follows (in the configuration file /etc/taos/taos.cfg):

```shell
dataDir [path] <level> <primary>
```

- path: The folder path of the mount point.
- level: The storage medium level, values are 0, 1, 2. Level 0 stores the newest data, Level 1 stores the next newest data, Level 2 stores the oldest data, omitted defaults to 0. Data flow between storage levels: Level 0 -> Level 1 -> Level 2. Multiple hard drives can be mounted at the same storage level, and data files at that level are distributed across all hard drives at that level. It should be noted that the movement of data across different levels of storage media is automatically done by the system, and users do not need to intervene.
- primary: Whether it is the primary mount point, 0 (no) or 1 (yes), omitted defaults to 1.
In the configuration, only one primary mount point is allowed (level=0, primary=1), for example, using the following configuration method:

```shell
dataDir /mnt/data1 0 1
dataDir /mnt/data2 0 0
dataDir /mnt/data3 1 0
dataDir /mnt/data4 1 0
dataDir /mnt/data5 2 0
dataDir /mnt/data6 2 0
```

**Note** 1. Multi-tier storage does not allow cross-level configuration, legal configuration schemes are: only Level 0, only Level 0 + Level 1, and Level 0 + Level 1 + Level 2. It is not allowed to only configure level=0 and level=2 without configuring level=1.
2. It is forbidden to manually remove a mount disk in use, and currently, mount disks do not support non-local network disks.

### Load Balancing

In multi-tier storage, there is only one primary mount point, which bears the most important metadata storage in the system, and the main directories of various vnodes are also located on the current dnode's primary mount point, thus limiting the write performance of that dnode to the IO throughput of a single disk.

Starting from TDengine 3.1.0.0, if a dnode is configured with multiple level 0 mount points, we distribute the main directories of all vnodes on that dnode evenly across all level 0 mount points, allowing these level 0 mount points to share the write load.

When network I/O and other processing resources are not bottlenecks, by optimizing cluster configuration, test results prove that the entire system's writing capability and the number of level 0 mount points have a linear relationship, that is, as the number of level 0 mount points increases, the entire system's writing capability also increases exponentially.

### Same-Level Mount Point Selection Strategy

Generally, when TDengine needs to select a mount point from the same level to create a new data file, it uses a round-robin strategy for selection. However, in reality, each disk may have different capacities, or the same capacity but different amounts of data written, leading to an imbalance in available space on each disk. In practice, this may result in selecting a disk with very little remaining space.

To address this issue, starting from 3.1.1.0, a new configuration minDiskFreeSize was introduced. When the available space on a disk is less than or equal to this threshold, that disk will no longer be selected for generating new data files. The unit of this configuration item is bytes. If its value is set as 2GB, i.e., mount points with less than 2GB of available space will be skipped.

Starting from version 3.3.2.0, a new configuration `disable_create_new_file` has been introduced to control the prohibition of generating new files on a certain mount point. The default value is `false`, which means new files can be generated on each mount point by default.

## Shared Storage

This section describes how to use shared storage in TDengine Enterprise. This feature is currently based on the generic S3 SDK and has been adapted for various S3 compatible platforms, allowing access to object storage services such as MinIO, Tencent Cloud COS, Amazon S3, etc. By configuring the appropriate parameters, most of the colder time-series data can be stored in S3 compatible services.

**Note** When used in conjunction with multi-tier storage, data saved on each storage medium may be migrated to shared storage and local data files deleted according to rules.

### Configuration Method

In the configuration file `/etc/taos/taos.cfg`, add parameters for shared storage access:

| Parameter Name        |   Description                                      |
|:-------------|:-----------------------------------------------|
| ssEnabled | Whether to enable shared storage or not, allowed values are `0`, `1` and `2`. `0` is the default, which means shared storage is disabled; `1` means only enable manual migration, and `2` means also enable auto migation. 
| ssAccessString | A string which contains various options for accessing the shared storage, the format is `<device-type>:<option-name>=<option-value>;<option-name>=<option-value>;...`, currently,  only Amazon S3 compatible object storage providers are supported, so the `device-type` should be `s3`, and please refer the next table for the possible options. |
| ssUploadDelaySec | How long a data file remains unchanged before being uploaded to shared storage, in seconds. Minimum: 1; Maximum: 2592000 (30 days), default value 60 seconds |
| ssPageCacheSize | Number of shared storage page cache pages, in pages. Minimum: 4; Maximum: 1024 * 1024 * 1024, default value 4096 |
| ssAutoMigrateIntervalSec | The trigger cycle for automatic upload of local data files to shared storage, in seconds. Minimum: 600; Maximum: 100000. Default value 3600 |


`ssAccessString` options for S3 compatible object storage providers:

Name            |   Description
----------------|----------------------------------------------
endpoint        | host name / ip address, and optional port number of the object storage server.
bucket          | bucket name.
protocol        | `https` or `http`, `https` is the default.     
uriStyle        | `virtualHost` or `path`, `virtualHost` is the default, but please note that some object storage providers only support one of them.
region          | object storage service region, optional.
accessKeyId     | your access key id.              
secretAccessKey | your secret access key.
chunkSize       | chunk size in MB, files larger than this size will use multipart upload, default is 64.
maxChunks       | max number of allowed chunks in a multipart upload, default is 10000.
maxRetry        | max retry times when encounter retryable errors, default is 3, negative value means unlimited retry.

For example:

```
ssAccessString s3:endpoint=s3.amazonaws.com;bucket=mybucket;uriStyle=path;protocol=https;accessKeyId=AKMYACCESSKEY;secretAccessKey=MYSECRETACCESSKEY;region=us-east-2;chunkSize=64;maxChunks=10000;maxRetry=3
```

### Check Configuration Parameter Availability

After configuring shared storage in `taos.cfg`, the availability of the configured shared storage service can be checked using the `taosd` command with the `checkss` parameter:

```shell
taosd --checkss
```

If the configured shared storage service is inaccessible, this command will output the corresponding error information during execution.

### Create a DB Using Shared Storage

After configuration, you can start the TDengine cluster and create a database using shared storage, for example:

```sql
create database demo_db duration 1d ss_keeplocal 3d;
```

After writing time-series data into the database `demo_db`, time-series data older than 3 days will automatically be segmented and migrated into shared storage.

If the value of `ssEnabled` is `2`, mnode issues shared storage data migration check commands every hour. If there is time-series data that needs to be uploaded, it will automatically be segmented and migrated into shared storage. This process can also be manually trigger using SQL commands, with the following syntax:

```sql
ssmigrate database <db_name>;
```

Note that if the value of `ssEnabled` is `1`, mnode will never trigger the migration process automatically and user must trigger it manually if a migration is desired.

Detailed DB parameters are shown in the table below:

| #    | Parameter         | Default | Min | Max  | Description                                                         |
| :--- | :----------- | :----- | :----- | :------ | :----------------------------------------------------------- |
| 1    | ss_keeplocal | 365    | 1      | 365000  | The number of days data is kept locally, i.e., how long data files are retained on local disks before they can be migrated to shared storage. Default unit: days, supports m (minutes), h (hours), and d (days) |
| 2    | ss_chunkpages | 131072 | 131072 | 1048576 | The size threshold for migrating objects, same as the tsdb_pagesize parameter, unmodifiable, in TSDB pages |
| 3    | ss_compact   | 1      | 0      | 1       | Whether to automatically perform compact operation when before the first migration of TSDB files |

### Estimation of Read and Write Operations for Object Storage

The cost of using object storage services is related to the amount of data stored and the number of requests. Below, we discuss the processes of data upload and download separately.

#### Data Migration

When the TSDB time-series data exceeds the time specified by the `ss_keeplocal` parameter, the leader vnode will split the related data files into multiple file blocks, each with a default size of 512 MB (`ss_chunkpages * tsdb_pagesize`), except the last file block. All of the file blocks are uploaded to the shared storage, and all of the file blocks except the last one are removed from local disk.

When creating a database, you can adjust the size of each file block through the `ss_chunkpages` parameter, thereby controlling the number of uploads for each data file.

Other types of files such as head, stt, sma, etc., are also uploaded to the shared storage, but retained on the local file system to speed up pre-computed related queries.

The leader vnode also creats/updates a manifests file in the shared storage to save the migration information after the migration process finishes successfully.

The follower vnodes will then download the last block of the data file and all other files to local from the shared storage to complete the overall migration process, they rely on the manifests file to decide which specific files should be downloaded.

That's, the read/write count during data migration is about:

```
Read/Write Count = Number of Data Blocks + (Number of Other Files + 2) x Number of vnodes
```

#### Data Download

During query operations, if data in object storage needs to be accessed, TSDB does not download the entire data file. Instead, it calculates the position of the required data within the file and only downloads the relevant data into the TSDB page cache, then returns the data to the query execution engine. Subsequent queries first check the page cache to see if the data has already been cached. If the data is cached, it is used directly from the cache, thus effectively reducing the number of times data is downloaded from object storage.

Adjacent multiple data pages are downloaded as a single data block from object storage to reduce the number of downloads. The size of each data page is specified by the `tsdb_pagesize` parameter when creating the database, with a default of 4 KB.

```text
Download Count = Number of Data Blocks Needed for Query - Number of Cached Data Blocks
```

The page cache is a memory cache, and data needs to be re-downloaded after a node restart. The cache uses an LRU (Least Recently Used) strategy, and when there is not enough cache space, the least recently used data will be evicted. The size of the cache can be adjusted through the `ssPageCacheSize` parameter; generally, the larger the cache, the fewer the downloads.

## Azure Blob Storage

Support for Azure Blob object storage is temporarily disabled, if you are using this service before the release of TDengine version 3.3.7.0, please wait for the new version.

<!--

This section describes how to use Microsoft Azure Blob object storage in TDengine Enterprise. This feature is an extension of the 'Object Storage' feature discussed in the previous section and depends additionally on the Flexify service's S3 gateway. With proper parameter configuration, most of the colder time-series data can be stored in the Azure Blob service.

### Flexify Service

Flexify is an application in the Azure Marketplace that allows S3-compatible applications to store data in Azure Blob Storage through the standard S3 API. Multiple Flexify services can be used to establish multiple S3 gateways for the same Blob storage.

For deployment methods, please refer to the [Flexify](https://azuremarketplace.microsoft.com/en-us/marketplace/apps/flexify.azure-s3-api?tab=Overview) application page.

### Configuration Method

In the configuration file /etc/taos/taos.cfg, add parameters for S3 access:

```text
s3EndPoint   http //20.191.157.23,http://20.191.157.24,http://20.191.157.25
s3AccessKey  FLIOMMNL0:uhRNdeZMLD4wo,ABCIOMMN:uhRNdeZMD4wog,DEFOMMNL049ba:uhRNdeZMLD4wogXd
s3BucketName td-test
```

- Multiple items can be configured for s3EndPoint and s3AccessKey, but the number of items must match. Use ',' to separate multiple configuration items. Only one item can be configured for s3BucketName
- Each set of `{s3EndPoint, s3AccessKey}` is considered to correspond to one S3 service, and one service will be randomly selected each time an S3 request is initiated
- All S3 services are considered to point to the same data source, and operations on various S3 services are completely equivalent
- If an operation fails on one S3 service, it will switch to another service, and if all services fail, the last generated error code will be returned
- The maximum number of S3 services that can be configured is 10

### Without Relying on Flexify Service

The user interface is the same as S3, but the configuration of the following three parameters is different:

| #    | Parameter     | Example Value                              | Description                                                  |
| :--- | :------------ | :----------------------------------------- | :----------------------------------------------------------- |
| 1    | s3EndPoint    | `https://fd2d01c73.blob.core.windows.net`    | Blob URL                                                     |
| 2    | s3AccessKey   | fd2d01c73:veUy/iRBeWaI2YAerl+AStw6PPqg==  | Colon-separated user accountId:accountKey                    |
| 3    | s3BucketName  | test-container                             | Container name                                               |

The `fd2d01c73` is the account ID; Microsoft Blob storage service only supports the Https protocol, not Http.

-->
