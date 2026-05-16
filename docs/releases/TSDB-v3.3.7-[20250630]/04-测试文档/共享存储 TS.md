# 共享存储 TS

## 1. 测试目标

1. 测试共享存储的功能是否正常
2. 测试共享存储是否能从之前版本的 S3 功能升级
3. 测试共享存储对性能的影响是否符合要求

## 2. 相关链接

JIRA：[TS-6107](https://jira.taosdata.com:18080/browse/TS-6107)

## 3. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025-7-8 | 0.1 | 张博民 | 文档撰写 |

## 4. 已知问题

1. 共享存储的数据迁移（Migration）通过异步任务完成，其中，主节点的 Migration 是从本地将数据上传到共享存储；从节点的 Migrattion 是从共享存储中下载数据到本地。由于系统中 Commit、Merge、Compact、Retention 等也是通过异步任务实现的，而异步任务在不同节点上的执行次序可能不同且无法控制，故为避免数据损坏或丢失，一旦程序检测到以下情况，会停止迁移：
   - 数据迁移任务已被加入异步任务队列，如在执行之前主节点出现了新的 Commit，则本次迁移会失败。但再次启动迁移会成功；
   - 数据迁移任务已被加入异步任务队列，如在执行之前主节点没有新的 Commit，但从节点有新的 Commit，则主节点上传成功，从节点下载失败；在不出现切主且满足其它迁移条件的情况下，再次启动数据迁移会成功；
   - 如果主节点上传成功但从节点下载失败后出现了切主，则新的主节点无法完成数据上传，Migration 会一直失败。将主节点切回可再次成功执行 Migration。
   - 执行 Compact 后，会将远端存储中的数据全部拉回本地，此时，新的 Migration 会一直失败。手动将远端存储中的文件删除后可恢复。
2. 如果开启了多级存储，各节点的存储层级和每个层级的挂载点数量必须相同，否则从节点下载数据时可能失败。

## 5. 测试环境

**服务器：**192.168.3.195，192.168.3.196，192.168.3.197
**CPU：**4核
**内存：**10G
**硬盘：**200G
**操作系统：**Ubuntu 22.04.5 LTS
**TDengine 旧版：**v3.3.6.15
**TDengine 新版：**
- **TDengine: **bcc25e56bc3c2b0785d016a5676a6d8bd31cecc5
- **TDinternal: **607330832b6e2f2dd71b6a16b315d0463a4b6eef
**远端存储：**minio, http://192.168.1.52:9000/

## 6. 功能测试用例

功能测试用例从之前的 S3 相关测试用例改造而来，CI 通过则功能测试通过。

## 7. 升级过程测试用例

1. 安装旧版程序，使用 `taosBenchmark -v 2 -a 3` 生成一批数据。
2. 执行 `delete from meters where ts < '2017-07-14 10:40:00.050'` 删除部分数据。
3. 使用 `s3migrate database test` 命令将数据迁移到 S3。
4. 执行 `select * from test.meters`，记录总数据条数。
5. 执行 `select avg(current) from test.meters where voltage > 245`，记录查询结果。
6. 升级新版程序，并使用升级工具完成数据文件升级。
7. 执行 `select * from test.meters`，与第 3 步的结果对比。
8. 执行 `select avg(current) from test.meters where voltage > 245`，与第 4 步的结果对比。
9. 使用 `taosBenchmark -v 2 -a 3 -Q -s 1500001000000`再次写入一批数据。
10. 执行 `delete from meters where ts < '2017-07-14 10:40:00.100'` 删除部分数据。
11. 执行 `select * from test.meters`，记录总数据条数。
12. 执行 `select avg(current) from test.meters where voltage > 245`，记录查询结果。
13. 使用 `ssmigrate database test` 再次触发数据迁移。
14. 验证本地和远端数据文件的正确性。
15. 执行 `select * from test.meters`，与第 11 步的结果对比。
16. 执行 `select avg(current) from test.meters where voltage > 245`，与第 12 步的结果对比。

## 8. 性能测试用例

### 8.1 写入性能

数据写入都发生在本地磁盘，故共享存储并不影响写入性能。

### 8.2 查询性能

#### 8.2.1 数据准备

在旧版本上创建数据库：`create database test vgroups 2 replica 3 duration 1d s3_keeplocal 3d;`
使用 taosBenchmark 按如下脚本生成数据：
```json
{
  "filetype": "insert",
  "cfgdir": "/etc/taos",
  "host": "192.168.3.195",
  "port": 6030,
  "user": "root",
  "password": "taosdata",
  "connection_pool_size": 8,
  "thread_count": 4,
  "create_table_thread_count": 4,
  "result_file": "./insert_res.txt",
  "confirm_parameter_prompt": "no",
  "num_of_records_per_req": 10000,
  "prepared_rand": 10000,
  "chinese": "no",
  "escape_character": "yes",
  "continue_if_fail": "no",
  "databases": [{
    "dbinfo": {
      "name": "test",
      "drop": "no",
      "vgroups": 2,
      "replica": 3,
      "precision": "s"
    },
    "super_tables": [{
      "name": "meters",
      "child_table_exists": "no",
      "childtable_count": 10000,
      "childtable_prefix": "d",
      "auto_create_table": "no",
      "batch_create_tbl_num": 5,
      "data_source": "rand",
      "insert_mode": "taosc",
      "non_stop_mode": "no",
      "line_protocol": "line",
      "insert_rows": 864000,
      "childtable_limit": 0,
      "childtable_offset": 0,
      "interlace_rows": 0,
      "insert_interval": 0,
      "partial_col_num": 0,
      "timestamp_step": 1000,
      "start_timestamp": "2025-07-07 08:00:00.000",
      "sample_format": "csv",
      "sample_file": "./sample.csv",
      "use_sample_ts": "no",
      "tags_file": "",
      "columns": [{
        "type": "FLOAT",
        "name": "current",
        "count": 1,
        "max": 12,
        "min": 8
      }, {
        "type": "INT",
        "name": "voltage",
        "max": 225,
        "min": 215
      }, {
        "type": "FLOAT",
        "name": "phase",
        "max": 1,
        "min": 0
      }],
      "tags": [{
        "type": "TINYINT",
        "name": "groupid",
        "max": 10,
        "min": 1
      }, {
        "type": "BINARY",
        "name": "location",
        "len": 16,
        "values": ["San Francisco", "Los Angles", "San Diego",
          "San Jose", "Palo Alto", "Campbell", "Mountain View",
          "Sunnyvale", "Santa Clara", "Cupertino"
        ]
      }]
    }]
  }]
}
```

数据总计 86.4 亿条，每个 vgroup 10 个数据文件，共计 20 个数据文件，单个数据文件约 3.3 G。根据数据库配置参数，每个 vgroup 的前七个数据文件的会被迁移到共享存储。

#### 8.2.2 查询语句

| 1 | 纯远端存储上的查询 | 投影 | select * from meters where ts>='2025-07-08 06:00:00' and ts < '2025-07-08 10:00:00'; |
| --- | --- | --- | --- |
|  |  | 聚合 | select count(ts) from meters where ts>='2025-07-07 08:00:00' and ts < '2025-07-14 08:00:00'; |
| 2 | 远端存储( 25%) + 本地(75%) 组合查询 | 投影 | select * from meters where ts>='2025-07-14 04:00:00' and ts < '2025-07-14 20:00:00'; |
|  |  | 聚合 | select count(ts) from meters where ts>='2025-07-13 08:00:00' and ts < '2025-07-17 08:00:00'; |
| 3 | 整体查询 | 聚合 | select count(ts) from meters; |
| 4 | First 首行查询 | 投影 | select first(*) from meters; |
| 5 | Last 未行查询 | 投影 | select last(*) from meters; |

**注：**由于投影查询耗时极长，上述查询语句缩小了投影查询的时间范围。

#### 8.2.3 测试步骤

1. 使用旧版本程序准备测试数据。
2. 使用旧版程序执行查询语句，每个查询连续执行两次，记录查询耗时，然后重启服务端和客户端。
3. 升级至新版本。
4. 使用新版程序执行查询语句，每个查询连续执行两次，记录查询耗时，然后重启服务端和客户端。

#### 8.2.4 测试结果

|  |
|  |
| 第一次 | 第二次 | 第一次 | 第二次 |
| 1 | 纯远端存储上的投影查询 144000000 rows | 156.309035s 135.504594s 129.368539s | 133.928453s 124.175787s 131.505446s | 118.377755s 117.877872s 118.141833s | 119.054379s 119.238124s 118.879153s |
| 2 | 纯远端存储上的聚合查询 | 1.487699s 1.464287s 1.408648s | 1.287363s 1.621088s 1.365266s | 1.352125s 1.346093s 1.512201s | 1.554906s 1.371626s 1.303595s |
| 3 | 组合投影查询 576000000 rows | 397.788335s 410.222221s 399.234373s | 403.134506s 396.574639s 397.402816s | 405.480899s 386.509248s 393.594427s | 391.323102s 388.926462s 388.899168s |
| 4 | 组合聚合查询 | 0.929304s 0.950437s 0.945335s | 0.802898s 0.938871s 0.901231s | 1.063403s 1.081706s 1.036833s | 0.955046s 0.840977s 0.922825s |
| 5 | 整体查询 | 1.849090s 1.512197s 1.816568s | 1.856378s 1.613880se 1.531267s | 1.698992s 1.611636s 1.465578s | 1.499867s 1.803195s 1.583347s |
| 6 | 首行查询 | 0.324827s 0.379394s 0.305715s | 0.307947s 0.295334s 0.270065s | 0.510224s 0.368820s 0.299173s | 0.289999s 0.353959s 0.287591s |
| 7 | 末行查询 | 7.627315s 9.687561s 8.049464s | 1.117185s 1.012072s 1.409818s | 6.243065s 0.920608s 1.033435s | 1.038853s 1.019218s 1.029879s |

理论上，共享存储新旧版之间不会存在明显的查询效率差异。以上结果说明，新旧版本程序查询效率与旧版总体持平或略优。其中，纯远端存储的投影查询效率有一定程度的提高，这可能由于远端存储使用的 minio 服务并非专用导致。末行查询复测时，第一次执行的效率明显提高，应为其他模块改进的结果。

## 9. 稳定性测试

使用`create database test vgroups 2 replica 3 duration 1d ss_keeplocal 3d`创建数据库。
使用 taosBenchmark 按如下脚本写入数据，连续写入十天，每天检查本地数据和远端数据的正确性：
```json
{
    "filetype": "insert",
    "cfgdir": "/etc/taos",
    "host": "192.168.3.195",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "connection_pool_size": 8,
    "thread_count": 4,
    "create_table_thread_count": 4,
    "result_file": "./insert_res.txt",
    "confirm_parameter_prompt": "no",
    "num_of_records_per_req": 10000,
    "prepared_rand": 10000,
    "chinese": "no",
    "escape_character": "yes",
    "continue_if_fail": "no",
    "databases": [{
        "dbinfo": {
            "name": "test",
            "drop": "no",
            "vgroups": 2,
            "replica": 3,
            "precision": "s"
        },
        "super_tables": [{
            "name": "meters",
            "child_table_exists": "no",
            "childtable_count": 10000,
            "childtable_prefix": "d",
            "auto_create_table": "no",
            "batch_create_tbl_num": 5,
            "data_source": "rand",
            "insert_mode": "taosc",
            "non_stop_mode": "no",
            "line_protocol": "line",
            "insert_rows": 864000,
            "childtable_limit": 0,
            "childtable_offset": 0,
            "interlace_rows": 1,
            "insert_interval": 1000,
            "partial_col_num": 0,
            "timestamp_step": 1000,
            "start_timestamp": "2025-07-18 08:00:00.000",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "use_sample_ts": "no",
            "tags_file": "",
            "columns": [
                {"type": "FLOAT", "name": "current", "count": 1, "max": 12, "min": 8 },
                { "type": "INT", "name": "voltage", "max": 225, "min": 215 },
                { "type": "FLOAT", "name": "phase", "max": 1, "min": 0 }
            ],
            "tags": [
                {"type": "TINYINT", "name": "groupid", "max": 10, "min": 1},
                {"type": "BINARY",  "name": "location", "len": 16,
                 "values": ["San Francisco", "Los Angles", "San Diego",
                           "San Jose", "Palo Alto", "Campbell", "Mountain View",
                           "Sunnyvale", "Santa Clara", "Cupertino"]
                }
            ]
        }]
   }]
}
```

## 10. 参考文档

[共享存储 FS](https://taosdata.feishu.cn/wiki/TEWIw8cpBiAYlyk2zWvczJCKn6g)
