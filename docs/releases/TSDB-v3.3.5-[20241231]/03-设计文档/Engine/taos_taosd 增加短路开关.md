# taos/taosd 增加短路开关

## 背景

- 在定位性能问题时，需要跳过某个模块以找到性能瓶颈。因此，增加一个短路开关进行控制。
- [TD-32907](https://jira.taosdata.com:18080/browse/TD-32907) [[性能] 在 taos/taosd 写入路径增加短路开关](https%3A%2F%2Fjira.taosdata.com%3A18080%2Fbrowse%2FTD-32907)。

## 实现原理

- 增加一个全局控制参数 bypassFlag，通过位运算控制跳过某个功能模块，支持位 `或运算` (注：在配置时只支持 `或运算的结果`，不直接支持`或运算`)

## 3. 控制参数 

### 3.1 参数说明

- bypassFlag 默认取值 为 0，其支持的位取值如下：

| 位取值 | 含义 | 作用范围 |
| --- | --- | --- |
| 1 | 针对写入消息，在 taos 客户端发送 RPC 消息前返回成功，不执行后续的写入流程。 | taos 客户端 |
| 2 | 针对写入消息，在 taosd 服务端收到 RPC 消息后返回成功，不执行后续的写入流程。 | taosd 服务端 |
| 4 | 针对写入消息，在 taosd 服务端写入内存 buffer (数据库 buffer 参数，默认值 256 MB)前返回，不执行后续的写入流程。 | taosd 服务端 |
| 8 | 针对写入消息，在 taosd 服务端执行 TSDB 落盘前返回，不执行后续的写入流程。 | taosd 服务端 |

### 3.2 参数设置

- 支持通过 taos.cfg 配置文件修改。该修改会永久生效，修改后需要重启 taosd 才能生效。示例：
```plaintext {wrap}
bypassFlag   0    // 不包含短路控制，默认值
bypassFlag   8    // 在 taosd 服务端不执行 TSDB 落盘
```

- 支持通过命令动态修改。动态修改的优先级更高，会覆盖 taos.cfg 中相同配置项的内存值；动态修改目前不支持持久化，重启后失效：
```plaintext
alter all dnodes 'bypassFlag 8'; // 将所有 dnode 的短路开关改为 8: taosd 服务端不执行 TSDB 落盘
alter dnode 1 'bypassFlag 2'; // 将 dnode 1 的短路开关改为 2: taosd 服务端收到写入消息后即返回成功，不执行后续写入流程
alter dnode 2 'bypassFlag 4'; // 将 dnode 2 的短路开关改为 4: taosd 服务端写入消息在写入内存 buffer 前返回，不执行后续写入流程
alter all dnodes 'bypassFlag 0'; // 将所有 dnode 的短路开关恢复为默认值 0，正常写入
alter local 'bypassFlag 1'; // 将 taos 的短路开关改为 1: taos 客户端针对写入消息在发送 RPC 消息前返回，不执行后续写入流程 
alter local 'bypassFlag 0'; // 将 taos 的短路开关恢复为默认值 0，正常写入
```

### 3.3 参数查看

#### 3.3.1 查看所有 dnode 的短路开关

- select * from information_schema.ins_dnode_variables where name = 'bypassFlag'; 
```sql
taos> select * from information_schema.ins_dnode_variables where name = 'bypassFlag'; 
  dnode_id   |      name       |      value      |   scope    |      info       |
=================================================================================
           1 | bypassFlag      | 8               | both       |                 |
Query OK, 1 row(s) in set (0.003179s)
```

#### 3.3.2 查看某个 dnode 的短路开关

- show dnode {dnodeId} variables like 'bypass%';  
或 select * from information_schema.ins_dnode_variables where name = 'bypassFlag' and dnode_id={dnodeId};
```plaintext {wrap}
taos> show dnode 1 variables like 'bypass%';
  dnode_id   |      name       |      value      |   scope    |      info       |
=================================================================================
           1 | bypassFlag      | 8               | both       |                 |
Query OK, 1 row(s) in set (0.003337s)

taos> select * from information_schema.ins_dnode_variables where name = 'bypassFlag' and dnode_id=1;
  dnode_id   |      name       |      value      |   scope    |      info       |
=================================================================================
           1 | bypassFlag      | 2               | both       |                 |
Query OK, 1 row(s) in set (0.003349s)
```

3.3.3 查看 taos 客户端的短路开关
- show local variables
```c {wrap}
taos> show local variables;
      name       |      value      |  scope   |      info       |
=================================================================
 ...                                                            
 bypassFlag      | 1               | both     |                 |
```

## 4 功能/性能测试

### 4.1 测试环境

- 12Core/16G 虚拟机
- 硬盘性能：
```c {wrap}
dd if=/dev/zero of=testfile bs=1M count=1024 oflag=direct
1073741824 bytes (1.1 GB, 1.0 GiB) copied, 5.34917 s, 201 MB/s // 写入

dd if=testfile of=/dev/null bs=1M count=1024 iflag=direct
1073741824 bytes (1.1 GB, 1.0 GiB) copied, 5.37819 s, 200 MB/s // 读取
```

### 4.2 测试分支

- 3.0
```c {wrap}
TDengine Enterprise Edition
taosd version: 3.3.4.3.alpha compatible_version: 3.0.0.0
git: 7e17f6366b301837124c22524ba3bc5588087b22
gitOfInternal: bac6789d0a5f9068f825181e7d7289dd924adf4c
build: Linux-x64 2024-11-18 17:58:57 +0800
```

- enh/TD-32907-3.0
```c {wrap}
TDengine Enterprise Edition
taosd version: 3.3.4.3.alpha compatible_version: 3.0.0.0
git: 7e17f6366b301837124c22524ba3bc5588087b22
gitOfInternal: bac6789d0a5f9068f825181e7d7289dd924adf4c
build: Linux-x64 2024-11-18 17:58:57 +0800
```

### 4.3 测试脚本和执行步骤

-  [3.0.json](https://taosdata.feishu.cn/wiki/YmTiw0CJMiCX2FkEautcjd2ynze)
```c {wrap}
taosBenchmark -f 3.0.json
```

### 4.4 测试报告

- enh/TD-32907-3.0 分支，bypassFlag 取 1/2/4/8/0 时，写入性能依次下降，符合预期，查询结果也符合预期。
- enh/TD-32907-3.0 分支，bypassFlag 取 0 时，与 3.0 分支相比，写入和查询性能相近，未见明显差异。

| 测试分支 | 写入性能 | 查询性能(select * from stb) |
| --- | --- | --- |
| [enh/TD-32907-3.0](https://jira.taosdata.com:18080/browse/TD-32907) bypassFlag=1 写入在 taos 客户端不发送 RPC 消息 | [11/18 18:45:56.040357] SUCC: Spent 15.037280 (real 13.207638) seconds to insert rows: 10000000 with 20 thread(s) into db 665013.89 (real 757137.65) records/second [11/18 18:45:56.040404] SUCC: insert delay, min: 14.4490ms, avg: 26.4153ms, p90: 43.0110ms, p95: 52.2910ms, p99: 64.8520ms, max: 105.7510ms | Query OK, 0 row(s) in set (0.015782s) |
| [enh/TD-32907-3.0](https://jira.taosdata.com:18080/browse/TD-32907) bypassFlag=2 写入在 taosd 服务端收到 RPC 消息后立即返回 | [11/18 18:47:06.519770] SUCC: Spent 15.721824 (real 13.984636) seconds to insert rows: 10000000 with 20 thread(s) into db 636058.51 (real 715070.45) records/second [11/18 18:47:06.519828] SUCC: insert delay, min: 15.0000ms, avg: 27.9693ms, p90: 40.0370ms, p95: 46.6840ms, p99: 63.7480ms, max: 91.2090ms | Query OK, 0 row(s) in set (0.014231s) |
| [enh/TD-32907-3.0](https://jira.taosdata.com:18080/browse/TD-32907) bypassFlag=4 写入在 taosd 服务端不写入内存 buffer(mem) | [11/18 18:48:17.805477] SUCC: Spent 15.675947 (real 14.486347) seconds to insert rows: 10000000 with 20 thread(s) into db 637919.99 (real 690305.15) records/second [11/18 18:48:17.805542] SUCC: insert delay, min: 14.8700ms, avg: 28.9727ms, p90: 40.2260ms, p95: 47.2180ms, p99: 65.8380ms, max: 1382.8550ms | Query OK, 0 row(s) in set (0.014122s) |
| [enh/TD-32907-3.0](https://jira.taosdata.com:18080/browse/TD-32907) bypassFlag=8 写入在 taosd 服务端不落盘 | [11/18 18:49:18.126402] SUCC: Spent 16.236726 (real 15.183044) seconds to insert rows: 10000000 with 20 thread(s) into db 615887.71 (real 658629.46) records/second [11/18 18:49:18.126449] SUCC: insert delay, min: 15.3080ms, avg: 30.3661ms, p90: 43.1040ms, p95: 51.7180ms, p99: 75.1010ms, max: 617.6440ms | Query OK, 709000 row(s) in set (2.119695s) 硬盘中无数据 |
| [enh/TD-32907-3.0](https://jira.taosdata.com:18080/browse/TD-32907) bypassFlag=0 默认值，正常写入/查询。 | [11/18 18:41:00.571442] SUCC: Spent 23.046735 (real 19.465520) seconds to insert rows: 10000000 with 20 thread(s) into db 433900.94 (real 513728.89) records/second [11/18 18:41:00.571498] SUCC: insert delay, min: 15.2470ms, avg: 38.9310ms, p90: 45.2210ms, p95: 53.6690ms, p99: 77.0350ms, max: 3467.9400ms [11/18 18:42:43.950108] SUCC: Spent 24.107136 (real 20.173327) seconds to insert rows: 10000000 with 20 thread(s) into db 414814.93 (real 495704.06) records/second [11/18 18:42:43.950168] SUCC: insert delay, min: 15.2260ms, avg: 40.3467ms, p90: 44.0010ms, p95: 52.5380ms, p99: 81.5620ms, max: 3177.0900ms | Query OK, 10000000 row(s) in set (11.744384s) Query OK, 10000000 row(s) in set (11.286749s) Query OK, 10000000 row(s) in set (11.921185s) Query OK, 10000000 row(s) in set (13.530213s) Query OK, 10000000 row(s) in set (11.581240s) |
| 3.0 正常写入/查询。 | [11/18 18:31:31.847135] SUCC: Spent 23.321776 (real 20.494535) seconds to insert rows: 10000000 with 20 thread(s) into db 428783.81 (real 487934.95) records/second [11/18 18:31:31.847175] SUCC: insert delay, min: 15.9420ms, avg: 40.9891ms, p90: 48.5610ms, p95: 59.3400ms, p99: 86.6020ms, max: 3199.5270ms [11/18 18:36:55.474812] SUCC: Spent 23.387241 (real 20.279102) seconds to insert rows: 10000000 with 20 thread(s) into db 427583.57 (real 493118.48) records/second [11/18 18:36:55.474864] SUCC: insert delay, min: 15.8570ms, avg: 40.5582ms, p90: 45.8300ms, p95: 55.1880ms, p99: 80.7220ms, max: 3521.9510ms [11/18 18:56:26.892515] SUCC: Spent 22.967188 (real 19.869355) seconds to insert rows: 10000000 with 20 thread(s) into db 435403.76 (real 503287.60) records/second [11/18 18:56:26.892568] SUCC: insert delay, min: 15.8950ms, avg: 39.7387ms, p90: 46.6920ms, p95: 54.7470ms, p99: 78.0850ms, max: 3314.9970ms | Query OK, 10000000 row(s) in set (11.521982s) Query OK, 10000000 row(s) in set (12.632993s) Query OK, 10000000 row(s) in set (11.636003s) Query OK, 10000000 row(s) in set (11.449001s) |
