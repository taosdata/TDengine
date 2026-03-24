# TS-5215 - [交付][沃太能源] 支持微软对象存储（直连 Azure Blob）Test Spec

## 1. 测试目标

测试 TDengine 直连 Azure Blob 相关功能，性能，兼容及稳定性，功能规格见：[S3 支持多次写入 - 功能规格](https://taosdata.feishu.cn/wiki/Nvd1wb8iKiw2z6kNhSAcJh9ansb)

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.10.25 | 1.0 | @贾靖斌 | New |
| 2024.10.30 | 1.1 | @金明垒 | 更新 s3_chunksize 及 s3_compact 默认值 |

## 3. 测试范围

- 可无限次历史数据补录
- 数据正确性验证（正常写入及补录历史数据）
- 运维功能验证
- 断网情况处置正确性
- 异常测试

## 4. 测试结论

功能，性能，兼容性，稳定性符合预期，测试通过。

## 5. 开发质量报告

结论：本特性/优化的开发质量是：

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 3 |
| 严重 Bug 总数 | 1 |

## 6. 已知问题和限制

参考 [S3 支持多次写入 - 功能规格](https://taosdata.feishu.cn/wiki/Nvd1wb8iKiw2z6kNhSAcJh9ansb)

## 7. 测试环境

- azure blob 
- OS：Linux（taosdata@20.106.225.8 ubuntu 22.04, taosdata@135.237.96.100 ubuntu 20.04）

## 8. 测试用例

### 8.1 功能测试

以下 case 的测试脚本为：community/tests/army/storage/bloblablob.py
运行命令：
```bash
cd ./community/tests/army
python3 ./test.py -f storage/blob/ablob.py
python3 ./test.py -f storage/blob/ablob.py -N 3
```


| 序号 | 分类 | 测试项 | 测试步骤 | 预期结果 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | s3_keeplocal 正常参数测试 [1 ~ 365000] 256 1024 | 1. create database db1 s3_keeplocal 256; 1. select `s3_keeplocal` from information_schema.ins_databases where name = "db1"; | 成功创建 db 且校验通过 | pass |  |
| 2 | s3_keeplocal 边界值测试 [1, 365000] 下界，上界 | create database db1 s3_keeplocal 1; create database db2 s3_keeplocal 365000; | 成功创建 db | pass |  |
| 3 | s3_keeplocal 参数非法值测试 小于于边界值 ，负数，大于边界值 | create database db2 s3_keeplocal -1; create database db2 s3_keeplocal 0; create database db2 s3_keeplocal 365001; | 创建 db 失败 | pass |  |
| 4 | s3_keeplocal 参数默认值测试 | create database db2; select `s3_keeplocal` from information_schema.ins_databases where name = "db2"; | 默认值为 525600m | pass |  |
| 5 | s3_chunkpages 参数正常值测试 [131072, 1048576] 60000， 820000 | 1. create database db1 s3_chunkpages 60000; 1. select `s3_chunkpages` from information_schema.ins_databases where name = "db1"; | 成功创建 db 且校验通过 | pass |  |
| 6 | s3_chunkpages 参数边界值测试 [131072 , 1048576] | create database db1 s3_chunkpages 131072; create database db2 s3_chunkpages 1048576; | 成功创建 db 且校验通过 | pass |  |
| 7 | s3_chunkpages 参数非法值测试 -1, 0, 900000000 | create database db2 s3_chunkpages -1; create database db2 s3_chunkpages 0; create database db2 s3_chunkpages 900000000; | 创建 db 失败 | pass |  |
| 8 | s3_chunkpages 参数默认值测试 | 1. create database db2; 1. select `s3_chunkpages` from information_schema.ins_databases where name = "db2"; | 默认值为 131072 | pass |  |
| 9 | s3_compact 设置为合法值 [0，1] | 1. create database db1 s3_compact 0; 1. create database db2 s3_compact 1; 1. 校验 select `s3_compact` from information_schema.ins_databases where name like "db%"; | 成功创建 db 且校验通过 | pass |  |
| 10 | s3_compact 设置为非法值[-1, 100] | create database db2 s3_compact -1; create database db2 s3_compact 100; | 创建 db 失败 | pass |  |
| 11 | s3_compact 参数默认值测试 | create database db2; select `s3_compact` from information_schema.ins_databases where name = "db2"; | 默认值为 1 | pass |  |
| 12 | s3MigrateEnabled 参数设置合法值[0,1] 0 和 1 | 1. taos.cfg 分别配置 s3MigrateEnabled 为 0 和 1 启动 taosd； 1. select `value` from ins_dnode_variables where name = "s3MigrateEnabled"; | taosd 可以正常启动且校验通过 | pass |  |
| 13 | s3MigrateEnabled 参数设置非法值 -1， 100 | 1. taos.cfg 分别配置 s3MigrateEnabled 为 -1 和 100 启动 taosd； 1. select `value` from ins_dnode_variables where name = "s3MigrateEnabled"; | taosd 启动失败 | pass |  |
| 14 | s3MigrateEnabled 默认值测试 | 1. taos.cfg 不配置 s3MigrateEnabled 启动 taosd 1. select `value` from ins_dnode_variables where name = "s3MigrateEnabled"; | 默认值 0 | pass |  |
| 15 | 参数逻辑验证 | 1. s3_keeplocal 配置为 2d； 1. s3MigrateIntervalSec 配置为 600s； 1. s3MigrateEnabled 配置为 1； 1. s3_compact 配置为 1； 1. 写入时间范围 1d-2d 内的数据； 1. 等待 600s； 1. 修改系统时间，使数据超过 keep 过期； 1. 等待 600s； 1. 查看日志 compact 信息； 1. 修改 s3_compact 0 并重启 taosd； 1. 继续写入过期数据至另一个文件组，等待 600s； 1. 查看日志 compact 信息； 1. 修改 s3MigrateEnabled 配置为 0 并重启 taosd； 1. 继续写入过期数据至另一个文件组，等待 600s； | 6. 不会自动上传； 8. 数据自动上传； 1. 自动 compact； 11. 数据自动上传； 1. 不自动 compact； 14. 数据不会上传； |  | pass |  |
| 16 | 参数合法时测试 taosd --checks3 | 1. 配置合法连接参数 1. taosd --checks3 | 返回一系列 success 信息 | pass |  |
| 17 | 参数非法时测试 taosd --checks3 | 1. 配置非法 s3EndPoint 执行 taosd --checks3； 1. 配置非法 s3AccessKey 执行 taosd --checks3； 1. 配置非法 td-test 执行 taosd --checks3； | 1. Reason Phrase 为空；sdk 返回的详细信息为空； 1. Unexpected end of Base64 encoded string； 1. The specified container does not exist. | pass |  |
| 18 | 补录历史数据 | 补录历史数据无限制 | 1. 开始时间相同，分别以以时间间隔 5s、3s、7s 写入 1000 W行数据 1. 每次写入后记录本地 DATA 文件大小 1. 记录本地文件大小后执行手工上传命令，上传DATA 到 Azure Blob 1. 每次上传后再记录本地 DATA 文件大小，预期是在 1G 内 | 4. 每次上传后再记录本地 DATA 文件大小，预期是在 1G 内，数据都上传完成后，预期是本地 DATA 文件在一个块（1G）内大小,同时查询服务器3s 5s 7s 时间间隔的数据都在，证明多次补录历史数据都能上传到服务器，功能正常 | pass |  |
| 19 | 顺序写入正确性 | 1. 以时间间隔 10s 顺序写入 1000W 行数据，列值通过 TS 时间列计算出； 1. 检查数据都上传至 Azure Blob 后（本地DATA文件小于1个块大小 ）； 1. 查询服务器上数据是否与写入时的数据一致及验证各列数据是否为 TS 列计算后的结果； | 校验通过 | pass |  |
| 20 | 乱序及更新写入正确性 | 1. 在上步顺序写入基础上，开始时间相同，再以时间间隔 7s 再写入一批，此时数据为乱序及更新都混合都有； 1. 检查数据都上传至 Azure Blob 后（本地DATA 文件块小于1G）； 1. 查询服务器上数据是否与写入时的数据一致及验证各列数据是否为 TS 列计算后的结果； | 校验通过 | pass |  |
| 21 | 删除数据的正确性 | 1. 每表按点删除 1000 条； 1. 按时间段删除 10 W 条； 1. 校验结果； | 校验通过 | pass |  |
| 22 | 掺杂非 S3 节点 | 3 节点集群，配置 2 个使用 Azure Blob 存储, 一个不使用 S3 本地存储 | 都可正常独立运行 | pass |  |
| 23 | 掺杂节点转为 S3 节点 | 上步中 3 节点服务器，把另外一个非 Azure Blob 节点也配置成 Azure Blob | 配置后三个节点都使用 S3 存储，数据不丢失，不崩溃 | pass |  |
| 24 | 写入断网 | 写入都先写到本地，在写入期间断网 | 不影响写入，不崩溃 | pass |  |
| 25 | 查询断网 | 在查询期间断网 | 从 Azure Blob 下载的查询会中断，返回 Azure Blob 连接失败错误码 | pass |  |
| 26 | 上传断网 | 在上传期间断网 | 无 crash 现象，网络恢复后可续传 | pass |  |
| 27 | 流计算断网 | 流计算的计算数据在 Azure Blob 上（使用 fill_history 选项创建流），计算期间发生断网后，再恢复 | 流仍能继续计算 | pass |  |
| 28 | 订阅断网 | 订阅后断网并恢复 | 订阅都是从 WAL 中消费， WAL 不会上传, 断网对订阅无任何影响 | pass |  |
| 29 | 正常停止 | 服务正常停止及重启 | 上传功能正常，不崩溃，不丢数据 | pass |  |
| 30 | 异常停止 | 集群一个或多个节点反复异常重启 | 上传功能正常，不崩溃，不丢数据 | pass |  |
| 31 | 长时间停止 | 集群一个节点长时间 offline 后，重新连接到集群后 | 上传功能正常，不崩溃，不丢数据 | pass |  |
| 32 | 存储空间不足 | 本地磁盘不足 | 本地磁盘剩余空间 小于 2G 拒绝写入请求 | 等待本地 DATA 文件数据上传至 Azure Blob 腾出本地空间，又可继续写入，程序不崩溃 | pass |  |
| 33 | Blob 不可用状态 | S3 不可用状态测试 | 制造 Azure Blob 存储空间满、Azure Blob 配置的 BUCKET 不存在及其它所有可能导致 Azure Blob 无法上传的场景 | 本地文件会不断膨胀，最后达到本地磁盘满，在修复 Azure Blob 无法上传的故障后，DATA 文件转移至 S3, 释放本地空间，本地又可以继续写入 | pass |  |
| 34 | DATA 文件过期 | Azure Blob 上数据所在 DATA 文件全部过期 | 过期一天后的 DATA 文件会被自动删除并释放空间 | pass |  |
| 35 | 删除数据库 | 删除 Azure Blob 对应数据库 | 相对应的 data 文件预期会在删除命令结束后即被同步删除 | pass |  |
| 36 | 删除表及 TS 范围删除 | 删除表及 Azure Blob 范围内数据； | compact 后空间可以释放 | pass |  |
| 37 | 手工触发上传命令 | s3migrate 命令可手工触发上传DATA 文件至 S3 ，验证功能的正确性 | 1. 写入一定量数据，可触发上传 Azure Blob； 1. s3migrate database db1； 1. 校验结果 | 命令可手工触发上传DATA 文件至 Azure Blob，且结果校验通过 | pass |  |
| 38 | alter replica 支持测试 | 1. 写入一定量数据，并已手动 s3migrate； 1. alter db replica； | 成功 | pass |  |
| 39 | compact 支持测试 | 1. 写入一定量数据，并已手动 s3migrate； 1. Compact db**；** | 成功 | pass |  |
| 40 | redistribute 支持测试 | 1. 写入一定量数据，并已手动 s3migrate； 1. redistribute vgroup ..； | 成功 | pass |  |
| 41 | rebalance 支持测试 | 1. 写入一定量数据，并已手动 s3migrate； 1. reblance ..**；** | 成功 | pass |  |
| 42 | create dnode 支持测试 | 1. 写入一定量数据，并已手动 s3migrate； 1. 新增额外 dnode**；** | 成功 | pass |  |
| 43 | drop dnode 支持测试 | 1. 写入一定量数据，并已手动 s3migrate； 1. 删除一个 dnode**；** | 成功 | pass |  |

### 8.2 兼容性

|  | **是/否** | **不兼容原因** |
| --- | --- | --- |
| **和历史功能兼容** | 与 s3 原有功能兼容 | 无 |
| **升级后是否可回退** | 数据未上传到 blob 前可回退，上传后不可回退 | 无 |
| **订阅和流兼容** | 与原有功能兼容 | 无 |

### 8.3 性能

| 性能 | 策略 | 预期结果 | 实际结果 |
| --- | --- | --- | --- |
| 写入 | 分别在使用和不使用 Blob 的时候使用同一份 taosBenchmark json 进行写入 | 性能相当 | 性能一致 |
| 查询 | 分别在使用和不使用 Blob 的时候使用同一份数据集进行查询 | 使用 Blob 时多出的时间主要是数据下载的时间 | 首次查询多出数据下载时间 |
| 上传 | 手动 s3migrate，记录性能 | 上传速度 ~= 带宽 | 上传时可充分利用带宽i |

- 性能测试步骤
1. 创建3节点集群3副数据库 (3个节点在同一台机器上）
2. taosBenchmark 写入数据, taosBenchmark -f storage/blob/perf.json (500 子表，每表 20 万数据）
3. 记录DATA 文件在本地时各 SQL 查询耗时
4. 配置 blob 参数，重启服务，开始 blob 上传
5. 使用网络监控记录数据开始上传时间
6. 使用网络监控记录数据上传结束时间
7. 确认数据都已上传及上传大小与在本地时数据大小做核实
8. 记录本地及 blob 上数据文件大小
9. 开始查询测试，记录时间
- 记录结果

| 序号 | SQL | 本地 | 传至 AzureBlob |
| --- | --- | --- | --- |
| 1 | 写入 1亿数据，数据量 | 1亿 | 1亿 |
| 2 | 持续多长时间后 DATA 上传完 | - | 5分钟内 |
| 3 | Select * from db.stb | 185.078s | 840.935s |
| 4 | Select count(*) from db.stb | 0.123s | 0.114s |
| 5 | Select * from db.stb where ts = '2020-09-13 14:00:00.000'; | 1.329s | 22.580s |
| 6 | Select * from db.stb where ts>='2020-09-13 14:00:00.000' and ts<'2020-09-13 14:00:10.000' ; | 1.387s | 20.378s |
| 7 | Select * from db.stb where ti>126; | 82.436s | 801.561s |
| 8 | select count(*) from db.stb interval(1s); | 5.719s | 171.027s |
| 9 | Select tbname, count(*) from db.stb group by tbname | 0.373 | 0.357s |
| 10 | DATA 文件存储大小分布 (上传至 blob 后) | 2G | 25G |
| 11 | Select * from db.d0; | 0.548s | 4.422s |

### 8.4 稳定性

s3MigrateIntervalSec 设置为最小，持续写入，满足 migrate 条件，并叠加查询、订阅、流计算、compact/redistribute/重启等运维操作，压测 48 小时。
无正确性，内存等问题。发现 stream checkpoint 一处目录路径问题，已提交修复。

## 9. Jira

TS-5248


TS-5215

## 10. 参考文档 

[需求说明：支持 S3（修订版）](https://taosdata.feishu.cn/wiki/RFYOwfYq9ibw69k1YeocVE2BnXe)
[S3 支持多次写入 - 功能规格](https://taosdata.feishu.cn/wiki/Nvd1wb8iKiw2z6kNhSAcJh9ansb)
[S3 API for Azure Blob Storage (Flexify.IO)](https://taosdata.feishu.cn/wiki/Hq9dw8BIpiZRhGkHXdMcNQiYncd)
[S3 支持多次写-Test Spec](https://taosdata.feishu.cn/wiki/VPPHwfMTQis1XFkbiDhcKsiTnAh)
