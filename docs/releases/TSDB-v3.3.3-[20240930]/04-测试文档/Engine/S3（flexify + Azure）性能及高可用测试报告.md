# S3（flexify + Azure）性能及高可用测试报告

## 一、环境信息

| 测试时间 | 2024.08.09～2024.08.21 |
| --- | --- |
| 测试人 | 许云龙 |
| 环境 | 本地环境：单节点，4U(Intel(R) Xeon(R) Silver 4214R CPU @ 2.40GHz) MEMORY 8G DISK 300G 生产环境：3节点，64U(Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz) MEMORY 256G DISK 2+8T |
| 网络 | 本地环境：微软云tddata在新加坡，taos集群在北京，通过本机flexify(docker)连接 生产环境：微软云tddata和taos集群均在新加坡，通过本机flexify(docker)连接 |
| 集群 | 3dnode，3vgroup，1replica |

## 二、部署架构

待开发文档完成后补充

## 三、性能测试

- 测试结论
  - 写入速度
    - 写入都在本地，所以写入速度预期和不使用S3相当
  - 查询速度
    - S3查询速度较本地直接查询性能有一定降低，在flexify和blob都在本地的情况下预估下降20%左右，该性能主要取决于网络下载速度，与flexify与blob之间的网络延时强相关。多flexify节点的性能提升不明显
  - 上传S3速度
    - 没有查询性能那么依赖网络，但依然和flexify与blob之间的网络延时有关，多flexify节点的性能提升不明显
- 测试步骤
   - 创建3节点单副本集群
   - 创建1个超级表，全类型普通列及TAG
   - 生成子 表500 个，每个子表2000万数据
   - s3_keeplocal、s3_chunksize、s3_compact、tsdb_pagesize均为默认配置
   - taosBenchmark 写入数据, 记录写入时间
   - 记录DATA 文件在本地时各SQL 查询耗时
   - 配置 S3 参数，重启服务，使用s3migrate database开始 S3 上传
   - 使用网络监控记录数据开始上传时间
   - 使用网络监控记录数据上传结束时间
   - 确认数据都已上传及上传大小与在本地时数据大小做核实
   - 记录本地及S3 上数据文件大小
   - 开始查询测试，记录时间
- 记录结果

| 序号 | SQL | 本地测试环境不传S3 3vgroup,1replica | 本地测试环境+S3 3vgroup,1replica | 生产环境+S3 3vgroup,1replica | 生产环境+S3+多点flexify(3节点) 3vgroup,1replica |
| --- | --- | --- | --- | --- | --- |
| 1 | 数据量 | 1亿 | 1亿 | 100亿(500张子表，每张2000万行) | 100亿(500张子表，每张2000万行) |
| 2 | 写入 100亿数据 | 5分钟 | - | 4小时 | 4小时 |
| 3 | 持续多长时间后 DATA 上传完 | - | 8 分钟 | 1小时 | 55分钟 |
| 5 | Select count(*) from stb | 0.065s | 0.060s | 1.15s | 0.79s |
| 6 | Select * from stb where ts = '2020-09-13 21:26:40'; | 0.027s | 0.028s | 0.43s | 0.10s |
| 7 | Select * from stb where ts>='2020-09-13 21:26:40' and ts<'2020-09-13 21:27:40' ; | 0.073s | 0.032s | 0.17s | 0.15s |
| 9 | select count(*) from stb interval(1s); | 61s | 由于连接s3网络延迟高性能很差 | >1h |  |
| 10 | Select tbname, count(*) from stb group by tbname | 0.48s | 0.32s | 31.05s | 24.58s |
| 11 | DATA 文件存储大小分布 (上传至S3后) | 1.5G | 768M(S3) 713M(本地) | 115G(S3) 21G(本地) | 115G(S3) 21G(本地) |
| 12 | Select * from d0; | 1.03s | 由于连接s3网络延迟高性能很差 | 165s | 236.37s |
| 13 | Select * from stb; | 68s | 由于连接s3网络延迟高性能很差 | >5h |  |

## 四、运维测试

| 序号 | 运维功能点 | 预期结果 | 现场测试结果 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | s3migrate 测试 | 立即上传s3 | pass |  |
| 2 | 自动上传s3测试 | Success | pass | s3MigrateEnabled 参数控制 触发周期默认1小时 s3MigrateIntervalSec 参数控制 |
| 3 | alter replica 测试 | Success | pass |  |
| 4 | compact 测试 | Success | pass |  |
| 5 | redistribute 测试 | Success | pass |  |
| 6 | rebalance 测试 | Success | pass |  |
| 7 | create dnode 测试 | Success | pass |  |
| 8 | drop dnode 测试 | Success | pass | 1亿数据量耗时1700s，drop完成后全数据查询无异常 |
| 9 | drop table 测试 | S3存储中数据可以对应删除 | pass | 一次只会删除一整块数据 |
| 10 | drop db 测试 | S3存储中数据可以对应删除 | pass |  |
| 11 | restore dnode 测试 | Success | pass | 删除整个data文件夹，再restore后，数据恢复正常 |
| 12 | restore vgroup 测试 | Success | pass | 删除vnode文件夹，再restore后，数据恢复正常 |

## 五、高可用测试

注：flexify支持多点部署，可以配置多个s3EndPoint、s3AccessKey参数，但s3BucketName只能有一个

| 序号 | 测试点 | 预期结果 | 现场测试结果 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | 数据一致性 | 通过不同flexify读写保持数据一致 | pass |  |
| 2 | 单点故障上传测试 | 故障1台flexify数据正常上传 | pass | 性能未见明显影响 |
| 3 | 单点故障查询测试 | 故障1台flexify数据正常查询 | pass | 性能未见明显影响 |
| 4 | 多点故障上传测试 | 故障2台flexify数据正常上传 | pass |  |
| 5 | 多点故障查询测试 | 故障2台flexify数据正常查询 | pass |  |
| 6 | 上传过程中故障 | 上传s3过程中，restart 单节点taosd，上传正常 | pass |  |
| 7 | 读取过程中故障 | 大数据查询过程中，restart 单节点taosd，查询正常 | pass |  |

## 六、问题跟踪

1. 需求：
  TD-31289

1. 
  TD-31433

1. 
  TD-31446

1. 
  TD-31495

1. 
  TS-5274

1. 
  TD-31592

1. 
  TD-31594

1. 
  TD-31604

## 七、注意事项/优化点

- [ ] 在无授权时，执行s3migrate命令是否应该提示License expired for xxx function，目前无任何提示，s3migrate命令执行成功，但无法上传
- [x] drop dnode 不停进程，直接create 回去后报 dnodeId not match，删除data数据，restart进程后恢复。
- [x] 加入新dnode，并将原有db 从单副本更改为3副本，报Vnode didn't catch up its leader，排查过后是性能相关问题
- [x] 多flexify场景，s3EndPoint和s3AccessKey参数可以配置多个，但是s3BucketName参数（就是上传文件的目标bucket）目前也必须配置多个，而且是相同的。如果配置多个不同的bucket，行为可能不符合预期。
- [ ] taosd --checks3 相关：1、目前不支持自定义文件大小check，2、目前在check多个时其中一个失败会block之后的check
- [ ] flexify和blob之间的网络要有一定要求，网络较差会导致大文件上传超时，整个功能不可用，也会大幅影响查询和上传性能。

## 八、参考

[S3 支持多次写入 - 功能规格](https://taosdata.feishu.cn/wiki/Nvd1wb8iKiw2z6kNhSAcJh9ansb)
[S3 支持多次写-Test Spec](https://taosdata.feishu.cn/wiki/VPPHwfMTQis1XFkbiDhcKsiTnAh)
[S3（flexify + Azure）测试报告](https://taosdata.feishu.cn/wiki/Rx5bw1PARivjApkij1rc1BIuntX)

## 九、测试自动化

- 简介：
PR：https://github.com/taosdata/TDengine/pull/27758
case沿用s3Basic.py的case架构，由于s3的基础case在之前的s3Basic.py已经有覆盖，所以新case集删除了s3配置相关、和大部分query的验证，并针对flexify场景增加了连接多flexify和azure上的文件验证
- flexify相关：
在ci机器192.168.1.49上起了2个docker，分别绑定了80和81端口，作为多flexify的节点，
后端blob连接的是在微软云上申请的 td-test
> ⚠ 下载失败，需在飞书中查看 (token: QKTibLSC4ovHnWxXuuocn5mEnPe)

- case：
https://github.com/taosdata/TDengine/blob/test/3.0/flexify/tests/army/storage/s3/s3azure.py
- self.insertData()：插入数据
- self.snapshotAgg()：保存快照，后面case中比对
- self.doAction()：case主程序
  - self.flushDb(show=True)：数据清洗
  - self.migrateDbS3()：上传s3
  - self.checkUploadToS3()：根据本地文件大小轮询检查上传s3是否成功
- self.checkAzureDataExist()：判断azure上传成功
- self.checkAggCorrect()：根据快照验证上传后数值是否正确
- self.checkInsertCorrect()：检测插入的数据量是否正确
- self.dropDb()：删除db
- self.checkAzureDataNotExist()：检测azure上文件是否同步删除成功
- 直连微软azure：
https://github.com/taosdata/TDengine/blob/test/3.0/flexify/tests/army/storage/s3/azure.py
使用自己构造signature的方式，不需要安装sdk，减少依赖
ak存在一台虚拟机上，通过nginx连接：http://192.168.0.21/azure_account_key.txt
- 测试执行命令行：
  - python3 ./test.py -f s3/s3azure.py -N 3
