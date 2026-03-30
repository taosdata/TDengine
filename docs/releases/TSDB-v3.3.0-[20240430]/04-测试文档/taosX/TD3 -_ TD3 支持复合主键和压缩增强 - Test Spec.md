# TD3 -> TD3 支持复合主键和压缩增强 - Test Spec

## 1. 测试目标

在 TDengine 3.3.0.0 版本中引入了复合主键和压缩增强的参数配置，taosX 数据同步功能也进行了相应的适配，形成了本次测试主要的测试目标：
- 支持数据源包含复合主键的表结构的数据同步、数据备份、数据恢复
- 支持数据源包含压缩增强功能的表的数据同步、数据备份、数据恢复

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-03-27 | 0.1 | @贾晨阳 |  |
| 2024-04-23 | 1.0 | @贾晨阳 |  |

## 3. 测试范围

这里用于描述本需求的覆盖范围：
- 数据源中包含复合主键的表和数据的数据同步、数据备份、数据恢复
- 数据源包含不同级别压缩增强列的表的数据同步、数据备份、数据恢复
- 数据源中包含复合主键且设置了不同级别压缩增强的表的数据同步、数据备份、数据恢复

## 4. 测试结论

本次测试验证内容：
1. 数据源和目标源均为为3.3.0.0版本且schema中包含/不包含复合主键和压缩增强参数的超级表、普通表的数据同步、数据备份、数据恢复，验证通过
2. 数据源版本低于3.3.0.0，目标源版本为3.3.0.0，超级表、普通表的数据同步、数据备份、数据恢复，验证通过
3. 数据源版本为3.3.0.0，目标源版本低于3.3.0.0，不支持同步、备份、恢复，验证通过
4. 低版本taosx不支持恢复高版本数据备份文件，验证通过
5. 高版本taosx支持恢复低版本数据备份文件，验证通过

## 5. 开发质量报告（Quality）

结论：本特性的开发质量是一般

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 2 |
| Bug 总数 | 4 |
| 严重 Bug 总数 | 0 |

其中，基础用例不通过的问题 [TD-29746](https://jira.taosdata.com:18080/browse/TD-29746) ，根因是数据订阅问题而非 taosx 本身的问题，但开发自测时并未发现。

## 6. 已知问题和限制（Limitation）

- explorer 目前暂不支持将备份的数据恢复至其他目标库，所以该版本备份恢复功能通过 CLI 方式验证
- 
  TD-29791

- 对于低版本的 taosx （1.5.x及更老的版本），在恢复1.0版本的数据文件时会报错并以 panic 方式结束进程，我们认为该行为符合预期。
- 涉及命令行的功能测试主要在linux上进行。

## 7. 测试环境（Environment）

- OS: taosx 运行在Linux 上
- Browser: Chrome

## 8. 测试数据（Data）

在TDengine数据源中创建超级表/普通表，表结构分别为：不含复合主键、包含复合主键且符合主键类型分别int、bigint、int unsigned、bigint unsigned、varchar。
普通表schema示例：
```sql
CREATE TABLE t (
    ts TIMESTAMP, 
    obj_id VARCHAR(64) PRIMARY KEY,
    data1 FLOAT,
    data2 int
);
```

超级表schema示例：
```sql
CREATE TABLE stb (
    ts TIMESTAMP, 
    obj_id VARCHAR(64) PRIMARY KEY,
    data1 FLOAT,
    data2 int
)tags(t0 int);
```

## 9. 测试用例（Cases）

### 9.1 功能

在提测时，开发应保证 basic 类型的用例全部通过。
测试依赖TDengine最新3.0分支，在build时添加参数：
```sql
cmake .. -DBUILD_TOOLS=true -DBUILD_CONTRIB=true -DVERNUMBER=3.3.0.0.alpha
```

以下测试用例中，若无特殊说明，数据源和目标库的 TDengine 均为 3.3.0.0 版本。
超级表建表语句：
```bash
CREATE TABLE PK_TEST.`stb1` (
                    `ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'zlib' LEVEL 'low', 
                    `pk_1` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY, 
                    `c1` tinyint ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'low', 
                    `c2` TINYINT UNSIGNED ENCODE 'simple8b' COMPRESS 'zlib' LEVEL 'medium', 
                    `c3` smallint ENCODE 'simple8b' COMPRESS 'zstd' LEVEL 'high',
                    `c4` SMALLINT UNSIGNED ENCODE 'simple8b' COMPRESS 'xz' LEVEL 'low',
                    `c5` int ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium',
                    `c6` INT UNSIGNED ENCODE 'simple8b' COMPRESS 'zlib' LEVEL 'high',
                    `c7` bigint ENCODE 'delta-i' COMPRESS 'zstd' LEVEL 'low',
                    `c8` timestamp ENCODE 'delta-i' COMPRESS 'xz' LEVEL 'medium',
                    `c9` BIGINT UNSIGNED ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'high',
                    `c10` float ENCODE 'delta-d' COMPRESS 'zlib' LEVEL 'low',
                    `c11` double ENCODE 'delta-d' COMPRESS 'tsz' LEVEL 'medium',
                    `c12` binary(30) ENCODE 'disabled' COMPRESS 'xz' LEVEL 'high',
                    `c13` nchar(20) ENCODE 'disabled' COMPRESS 'zstd' LEVEL 'low',
                    `c14` bool ENCODE 'bit-packing' COMPRESS 'xz' LEVEL 'medium'
) TAGS (`groupid` INT, `location` VARCHAR(24))
```


普通表建表语句：
```bash
CREATE TABLE PK_TEST.`ntb1` (
                    `ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'zlib' LEVEL 'low', 
                    `pk_1` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY, 
                    `c1` tinyint ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'low', 
                    `c2` TINYINT UNSIGNED ENCODE 'simple8b' COMPRESS 'zlib' LEVEL 'medium', 
                    `c3` smallint ENCODE 'simple8b' COMPRESS 'zstd' LEVEL 'high',
                    `c4` SMALLINT UNSIGNED ENCODE 'simple8b' COMPRESS 'xz' LEVEL 'low',
                    `c5` int ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium',
                    `c6` INT UNSIGNED ENCODE 'simple8b' COMPRESS 'zlib' LEVEL 'high',
                    `c7` bigint ENCODE 'delta-i' COMPRESS 'zstd' LEVEL 'low',
                    `c8` timestamp ENCODE 'delta-i' COMPRESS 'xz' LEVEL 'medium',
                    `c9` BIGINT UNSIGNED ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'high',
                    `c10` float ENCODE 'delta-d' COMPRESS 'zlib' LEVEL 'low',
                    `c11` double ENCODE 'delta-d' COMPRESS 'tsz' LEVEL 'medium',
                    `c12` binary(30) ENCODE 'disabled' COMPRESS 'xz' LEVEL 'high',
                    `c13` nchar(20) ENCODE 'disabled' COMPRESS 'zstd' LEVEL 'low',
                    `c14` bool ENCODE 'bit-packing' COMPRESS 'xz' LEVEL 'medium'
)
```

复合主键列差异如下：
```bash
`pk_1` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY, 

`pk_1` INT UNSIGNED ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY, 

`pk_1` bigINT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY, 

`pk_1` BIGINT UNSIGNED ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY, 

`pk_1` varchar(20) ENCODE 'disabled' COMPRESS 'xz' LEVEL 'medium' PRIMARY KEY, 
```

数据写入：
```bash
insert into  `pk_test`.`stb4_tb0` values (NOW, 1, 1, 1, 1, 1, 1, 1, 1, NOW, 1, 1.1, 1.1, "中文1", "中文1", true),
(NOW, 2, 2, 2, 2, 2, 2, 2, 2, NOW, 2, 2.2, 2.2, "中文2", "中文2", false),
(NOW, 3, 3, 3, 3, 3, 3, 3, 3, NOW, 3, 3.3, 3.3, "中文3", "中文3", true);
```


同步完成之后 diff 建表语句来比较 schema 是否一致。
|  | Description | Expected Results | result for developer | Result | Jira | Automated | Memo |
| --- | --- | --- | --- | --- | --- | --- | --- |
| basic | 1.数据源库中超级表schema使用复合主键，类型为int
2.创建数据同步任务到目标库中 | 目标库中数据与源库中数据一致 |  | Pass |  |  | 建表语句：
CREATE STABLE `meters` (
    `ts` TIMESTAMP, 
    `pk_int` int primary key,
    `current` FLOAT, 
    `voltage` INT, 
    `phase` FLOAT) 
TAGS (`groupid` INT, `location` VARCHAR(24))
insert into `pk_test`.`d1` VALUES(NOW, 2, 13.4, 12, 22), (NOW, 1, 13.4, 12, 22); |
| basic | 1.数据源库中超级表schema使用复合主键，类型为int
2.将数据库备份到本地文件
3.从本地文件恢复到目标库 | 目标库中数据与源库中数据一致 |  | Pass | [TD-29738](https://jira.taosdata.com:18080/browse/TD-29738) |  | 3 |
| basic | 1.源库中指定列增加压缩参数
2.创建同步任务 | 目标库中对应子表的指定列包含相同的压缩参数 |  | Pass | [TD-29746](https://jira.taosdata.com:18080/browse/TD-29746) |  | 建表语句：（指定二级压缩算法为 xz）
CREATE STABLE `meters` (
    `ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'xz' LEVEL 'high', 
    `pk_int` int ENCODE 'simple8b' COMPRESS 'xz' LEVEL 'high' PRIMARY KEY,
    `current` FLOAT ENCODE 'delta-d' COMPRESS 'xz' LEVEL 'high', 
    `voltage` INT ENCODE 'simple8b' COMPRESS 'xz' LEVEL 'high', 
    `phase` FLOAT ENCODE 'delta-d' COMPRESS 'xz' LEVEL 'high') 
TAGS (`groupid` INT, `location` VARCHAR(24)) |
| 基于订阅/查询的数据同步任务（自动创建topic，带with meta） | 1.数据源库中创建普通表，带复合主键和压缩参数
2.创建数据同步任务，同步普通表 | 目标库中同步的普通表与源库中一致 |  | Pass |  |  | 建表语句见上
查询方式验证通过 |
|  | 1.数据源库中创建超级表，符合主键类型为bigint，带压缩参数
2.创建数据同步任务，同步超级表 | 目标库中数据与源库中数据一致 |  | Pass |  |  | 查询方式验证通过 |
|  | 1.数据源库中创建超级表，符合主键类型为uint，带压缩参数
2.创建数据同步任务，同步超级表 | 目标库中数据与源库中数据一致 |  | Pass |  |  | 查询方式验证通过 |
|  | 1.数据源库中创建超级表，符合主键类型为ubigint，带压缩参数
2.创建数据同步任务,同步超级表 | 目标库中数据与源库中数据一致 |  | Pass |  |  | 查询方式验证通过 |
|  | 1.数据源库中创建普通表，符合主键类型为bigint，带压缩参数
2.创建数据同步任务,同步超级表 | 目标库中数据与源库中数据一致 |  | Pass |  |  | 查询方式验证通过 |
|  | 1.数据源库中创建普通表，符合主键类型为uint，带压缩参数
2.创建数据同步任务,同步普通表 | 目标库中数据与源库中数据一致 |  | Pass |  |  | 查询方式验证通过 |
|  | 1.数据源库中创建普通表，符合主键类型为ubigint，带压缩参数
2.创建数据同步任务,同步普通表 | 目标库中数据与源库中数据一致 |  | Pass |  |  | 查询方式验证通过 |
| 目标端表已存在的情况下，分别基于订阅/查询的数据同步 | 1.数据源库中创建超级表、子表，带复合主键
2.目标库中创建同名超级表、子表，且schema相同，带复合主键
3.创建数据同步任务 | 目标库中数据与源库中数据一致 |  | Pass |  |  | 订阅没有问题
查询方式验证通过 |
|  | 1.数据源库中创建超级表、子表，带复合主键
2.目标库中创建同名超级表、子表，列名相同，但第二列不为复合主键
3.创建数据同步任务 | 任务报错 |  | Pass | [TD-29767](https://jira.taosdata.com:18080/browse/TD-29767) |  | 查询方式存在相同问题，确认是预期行为 |
|  | 1.数据源库中创建超级表、子表，带复合主键
2.目标库中创建同名超级表、子表，schema不同
3.创建数据同步任务 | 任务报错 |  | Pass |  |  | 订阅符合预期。
表 Schema 不同，在写入的时候会报失败。
Error: [0] writing data message error: write table with raw block failed: Write raw block into target error after 0x0118 fix: [0x0118] Internal error: `Invalid parameters`: Internal error: `Invalid parameters`, block: Table view with 9 rows, 16 columns, table name "stb4_tb0"
查询方式验证通过。 |
|  | 1.数据源库中创建超级表、子表，带压缩参数
2.目标库中创建同名超级表、子表，schema不同
3.创建数据同步任务 | 任务报错 |  | Pass |  |  | Schema 不同写入会失败。
查询方式验证通过 |
|  | 1.数据源库中创建超级表、子表，带压缩参数
2.目标库中创建同名超级表、子表，列名相同，但不带压缩参数
3.创建数据同步任务 | 目标库中数据与源库中数据一致 |  |  | [TD-29746](https://jira.taosdata.com:18080/browse/TD-29746) |  | 查询方式验证通过 |
|  | 1.数据源库中创建超级表、子表，带压缩参数
2.目标库中创建同名超级表、子表，且schema相同，压缩参数描述一致
3.创建数据同步任务 | 目标库中数据与源库中数据一致 |  | Pass |  |  | 查询写入正常 |
|  | 1.数据源库中创建超级表、子表，带压缩参数
2.目标库中创建同名超级表、子表，且schema相同，压缩参数描述不一致
3.创建数据同步任务 | 数据正常写入且一致 |  | Pass |  |  | 订阅写入正常 |
| 手动提前创建topic（不含with meta） | 1.数据源库中创建超级表、子表，带复合主键，创建topic：create topic dbname as database dbname （订阅db）
2.目标库中提前创建相同schema的超级表和子表
3.创建数据同步任务 | 任务正常执行，目标库中数据与源库中数据一致 |  | Pass |  |  |  |
|  | 1.数据源库中创建超级表、子表，带复合主键，创建topic：create topic dbname as database dbname （订阅db）
2.目标库中不创建schema
3.创建数据同步任务 | 任务报错 |  | Pass | [TD-29774](https://jira.taosdata.com:18080/browse/TD-29774) |  |  |
|  | 1.数据源库中创建超级表、子表，带复合主键
2.目标库中创建同名超级表、子表，列名相同，但第二列不为复合主键
3.创建数据同步任务 | 数据能够同步，但是相同时间戳的数据会覆盖。 |  | Pass |  |  | 不会报错 |
|  | 1.数据源库中创建超级表、子表，带复合主键，创建topic：create topic stbname as stable stbname （订阅stb）
2.创建数据同步任务 | 目标库中创建普通表来写入子表的数据 |  | Pass |  |  | 会自动创建不存在的超级表 |
| 压缩参数 | 1. 源库中创建超级表，不指定压缩等级
1. 创建数据同步任务 | 目标库中对应表的压缩等级为1级压缩 |  | Pass |  |  | 查询方式验证通过 |
|  | 1. 源库中创建超级表，指定列的压缩等级
1. 创建数据同步任务 | 目标库中对应表的指定列的压缩等级与数据源一致 |  | Pass |  |  | 查询方式验证通过 |
| 数据备份及恢复 | 1.源库中超级表包含复合主键
2.启动备份任务
3.将备份的文件恢复至目标TD | 任务正常执行，目标库中表结构正确 |  | Pass |  |  | 备份语句：
taosx run -f "tmq+ws://u2-14:6041/pk_test?group.id=g11" -t "local:/data/qin/backup/" -v
恢复语句：
taosx run -f "local:/data/qin/backup/" -t "taos+ws://u2-14:6041/pk_test_backup1?assert" -v --yes-i-really-mean-it |
|  | 1.源库中超级表包含非默认的压缩参数
2.启动备份任务
3.将备份的文件恢复至目标TD上 | 任务正常执行，目标库中表结构正确 |  | Pass | [TD-29746](https://jira.taosdata.com:18080/browse/TD-29746) |  |  |
|  | 1.源库中超级表包含复合主键及压缩参数
2.启动备份任务
3.将备份的文件恢复目标TD上 | 任务正常执行，目标库中表结构正确 |  | Pass | [TD-29746](https://jira.taosdata.com:18080/browse/TD-29746) |  |  |
|  | 1.数据源库中创建超级表、子表，带复合主键，创建topic：create topic dbname as database dbname （订阅db，不带with meta）
2.目标库中提前创建相同schema的超级表和子表
3.创建数据备份任务，并恢复至目标TD | 任务正常执行，目标库中数据与源库中数据一致 |  | Pass |  |  | 备份命令：
taosx run -f "tmq+ws://root:taosdata@u2-14:6041/pk_test_stb1?group.id=gt1" -t "local:/data/qin/backup/" -v
恢复命令：
taosx run -f "local:/data/qin/backup/" -t "taos+ws://u2-14:6041/pk_test_backup2?assert" -v --yes-i-really-mean-it |
|  | 1.数据源库中创建超级表、子表，带复合主键，创建topic：create topic dbname as database dbname （订阅db，不带with meta）
2.目标库中不创建 schema
3.创建数据备份任务，并恢复至目标TD | 任务报错 |  | Pass |  |  | 备份语句：
taosx run -f "tmq+ws://u2-14:6041/pk_test_db?group.id=gt11" -t "local:/data/qin/backup/" -v
恢复语句：
taosx run -f "local:/data/qin/backup/" -t "taos+ws://u2-14:6041/pk_test_backup3?assert" -v --yes-i-really-mean-it
目前会报错，需要确认是否与数据同步的行为一致。
暂不与数据同步保持一致。 |
|  | 1.数据源库中创建超级表、子表，带复合主键，（订阅超级表，不带with meta）
2.目标库中创建同名超级表、子表，列名相同，但第二列不为复合主键
3.创建数据备份任务，并恢复至目标TD | 数据成功恢复，相同时间戳的数据会被覆盖 |  | Pass |  |  | 备份语句：
taosx run -f "tmq+ws://u2-14:6041/pk_test_db?group.id=gt11" -t "local:/data/qin/backup/" -v
恢复语句：
taosx run -f "local:/data/qin/backup/" -t "taos+ws://u2-14:6041/pk_test_same_col_no_pk" -v --yes-i-really-mean-it |
|  | 1.数据源库中创建超级表、子表，带复合主键，创建topic：create topic stbname as stable stbname （订阅stb，不带with meta）
3.创建数据备份任务，并恢复至目标TD | 任务报错 |  | Pass |  |  | 恢复命令：
taosx run -f "local:/data/qin/backup/" -t "taos+ws://u2-14:6041/pk_test_backup4?assert" -v --yes-i-really-mean-it |

### 9.2 可用性

无。

### 9.3 可靠性

无。

### 9.4 性能

无。

### 9.5 安全性

无。

### 9.6 兼容性

以下测试用例中：
1. 数据备份生成文件格式（0.0、0.1主要用于描述不同版本组合下生成的数据文件，测试中不对文件格式本身进行验证）
2. 低版本taosx 采用main分支编译（版本号 1.5.x）

|  | Description | Expected Results | Result | Jira | Automated | Memo |
| --- | --- | --- | --- | --- | --- | --- |
| 数据同步(订阅/查询) | 源TDengine版本<3.3.0.0
目标TDengine版本>=3.3.0.0 | 正常同步 | Pass |  |  |  |
|  | 源TDengine版本>=3.3.0.0
目标TDengine版本>=3.3.0.0 | 正常同步 | Pass |  |  |  |
|  | 源TDengine版本>=3.3.0.0
目标TDengine版本<3.3.0.0 | 创建任务时报错 | Pass |  |  | 报错信息：License error: Source version is 3.3.0 or later, but target version is earlier than 3.3.0, which is not supported.
订阅和查询均符合预期 |
| 数据备份 | 源TDengine版本<3.3.0.0
taosx版本>=1.6.0 | 正常备份，文件格式为1.0 | Pass |  |  |  |
|  | 源TDengine版本<3.3.0.0
taosx版本<1.6.0 | 正常备份，文件格式为0.0 | Pass |  |  |  |
|  | 源TDengine版本>=3.3.0.0
taosx版本<1.6.0 | 正常备份，文件格式为0.0 | Pass |  |  |  |
|  | 源TDengine版本>=3.3.0.0
taosx版本>=1.6.0 | 正常备份，文件格式为1.0 | Pass |  |  |  |
|  | 源TDengine版本<3.3.0.0
taosx版本>=1.6.0 | 正常备份，文件格式为1.0 | Pass |  |  |  |
| 数据恢复 | 文件格式为0.0
taosx版本<1.6.0
目标TDengine版本<3.3.0.0 | 正常恢复数据 | Pass |  |  |  |
|  | 文件格式为0.0
taosx版本>=1.6.0
目标TDengine版本>=3.3.0.0 | 正常恢复数据 | Pass | [TD-29792](https://jira.taosdata.com:18080/browse/TD-29792) |  | 备份文件正确，恢复时子表丢失 |
|  | 文件格式为1.0
taosx版本<1.6.0
目标TDengine版本<3.3.0.0 | 不支持，报错 | Pass |  |  | 报文件版本不匹配错误（We're so sorry that we cant read in-compatible version 1.0 at 0.0 app） |
|  | 文件格式为1.0
taosx版本>=1.6.0
目标TDengine版本<3.3.0.0 | 不支持，报错 | Pass |  |  | 报文件版本不匹配错误（We're so sorry that we cant read in-compatible version 1.0 at 0.0 app） |
|  | 文件格式为1.0
taosx版本<1.6.0
目标TDengine版本>3.3.0.0 | 不支持，报错 | Pass |  |  | 报文件版本不匹配错误（We're so sorry that we cant read in-compatible version 1.0 at 0.0 app） |
|  | 文件格式为1.0
taosx版本>=1.6.0
目标TDengine版本>=3.3.0.0 | 正常恢复数据 | Pass |  |  |  |

### 9.7 本地化

该功能修改不涉及UI变更。

## 10. 待讨论问题

## 11. Jira（Bugs or Improvement）

此feature相关的所有Jira, 标题中应包含统一的标签: tmq，taosx
epic：taosx1.6.0

TD-29746


TD-29792


TD-29774


TD-29738


TD-29767


## 12. 测试计划 (Plan)

见

## 13. 测试备忘 (Note)

关于复合主键的用法：
在建表时指定第二列为primary key，目前只支持int32，int64，uint32，uint64，varchar 共5种数据类型。
```sql {wrap}
CREATE TABLE t (
    ts TIMESTAMP, 
    obj_id VARCHAR(64) PRIMARY KEY,
    data1 FLOAT,
    data2 int
);
```

关于压缩参数的用法：
encode_type:   一级压缩
compress_type:  二级压缩
level:  特指二级压缩的级别，默认值为medium,  支持简写为 'h'/'l'/'m'
如果只指定compress_type而没有指定encode_type，则一级压缩采用默认值；
如果只指定encode_type而没有指定compress_type，则只采用一级压缩，不采用二级压缩；
如果encode_type和compress_type均不指定，则一级压缩和二级压缩均为默认值
```sql {wrap}
create TABLE t (
    ts TIMESTAMP ENCODE 'Simpple8B' COMPRESS 'lz4' level 'high',
    i int ENCODE 'XOR' COMPRESS 'gzip' Level 'high',
    j float COMPRESS 'tsz' Level 'medium',
    k int ENCODE 'XOR', 
    m BLOB COMPRESS 'disabled',
    n double 
)
```

## 14. 参考文档 (Reference)

这里用于添加对该需求测试有帮助的文档链接：
- [数据复制与同步支持 复合主键 + 压缩增强](https://taosdata.feishu.cn/wiki/ZmNiwQwGdiN3Lsk1AZ8cNngInlb)
- [TDengine 压缩增强](https://taosdata.feishu.cn/wiki/St4WwSX5Ei3VfMk3yMUcv2DMnMh)
- [复合主键](https://taosdata.feishu.cn/wiki/OLQjwCpQhiRFE3kS8Uvc3sornRb)
