# OPC CSV 文件合法性校验 Test Spec 

## 1. 测试目标

- 确保 OPC CSV 文件合法性的校验功能的引入不会影响之前的功能
- 验证 taosx 对于 OPC UA/DA 上传使用的文件能够做有效的合法性校验

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2023-03-20 | 0.1 | @秦冲 | 初稿 |
|  |  |  |  |

## 3. 测试范围

这里用于描述本需求的覆盖范围：
- OPC UA 使用文件上传创建采集任务
- OPC DA 使用文件上传创建采集任务

## 4. 测试结论

测试过程中涉及到 OPC UA 和 OPC DA 的文件校验：
- 文件编码非 UTF 8，会返回文件编码不符合并提示文件编码需为 UTF 8。验证通过。
- 文件列数据非法（重复列，列缺失，TAG 列配置错误等）能够成功校验并提示错误原因。验证通过。
- 文件行数据非法（关键行数据缺失、type 配置为非法类型、transform 表达式非法等）能够成功校验并提示错误原因。验证通过。
- 单个任务能够完成一万点位每秒的速度写入。验证通过。
相关问题和限制见第 6 节。

## 5. 开发质量报告

结论：本特性/优化的开发质量是一般。（优，良，一般，差，很差）
整个功能的测试中，文件编码的合法性校验较为顺利没有出现错误。CSV 文件的 Header 处理除 Tag 外其他的校验较为准确，Tag 的格式校验正确但是新增和修改在测试发现问题。CSV 文件的行数据的校验问题较多，出现许多行数据中的数据非法，但是没有成功校验出来，上传之后会返回 CSV 文件有效，但是创建任务之后任务无法按照预期正常运行。

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 12 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

这里用于记录产品使用上的一些限制，包括不支持的场景等，以及在发版时没有解决的minor issues.
- OPC 数据写入若使用宽表，则必须提前建表，否则会写入失败。
- OPC 点位在经过 transform 之后，浮点型数据会有精度丢失。
- 文件编码校验的过程中可能会出现实际的文件编码与错误提示的文件编码不符的情况（比如 VS Code 使用 ISO-8859-1 保存之后提交，taosx 会提示文件编码是 windows-1225），不过会正确提示文件编码应该是 UTF8 的。
- Transfrom 的配置只支持数值类型，不支持例如字符串添加前缀这样的操作
- 表名和列名只校验长度，若含有特殊字符 . 和 ` ，会替换为 _
- 暂不支持通过配置文件进行 TAG 的修改（即表存在，配置的 TAG 包含表不存在的 TAG）。

## 7. 测试环境

- taosx、Explorer、linux agent、taosd、taosadapter 部署在 192.168.2.18
- windows agent 部署在 192.168.2.16
- OPC UA Server 使用 192.168.2.16 和 192.168.1.66 部署的 Kepware 和 Prosys OPC UA Simulaltion
- OPC DA Server 使用 192.168.2.16 和 192.168.1.66 部署的 Matrikon OPC DA Simulation

## 8. 测试数据 (Optional)

这里用于描述性能、稳定性测试时的数据准备工作，包括但不局限于：
- 192.168.1.66 的 OPC UA 包含 8万点位，OPC DA 包含 10万点位
- 192.168.2.16 的 OPC UA 和 OPC DA 用于功能测试，仅包含少量点位

## 9. 测试用例

### 9.1 功能

标记 basic 的用例为基础用例，开发同学在提测时，请保证基础用例全部通过，并填写状态。
| 类型 | 用例描述 | 期望行为 | 基础用例状态 | 测试状态 | Memo | 是否添加到自动化中 |
| --- | --- | --- | --- | --- | --- | --- |
| basic【已有功能】 | 创建 OPC UA/DA 任务使用 UTF8 编码的正常格式文件
包含所有列：rts 在左侧，transform 都不配置，超级表及子表都使用占位符 | 任务创建成功，创建的子表和超级表的 Schema 符合配置（rts 作为主键列） | Pass | Pass |  |  |
| basic【已有功能】 | 创建 OPC UA/DA 任务使用 UTF8 编码的正常格式（包含所有列）文件
包含所有列：rts 在右侧，transform 都配置，超级表及子表都使用确定的名称 | 任务创建成功，创建的超级表和子表 ts 作为主键列，写入的数据符合 transform 结果。 | Pass | Pass | [TD-29461](https://jira.taosdata.com:18080/browse/TD-29461)
可以允许精度丢失 |  |
| 【已有功能】 | 创建 OPC UA/DA 任务使用 UTF8 编码的正常格式（包含所有列）文件
不配置 ts 与 rts 列 | 任务创建成功，ts 作为主键列 |  | Pass | [TD-29450](https://jira.taosdata.com:18080/browse/TD-29450) |  |
|  | 创建 OPC UA/DA 任务使用 UTF8 with BOM 编码的正常格式文件 | 任务创建成功，数据正常写入 |  | Pass |  |  |
| basic | 创建 OPC UA/DA 任务使用非 UTF8 编码的正常格式文件（GB18030） | 任务创建失败，提示文件编码格式非 UTF8 | Pass | Pass | 提示信息为：check csv file failed, 
cause: invalid CSV file encoding: GBK, 
only UTF-8 or UTF-8 BOM 
supported |  |
|  | 创建 OPC UA/DA 任务使用非 UTF8 编码的正常格式文件（GBK） | 任务创建失败，提示文件编码格式非 UTF8 |  | Pass |  |  |
|  | 创建 OPC UA/DA 任务使用非 UTF8 编码的正常格式文件（ISO-8859-1/Latin-1） | 任务创建失败，提示文件编码格式非 UTF8 |  | Pass |  |  |
| basic | 创建 OPC UA/DA 任务包重复 ts_col 列的正常格式文件 | 任务创建失败，提示文件包含重复列 | Pass | Pass |  |  |
| 重复列 | 创建 OPC UA/DA 任务包含 point_id（tag_name）\stable\tbname\enable\value_col\value_transform\type\quality_col\ts_col\received_ts_col\ts_transform\received_ts_transform 等重复列的正常格式文件 | 任务创建失败，提示文件包含重复列 |  | Pass |  |  |
| TAG 列 | 使用不包含 TAG 列的配置文件创建任务，同时写入的库中不存在超级表 | 创建任务成功，对应的超级表创建成功，包含默认的 TAG point_id 和 point_name |  | Pass |  |  |
| TAG 列 | 使用不包含 TAG 列的配置文件创建任务，同时写入的库中存在超级表和子表 | 创建任务成功，超级表的 Schema 不会被修改 |  | Pass | CREATE STABLE `opcua`.`stb_int` 
(`ts` TIMESTAMP, `quality` INT, 
`rts` TIMESTAMP, `val` VARCHAR(128)) 
TAGS (`tag1` VARCHAR(256), `tag2` int)

CREATE TABLE `opcua`.`tb2` 
USING `opcua`.`stb_int` (`tag1`, 
`tag2`) TAGS ("tb2-tag1", 33) |  |
| TAG 列 | 使用包含4列 TAG 列的（两列 Tag 列不存在于已有的超级表中，一列存在且类型相同，一列存在类型为 VARCHAR 长度比已有的定义长）配置文件创建任务，同时写入的库中存在超级表 | 创建任务成功，超级表新增不存在的 TAG 列，修改已有的 TAG 定义 |  | Fail | [TD-28797](https://jira.taosdata.com:18080/browse/TD-28797) |  |
| TAG 配置错误 | 创建的 OPC UA/DA 任务包含重复的 TAG 列 tag::varchar(128)::name,tag::varchar(128)::name | 任务创建失败，提示文件包含重复标签 |  | Pass | 只会提示列重复
check csv file failed, 
cause: duplicated column
 name: name |  |
| TAG 配置错误 | 创建的 OPC UA/DA 任务包含错误的 TAG 列配置 tag::varcharchar(128)::name/tag::varchar(128))::name/tag22::varchar(128))::name2/tag::varchar(128):::name/
tag::varchar(128)::value/
tag::varchar(128)::dou.ble/
tag::varchar(128)::VALUE | 任务创建失败，提示 TAG 配置错误 |  | Pass | tag::varcharchar(128)::name 通过
tag::varchar(128))::name2 
未提示错误，但是建表结果符合预期
tag22::varchar(128))::name2 未提示错误
，建表结果符合预期
其他符合预期 |  |
| TAG 长度超出 | 创建的 OPC UA/DA 任务包含 TAG 列配置 tag::varcharchar(20)::name，列数据中包含长度大于 20 的 tag 值 | 子表创建失败，该点位数据写入失败 |  | Pass | 日志中可以观察到子表创建失败 |  |
|  | 创建 OPC UA/DA 任务使用的文件缺少必填列（point_id/tagname，tbname，stable） | 任务创建失败，提示文件缺少列 |  | Pass |  |  |
|  | 创建 OPC UA/DA 任务使用的文件只有必填列（point_id/tagname，tbname，stable） | 任务创建成功，数据写入正常 |  | Pass |  |  |
|  | 创建 OPC UA/DA 任务使用的文件必填列（point_id/tagname，tbname，stable）为空 | 任务创建失败，提示必填列不能为空 |  | Pass | stable 列必须要有但是列可以为空，这样有些矛盾
子表名同样。
[TD-29525](https://jira.taosdata.com:18080/browse/TD-29525)（已修复） |  |
| basic | 创建 OPC UA/DA 任务使用 point_id/tag_name 重复行的文件 | 任务创建失败，提示 point_id 有重复 | Pass | Pass |  |  |
| basic | 创建 OPC UA/DA 任务使用不同 point_id/tag_name 使用相同 stable 和 subtable 的文件，不同 point_id/tag_name 使用的 value 列名不同 | 任务创建成功，数据写入成功，超级表 Schema 为宽表格式 | Pass | Pass | 若使用宽表，则必须提前建表。
建表语句：
CREATE STABLE `opcua`.`stb1` (`ts` TIMESTAMP, 
val1 double, val2 int, val3 double, val4 bool, 
val5 varchar(256)) TAGS (`point_id` VARCHAR(256), 
`point_name` VARCHAR(256))

CREATE TABLE `opcua`.`tb1` using `opcua`.`stb1`  
(`point_id`, `point_name`) TAGS ("point_id1", "point_name1"); |  |
|  | 创建 OPC UA/DA 任务使用不同 point_id/tag_name 使用相同 stable 和 subtable 的文件，不同 point_id/tag_name 使用的 value 列名相同 | 任务创建失败，提示 point_id/tag_name 使用的表名和列名不能同时相同 |  | Pass | [TD-29534](https://jira.taosdata.com:18080/browse/TD-29534) |  |
|  | 创建 OPC UA/DA 任务使用不同 point_id/tag_name 使用相同 stable 和 subtable 的文件，4097 列个不同 point_id/tag_name 使用的 value 列名不同 | 任务创建失败，提示使用相同超级表的点位超过 4096 个 |  |  | 由于目前宽表模式需要提前建表
，所以该用例无效。 |  |
| 行数据非法 | 创建 OPC UA/DA 任务使用的文件中的行数据非法。
1.point_id、tag_name 不存在；
2.enable 设置为 2 或其他字符；
3.stable 与 tbname 包含"."、中文等字符
4.value_col 包含 value,-,.,中文
5.value_transform 使用非法的 Rhai 引擎表达式：if value > 1 value
6.type 使用不支持的字符 unint
7.quality_col/received_ts_col/
ts_col 包含 -
8.ts_transform 使用非法的 ts + 8h
9.received_ts_transform 使用非法的 ts + 8h
10.ts_col，rts_col 同时为空 | 任务创建失败，提示 CSV 文件不合法 |  | Pass | 1.通过
2.enable 为 0 或 22 也会生效。[TD-29535](https://jira.taosdata.com:18080/browse/TD-29535)
3.表名中包含 . 的话会替换为 _，但是无法正确处理中文。
但提示文件有效。[TD-29538](https://jira.taosdata.com:18080/browse/TD-29538)
4.[TD-29538](https://jira.taosdata.com:18080/browse/TD-29538)
5.无法校验非法的 transform 表达式 [TD-29550](https://jira.taosdata.com:18080/browse/TD-29550) （已修复
6.无法校验数据类型是否正确 [TD-29547](https://jira.taosdata.com:18080/browse/TD-29547) （已修复
7.通过。列名中的 . 不能是最后一个字符。
8 和 9 [TD-29550](https://jira.taosdata.com:18080/browse/TD-29550) （已修复）
10.通过，此时 rts 不会作为表字段，ts_col 会使用默认值 ts |  |
|  | 创建 OPC UA/DA 的任务使用包含 enable 为 0 的配置文件 | 配置为 0 的点位对应的子表被删除 |  | Pass | [TD-29535](https://jira.taosdata.com:18080/browse/TD-29535) |  |
|  | 使用空的配置文件创建任务 | 创建任务失败，提示 Header 为空 |  | Pass | 提示 check csv file failed, cause: point_id is required |  |
|  | 使用缺少 Header 的配置文件创建任务 | 创建任务失败，提示 Header 为空 |  | Pass |  |  |
|  | 使用只有 Header 的配置文件创建任务 | 创建任务失败，数据行为空 |  | Pass | [TD-29552](https://jira.taosdata.com:18080/browse/TD-29552)（已修复） |  |
|  | 使用列数不统一的配置文件创建任务 | 创建任务失败，提示具体某一列数据有问题 |  | Pass | check csv file failed, cause: failed to read csv line in file: 
@./files/1712826033378/OPC 模板测试测试用例28.csv, 
cause: CSV error: record 1 (line: 2, byte: 160): 
found record with 8 fields, 
but the previous record has 14 fields |  |
|  | 不使用 Agent 使用正常的模板创建 OPC UA 任务 | 任务创建成功，数据写入成功 |  | Pass |  |  |

### 9.2 可用性

### 9.3 可靠性


### 9.4 性能

| 类型 | 用例描述 | 期望行为 | 基础用例状态 | 测试状态 | Memo | 是否添加到自动化中 |
| --- | --- | --- | --- | --- | --- | --- |
|  | 创建 OPC UA 单个任务使用 1 万点位的点位配置 | 任务创建成功，可以做到 1 万每秒的数据写入 | Pass |  |  |  |

### 9.5 安全性

测试用例包括但不局限于：
- 日志中是否包含敏感信息？

### 9.6 兼容性

测试用例包括但不局限于：
- 升级安装后，老版本（上一个版本）下创建的任务，能否继续执行？
| 类型 | 用例描述 | 期望行为 | 基础用例状态 | 测试状态 | Memo | 是否添加到自动化中 |
| --- | --- | --- | --- | --- | --- | --- |
|  | 使用旧模板创建任务 | 任务创建失败，提示 Header 设置有误 |  | Pass | 不兼容旧模板，提示 point_id 不能为空 |  |


### 9.7 本地化

无

## 10. 问题(Optional)

无

## 11. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: active-standby

## 12. 测试计划 (Optional)

这里用于计划此 feature 测试的开始和结束时间。

## 13. 测试备忘 (Optional)

这里用于记录测试过程中发现的，与产品行为相关的一些重要信息。

## 14. 参考文档 (Optional)

- [OPC CSV 文件合法性校验](https://taosdata.feishu.cn/wiki/Jl79wKnOviHc7ikLBXlc6ENDnqb)
