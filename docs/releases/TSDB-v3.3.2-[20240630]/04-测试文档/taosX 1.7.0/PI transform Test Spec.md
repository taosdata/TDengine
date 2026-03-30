# PI transform Test Spec

## 1. 测试目标

本次测试主要目标：
- 验证 PI/PIbackfill 多列模式下的 transform 功能的正确性
- 验证当 attribute（包含child attribute） data reference 为 fomula 时，同步到 TDengine 中作为 tag 存储的正确性
- 验证当 attribute（包含child attribute） data reference 为 analysis 时，同步到 TDengine 中作为 column 存储的正确性
- 验证PIbackfill 支持断点续传功能

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.06.15 | v0.0 | @贾晨阳 | 初稿完成 |
| 2024.06.25 | v1.0 | @贾晨阳 | 测试完成 |

## 3. 测试范围

这里用于描述本需求的覆盖范围：
- PI/PIbackfill 获取默认transform配置功能
- PI/PIbackfill 使用默认transform配置进行数据接入
- PI/PIbackfill 使用自定义transform配置进行数据接入
- PIbackfill 支持断点续传功能

## 4. 测试结论

本次测试共验证并通过了以下内容：
- Cargill 项目中新增的功能，包括：
  - 支持 child attribute 导入TDengine
  - 支持 formula 类型数据作为 tag 导入TDengine
  - 支持 analysis 类型数据作为column 导入TDengine
- 多列模式下 transform功能的正确性，包括：
   - 修改超级表名
   - 修改子表名
   - 增删 column 
   - 增删 tag 
   - 修改列名和标签名
   - 添加过滤规则
   - 更改列的映射规则
   - 增删点位或增删元素
   - 增删超级表
- PIbackfill 断点续传功能

## 5. 开发质量报告

结论：本特性/优化的开发质量是优

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 46 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

- 由于未来会限制单列模式和多列模式的检索条件，为了避免不必要的工作，测试进行以下划分：
  - 当 server 配置为 PI Data Archive Only 时，只验证通过 point 方式检索；
  - 当 server 配置为 PI Data Archive and Asset Framework Server 时，只验证通过 template 方式检索。
- 本次测试不开展单列模式的验证

## 7. 测试环境

- taosx、taos-explorer：192.168.2.10，linux
- taosx-agent、pi-connector：192.168.0.34、windows server

## 8. 测试数据 (Optional)

测试数据采用cargill库的template

## 9. 测试用例

### 9.1 功能

| case type | Description | Expected Results | Result | Jira | Automated | Memo |
| --- | --- | --- | --- | --- | --- | --- |
| 获取默认配置 | 1. PI 系统配置选择“PI Data Archive Only”
1. 选择【单列模型】，过滤方式为【points】，设置条件为“Meter_1000001_Current”，3.执行【下载默认配置】 | 下载的配置文件中，内容包含了当前PI 系统中满足过滤条件的PIpoints的信息 | pass | [https://jira.taosdata.com:18080/browse/TD-30079](https://jira.taosdata.com:18080/browse/TD-30079) |  |  |
|  | 1. PI 系统配置选择“PI Data Archive Only”
1. 选择【单列模型】，过滤方式为【points】，设置条件空，即全查询
3.执行【下载默认配置】 | 下载的配置文件中，内容包含了当前PI 系统中PIpoints的信息 | pass |  |  |  |
|  | 1. PI 系统配置选择“PI Data Archive Only”
1. 选择【单列模型】，过滤条件为“Meters*”【下载默认配置】
2. 上传下载的配置 | 下载的配置文件中，内容包含了当前PI 系统中满足过滤条件的PIpoints的信息 | pass |  |  |  |
|  | 1. PI 系统配置选择“PI Data Archive and Asset Framework Server”
1. 选择【单列模型】，过滤条件为空，即查询全部template，【下载默认配置】 | 下载的配置文件中，内容包含了当前PI 系统中所有template配置 | pass |  |  |  |
|  | 1. PI 系统配置选择“PI Data Archive and Asset Framework Server”
1. 选择【单列模型】，过滤条件为空，即查询全部template，【下载默认配置】 | 下载的配置文件中，内容包含了当前PI 系统中所有template配置 | pass |  |  |  |
|  | 1. PI 系统配置选择“PI Data Archive Only”
1. 选择【单列模型】，过滤条件为空，【下载默认配置】
2. 上传下载的配置，创建任务 | 下载的配置文件中，内容包含所有PIpoint，在TDengine中会写入所有PI system中的point数据 |  |  |  |  |
| 使用默认配置创建多列模型任务 | 1. PI 系统配置选择“PI Data Archive and Asset Framework Server”
2.选择【多列模型】，过滤条件为“template“，”Equipment Tag”
1. 下载默认配置，创建任务 | 任务创建成功，在TDengine中创建满足符合条件的element子表，且schema满足配置规则。 | pass | [https://jira.taosdata.com:18080/browse/TD-30085](https://jira.taosdata.com:18080/browse/TD-30085)
[https://jira.taosdata.com:18080/browse/TD-30078](https://jira.taosdata.com:18080/browse/TD-30078) |  |  |
|  | 1. PI 系统配置选择“PI Data Archive and Asset Framework Server”
2.选择【多列模型】，过滤条件为空，即获取全部template
1. 下载默认配置，创建任务 | 任务创建成功，在TDengine中创建满足符合条件的element子表，且schema满足配置规则。 | pass | https://jira.taosdata.com:18080/browse/TD-30078 |  |  |
|  | 1. PI 系统配置选择“PI Data Archive and Asset Framework Server”
2.选择【多列模型】，过滤条件为“template——Template*”
1. 下载默认配置，创建任务 | 正确下载所有“Template”开头的模版下的所有element的默认配置文件 | pass |  |  |  |
|  | 1. PI 系统配置选择“PI Data Archive and Asset Framework Server”
2.选择【多列模型】，按template，不设置过滤条件
1. 下载默认配置，创建任务 | 正确下载包含所有template的默认配置文件 | pass | [https://jira.taosdata.com:18080/browse/TD-30112](https://jira.taosdata.com:18080/browse/TD-30112) |  |  |
|  | 1. PI 系统配置选择“PI Data Archive and Asset Framework Server”
2.选择【多列模型】，按template过滤，过滤条件设置为“OP Budget”
1. 下载默认配置，创建任务 | 正确创建任务，TDengine中正确建表，覆盖所有链接该template的element，且element的数据结构正确。 | pass | https://jira.taosdata.com:18080/browse/TD-30122 |  |  |
| data reference | 1. 在PI system 中配置 attribute的data reference为 formula 类型 | 对应attribute在TDengine中以tag形式进行存储 | pass |  |  |  |
|  | 1. 在PI system 中动态修改formula类型attribute的值 | 对应TDengine中的tag值同步变更 | pass |  |  |  |
|  | 1. 在PI system 中配置 attribute的data reference为 analysis 类型 | 对应attribute在TDengine中以column形式进行存储 | pass |  |  |  |
|  | 1. 在PI system 中配置 attribute的data reference为 None 类型 | 对应attribute在TDengine中以tag形式进行存储 | pass |  |  |  |
|  | 1. 在PI system 中动态修改None类型attribute的值 | 对应TDengine中的tag值同步变更 | pass |  |  |  |
|  | 1. 在PI system中配置 attribute的输入值为异常值（即无效输入） | 写入TDengine中对应列的value为null，status为-1 | pass | https://jira.taosdata.com:18080/browse/TD-30277 |  |  |
| child attribute | 1. 在PI system中配置 child attribute，其data reference 类型为PI point | 写入TDengine中对应列为column列 | pass |  |  |  |
|  | 1. 在PI system中配置 child attribute，其data reference 类型为None/formula/string builder | 写入TDengine中对应列为tag列 | pass |  |  |  |
| transform
（映射） | 1. 修改配置文件：在超级表配置中增加一个column，并指定为固定值。
1. 提交新配置文件，提交任务 | TDengine中对应超级表新增一个column，该列值与配置的固定值一致 | pass |  |  |  |
|  | 1. 修改配置文件：在超级表配置中增加一个column，并指定为一个PI中的attribute。
1. 提交新配置文件，提交任务 | TDengine中对应超级表新增一个column，该列值与配置的PI中的对应attribute一致 |  |  |  |  |
|  | 1. 修改配置文件：在超级表配置中增加一个tag，并指定为固定值。
1. 提交新配置文件，提交任务 | TDengine中对应超级表新增一个tag，该列值与配置的固定值一致 | pass |  |  |  |
|  | 1. 修改配置文件：在超级表配置中增加一个tag，并指定为PI中一个静态attribute。
1. 提交新配置文件，提交任务 | TDengine中对应超级表新增一个tag，该列值与配置的PI中对应静态属性一致 | pass |  |  |  |
|  | 1. 修改配置文件：在超级表配置中增加一个tag，并指定为PI中一个动态attribute。
1. 提交新配置文件，提交任务 | TDengine中对应超级表新增一个tag，该tag值为对应attribute的初始值 |  |  |  |  |
|  | 1.修改配置文件：修改超级表配置中的一个column的类型，修改为其映射attribute支持的数据类型（配置文件中column列由int修改为bigint，attribute为int32）
2.提交新配置文件，创建任务 | TDengine中对应col正常写入 | pass |  |  |  |
|  | 1.修改配置文件：修改超级表配置中的一个column的类型，修改为其映射attribute不支持的数据类型（配置文件中column列由double修改为int，attribute为double）
2.提交新配置文件，创建任务 | 高精度类型映射到低精度类型时会截断 | pass |  |  |  |
|  | 1.修改配置文件：修改超级表配置中的一个column的类型，修改映射attribute支持的数据类型（配置文件中column列由double修改为varchar，attribute为double）
2.提交新配置文件，创建任务 | TDengine中对应col正常写入 | pass |  |  |  |
|  | 1.修改配置文件：修改超级表名
2.提交新配置文件，创建任务 | 创建成功，创建的超级表名满足修改后的超级表名称 | pass |  |  |  |
|  | 1.修改配置文件：修改子表名
2.提交新配置文件，创建任务 | 创建成功，创建的子表名满足修改后的子表命名规则 | pass |  |  |  |
|  | 1.修改配置文件：修改新增一列column，其映射为PI中指定attribute*2
2.提交新配置文件，创建任务 | TDengine中对应column的值满足配置 | pass |  |  |  |
|  | 1.修改配置文件：修改新增一列column，其映射为PI中指定attribute1 + attribute2
2.提交新配置文件，创建任务 | TDengine中对应column的值满足配置 | pass |  |  |  |
| filter | 1. 修改配置文件：配置filter: attribute>1000，过滤出所有大于1000的attribute值 | TDengine中对应列的值满足filter规则 | pass |  |  |  |
| pibackfill 断点续传 | 1. 启动backfill任务，任务执行过程中stop，再重启任务 | 从连接器日志中可查看从断点启动的记录 | pass |  |  | [task:216]06/24 10:55:55.701 16120 [17] INFO  TDPIConnector.Core.Backfill - Backfill element Equipment Tag:a703a9be-1d93-11ef-bf15-00505695feda from breakpoint 2024/5/13 14:14:36. |
|  | 1. 启动backfill任务，任务执行过程中stop,
1. 删除目标库中超级表及数据，再重启任务 | 目标库中超级表重新创建，first(*) 的时间戳为断点时间 | pass |  |  |  |

### 9.2 可用性

无

### 9.3 可靠性

无

### 9.4 性能

本次测试未单独开展性能测试。

### 9.5 安全性

无

### 9.6 兼容性

本次修改后，将不再兼容旧版本任务，不开展兼容性测试。

### 9.7 本地化

无

## 10. 待讨论(Optional)

无

## 11. Jira

<!-- Unsupported block type: 999 -->

## 12. 测试计划 (Optional)

1. 依据 cargill 项目，对多列模式默认transform规则进行验证
2. 验证多列模式 transform 功能正确性
3. 验证断点续传功能正确性

## 13. 测试备忘 (Optional)

### 13.1 单列模型配置文件示例

|  |
|  |
| # UOM 1 |  |  |  |  |
| **SuperTable** | horsepower |  |  |  |
| **SubTable** | ${point_id} |  |  |  |
| **Filter** | $value > 0 |  |  |  |
| ts | KEY | timestamp | $ts |  |
| value | COLUMN | int | $value |  |
| status | COLUMN | int | $status |  |
| path | TAG | NCHAR(100) | $path |  |
| point_id | TAG | NCHAR(100) | $point_id |  |
| point_name | TAG | NCHAR(100) | $point_name |  |
| point_class | TAG | NCHAR(100) | $point_class |  |
| point_source | TAG | NCHAR(100) | $point_source |  |
| eng_units | TAG | NCHAR(100) | $eng_units |  |
| descriptor | TAG | NCHAR(100) | $descriptor |  |
| exdesc | TAG | NCHAR(100) | $exdesc |  |
| source_tag | TAG | NCHAR(100) | $source_tag |  |
| element_paths | TAG | NCHAR(200) | $element_paths |  |
| # UOM 2 |  |  |  |  |
| ***SuperTable*** | kilowatt |  |  |  |
| **SubTable** | ${point_id} |  |  |  |
| ts | KEY | timestamp | $ts |  |
| Value | COLUMN | int | $value |  |
| path | Tag |  |  |  |
| point_name | Tag | NCHAR(100) | $point_name |  |
| point_class | Tag | NCHAR(100) | $point_class |  |
| point_source | Tag | NCHAR(100) | $point_source |  |
| eng_units | Tag | NCHAR(100) | $eng_units |  |
| descriptor | Tag | NCHAR(100) | $descriptor |  |
| exdesc | Tag | NCHAR(100) | $exdesc |  |
| source_tag | Tag | NCHAR(100) | $source_tag |  |
| element_paths | Tag | NCHAR(100) | $elements |  |
| Point Name 1 | Point | SuperTable A |  |  |
| Point name 2 | Point | SuperTable B |  |  |
| Point Name 3 | Point | SuperTable A |  |  |
| Point Name 4 | Point | SuperTable B |  |  |

### 13.2 多列模型配置文件示例

|  |
| --- |
| **SuperTable** | smart_meter |  |  |  |
| **Template** | SmartMeter |  |  |  |
| **SubTable** | ${element_id} |  |  |  |
| ts | KEY | timestamp | $ts |  |
| col1 | COLUMN | float | $Metric1 |  |
| col2 | COLUMN | int | $Metric2 |  |
| col3 | COLUMN | double | $Metric3 |  |
| element_name | TAG | NCHAR(100) | $element_name |  |
| path | TAG | NCHAR(100) | $path |  |
| tag1 | TAG | NCHAR(100) | $attribute1 |  |
| tag2 | TAG | NCHAR(100) | $attribute2 |  |
| **SuperTable** | car |  |  |  |
| **Template** | car |  |  |  |
| **SubTable** | ${element_name} |  |  |  |
| **Filter** | $metric1 > 0 |  |  |  |
| ts | KEY | timestamp | $ts |  |
| col1 | COLUMN | float | $metric1 |  |
| col2 | COLUMN | int | $metric2 |  |
| col3 | COLUMN | double | $metric3 |  |
| col4 | COLUMN | float | $metric1 + $metric2 |  |
| element_name | TAG | NCHAR(100) | $element_name |  |
| path | TAG | NCHAR(100) | $path |  |
| attribute1 | TAG | NCHAR(100) | $attribute1 |  |
| attribute2 | TAG | NCHAR(100) | $attribute2 |  |
| attibute3 | TAG | NCHAR(100) | $attribute3 |  |
| Element 1 | Element | SuperTable1 | Element_ID1 | Element Path1 |
| Element 2 | Element | SuperTable2 | Element_ID2 | Elemen Path2 |
| Element 3 | Element | SuperTable3 | Element_ID3 | Element Path3 |
| Element 4 | Element | SuperTable4 | Element_ID4 | Element Path4 |
| Element 5 | Element | SuperTable5 | Element_ID5 | Element Path5 |

## 14. 参考文档 (Optional)

[PI System Transformation](https://taosdata.feishu.cn/wiki/HSwGwyCBoiBYEXkCjcicNQhBnyb)
