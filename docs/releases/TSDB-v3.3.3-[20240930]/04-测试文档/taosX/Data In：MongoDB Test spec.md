# Data In：MongoDB Test spec

## 1. 测试目标

- 验证MongoDB数据库数据迁移、数据同步至TDengine

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.07.12 | 0.1 | 聂敏慧 | Initial Draft |

## 3. 测试范围

本需求的覆盖范围：
- MongoDB 指定时间区间的历史数据迁移
- MongoDB 通过指定起始时间进行实时数据同步
- MongoDB 按时间进行分表场景

## 4. 测试结论

- MongoDB 支持指定时间区间的历史数据迁移
- MongoDB 支持实时数据同步
- MongoDB 支持按时间进行分表场景的数据同步

## 5. 开发质量报告

结论：本特性/优化的开发质量是

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 15 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

- 分库分表拉取示例数据时，起始时间的表必须存在
- 查询模版中的时间区间必须有闭区间
- 查询模版中子表字段是大小写敏感的，子表字段仅支持字符、数字和布尔类型
- MongoDB 中 binaryData 类型同步到 TDengine 中的数据不正确 
  TD-31014

## 7. 测试环境

- OS: Windows, Linux
- Browser: Chrome

## 8. 测试用例

数据类型包含如下表：

| MongoDB 中数据类型 |  | TDengine中数据类型 |
| --- | --- | --- |
| double | 浮点型 | double |
| String | 字符串 | nchar |
| object | 对象 | nchar |
| Array | 数组 | nchar |
| Binary data | 二进制数据 | nchar |
| ObjectId | 对象id | nchar |
| Boolean | 布尔 | bool |
| Date | 日期 | Timestamp |
| Null | 空值 | nchar |
| Regular Expression | 正则表达式 | nchar |
| Java script | javascript脚本 | nchar |
| 32-bit Interger | 整型 int | int |
| Timestamp | 时间戳 | Timestamp |
| 64-bit Interger | 长整型 | bigint |
| Decimal 128 | 小数 | nchar |
| Min Key | 最小值 | nchar |
| Max Key | 最大值 | nchar |
| Document 类型 |  | nchar |

测试数据如下：
```sql
  {
    _id: ObjectId('6694897f07ecc02eb8f49838'),
    name: 'MongoDB',
    sn: 'AL7011024040493',
    versions: [ 'v3.2', 'v3.0', 'v2.6' ],
    objectvalue: {
      x: 203,
      y: [ -544011899, 1929232653, -2109234038 ],
      z: {
        key1: 'taosx test',
        key2: [ -657737831, 697978395, -1984427108 ],
        key3: [ 'v3.2', 'v3.0', 'v2.6' ],
        key4: { arr: [ { item: 'a', count: 10 }, { item: 'b', count: 20 } ] }
      },
      x1: [ { item: 'a', count: 10 }, { item: 'b', count: 20 } ]
    },
    nestedArray0: [ { item: 'a', count: 10 }, { item: 'b', count: 20 } ],
    nestedArray1: [ [ 1, 2, 3 ], [ 4, 5, 6 ] ],
    createtime: ISODate('2024-07-15T02:29:19.306Z'),
    uploadtime: ISODate('2024-07-15T02:29:19.306Z'),
    boolvalue: true,
    int32value: -2074373401,
    int64value: Long('-6159205678003197181'),
    doublevalue: 0.6150210612884383,
    decimalvalue: Decimal128('0.991588650078507'),
    nullvalue: null,
    strvalue0: 'ffc36c25-fb44-48cf-89e8-1acdac67ed23',
    strvalue1: '123.1234567',
    strvalue2: '123456789',
    strvalue3: '123.000',
    objectidvalue: ObjectId('6694897f07ecc02eb8f49837'),
    fac: [
      '50.0',   '50.001', '50.0',   '50.0',
      '50.001', '50.0',   '50.0',   '50.002',
      '50.001', '50.001', '50.001', '50.001',
      '50.001', '50.001', '50.001', '50.001',
      '50.001', '50.0',   '50.001', '50.0',
      '50.001', '50.001', '50.002', '50.001',
      '50.002', '50.001', '50.001', '50.001',
      '50.001', '50.001', '50.001', '50.001',
      '50.0',   '50.002', '50.001', '50.0',
      '50.001', '50.001', '50.001', '50.001',
      '50.001', '50.0',   '50.002', '50.002',
      '50.001', '50.001', '50.0',   '50.001',
      '50.001', '50.001'
    ],
    ppv: [
      '0', '0', '0', '0', '0', '0', '0', '0',
      '0', '0', '0', '0', '0', '0', '0', '0',
      '0', '0', '0', '0', '0', '0', '0', '0',
      '0', '0', '0', '0', '0', '0', '0', '0',
      '0', '0', '0', '0', '0', '0', '0', '0',
      '0', '0', '0', '0', '0', '0', '0', '0',
      '0', '0'
    ],
    fm_flag: [ 1933950488, 370278134, -1642575319 ],
    timestamp: Timestamp({ t: 1721130179, i: 583 }),
    binaryData: Binary.createFromBase64('SGVsbG8sIE1vbmdvREI=', 0),
    javascript: "function greet(name) { return 'Hello, ' + name; }",
    minkey: MinKey(),
    maxkey: MaxKey(),
    pattern: /abc/
  }

```

### 8.1 功能

| 测试类型 | 测试场景 | 测试步骤 | 测试结果 | 结果 | 备注 |
| --- | --- | --- | --- | --- | --- |
| basic | 基本场景 | 配置有效的服务地址和端口、选择Username/Password方式认证，配置正确的用户名密码，认证数据库 | 连通性校验通过 | PASS |  |
| basic | 基本场景 | 1. 在数据源中创建表包含类型double, string，array, ObjectId, Boolean,Date, null, int32, int64 2. 在TDengine中提前创建schema一致的超级表 3. 任务配置中设置匹配的transformer规则为mapping，创建任务 | 任务创建成功，对应数据源表中的数据正确写入TDengine中 | PASS |  |
| 连接配置 | 连通性 | 配置正确的地址，端口 | 连通性校验通过 | PASS |  |
| 连接配置 | 连通性 | 使用错误的访问地址信息、端口 | 连通性校验不通过，并有合适的提示信息 超时时间：30s 提示信息：Failed to connect to dsn: timed out | PASS |  |
| 连接配置 | 连通性 | 地址为空或者端口为空 | 前端非空提示 | PASS |  |
| ~~连接配置~~ ~~(0731版本测试)~~ | ~~负载均衡测试~~ | ~~后续补充~~ |  |  |  |
| ~~连接配置~~ ~~(0731版本测试)~~ | ~~是否直连~~ | ~~后续补充~~ |  |  |  |
| ~~连接配置~~ ~~(0731版本测试)~~ | ~~副本名称~~ | ~~后续补充~~ |  |  |  |
| ~~连接配置~~ ~~(0731版本测试)~~ | ~~超时阈值~~ | ~~后续补充~~ |  |  |  |
| 认证测试 | Username/Password | 开启用户名密码认证，使用正确的用户名密码，认证数据库 | 连通性校验通过 | PASS |  |
| 认证测试 | Username/Password | 开启用户名密码认证，使用错误的用户名或错误的密码，或者错误的认证数据库 | 连通性校验不通过，并有合适的提示信息 错误原因：Failed to connect to dsn: Kind: SCRAM failure: Authentication failed., labels: {} | PASS |  |
| ~~认证测试~~ ~~(0731版本测试)~~ | ~~其他认证机制测试~~ | ~~后续补充~~ |  |  |  |
| ~~连接选项~~ | ~~压缩器测试~~ | ~~选择为空~~ |  |  |  |
| ~~连接选项~~ | ~~压缩器测试~~ | ~~选择snappy~~ |  | ~~FAIL~~ |  |
| ~~连接选项~~ | ~~压缩器测试~~ | ~~选择zlib~~ |  | ~~FAIL~~ |  |
| ~~连接选项~~ | ~~压缩器测试~~ | ~~选择zstd~~ |  | ~~FAIL~~ |  |
| SSL证书 | SSL证书测试 | tls选择为false | 不显示CA证书文件路径和密钥文件路径 | PASS |  |
| SSL证书 | SSL证书测试 | tls选择为true, 上传正确的ca文件和证书文件 | 连通性检查通过 | PASS |  |
| SSL证书 | SSL证书测试 | tls选择为true, 上传错误的ca文件或者错误的证书文件 | 连通性检查不通过，有错误提示 | PASS |  |
| ~~版本信息~~ | ~~驱动版本测试~~ | ~~后续补充~~ |  |  |  |
| ~~版本信息~~ | ~~接口版本测试~~ | ~~后续补充~~ |  |  |  |
| transformer (0731版本测试) | json解析：depth | Json层级为3层（包含最外层），depth=0 | json只展开最外层，并且数据能成功入库 | PASS |  |
| transformer (0731版本测试) | json解析：depth | Json层级为3层（包含最外层），depth=1 | json向内展开一层，并且数据能成功入库 | PASS |  |
| transformer (0731版本测试) | json解析：depth | Json层级为3层（包含最外层），depth=2 | json向内展开二层，并且数据能成功入库 | PASS |  |
| transformer (0731版本测试) | json解析：depth | Json层级为3层（包含最外层），depth=5 | json向内展开二层，并且数据能成功入库 | PASS |  |
| transformer (0731版本测试) | json解析：depth | Json层级为3层（包含最外层），depth=-1 | 前端会转成0 | PASS |  |
| transformer | 从列中提取或拆分：join | 对array进行join操作, [1,2,3] | array 中的子元素根据连接符拼接，并且数据能成功入库 | PASS |  |
| transformer | 从列中提取或拆分：join | 对array进行join操作, [ { item: 'a', count: 10 }, { item: 'b', count: 20 } ] | array 中的子元素根据连接符拼接，并且数据能成功入库 | PASS |  |
| transformer | 从列中提取或拆分：join | 对array进行join操作, [ [ 1, 2, 3 ], [ 4, 5, 6 ] ] | array 中的子元素根据连接符拼接，并且数据能成功入库 | PASS |  |
| transformer | 从列中提取或拆分：join | 对非array类型进行join操作 | 前端增加限制，非array类型不能进行join操作 | PASS |  |
| transformer | 从列中提取或拆分：join | join with参数为空 | 使用空字符串连接 | PASS |  |
| transformer | 从列中提取或拆分：join | join with使用字符串 | 能正常Join | PASS |  |
| 数据查询参数 | 库名 | 输入的库名不存在 | 拉取不到数据，no data found | PASS |  |
| 数据参数参数 | 库名 | 输入多个库名 | 拉取不到数据，no data found | PASS |  |
| 数据参数参数 | 库名 | 库名为空 | 前端非空提示 | PASS |  |
| 数据查询参数 | 表名 | 输入的表名不存在 | 拉取不到数据，no data found | PASS |  |
| 数据查询参数 | 表名 | 输入多个表名 | 拉取不到数据，no data found | PASS |  |
| 数据查询参数 | 表名 | 表名为空 | 前端非空提示 | PASS |  |
| 数据查询参数 | 时间占位符测试 | 查询模版时间起止占位符异常校验： 1.配置起止占位符格式不匹配，配置${start_datetime}和${end_timestamp} 2.只配置${start_datetime} 3.只配置${end_datetime} 4.只配置${start_timestamp} 5.只配置${end_timestamp} 6.查询模版中没有时间占位符 | 提示错误 | PASS |  |
| 数据查询参数 | 查询模版 | 查询模版语法错误 | 提示错误 | PASS |  |
| 数据查询参数 | 时间占位符测试 | 1. 使用时间占位符start_datetime,end_datetime配置起始时间和终止时间，在数据源中对应时间区间没有任何数据 2.使用时间占位符start_timestamp,end_timestamp配置起始时间和终止时间，在数据源中对应时间区间没有任何数据 | 获取的示例数据为空并提示错误 | PASS |  |
| 数据查询参数 | 查询模版 | 语法错误 | 提示错误 | PASS |  |
| 数据查询参数 | 查询模版 | 查询模版为空 | 前端非空提示 | PASS |  |
| 数据查询参数 | 起始时间、结束时间 | 结束时间小于起始时间 | 前端提示错误信息 | PASS |  |
| 数据查询参数 | 起始时间、结束时间 | 起始时间为空 | 前端非空提示 | PASS |  |
| 数据同步 | 历史数据同步 | 使用UI中的起始时间和终止时间作为时间区间： 1. 在数据源中创建包含测试数据中所有字段的表并写入数据 2. 在TDengine中提前创建schema一致的超级表 3. 任务配置中配置sql模板，使用时间占位符start_datetime,end_datetime，配置起始时间、终止时间有效，设置匹配的transformer规则分别配置为mapping、value、sum、format、expr规则 | TDengine中对应表写入数据且数据一致 | PASS |  |
| 数据同步 | 历史数据同步 | 使用UI中的起始时间和终止时间作为时间区间： 1. 在数据源中创建包含测试数据中所有字段的表并写入数据 2. 在TDengine中提前创建schema一致的超级表 3. 任务配置中配置sql模板，使用时间占位符start_timestamp,end_timestamp，配置起始时间、终止时间有效，设置匹配的transformer规则分别配置为mapping、value、sum、format、expr规则 | TDengine中对应表写入数据且数据一致 |  |  |
| 数据同步 | 实时数据同步 | 使用UI中的起始时间和终止时间作为时间区间： 1. 在数据源中创建包含测试数据中所有字段的表并写入数据 2. 在TDengine中提前创建schema一致的超级表 3. 任务配置中配置sql模板，使用时间占位符start_datetime,end_datetime，配置起始时间、终止时间有效，设置匹配的transformer规则分别配置为mapping、value、sum、format、expr规则 4.通过测试程序持续写入实时数据 | TDengine中写入表的数据最早时间满足配置的起始时间，且实时有新数据写入，任务不会自动停止 | PASS |  |
| 数据同步 | 实时数据同步 | 使用UI中的起始时间和终止时间作为时间区间： 1. 在数据源中创建包含测试数据中所有字段的表并写入数据 2. 在TDengine中提前创建schema一致的超级表 3. 任务配置中配置sql模板，使用时间占位符start_timestamp,end_timestamp，配置起始时间、终止时间有效，设置匹配的transformer规则分别配置为mapping、value、sum、format、expr规则 4.通过测试程序持续写入实时数据 | TDengine中写入表的数据最早时间满足配置的起始时间，且实时有新数据写入，任务不会自动停止 |  |  |
| 数据同步 | 分库分表 | 1. 在数据源中构建库名(${Y})，表名中带日期的表结构(${M}) test_db1_2020 test_db1_2021 test_db1_2023 表： tb_1 tb_2 tb_3 tb_4 ... tb_12 2. transformer规则使用mapping映射到超级表中 | TDengine中对应表写入数据且数据一致 | PASS |  |
| 数据同步 | 分库分表 | 1. 在数据源中构建库名(${Y})，表名中带日期的表结构(${M}, ${D}) test_db2_2020 test_db2_2021 test_db2_2023 表： tb_1_1 tb_1_2 tb_1_3 ... tb_1_31 tb_2_1 ... tb_2_29 tb_3_1 ... tb_3_31 2. transformer规则使用mapping映射到超级表中 | TDengine中对应表写入数据且数据一致 | PASS |  |
| 数据同步 | 分库分表 | 1. 在数据源中构建库名(${Y})，表名中带日期的表结构月份中有些填充2位，有些是1位 test_db3_2020 test_db3_2021 test_db3_2023 表： tb_01 tb_2 tb_03 tb_4 ... tb_12 2.分别使用占位符(${m}, ${M})测试 3. transformer规则使用mapping映射到超级表中 | TDengine中对应表写入数据且数据一致 | PASS |  |
| 数据同步 | 分库分表 | 1. 在数据源中构建库名(%Y)，表名中带日期的表结构，月份中有些填充2位，有些是1位；日期中有些填充2位，有些是1位 test_db4_2020 test_db4_2021 test_db4_2023 表： tb_01_01 tb_01_02 tb_01_03 ... tb_01_31 tb_02_01 ... tb_02_29 tb_03_01 ... tb_03_31 2.分别使用占位符(${m}, ${d}, ${M}, ${D}) 3. transformer规则使用mapping映射到超级表中 | TDengine中对应表写入数据且数据一致 | PASS |  |
| 数据同步 | 分库分表 | 1. 在数据源中构建库名(%y)，表名中带日期的表结构，表名带日期（一年中的第几天） test_db5_00 test_db5_05 test_db5_20 表： tb_1 tb_002 ... tb_365 2.分别使用${j}，${J} 3.transformer规则使用mapping映射到超级表中 | TDengine中对应表写入数据且数据一致 | PASS |  |
| transformer默认值 | transformer默认值 | 为每个mapping的数据类型配置默认值，并在数据源中写入对应字段空值 String, int, NULL, boolean， array， object | 写入TDengine的对应列的数据应为transformer中配置的默认值 | PASS |  |
| transformer默认值 | transformer默认值 | 为每个mapping的数据类型配置默认值，数据源中的数据有字段缺失 String, int, NULL, boolean， array， object | 写入TDengine的对应列的数据应为transformer中配置的默认值 | PASS |  |
| 断点测试 |  | 启动任务后编辑任务，修改endtime： 1. endtime < checkpoint; 2. endtime >= checkpoint | 1. 任务直接结束，无新数据迁移 2. 任务运行至endtime的数据时结束 | PASS |  |
| 高级选项 | 最大读取并发数 | 配置最大读并发数为最大值、最小值 | 下发任务中该参数值与设置一致 | PASS |  |
| 高级选项 | 最大读取并发数 | 配置最大读并发数为边界外值/非法值 | 前端限制无法设置 | PASS |  |
| 高级选项 | 批次大小 | 配置批次大小为最大值、最小值 | 下发任务中该参数值与设置一致 | PASS |  |
| 高级选项 | 批次大小 | 配置批次大小为边界外值/非法值 | 前端限制无法设置 | PASS |  |
| 任务编辑 |  | 任务编辑状态，尝试编辑sql语句 | 前端限制无法编辑 | PASS |  |
| 任务编辑 |  | 启动任务后尝试编辑sql语句 | 前端限制无法编辑 | PASS |  |
| 乱序测试 | 子表字段 | 配置1个子表字段，按照ts列排序 | 乱序问题解决 | PASS |  |
|  |  | 配置2个子表字段，按照ts列排序 | 乱序问题解决 | PASS |  |
|  |  | 配置不存在的子表字段 | 获取示例数据时，提示错误 | PASS |  |
|  | 查询模版 | 子表字段在查询模版中配置不正确 | 获取示例数据时，提示错误 | PASS |  |
|  |  | 输入错误的查询语句 | 获取示例数据时，提示错误 | PASS |  |

1亿条数据，子表字段： sn 1w个子表
Buffer 100MB 304G - 302G 
Buffer 256MB 304G - 302G

1亿条数据子表字段： sn1 100个子表
Buffer 100MB 349G - 342G 
Buffer 100MB 341G - 333G

### 8.2 可用性

- UI 是否美观
- 交互是否合理
- 是否存在错别字

### 8.3 稳定性

测试场景：100列，配置子表字段
运行20+个小时，任务运行稳定

### 8.4 性能

测试场景：1亿多条，300G数据，100列（double, string类型），配置子表字段
写入性能约2w条/s， 性能在客户机器下是我们的3倍多（测试环境我们性能受mongodb数据源所在机器性能有限制，在任务运行时，mongodb数据源机器的IO在97%左右）

### 8.5 安全性

无

### 8.6 兼容性

- 

### 8.7 本地化

测试用例：
- 点击切换语言按钮后，UI上的所有元素是否按照选择的语言，正确展示

## 9. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: [DataIn MongoDB]
<!-- Unsupported block type: 999 -->

## 10. 参考文档 

[Data In: MongoDB](https://taosdata.feishu.cn/wiki/Ep7LwSBqxiR5zNk12locOQL4nie)
[FS - 解决 MongoDB 与关系型数据库迁移数据乱序问题](https://taosdata.feishu.cn/wiki/CqqSwJaXdi6LWXkCnsgcQodbnJh)
