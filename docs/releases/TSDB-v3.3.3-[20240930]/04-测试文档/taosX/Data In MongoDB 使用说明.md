# Data In MongoDB 使用说明

## 1. 快速指南

#### 1.0.1 历史数据同步

1. 登录 explorer -> 选择 "数据写入" -> 选择 "新增数据源" -> 类型选择 MongoDB
2. 填写任务名称，代理（可选）， 目标数据库
3. 连接配置： 配置服务地址和服务端口
4. 认证：填写用户名，密码和认证数据库
5. 数据查询配置
  - 填写 mongoDB 需要同步的数据库名，集合名
  - 查询模版：必须带时间占位符，并且成对出现，时间占位符的说明见补充说明2.2。如：
    `{"createtime":{"$gte":${start_datetime}，"$lt":${end_datetime}}}`
  - 选择起始时间和结束时间
1. 从服务器检索示例数据
2. transformer 解析： 选择 json 解析，点击预览按钮
3. transformer 从列中提取或拆分： 对于 array 类型， 可以选择 join 操作来拼接 array 中的元素
比如对于字段 "version": ["v3.2"，"v3.0"，"v2.6"] 配置 join， 连接符号设置为"-"， 转换结果为"v3.2-v3.0-v2.6"
![](./images/img_KTXObPLjooa02WxhjfMc6DXpnCT.png)

1. transformer 映射：配置超级表的映射，数据类型映射见补充说明1

#### 1.0.2 实时数据同步

步骤同历史数据同步，但在第 5 步数据查询配置中，结束时间配置为空

#### 1.0.3 分库分表同步

MongoDB 数据源中，有数据库按年份分库，库中按月、按日分表的场景。如果期望将 MongoDB 的多张表映射到一张 TDengine 超级表，其配置步骤同历史数据同步，但在第 5 步数据查询中，需要按以下方式配置：
- 填写数据库名和集合名，数据库名和集合名中可使用时间占位符，见补充说明2.1。如 test_db1_${Y}， tb_${M}_${D}。从服务器检索数据时，起始时间的表必须存在

## 2. 注意事项和已知问题

- 连接选项（测试验证中）
- SSL 证书（测试验证中）
- 分库分表场景下，从服务器检索数据时，起始时间的表必须存在
- transformer 解析： 使用 json 解析，下拉框中对数组类型的解析不正确。可以不选择下拉框中的属性，默认获取所有属性
- transformer json 解析后对数组类型的显示不带括号。如 ["v3.2"，"v3.0"，"v2.6"] 在 json 解析后前端显示为 v3.2，v3.0，v2.6 （explorer 优化）
- transformer  从列中提取或拆分， join 操作，只对 array 类型生效， 其他类型选择 join 操作会报错。后面的版本会对 explorer 增强，非 array 类型不能选择 join 操作。 （explorer 优化）
- 使用 transformer 创建超级表， 会根据 transformer 前面步骤解析出来的字段来自动填充超级表字段。如果解析出来的字段有 TDengine 中的关键词时，需要加上转义符 ``， 或者修改超级表的字段名（explorer 优化）
- 对于空值在 transformer mapping 时配置默认值：目前只支持 string 类型，其他类型的配置还不生效 （taosx 已优化）
- MongoDB 中 binaryData 类型同步到 TDengine 中的数据不正确 （taosx 优化）
- 任务执行后，指标的统计不准确，指标只能提供参考 （taosx 优化）

## 3. 补充说明

#### 3.0.1 数据类型映射

详见下表：

| MongoDB 中数据类型 | 中文说明 | TDengine 中数据类型 |
| --- | --- | --- |
| double | 浮点型 | double |
| String | 字符串 | varchar |
| object | 对象 | varchar |
| Array | 数组 | varchar |
| Binary data | 二进制数据 | varchar |
| ObjectId | 对象 id | varchar |
| Boolean | 布尔 | bool |
| Date | 日期 | Timestamp |
| Null | 空值 | varchar |
| Regular Expression | 正则表达式 | varchar |
| Java script | javascript 脚本 | varchar |
| 32-bit Interger | 整型 int | varchar |
| Timestamp | 时间戳 | Timestamp |
| 64-bit Interger | 长整型 | bigint |
| Decimal 128 | 小数 | varchar |
| Min Key | 最小值 | varchar |
| Max Key | 最大值 | varchar |

#### 3.0.2 时间占位符模版

##### 3.0.2.1 数据库名与集合名中的时间占位符

| name | description | Example | 备注 |
| --- | --- | --- | --- |
| Y | 年，完整的公历年表示，零填充的 4 位整数。 | 2001 |  |
| y | 年，公历年除以 100，零填充的 2 为整数。 | 01 |  |
| m | 月，整数月份（01 - 12） | 07 |  |
| M | 月，整数月份（1 - 12） | 7 |  |
| B | 月，月份英文全拼 | July | 测试验证中 |
| b | 月，月份英文的缩写（3 个字母） | Jul | 测试验证中 |
| d | 日，日期的数字表示（01 - 31） | 08 |  |
| D | 日，日期的数字表示（1 - 31） | 8 |  |
| j | 日，一年中的第几天（001 - 366） | 089 |  |
| J | 日，一年中的第几天（1 - 366） | 89 |  |
| F | 日，相当于 `${Y}-${m}-${d}` | 2001-07-08 | 测试验证中 |

##### 3.0.2.2 查询模版中的时间占位符

在查询模版中必须包含时间占位符。时间占位符必须成对出现。
1. ${start_datetime}、${end_datetime}：对应后端 datetime 类型字段的筛选，如：{"createtime":{"$gte":${start_datetime}，"$lt":${end_datetime}}} 
2. ${start_timestamp}、${end_timestamp}：对应后端 timestamp 类型字段的筛选，如：{"timestamp":{"$gte":${start_timestamp}，"$lt":${end_timestamp}}}
