# KingHistorian DataIn FS

## 1. 背景

根据 [神东需求](https://taosdata.feishu.cn/wiki/AbpAwW0fliCfm0kOwHMc5reknAc)，需要开发 King Historian 的数据接入。
JIRA：
- https://jira.taosdata.com:18080/browse/TX-696
- https://jira.taosdata.com:18080/browse/TS-7379
- [需求报告-TX-696](https://taosdata.feishu.cn/wiki/JO1OwnJVJiR3d7kUeWocn6F4nTb)

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/10/13 | 0.1 | @杨志宇 |  |
|  |  |  |  |

## 3. 定义

- Tag: King Historian 中的点位数据，类似于 OPC 中的点位。一个 Tag 就是一组 KVQT 数据。
- TagName：King Historian 中的 Tag 名称。
- TagId：King Historian 中的 Tag ID。
- DataType：King Historian 中的数据类型，包括：
  - Number: boolean, int8, int16, int32, int64, float, double
  - String: VARCHAR(n), CHAR(n), NVARCHAR(n), NCHAR(n)
  - Timestamp：时间戳。
  - Fixed BLOB：BINARY[(n)]，固定长度的二进制。
  - Variable BLOB：VARBINARY，n 个字节变长二进制。
  - Decimal、Digital、Float16：小数、数字、16为浮点数
  - Unknown Type：未知的类型
- DataLength：King Historian 中的数据类型所占的字节数。

## 4. 行为说明

### 4.1 Explorer

King Historian 的数据是 KVQT 格式的，和 OPC 数据相同，因此，King Historian 的 UI 和 OPC 尽量保持一致。

#### 4.1.1 连接和认证

![](./images/img_UdlybECbNonmdRx90uocF5ugndb.png)

- 需要配置 Server 地址、端口、用户名、密码；
- 连接超时：单位是秒，默认为 0（永不超时）

#### 4.1.2 Tag 配置

![](./images/img_ETS8b5kdgotRWhxZH7Mc1zBEnEf.png)

仅支持使用 CSV 配置，使用 CSV 配置 KingHistorian 的规则和 OPC DA 一致。
```bash
tag_name,enabled,stable,tbname,value_col,value_transform,type,quality_col,ts_col,ts_transform,request_ts_col,request_ts_transform,received_ts_col,received_ts_transform,tag::VARCHAR(200)::name
Tag0,1,kh_{type},t_{tag_name},val,,int8,quality,ts,,qts,,rts,,Constant
```

#### 4.1.3 采集配置

King Historian 支持历史数据查询（而 OPC 不支持），因此，采集配置需要分为：历史数据迁移和实时数据同步。

##### 4.1.3.1 历史数据迁移

![](./images/img_HcEYb6osGoBDZQxBEUbcnUecncf.png)

##### 4.1.3.2 实时数据同步

![](./images/img_OHdZbbIgsoRRWaxCcHhc1spSnsc.png)

#### 4.1.4 高级选项

![](./images/img_Ws9ubz0Caow8HxxS21OcOBYDnCb.png)

### 4.2 命令行

```bash
taosx run --from "kinghist://user:passwd@host:port?csv_config=@./a.csv" --to "taos://host:port/database"
```

### 4.3 连通性校验

配置了连接和认证信息后，可以检查 King Historian 的连通性。

### 4.4 CSV 的合法性校验

Csv 的合法性和 OPC DA 的合法性一样。

### 4.5 异常处理

1. 在创建到 KingHistorian 的连接时，如果超过`conn_timeout`没有建立连接，则报错。
2. 在 kinghistorain_to_taos 任务运行过程中，如果出现连接中断的情况，尝试重新建立连接，重试`max_retries`次，每次重试之间间隔`retry_interval`秒。

### 4.6 KingHistorian 的 DSN 参数

```shell {wrap}
kinghist://[username]:[passwd]@[host]:[port]/[params...]
```


| **参数** | **说明** | **值域** | **必填** |
| --- | --- | --- | --- |
| host | KingHistorian 的 host | Hostname 或 IP | 是 |
| port | KingHistorian 的端口，默认为 5678 | port | 否 |
| username | KingHistorian 用户名 | 用户名 | 是 |
| passwd | KingHistorian 密码 | 密码 | 是 |
| conn_timeout | 连接超时，单位是秒 | 正整数 | 否 |
| csv_config_file | csv 配置文件的路径 | 路径 | 是 |
| mode | 数据模式，历史数据迁移/实时数据同步 | - history：历史数据 - realtime：实时数据 | 是 |
| start | 历史数据，查询的开始时间 | 日期时间 | 是 |
| end | 历史数据，查询的结束时间，默认值是当前时间 | 日期时间 | 否 |
| time_range | 历史数据，查询的窗口大小，默认值是1天 | 时间间隔 | 否 |
| restro | 历史数据，允许乱序的时间跨度，默认值是0分钟 | 时间间隔 | 否 |
| interval | 历史数据，两次查询之间的间隔，单位是：毫秒（ms），默认值是 1000 | 正整数 | 否 |
| min_elapsed | KingHistorian 订阅时的最小间隔时间，单位：毫秒，默认值是 1000 | 正整数 | 否 |
| max_retries | 最大错误重试次数。默认为：10 | 正整数，>= 0 | 否 |
| retry_interval | 错误重试的间隔，单位：秒。默认为 5s。 | 正整数，>= 5s | 否 |

## 5. 性能

无

## 6. 兼容性

无

## 7. 运维

无

## 8. 使用场景

1. 历史数据迁移：指定开始和结束时间，通过查询，将 KingHistorian 中的历史数据迁移至 TSDB。
2. 实时数据同步：通过订阅，实时同步 KingHistorian 数据到 TSDB。

## 9. 约束和限制

无

## 10. 常见错误和排查

无

## 11. 可观测性

KingHistorian 的数据与 OPC UA/DA 数据相似，使用 Point 类型的数据模式。
用户可以通过 explorer DataIn 中的指标，查看写入速度等性能指标。

## 12. 安装和卸载

无

## 13. 文档

无

## 14. 参考文档

无

## 15. 附录

DataSet 的结构
```json
[
  {
    "id": "_GROUPS"
  },
  {
    "id": "1",
    name: "OPC_数据类型示例.16 位设备.R 寄存器.Float1", // TagProperties.tag_name
    type: "float",  // TagProperties.data_type + TagProperties.data_length = IpcDataType
    options: [
      {
        name: "tag_name", // KingHistTag.name
        display: "OPC_数据类型示例.16 位设备.R 寄存器.Float1",  // TagProperties.tag_name
        description: "变量名", // KingHistTag.name_cn
        required: true
      },
      {
        name: "data_type", // KingHistTag.name
        display: "float",  // TagProperties.data_type
        description: "变量类型", // KingHistTag.name_cn
        required: true
      },
      {
        name: "data_length", // KingHistTag.name
        display: "",  // TagProperties.data_length
        description: "变量数据长度", // KingHistTag.name_cn
        required: true
      },
      {
        name: "description", // KingHistTag.name
        display: "64 位 IEEE 浮点数数组",  // TagProperties.description
        description: "变量描述", // KingHistTag.name_cn
        required: true
      },
      {
        name: "last_modified", // KingHistTag.name
        display: "2025-10-15T20:37:10.461",  // TagProperties.last_modified rfc3339
        description: "上次修改变量配置时间", // KingHistTag.name_cn
        required: true
      },
      {
        name: "last_modified_user", // KingHistTag.name
        display: "sa",  // TagProperties.last_modified_user
        description: "上次修改变量配置的用户", // KingHistTag.name_cn
        required: true
      },
      {
        name: "group_name", // KingHistTag.name
        display: "Root",  // TagProperties.tag_name
        description: "变量组", // KingHistTag.name_cn
        required: true
      },
      {
        name: "group_path", // KingHistTag.name
        display: "",  // TagProperties.tag_name
        description: "变量组路径", // KingHistTag.name_cn
        required: true
      }
    ]
  }
]
```
