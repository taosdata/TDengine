# taosAdapter 支持 HTTP JSON 写入 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-09-30 | - | 0.1 | 谭雪峰 | 编写 |
| 2025-10-24 | - | 0.2 | 谭雪峰 | 使用 jsonata 解析和转换 json |
| 2025-10-27 | - | 0.3 | 谭雪峰 | 根据评审内容修改 |
| 2025-10-30 | - | 0.4 | 谭雪峰 | 合并 tag 和 col 为 fields 简化配置 添加 timeFieldName 指定数据库中存储时间戳的列名 |
| 2025-11-06 | 2025-11-06 | 1.0 | 谭雪峰 | 修改 iso8601 为 datetime |
| 2025-11-10 | 2025-11-10 | 1.1 | 谭雪峰 | 修改 timeTimezone 为 timezone |

## 2. 背景

TS-6020

现在很多IoT设备采用MQTT，payload是JSON格式。但也有一些客户直接用HTTP Post发送数据。

## 3. 定义

1. POST：是 HTTP 协议中定义的一种请求方法。它主要用于向指定的资源提交数据，请求服务器进行处理。
2. JSON：是一种轻量级的数据交换格式。采用完全独立于编程语言的文本格式来存储和表示数据。
3. jsonata：JSONata 是一种轻量级的 JSON 数据查询和转换语言。能够通过简洁的路径表达式灵活地提取、计算和重组 JSON 数据。

## 4. 行为说明

### 4.1 配置

由于此配置复杂，无法支持命令行和环境变量配置，仅支持配置文件进行配置
1. 新增配置 input_json

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| enable | bool | 是否启用（默认为 true） |
| rules | Rule[] | 解析规则数组 |

1. 规则配置

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| endpoint | string | url端点 http://localhost:6041/input_json/v1/{endpoint} |
| db | string | 默认数据库名 |
| dbKey | string | 数据库名的 key，不能与 db 同时设置 |
| superTable | string | 默认超级表名 |
| superTableKey | string | 超级表名的 key，不能与 superTable 同时设置 |
| subTable | string | 默认子表名 |
| subTableKey | string | 子表名的key，不能与 subTable 同时设置 |
| timeKey | string | 时间路径，如果不设置则取收到数据时间 |
| timeFormat | string | 时间格式，当 timeKey 设置时有效，见 [taosAdapter 支持 HTTP JSON 写入 FS](https://taosdata.feishu.cn/wiki/Eb5CwW9QwiqUXjkmDMQcTDzInHh) |
| timezone | string | 解析时间所用时区设置，当 timeKey 设置时有效，IANA 时区格式，**默认值 taosAdapter 所在机器时区** |
| timeFieldName | string | 时间对应数据库列名 |
| fields | []Field | 写入字段配置（包含标签和列但不包括时间列） |
| transformation | string | jsonata 的转换规则 |

1. 字段配置

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| key | string | JSON 的 key，同时对应数据库的字段名 |
| optional | bool | 如果设置为 true，key 不存在时不会报错，生成 SQL 时将不会拼接该列 |

1. timestampFormat 预设的配置
预设以下配置，如果不满足要求可以按照 strftime 解析方式进行扩展 https://pkg.go.dev/github.com/ncruces/go-strftime@v1.0.0

| 配置 | 说明/格式 |
| --- | --- |
| unix | 秒级时间戳 |
| unix_ms | 毫秒级时间戳 |
| unix_us | 微秒级时间戳 |
| unix_ns | 纳秒级时间戳 |
| ansic | Mon Jan _2 15:04:05 2006 |
| rubydate | Mon Jan 02 15:04:05 -0700 2006 |
| rfc822z | 02 Jan 06 15:04 -0700 |
| rfc1123z | Mon, 02 Jan 2006 15:04:05 -0700 |
| rfc3339 | 2006-01-02T15:04:05Z07:00 |
| rfc3339nano | 2006-01-02T15:04:05.999999999Z07:00 支持秒级到纳秒级时间 |
| stamp | Jan _2 15:04:05 |
| stampmilli | Jan _2 15:04:05.000 |
| datetime | 2006-01-02 15:04:05.999999999 支持秒级到纳秒级时间 |

如果时间格式不包含时区信息则一定要配置时区配置 timeTimezone 来正确解析时间

### 4.2 JSON 格式

jsonata 文档：https://docs.jsonata.org/overview.html 
仅支持 jsonata 1.5.4 版本，可在 https://try.jsonata.org/ 尝试解析，解析时使用 1.5.4 版本 
![](./images/img_Yx51btKwUolZh7xvlJlcYbCZnld.png)

经过 jsonata 转换后需要变成打平的一维数组，每个元素为一行数据，例如
```json {wrap}
[
    {"db":"power","super_table_name":"meters","sub_table_name":"d_1001","location":"New York","id":1001,"time":"2025-10-23 15:30:11", "current": 15.5, "voltage": 220.0, "phase": 1},
    {"db":"power","super_table_name":"meters","sub_table_name":"d_1002","location":"Los Angeles","id":1002,"time":"2025-10-23 15:31:12", "current": 12.3, "voltage": 230.0, "phase": 2},
    {"db":"power","super_table_name":"meters","sub_table_name":"d_1003","location":"Chicago","id":1003,"time":"2025-10-23 15:32:13", "current": 14.8, "voltage": 225.0, "phase": 3}
]
```

### 4.3 数据类型

列与 tag 数据解析后根据 JSON 类型拼接成 SQL 进行写入。时间使用 jsonata 转换非常复杂将使用 go time 解析模块进行解析，解析后将转换为 rfc3339nano 格式进行写入

### 4.4 SQL 拼接规则

1. 对于相同库和超级表且转换后的 json 中所需的 key 都存在的数据将合成一个写入语句。
2. 当 json 中有所需的 key 不存在但设置了 optional 为 true 的数据将变成一条单独的写入语句，key 不存在的列将不指定
3. 生成的 sql 将拼接成接近 1M 的语句进行写入
4. 字符串数据将添加单引号进行包裹，并进行转义，规则如下
   - 忽略 `\0` 字符
   - `'`单引号将转义成两个字符 `''`
   - `\t` 字符将转义成三个字符 `\\t`
   - `\r` 字符将转义成三个字符 `\\r`
   - `\n` 字符将转义成三个字符 `\\n`
   - `\` 字符将转义成两个字符 `\\`
5. 写入 sql 使用自动建表语句，列名使用反引号包裹，如`insert into `power`.`meters` (`tbname`, `location`, `id`, `ts`, `current`, `voltage`, `phase`) values ('d_1001', 'New York', 1001, '2025-10-23T15:30:11+08:00', 15.5, 220.0, 1)"`

### 4.5 空运行

由于功能复杂，提供 dry_run 以供调试使用。在请求参数中添加 dry_run=true 将返回处理后的 JSON 以及生成的 SQL，不会进行数据写入。
比如：
```json {wrap}
curl -uroot:taosdata localhost:6041/input_json/v1/meters?dry_run=true -d '[{"db":"power","super_table_name":"meters","sub_table_name":"d_1001","location":"New York","id":1001,"time":"2025-10-23 15:30:11", "current": 15.5, "voltage": 220.0, "phase": 1}]'
```

响应：
```json
{
        "code": 0,
        "desc": "",
        "json": "[{\"db\":\"power\",\"super_table_name\":\"meters\",\"sub_table_name\":\"d_1001\",\"location\":\"New York\",\"id\":1001,\"time\":\"2025-10-23 15:30:11\", \"current\": 15.5, \"voltage\": 220.0, \"phase\": 1}]",
        "sql": ["insert into `power`.`meters` (`tbname`, `location`, `id`, `ts`, `current`, `voltage`, `phase`) values ('d_1001', 'New York', 1001, '2025-10-23T15:30:11+08:00', 15.5, 220.0, 1)"]
}
```

### 4.6 连接池

与 RESTFul 接口以及其他 schemaless 接口共用连接池

### 4.7 Batch 参数批量执行

此功能不在本周期开发完成
input_json.rules 下添加参数用来控制某个 endpoint 批量写入数据

| 配置 | 类型 | 说明 |
| --- | --- | --- |
| batch | bool | 是否批量写入，默认关闭 |
| batchSize | int | 获取到多少行后执行写入 |
| batchTimeout | int | 距上次写入最多等待时间（秒） |

1. 当 batch 设置为 true 时将启用批量写入，请求数据进行转换放入写入队列后将返回成功，不等待真正写入成功。
2. 当写入队列消费者获取到 batchSize 条数据或距上次写入超过batchTimeout后将执行写入，写入失败将在日志中打印 error日志以及写入的 SQL。
3. 增加请求参数 batch=false 指定当次请求直接写入不进入写入队列

#### 4.7.1 flush 接口

此功能不在本周期开发完成
在 batch 为 true 时提供 flush 接口，此接口功能是停止写入队列的新入队，当前写入队列所有数据写入到 TDengine，写入完成后再继续写入请求进入队列。
如果当前正在进行批量写入，则等待当前写入完成后将写入队列剩余内容取出立即进行写入 TDengine
请求样例
```json
curl --request POST -uroot:taosdata localhost:6041/input_json/v1/meters/flush
```

## 5. 性能

受 JSON 转换复杂度影响

## 6. 安全

1. 所有接口需要进行身份验证，验证方式与 sql 执行接口相同
2. sql注入考虑：
   - 关键字符已转义，即使构造出多条语句也只识别第一条语句。
   - 此接口只获取影响行数不获取查询结果，也不存在结果回显。

## 7. 兼容性

无。

## 8. 运维

1. 需要仔细配置配置文件，正式上线前预先进行写入测试。
2. 每个节点 taosAdapter 相关配置内容要相同。

## 9. 使用场景

使用 HTTP 上传 JSON 类型数据进行写入场景

## 10. 约束和限制

限制：
1. 需要提前创建 db 与超级表
2. 数据写入不会进行数据类型变更与长度变更
3. 批写入遇到错误时报错信息写入到日志
4. 批写入时重启存在丢数据风险
5. 如果传入数据存在大数字在转换过程会存在精度丢失情况，如果传入大数请使用字符串传递。比如纳秒时间戳需要使用字符串传递

## 11. 常见错误和排查

1. JSON 转换失败，排查日志
2. 写入失败，排查数据表的数据类型与数据长度是否满足要求
   - 0x388 Database not exist 数据库不存在
   - 0x2603 Table does not exist 超级表不存在
   - 0x2602 Invalid column name 列/tag不存在
   - 0x2653 Value too long for column/tag 数据长度不对
   - 0x216 syntax error 语法错误/数据类型不对应

## 12. 可观测性

1. dry_run 接口提供json转换和sql生成调试
2. error 日志记录错误信息
3. 添加如下指标：
  adapter_input_json 表

  | **field** | **type** | **is_tag** | **comment** |
| --- | --- | --- | --- |
| _ts | TIMESTAMP |  | 记录时间 |
| total_rows | DOUBLE |  | 当前采集周期收到数据行数 |
| success_rows | DOUBLE |  | 当前采集周期处理成功行数 |
| fail_rows | DOUBLE |  | 当前采集周期处理失败行数 |
| inflight_rows | DOUBLE |  | 正在处理中行数 |
| affected_rows | DOUBLE |  | 当前采集周期数据库影响行数 |
| url_endpoint | NCHAR | TAG | 配置的路由端点 |
| endpoint | NCHAR | TAG | taosAdapter 地址 |

## 13. 安装和卸载

无。

## 14. 文档

需要修改文档

## 15. 参考文档

1. jsonata:https://jsonata.org/

## 16. 附录
