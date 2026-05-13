# taosadapter 记录 STMT 到 CSV FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-07 | 2026-01-08 | 0.1 | 谭雪峰 | 编写 |
| 2026-01-12 | 2026-01-12 | 1.0 | 谭雪峰 | http 接口默认结束时间改为启动时间一小时后 |

## 2. 背景

taosadapter 记录 stmt2 绑定数据和执行结果到 CSV 文件

## 3. 定义

无

## 4. 行为说明

### 4.1 配置文件

1. 新增配置项 `log.enableStmtToCsvLogging` 布尔值，默认为 false，表示是否开启 stmt 记录功能。设置为 true 后将开启 stmt 记录到 csv 文件，开始记录时间为启动时间，结束时间为 `2300-01-01 00:00:00`。
2. 文件命名与日志相同规则 :`taosadapterStmt_{instanceId}_{yyyyMMdd}.csv[.index]`
3. 保留空间、文件切割、保存路径等使用 log 已存在参数：
`**log.path**`**: 保存路径**
`**log.keepDays**`** : 保留天数**
`**log.rotationCount**`**：最多保留份数**
`**log.rotationSize**`**：单个文件最大大小**
`**log.compress**`**：是否启用压缩**
`**log.reservedDiskSize**`**: 保留硬盘空间大小**

### 4.2 动态开启

通过发送 HTTP POST 请求到 `/record_stmt`接口来动态开启记录，使用与 `/rest/sql` 相同的鉴权方式，样例如下：
```bash {wrap}
curl --location --request POST 'http://127.0.0.1:6041/record_stmt' \
-u root:taosdata \
--data '{"start_time":"2026-01-07 17:00:00","end_time":"2026-01-07 18:00:00","location":"Asia/Shanghai"}'
```

如果所有参数都使用默认值则可以不传 data，样例如下
```bash
curl --location --request POST 'http://127.0.0.1:6041/record_stmt' \
-u root:taosdata
```

支持的参数项如下：
- start_time：【可选参数】开始采集的时间，格式为 `yyyy-MM-dd HH:mm:ss`，如果不设置则使用当前时间。
- end_time：【可选参数】结束采集的时间，格式为 `yyyy-MM-dd HH:mm:ss`，如果不设置则使用当前时间加一小时。
- location：【可选参数】解析采集开始和结束时间使用的时区信息，如果不设置则使用 taosAdapter 所在服务器时区。时区格式 IANA，例如：Asia/Shanghai

1. 成功返回 HTTP code 200，返回结构如下
```bash
{"code":0,"desc":""}
```

1. 失败返回 HTTP code 非 200，返回 json 结构如下，code 非 0 时 desc 描述错误内容
```bash
{"code":65535,"desc":"unmarshal json error"}
```

### 4.3 动态关闭

通过发送 HTTP DELETE 请求到 /record_stmt 接口来关闭，使用与 `/rest/sql` 相同的鉴权方式，样例如下：
```bash
curl --location --request DELETE 'http://127.0.0.1:6041/record_stmt' \
-u root:taosdata
```

成功返回 HTTP code 200 ,目前无失败
1. 任务存在时返回如下
```json
{
        "code": 0,
        "message": "",
        "start_time": "2025-07-16 17:00:00",
        "end_time": "2025-07-16 18:00:00"
}
```

- start_time 为取消任务配置的启动时间，时区为 taosAdapter 所在服务器时区
- end_time 为取消任务配置的结束时间，时区为 taosAdapter 所在服务器时区
1. 任务不存在时返回如下
```json
{
        "code": 0,
        "message": ""
}
```


### 4.4 查询状态

通过发送 HTTP GET 请求到 `/record_stmt` 接口来查询任务，使用与 `/rest/sql` 相同的鉴权方式，样例如下：
```bash
curl --location 'http://127.0.0.1:6041/record_stmt' \
-u root:taosdata
```

成功返回 HTTP code 200，返回样例如下
```json
{
        "code": 0,
        "desc": "",
        "exists": true,
        "running": true,
        "start_time": "2025-07-16 17:00:00",
        "end_time": "2025-07-16 18:00:00",
        "current_concurrent": 100
}
```

- code：错误码，0 为成功
- desc：错误信息，成功为空字符串
- exists：任务是否存在
- running：任务是否在运行期
- start_time：开始时间，时区为 taosAdapter 所在服务器时区
- end_time：结束时间，时区为 taosAdapter 所在服务器时区
- current_concurrent：当前记录并发度

### 4.5 记录文件格式

记录文件以 CSV 格式存储，不记录表头
1. TS：打印日志时间，格式为 `yyyy-MM-dd HH:mm:ss.SSSSSS`，时区为 taosAdapter 所在服务器时区
2. IP：客户端 IP
3. SourcePort：客户端端口
4. AppName：客户应用 AppName
5. User：当前连接的用户名
6. ConnType：连接类型（ws）
7. QID：请求 ID，保存为 16 进制
8. StartTime: 开始处理时间，格式为 `yyyy-MM-dd HH:mm:ss.SSSSSS`，时区为 taosAdapter 所在服务器时区
9. STMT2：stmt2 内存地址，保存为 16 进制
10. Action：操作（prepare、bind、exec）
11. Code：操作结果 0 代表成功，其他代表错误码
12. Duration(us)：执行时间，未执行完为 -1
13. Data：请求或结果数据
   - prepare 为准备的 sql
   - bind 为绑定数据，解析成功为 JSON 数据，解析失败将为原始二进制数据
      - Json 数据格式如下

    | 字段名 | 类型 | 描述 |
| --- | --- | --- |
| count | int | 绑定表数量 |
| table_names | 一维数组 | 表名 |
| tags | 二维数组 [表][列]{ type：列类型 data：一维数组，每个元素代表一行数据 1. varbinary、blob、geometry 解析为十六进制字符串 1. binary、nchar、json、decimal 解析为 utf-8 字符串 1. 数字类型转为数字 1. 时间戳类型转为数字 1. Bool 类型转为 bool 1. Null 转为 json null } | tag值 |
| cols | 同 tags | 列值 |

    样例：
    ```json {wrap}
    {
            "count": 1,
            "table_names": ["test1"],
            "tags": [
                    [{
                            "type": 9,
                            "data": [1726803356466]
                    }, {
                            "type": 1,
                            "data": [true]
                    }, {
                            "type": 2,
                            "data": [1]
                    }, {
                            "type": 3,
                            "data": [2]
                    }, {
                            "type": 4,
                            "data": [3]
                    }, {
                            "type": 5,
                            "data": [4]
                    }, {
                            "type": 6,
                            "data": [5.5]
                    }, {
                            "type": 7,
                            "data": [6.6]
                    }, {
                            "type": 11,
                            "data": [7]
                    }, {
                            "type": 12,
                            "data": [8]
                    }, {
                            "type": 13,
                            "data": [9]
                    }, {
                            "type": 14,
                            "data": [10]
                    }, {
                            "type": 8,
                            "data": ["binary"]
                    }, {
                            "type": 10,
                            "data": ["nchar"]
                    }, {
                            "type": 20,
                            "data": ["010100000000000000000059400000000000005940"]
                    }, {
                            "type": 16,
                            "data": ["76617262696e617279"]
                    }, {
                            "type": 17,
                            "data": ["12345.6789"]
                    }, {
                            "type": 21,
                            "data": ["98765.4321"]
                    }, {
                            "type": 18,
                            "data": ["7468697320697320626c6f622064617461"]
                    }]
            ],
            "cols": [
                    [{
                            "type": 9,
                            "data": [1726803356466, 1726803357466, 1726803358466]
                    }, {
                            "type": 1,
                            "data": [true, null, false]
                    }, {
                            "type": 2,
                            "data": [11, null, 12]
                    }, {
                            "type": 3,
                            "data": [11, null, 12]
                    }, {
                            "type": 4,
                            "data": [11, null, 12]
                    }, {
                            "type": 5,
                            "data": [11, null, 12]
                    }, {
                            "type": 6,
                            "data": [11.2, null, 12.2]
                    }, {
                            "type": 7,
                            "data": [11.2, null, 12.2]
                    }, {
                            "type": 11,
                            "data": [11, null, 12]
                    }, {
                            "type": 12,
                            "data": [11, null, 12]
                    }, {
                            "type": 13,
                            "data": [11, null, 12]
                    }, {
                            "type": 14,
                            "data": [11, null, 12]
                    }, {
                            "type": 8,
                            "data": ["binary1", null, "binary2"]
                    }, {
                            "type": 10,
                            "data": ["nchar1", null, "nchar2"]
                    }, {
                            "type": 20,
                            "data": ["010100000000000000000059400000000000005940", null, "010100000000000000000059400000000000005940"]
                    }, {
                            "type": 16,
                            "data": ["76617262696e61727931", null, "76617262696e61727932"]
                    }, {
                            "type": 17,
                            "data": ["12345.6789", null, "22345.6789"]
                    }, {
                            "type": 21,
                            "data": ["98765.4321", null, "88765.4321"]
                    }, {
                            "type": 18,
                            "data": ["7468697320697320626c6f622064617461", null, "7468697320697320616e6f7468657220626c6f622064617461"]
                    }]
            ]
    }
    ```

   - exec 为影响行数

## 5. 性能

受解析二进制耗时和大数据量写盘影响，功能开启时性能会显著降低

## 6. 安全

1. 记录文件会自动切割并保留空间，确保硬盘不被写满
2. 只有开启功能时进行记录，通过接口开启时需要进行验证
3. 记录不包含密码等机密信息

## 7. 兼容性

无

## 8. 运维

无。

## 9. 使用场景

指定时间范围内记录 STMT 执行信息

## 10. 约束和限制

约束：
1. 只允许有一个任务，如果存在任务（即使没有到达运行时间）再次开启任务或报错，需先关闭再开启。
2. 只处理 stmt2 的 WebSocket 请求。
3. 每个 stmt 执行结束之后进行记录。
限制：
1. 当日志目录剩余空间不足时不再记录
2. 当手动关闭或达到结束时间后进行flush

## 11. 常见错误和排查

1. 调用接口时可能产生以下错误：
   - "unmarshal json error" 解析请求json失败
   - "start record error: xxxx" 启动任务失败
   - "invalid location format" 时区解析失败
   - "invalid start time format" 开始时间解析失败
   - "invalid end time format" 解析结束时间失败
   - "start time xxx is after end time xxx" 结束时间比开始时间早
   - "end time xxx is in the past" 结束时间比当前时间早
   - "API 'record_stmt' does not allow concurrent execution" 开启或关闭接口未执行完就来新的请求
2. 当长时间没有被记录，以下是可能情况：
   - 日志目录空间不足
   - 执行未结束

## 12. 可观测性

任务启动时打印日志，结束时输出结果文件、打印日志

## 13. 安装和卸载

无

## 14. 文档

需要修改文档

## 15. 参考文档

[adapter支持记录SQL标准化输出 fs](https://taosdata.feishu.cn/wiki/XmVvwIV8NiuBq6kEAvpcz3Jvnrb)

## 16. 附录
