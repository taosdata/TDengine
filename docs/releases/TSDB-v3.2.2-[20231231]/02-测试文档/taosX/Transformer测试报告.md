# Transformer测试报告

## 1. Scope

只涉及以下3种类型的数据源：
- MQTT
- Kafka
- CSV

## 2. Limitation

- extract-split: 目前不支持remove和inplace参数，不支持多个分隔符。1.4.0只支持格式split：{sep:",", n: 2, names:[...]}
- filter: 暂不支持通过regex进行过滤
- mapping-generator: 目前只支持now
- mapping-expr: remove，trim，make_lower, make_upper, pad不支持
- Json 或文本数据目前只支持一条数据解析为一条记录

## 3. Notes

- 之前对于MQTT数据源，之前我们只支持JSON格式的payload, 现在可以支持plain text了，可通过split/regex解析
- 对于使用split/regex解析的字段，其类型均为varchar类型，如果要在filter中使用某个字段的值做过滤，需要使用相应的类型转换函数，例如：voltage.parse_int() > 220
- extract-json: 支持对字段重命名和类型转换，例如：voltage=v::double
- 关于filter
  - 在Explorer上，只允许输入一个filter
  - 复杂的逻辑可通过&&, ||, !来表达
  - 新增/编辑任务时，修改或者新增filter，需要点击filter后面的勾号按钮，提交任务时才会生效

## 4. Case

### 4.1 MQTT

| Type | Description | Expected Results | Result | Memo |
| --- | --- | --- | --- | --- |
| sanity | 消息体格式：JSON Object
extract: JSON Path, 提取时对字段重命名
filter: false
mapping: 映射 | 预览结果正确
数据同步正常 | Pass |  |
|  | 消息体格式：JSON Object
extract: JSON Path
filter: voltage > 200
mapping: 映射 | 预览结果正确
数据同步正常 | Pass |  |
|  | 消息体格式：Plain text
extract: split
filter: voltage > 200 && location.contains("California")
mapping: 电压采用固定值 | 预览结果正确
数据同步正常 | Pass |  |
|  | 消息体格式：Plain text
extract: regex
filter: voltage > 200
mapping: expr: voltage * 10 | 预览结果正确
数据同步正常 | Pass |  |
|  | 消息体格式：JSON Object
extract: JSON Path
filter: voltage > 200
mapping: join groupid, id with - | 预览结果正确
数据同步正常 | Pass |  |
|  | 消息体格式：JSON Object
extract: JSON Path
filter: voltage > 200
mapping: format ${location}-${groupid} | 预览结果正确
数据同步正常 | Pass |  |
|  | 消息体格式：JSON Object
extract: JSON Path
filter: voltage > 200
mapping: format ${location}-${groupid} | 预览结果正确
数据同步正常 | Pass |  |
|  | 消息体格式：JSON Object
extract: JSON Path
filter: voltage > 200
mapping: sum id, groupid | 预览结果正确
数据同步正常 | Pass |  |
|  | 消息体格式：JSON Object
extract: JSON Path
filter: voltage > 200
mapping: expr voltage * 10 / 2 | 预览结果正确
数据同步正常 | Pass |  |

### 4.2 Kafka

任务提交后任务同步数据报错：TD-27539 [transform]提交kafka任务同步数据失败报错（Done）
| Type | Description | Expected Results | Result | Memo |
| --- | --- | --- | --- | --- |
| Sanity | Message Body：1条数据
Extract: json
Filter: null
Mapping:选择映射

1. json提取支持json path
2. json提取支持array
3. 支持指定数据类型
4. 重命名  id=testid::double | sample out正确输出
任务启动数据正确入库 | Pass | 解析数组错误：[TD-27549](https://jira.taosdata.com:18080/browse/TD-27549)（Done）

布尔类型显示：[TD-27567 ](https://jira.taosdata.com:18080/browse/TD-27567)

在json中不能指定float->int（Done） |
|  | Message Body：1条数据
Extract: split
Filter: null
Mapping:选择映射

1. split(value,",")
2. split items number
3. multi separators（1.4.0不支持）
4. split at position(1.4.0不支持)
5. remove old field and use the new fields （1.4.0不支持） | sample out正确输出
任务启动数据正确入库 | Pass | 1.4.0只支持格式split：{sep:",", n: 2, names:[...]}
[TD-27643](https://jira.taosdata.com:18080/browse/TD-27643) |
|  | Message Body：1条数据
Extract: regex
Filter: null
Mapping:选择映射 | sample out正确输出
任务启动数据正确入库 |  |  |
|  | Message Body：1条数据
Extract: json
Filter: 根据值来筛选（==, !=, >, >=, <, <=, in, !in）
Mapping:选择映射

testid !in [12,13,15]&& testid in [9, 10, 16] && testid == 9 && testid != 1
testid in 10..100
testid > 13
testid <= 9
[12,10,8].contains(id)
message != "hello"
message in ["hello", "taosx"]
message in "abctaosx123" | sample out正确输出
任务启动数据正确入库 | Pass | fiter正确 |
|  | Message Body：1条数据
Extract: json
Filter: 字符串匹配
Mapping:选择映射

message.contains("lo")
message.len()==5
message.starts_with("he")/message.ends_with("he") | sample out正确输出
任务启动数据正确入库 | Pass |  |
|  | Message Body：1条数据
Extract: json
Filter: 组合(&&, \|\|)
Mapping:选择映射 | sample out正确输出
任务启动数据正确入库 | Pass |  |
|  | Message Body：1条数据
Extract: json
Filter: null
Mapping: 表达式（value, generator, join, sum, format） | sample out正确输出
任务启动数据正确入库 | Pass |  |
|  | Message Body：1条数据
Extract: json
Filter: null
Mapping: 表达式（expr）

replace, truncate,append | sample out正确输出
任务启动数据正确入库 | Pass | [TD-27611](https://jira.taosdata.com:18080/browse/TD-27611?src=confmacro)
（1.4.0暂不支持）
remove暂不支持 |
|  | Message Body：多条数据（字段一致）
Extract: json
Filter: null
Mapping:选择映射 | sample out正确输出
任务启动数据正确入库 | Pass |  |
|  | Message Body：多条数据
Extract: json
Filter: null
Mapping:选择映射

{"id":1,"groupid":2};{"id":1,"groupid":2,"location":"beijing"}
数据字段不同
数据字段的分隔符不一致 | sample out正确输出
任务启动数据正确入库 | Pass |  |
|  | Message Body：1条数据中有不同的分隔符
Extract: split
{"id":1,"groupid":2?"abc":3} | sample out正确输出
任务启动数据正确入库 |  | 1.4.0不支持 |
|  | Message Body：多条数据
Extract: split

{"id":1,"groupid":2};{"id":1?"groupid":2}
数据的分隔符不一样 | sample out正确输出
任务启动数据正确入库 |  | 1.4.0不支持 |
|  | Message Body：1条数据
Extract: json,输入的表达式前/后有空格 | sample out正确输出
任务启动数据正确入库 | Pass | [TD-25997](https://jira.taosdata.com:18080/browse/TD-25997) |
|  | Message Body：1条数据, 字符串长度76
Extract: json,定义字符长度为varchar(64) | sample out正确输出
任务启动数据正确入库 | Pass | 自动扩长 |
|  | Message Body：多条数据
1. 点击映射的计算得到2列输出
2. 增加fiter, 再点击映射的计算按钮输出的列为1列，显示也更新为1列 | sample out正确输出
任务启动数据正确入库 | Pass | [TD-27555](https://jira.taosdata.com:18080/browse/TD-27555?src=confmacro)
[[transform]点击映射的计算按钮得到的列出由2列变为1列时，显示不正确](https://jira.taosdata.com:18080/browse/TD-27555?src=confmacro) |
|  | 消息体：
{"id":1};{"id":2};{"id":3}
过滤条件：id>=3
映射： id： expr id*3 | sample out结果为{"id":9}
任务启动后，只有id>=3的才进行id*3的运算并入库 | Pass | [TD-27585](https://jira.taosdata.com:18080/browse/TD-27585) |
|  | 提交任务之后编辑，修改mapping | sample out结果正确
编辑后的任务正常执行 | Pass | [TD-27616](https://jira.taosdata.com:18080/browse/TD-27616) |
|  | 数据中含有时间戳 | sample out结果正确
编辑后的任务正常执行 | Pass |  |

### 4.3 CSV

| Type | Description | Expected Results | Result | Memo |
| --- | --- | --- | --- | --- |
| Sanity | 包含 Header 的单个 CSV 文件上传，配置解析入库规则，不使用 Transformer。 | 正确入库 | Pass | 必须要点击计算之后才能正常提交。
[https://jira.taosdata.com:18080/browse/TD-27601](https://jira.taosdata.com:18080/browse/TD-27601) |
|  | 包含 Header 的单个 CSV 文件上传，配置解析入库规则，不使用 Transformer。配置忽略前 2行。 | 正确入库，数据匹配。 | Pass |  |
|  | 不包含 Header 的单个 CSV 文件上传，配置解析入库规则，不使用 Transformer。 | 正确入库 | Pass |  |
|  | 配置 CSV 文件解析地址（CSV 文件包含 Header），配置解析入库规则，不使用 Transformer。 | 正确入库 | Pass |  |
|  | 配置 CSV 文件解析地址（CSV 文件不包含 Header），配置解析入库规则，不使用 Transformer。 | 正确入库 | Pass | [https://jira.taosdata.com:18080/browse/TD-27623](https://jira.taosdata.com:18080/browse/TD-27623) |
|  | 包含 Header 的单个 CSV 文件上传，配置解析入库规则，Transformer 配置部分列使用 json 提取。 | 示例输出正确，且能正确入库 | Pass |  |
|  | 包含 Header 的单个 CSV 文件上传，配置解析入库规则，Transformer 配置部分列使用正则提取。 | 示例输出正确，且能正确入库 | Pass |  |
|  | 包含 Header 的单个 CSV 文件上传，配置解析入库规则，Transformer 配置部分列使用 split 提取。 | 示例输出正确，且能正确入库 | Pass | [https://jira.taosdata.com:18080/browse/TD-27645](https://jira.taosdata.com:18080/browse/TD-27645) |
|  | 包含 Header 的单个 CSV 文件上传，配置解析入库规则，Transformer 配置部分 fliter 过滤部分列。 | 示例输出正确，且能正确入库 | Pass |  |
|  | 配置 CSV 文件解析地址（CSV 文件包含 Header），配置解析入库规则，不使用 Transformer。配置时间戳生成规则为 generator now | 示例输出正确，且能正确入库 | Pass |  |
|  | 配置包含一万行的 CSV 文件解析地址（CSV 文件包含 Header），配置解析入库规则（创建一万张子表），不使用 Transformer。配置时间戳生成规则为 generator now | 示例输出正确，且能正确入库 | Pass |  |

## 5. API

```bash
curl --request POST \
  --url http://192.168.2.11:6060/api/x/transform/sample/flat \
  --header 'Content-Type: application/json' \
  --header 'Trace-Id: 1a2b3c4d' \
  --data '{
  "input": [
    {
      "ts": "2023-11-27T06:22:29Z",
      "topic": "topic",
      "qos": "qos",
      "payload": "{   \"id\": 1,   \"data\": {     \"current\": 10.77,     \"voltage\": 221,     \"phase\": 0.77   },   \"groupid\": 7,   \"location\": \"California.SanDiego\" }"
    },
    {
      "ts": "2023-11-27T06:22:29Z",
      "topic": "topic",
      "qos": "qos",
      "payload": "{   \"id\": 2,   \"data\": {     \"current\": 10.77,     \"voltage\": 219,     \"phase\": 0.77   },   \"groupid\": 7,   \"location\": \"California.SanDiego\" }"
    }
  ],
  "parser": {
    "parse": {
      "payload": {
        "json": [
          "id",
          "$.data.current",
          "$.data.voltage=v",
          "groupid",
          "location"
        ]
      }
    },
    "mutate": [
      {
        "filter": [
          "id == 1 && v > 220"
        ]
      },
      {
        "map": {
          "ts": {
            "cast": "ts"
          },
          "id": {
            "cast": "id"
          },
          "current": {
            "cast": "current"
          },
          "v": {
            "expr": "v*10",
            "as": "int"
          }
        }
      }
    ],
    "model": {
      "name": "d{id}",
      "using": "meters_mqtt",
      "tags": [
        "groupid",
        "location"
      ],
      "columns": [
        "ts",
        "id",
        "current",
        "v"
      ]
    }
  }
}'
```


## 

## 6. Reference

- [taosX Predictable Transformer Pipeline](https://taosdata.feishu.cn/wiki/ZkkqwalAaipoyjkku03cAZ72nWe) 
- [Transform in taosX](https://taosdata.feishu.cn/wiki/wikcngRpSaZ2dSKpBHfwXtLh7qf)
- [How to use regex to extract in Explorer?](https://taosdata.feishu.cn/wiki/ETtsw9bD2ibsQqkXXtUcXMkInWh) 
- https://rhai.rs/playground/stable/
- https://rhai.rs/book/ref/index.html
  - https://rhai.rs/book/ref/operators.html
  - https://rhai.rs/book/ref/string-fn.html
