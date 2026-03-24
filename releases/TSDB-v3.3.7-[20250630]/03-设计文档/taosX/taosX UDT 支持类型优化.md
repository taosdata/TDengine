# taosX UDT 支持类型优化

## 1. 背景

https://jira.taosdata.com:18080/browse/TS-6884

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/07/23 | 1.0 | 霍琳贺 | 创建 |

## 3. 定义

1. Rhai：taosX 所使用的 Rust 嵌入式脚本引擎。
2. UDT：User Defined Transformer，用户自定义解析器。使用 Rhai 脚本进行文本解析。

## 4. 行为说明

接受 JSON 数组作为输入、任意内容作为输出。
输入示例如下：
```json {wrap}
[
 {"data": [
   {"ts": "2025-07-28T08:00:00Z", "val": 1, "device": "DEV1"},
   {"ts": "2025-07-28T08:00:00Z", "val": 1, "device": "DEV2"},
   {"ts": "2025-07-28T08:00:00Z", "val": 1, "device": "DEV3"}
 ]},
 {"data": [
   {"ts": "2025-07-28T09:00:00Z", "val": 1, "device": "DEV1"},
   {"ts": "2025-07-28T09:00:00Z", "val": 1, "device": "DEV2"},
   {"ts": "2025-07-28T09:00:00Z", "val": 1, "device": "DEV3"}
 ]}
]
```

UDT 示例如下：
```json {wrap}
let y = [];

for item in data {
  if item.type_of() == "array" {
    for ii in item {
      y.push(ii);
    }
  } else {
    y.push(item);
  }
}

y
```

解析为：
```json {wrap}
{"ts": "2025-07-28T08:00:00Z", "val": 1, "device": "DEV1"}
{"ts": "2025-07-28T08:00:00Z", "val": 1, "device": "DEV2"}
{"ts": "2025-07-28T08:00:00Z", "val": 1, "device": "DEV3"}
{"ts": "2025-07-28T09:00:00Z", "val": 1, "device": "DEV1"}
{"ts": "2025-07-28T09:00:00Z", "val": 1, "device": "DEV2"}
{"ts": "2025-07-28T09:00:00Z", "val": 1, "device": "DEV3"}
```

## 5. 性能

无。

## 6. 兼容性

兼容现有行为。

## 7. 运维

无。

## 8. 使用场景

1. 嵌套数组 JSON 解析
2. 单个 JSON 对象解析为多行数据
3. JSON 嵌套数组过滤

## 9. 约束和限制

无。

## 10. 常见错误和排查

无。

## 11. 可观测性

无。

## 12. 安装和卸载

该组件随 TDengine 产品安装包一同发布，随 TDengine 安装和卸载。

## 13. 文档

无。

## 14. 参考文档

1. RS - [Explorer 企业版试用时需要注册](https://taosdata.feishu.cn/wiki/BZ67wq7LriTD3YkqL8uc5hezn8d)

## 15. 附录

无。
