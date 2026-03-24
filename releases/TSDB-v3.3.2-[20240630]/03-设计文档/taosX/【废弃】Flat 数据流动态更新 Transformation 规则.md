# 【废弃】Flat 数据流动态更新 Transformation 规则

## 1. 背景

目前河北电力分布式光伏从营销用采系统 Kafka 消费数据入库 TDengine，拟采用 taosX 替换之前采用的nifi方案。
Kafka 主题里面混合存放了普通用户电能表、分布式光伏电能表信息，业务需求是，仅导入分布式光伏电能表的采集数据，忽略掉其他采集数据。同时分布式光伏电能表在持续部署中，需要不断地增加 DEV_ID 列表项。期望能够提供机制，可以动态地添加新的过滤项，从而收集新加入的电能表数据。数据示例如下：
```json {wrap}
{
    "DATA_ITEM_ID": "aaa-0123456",
    "MONITOR_OBJ_TYPE": "bbb",
    "MONITOR_OBJ_CODE": "ccc",
    "PRO_MGT_ORG_CODE": "hebei",
    "MGT_ORG_CODE": "ddd",
    "PUSH_DATE": "2024-3-20 12:23:30",
    "U2358": "223",
    "U2359": "219",
    "PHASE_FLAG": "1",
    "DATA_POINT_FLAG": "3",
    "DATA_DATE": "2024-3-20",
    "CMD_TYPE": "eee",
    "PRODUCT_CODE": "fff",
    "DEV_ID":"xxx-1",
    "TERMINAL_ID":"zzz"
}
```


## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/05/09 | 0.1 | 周营昭 | 初稿 |

## 3. 定义

无。

## 4. 行为说明

Explorer 提供动态更新 transform 规则的 api.

### 4.1 更新过滤条件

| URL | /api/-/tasks/{taskId}/filter | taskId 对应当前要更新的 data in，数据源列表中可以查看这个id. |
| --- | --- | --- |
| method | POST |  |
| header | Authorization: Basic ${秘钥} | 其中秘钥为 ${用户名}:${密码}经过Base64后的字符串，比如默认用户密码 root:taosdata 生成的秘钥为：cm9vdDp0YW9zZGF0YQ== |
| body | { "filter": "filter-expression" } | 其中filter-expression 需要根据实际情况编写，比如： DEV_ID.start_with("fenbushi") or (DEV_ID in ["id1", "id2", "id3"]) |
| 成功：{"code": 0, msg: null} | 成功 |
| 权限失败：{"code": 403, msg: "authentication failure"} | 权限认证失败，header 中的 Authorization 信息认证失败。 |
| 过滤表达式错误：{"code": 400, msg: "filter expression error"} | 过滤表达式编写错误 |

调用接口成功后，对应的 data in task 自动应用新的 filter expression.
优点：接口比较直观，条件容易设置
缺点：性能差，数据是经过一系列的 parse 和 extract 之后才被应用的规则，前面的步骤浪费了大量算力后被过滤掉。

### 4.2 更新源数据解析规则

| URL | /api/-/tasks/{taskId}/parser | taskId 对应当前要更新的 data in，数据源列表中可以查看这个id. |
| --- | --- | --- |
| method | POST |  |
| header | Authorization: Basic ${秘钥} | 其中秘钥为 ${用户名}:${密码}经过Base64后的字符串，比如默认用户密码 root:taosdata 生成的秘钥为：cm9vdDp0YW9zZGF0YQ== |
| body | { "parse": { "value": {"udf": "script"} } } | 其中 script 需要根据实际情况编写。 |
| 成功：{"code": 0, msg: null} | 成功 |
| 权限失败：{"code": 403, msg: "authentication failure"} | 权限认证失败，header 中的 Authorization 信息认证失败。 |
| 过滤表达式错误：{"code": 400, msg: "script error: xxxx"} | 过滤表达式编写错误 |

调用接口成功后，对应的 data in task 自动应用新的数据解析器.
优点：高性能，在初步解析时过滤掉不需要的数据。

## 5. 性能

性能和实施策略有关，如果使用 transformer 中的 filter 来过滤数据，则可能性能会比较差。如果在 parse 的 udf 中提前过滤掉不用的数据，则能保证性能。

## 6. 兼容性

无。

## 7. 运维

对运维要求比较高，注意以下几点：
1. 提前创建 data in 任务，获取 taskId；
2. 使用 filter 接口，则需要了解任务中配置的 transform 过程中的可用字段信息，编写 rhai 表达式；
3. 使用 parse 接口，则需要将过滤语句内嵌在 udf 代码片段中。

## 8. 使用场景

需求对应场景：动态更新采集数据范围。

## 9. 约束和限制

限制：外部接口调用时间频率控制在分钟级以上。

## 10. 常见错误和排查

无。

## 11. 可观测性

无。

## 12. 安装和卸载

无。

## 13. 文档

无。

## 14. 参考文档

1. [需求报告：taosX支持动态筛选条件](https://taosdata.feishu.cn/wiki/EOxEwn3VDi8trNkJJYXcFx3qn1g)

## 15. 附录

无。
