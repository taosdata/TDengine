# 支持将查询结果写入超级表 FS

## 1. 背景

JIRA [TS-6150](https://jira.taosdata.com:18080/browse/TS-6150)
需求 [支持将查询结果写入超级表 RS](https://taosdata.feishu.cn/wiki/UGn2wXineigNSfkzkyBcaZbFnjf)

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/06/20 | 1.0 | @彭荣坤 | 初稿 |
|  |  |  |  |

## 3. 定义

无

## 4. 行为说明

### 4.1 语法

```plaintext
INSERT INTO stb_name (tbname,field1_name, ...) subquery
```

### 4.2 语法说明

1. subquery 可以是任意的查询语句，查询结果的类型和顺序必须和前面指定的field name一一对应
2. 指定的第一个file_name必须是tbname，否则写入失败（后续会优化）；如果不指定tbname，会报错
3. 写入的子表不存在时，支持自动建表；写入的子表存在时，不会更新标签值，但是会写入指定列的数据
4. 在 field_name 列表中可不指定标签列
   - 不包含标签列，或者标签列不是对应超级表的全集时，如果需要创建子表，标签值将被设置为 NULL
5. 在 field_name 列表中必须指定数据列：
   - 数据列必须包含超级表中的主键时间戳列，如果涉及复合主键，还必须包含复合主键列
   - 当数据列不是对应超级表的全集时，未指定的数据列设置为 NULL

## 5. 性能

不涉及性能

## 6. 兼容性

无兼容性问题

## 7. 运维

无

## 8. 使用场景

直接将查询结果写入超级表

## 9. 约束和限制

1. SQL 语句中仅能包含一个超级表，不支持向多个超级表写入
2. 必须指定field name，并且指定的第一个file_name必须是tbname
3. 在 field_name 列表中必须指定col列，可不指定tag列
4. 只支持SQL写入，不支持其他方式

## 10. 可观测性

如果出现问题，需要打开trace日志

## 11. 安装和卸载

不涉及

## 12. 文档

官方文档 数据写入 超级表写入增加subquery的说明

## 13. 参考文档

[支持将查询结果写入超级表 RS](https://taosdata.feishu.cn/wiki/UGn2wXineigNSfkzkyBcaZbFnjf)
[支持将查询结果写入超级表  TS](https://taosdata.feishu.cn/wiki/YHIHwCX5AimZL3kLojpcjylnnDb)

## 14. 附录
