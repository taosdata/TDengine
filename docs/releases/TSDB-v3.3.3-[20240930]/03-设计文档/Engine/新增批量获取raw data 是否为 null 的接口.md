# 新增批量获取raw data 是否为 null 的接口

## 1. 背景

Python 语言循环调用 taos_is_null 接口判断数据是否为 null 的性能极差，c 语言没这个问题，提供批量获取 null 的接口给 python 等语言使用。
具体见如下jira:

TD-31242

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/08/27 | 0.1 | 王明明 | 创建 |
|  |  |  |  |

## 3. 定义

新增接口：
```sql
int taos_is_null_by_column(TAOS_RES *res, int columnIndex, bool result[], int *rows)

参数说明：
- res 为获取的结果。不合法，报错。
- columnIndex 为列的index。需根据 taos_fetch_fields 获取的列个数使用，不合法，报错。
- result 为bool型的数组，数组每个元素存储一行数据是否为null，需调用者定义后传入接口使用，不合法，报错。
- rows 为 result 数组的长度指针。指针值需大于0，否则报错。rows 指针的输入值为要校验的数据条数（可以小于真实的数据条数，如果大于，被设置为真实数据条数），rows 指针的返回值为 result 里有效的长度。

返回值：
- 0      成功。
- 小于0   失败。
```

## 4. 行为说明

```sql
bool        taos_is_null(TAOS_RES *res, int32_t row, int32_t col)
int        *taos_get_column_data_offset(TAOS_RES *res, int columnIndex);
```

上面两个接口为现有接口。taos_is_null 可以获取数据每行每列是否为空，需要一行一行调用，在python 语言中效率很低。
taos_get_column_data_offset 接口可以批量获取某一列数据所有行是否为 null，但是只是获取varchar 等类型数据的 offset，通过offset[i] == -1 来判断是否为空。

新增接口 taos_get_column_data_null 可获取所有类型数据是否为null，可代替taos_get_column_data_offset 接口使用。

## 5. 性能

通过taos_get_column_data_null 批量获取一列数据每行是否为 null 可极大的提升性能，特别在python 语言中，具体可查看jira里描述。c 语言提升不明显。

## 6. 兼容性

无。

## 7. 运维

无。

## 8. 使用场景

无。

## 9. 约束和限制

无。

## 10. 常见错误和排查

无。

## 11. 可观测性

无。

## 12. 安装和卸载

无。

## 13. 文档

无。

## 14. 参考文档

无。

## 15. 附录

无。
