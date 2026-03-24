# STMT2支持decimal类型写入-FS

## 1. 背景

JIRA [TS-6202](https://jira.taosdata.com:18080/browse/TS-6202)

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/07/31 | 1.0 | @彭荣坤 | 新建 |
|  |  |  |  |

## 3. 定义

**decimal：** 是一种用于高精度十进制浮点数运算的数据类型，通常用于需要精确计算的场景。TDengine 中实现成 int64 和 int128 两种类型，使用 scale 确定小数点。

## 4. 行为说明

数据类型：取决于precision 的大小，若 precision <= 18， 则使用 TSDB_DATA_TYPE_DECIMAL64， 否则使用 TSDB_DATA_TYPE_DECIMAL
参数绑定请用变长字符串形式的数据进行绑定，客户端会根据列类型自动转换成为相应精度的decimal写入。
小数点前的数据长度如果超出设定，则会溢出报错`TSDB_CODE_DECIMAL_OVERFLOW`；小数点后的数据溢出会四舍五入，保存指定scale长度的数据，注意：四舍五入也可以导致进位溢出。详细行为逻辑可以参考：[DECIMAL数据类型 FS](https://taosdata.feishu.cn/wiki/RQcswXCNXiNQamkMKWucmVrWnUc)
`taos_stmt2_get_fields`获取64位和128位decimal列类型的代码示例：
```cpp
// stb：CREATE STABLE `stmt2_testdb_20`.stb (ts TIMESTAMP, b1 DECIMAL(4,2), b2 DECIMAL(20,10)) TAGS (t INT)
// prepare sql：nsert into `stmt2_testdb_20`.? using `stmt2_testdb_20`.stb tags(1) values(?,?,?)
    
int             fieldNum = 0;
TAOS_FIELD_ALL* pFields = NULL;
code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
checkError(stmt, code, __FILE__, __LINE__);
ASSERT_EQ(fieldNum, 4);
ASSERT_STREQ(pFields[2].name, "b1");
ASSERT_EQ(pFields[2].type, TSDB_DATA_TYPE_DECIMAL64);
// 和其他数据类型的区别
ASSERT_EQ(pFields[2].precision, 4);
ASSERT_EQ(pFields[2].scale, 2);

ASSERT_STREQ(pFields[3].name, "b2");
ASSERT_EQ(pFields[3].type, TSDB_DATA_TYPE_DECIMAL);
// 和其他数据类型的区别
ASSERT_EQ(pFields[3].precision, 20);
ASSERT_EQ(pFields[3].scale, 10);
```

`taos_stmt2_bind_param`绑定2行64位decimal和128位decimal的代码示例：
```cpp
int64_t ts[3] = {1591060628000, 1591060629000};
char b1_data[64] =
      "99.9876"
      "1.0234";
char b2_data[128] =
      "1234567890.1234567890123"
      "1.23e+5";

int t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
int b1_len[2] = {7, 6};
int b2_len[2] = {24, 7};
  
TAOS_STMT2_BIND  col[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 2},
                            {TSDB_DATA_TYPE_DECIMAL64, &b1_data[0], &b1_len[0], NULL, 2},
                            {TSDB_DATA_TYPE_DECIMAL, &b2_data[0], &b2_len[0], NULL, 2}};
TAOS_STMT2_BIND* cols = &col[0];
TAOS_STMT2_BINDV bindv = {1, &tbnames, NULL, &cols};
code = taos_stmt2_bind_param(stmt, &bindv, -1);
```

## 5. 性能

对于只绑定执行一次或写入行数极少的场景，性能不如sql；其他场景性能优于sql

## 6. 兼容性

无兼容性问题

## 7. 运维

无

## 8. 使用场景

decimal参数绑定写入

## 9. 约束和限制

1. DECIMAL 类型仅支持普通列，暂不支持 tag 列。
2. 参数绑定DECIMAL 类型只支持字符串类型的数据绑定写入，不支持FLOAT/DOUBLE以及其他格式的数据（可能会在转换中被错误解析为0，该行为和taosc一致）。
3. 必须绑定数据，不支持运算表达式

## 10. 可观测性

无

## 11. 安装和卸载

不涉及

## 12. 参考文档

[Decimal 数据类型实现](https://taosdata.feishu.cn/wiki/MilpwK6UGigQdokX8aZc5LMnnxf)
[DECIMAL数据类型 FS](https://taosdata.feishu.cn/wiki/RQcswXCNXiNQamkMKWucmVrWnUc)
