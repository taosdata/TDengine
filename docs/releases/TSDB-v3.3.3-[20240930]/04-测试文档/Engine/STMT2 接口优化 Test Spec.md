# STMT2 接口优化 Test Spec

## 1. 测试目标

- STMT2支持原STMT旧接口的所有写入和查询场景
- STMT旧接口保留，用户仍可使用旧接口实现STMT

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.10.13 | 0.1 | 翟坤 | 初稿 |
| 2024.10.14 | 0.2 | 翟坤 | 完善测试用例和方案 |
|  |  |  |  |

## 3. 测试范围

### 3.1 功能测试范围

- STMT2支持写入和查询场景
- STMT旧接口回归测试用例通过，无新增问题

### 3.2 性能测试范围

- stmt2新接口写入性能不能低于性能优化后的旧版本3.3.2.9
- stmt2新接口查询性能不能低于性能优化后的旧版本3.3.2.9
- stmt2新接口写入性能高于7月优化前的写入性能

## 4. 测试结论

待补充...

## 5. 开发质量报告

结论：本特性/优化的开发质量是（优，良，一般，差，很差）

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 |  |
| 基础测试用例不通过 |  |
| Bug 总数 |  |
| 严重 Bug 总数 |  |

## 6. 已知问题和限制

因预留给测试的时间紧张且针对stmt的CI脚本很匮乏，本次测试根据stmt现有已知功能范围做基本功能覆盖，详细的测试补充工作，待以后集中需求梳理后补充详细的测试用例和脚本

## 7. 测试环境

待定

## 8. 测试数据 (Optional)

## 9. 测试用例

### 9.1 功能

#### 9.1.1 当前CI覆盖情况

| CI脚本 | 覆盖功能 |
| --- | --- |
| /root/chris/TDengine/tests/system-test/1-insert/test_stmt_set_tbname_tag.py | 1. 最基本的stmt数据插入 1. 查询类型 1. select * from log where bu < ? 1. select abs(?) from log where bu < ? 1. select abs(?) from log where nn= 'a? long string with 中文字符' 1. select CHAR_LENGTH(?) from log 1. select cast( ? as bigint) from log 1. select timediff('2021-07-21 17:56:32.590111',?,1a) from log 1. select count(?) from log 1. select bottom(bu,?) from log group by bu order by bu desc 1. select twa(?) from log |
| /root/chris/TDengine/tests/system-test/1-insert/test_stmt_muti_insert_query.py | 1. bind_param() 1. bind_param_batch() |
| /root/chris/TDengine/tests/system-test/1-insert/stmt_error.py | 错误验证： 1. 验证了数据bind时插入bianry类型的长度边界值 1. stmt bind param does not support normal value in sql 1. Timestamp data out of range |
| community/tests/script/api/batchprepare.c | 开发的测试脚本，3000多行的代码 |

#### 9.1.2 测试用例

| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| stmt2旧脚本测试 | stmt2新接口支持原有CI脚本 | CI - [test_stmt_set_tbname_tag.py](http://test_stmt_set_tbname_tag.py) | 将stmt接口更新为stmt2接口，测试通过 | Y | Pass | tests/army/stmt/stmt2/[test_stmt2_set_tbname_tag.py](http://test_stmt2_set_tbname_tag.py) |
|  |  | CI - [test_stmt_muti_insert_query.py](http://test_stmt_muti_insert_query.py) | 将stmt接口更新为stmt2接口，测试通过 | Y | Pass | case重复，不需要转化脚本 |
|  |  | CI - [stmt_error.py](http://stmt_error.py) | 将stmt接口更新为stmt2接口，测试通过 | Y |  |  |
| stmt2数据写入 | 普通表：不绑定表名、绑定数据 | 普通表：不绑定表名、绑定数据 | 支持 
insert into common_table values(?,?) | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py)：test_stmt_insert_common_table_without_bind_tablename |
|  | 普通表：绑定表名、绑定数据 | 普通表：绑定表名、绑定数据 | insert into ? values(?,?)，插入数据不会报错 | Y | Pass | 本次发版不支持,但数据可插入
test_stmt_insert_common_table_with_bind_tablename_data |
|  | 超级表：绑定 子表名、TAG 列、子表数据 | 超级表：绑定 子表名、TAG 列、子表数据 | 支持
insert into ? using super_table tags(?,?) values(?,?) | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py)：
test_stmt_insert_super_table_with_bind_ctablename_tags_data |
|  | 超级表：指定子表名称，绑定TAG 列、子表数据 | 超级表：指定子表名称，绑定TAG 列、子表数据 | 支持
insert into common_table using super_table tags (?,?) values (?,?) | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py)：
test_stmt_insert_super_table_with_bind_tags_data |
|  | 超级表：指定子表名称和TAG 列、绑定子表数据 | 超级表：指定子表名称和TAG 列、绑定子表数据 | 支持
insert into common_table using super_table tags (a,b) values (?,?) | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py)：
test_stmt_insert_super_table_with_bind_data |
|  | 超级表：指定TAG 列、绑定子表名称和子表数据 | 超级表：指定TAG 列、绑定子表名称和子表数据 | 支持
insert into ？ using super_table tags (a,b) values (?,?) | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py)：
test_stmt_insert_common_table_with_bind_ctablename_data |
|  | 不支持超级表自动建表语法 | 不支持超级表自动建表语法 | 支持
insert into super_table(tbname, location, groupId, ts, current, voltage, phase) values(?,?,?,?,?,?,?”) | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt_insert_super_table_auto_creat_table_with_bind_tags |
|  | 普通表bind 单表多行多列的数据结构 | 插入bind 单表多行多列的数据 | 数据写入成功 | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt2_insert_common_table_simple_table_muti_rows_muti_cols |
|  | 普通表bind 单表单行多列的数据结构 | 插入bind 单表单行多列的数据结构 | 数据写入成功 | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt2_insert_common_table_with_bind_tablename_data |
|  | 普通表bind 单表多行单列的数据结构 | 插入bind 单表多行单列的数据结构 | 数据写入成功 | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt2_insert_common_table_simple_table_muti_rows_single_cols |
|  | 普通表bind 多表多行多列的数据结构 | 插入bind 多表多行多列的数据 | 数据写入成功 | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt2_insert_common_table_with_bind_tablename_data |
|  | 普通表bind 多表单行多列的数据结构 | 插入bind 多表单行多列的数据结构 | 数据写入成功 | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt2_insert_common_table_with_bind_tablename_data |
|  | 普通表bind 多表多行单列的数据结构 | 插入bind 多表多行单列的数据结构 | 数据写入成功 | Y |  | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt2_insert_common_table_muti_table_muti_rows_single_cols |
|  | 超级表bind 单表多行多列的数据结构 | 插入bind 单表多行多列的数据 | 数据写入成功 | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt2_insert_super_table_simple_table_muti_rows_muti_cols |
|  | 超级表bind 单表单行多列的数据结构 | 插入bind 单表单行多列的数据结构 | 数据写入成功 | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt2_insert_super_table_simple_table_single_rows_muti_cols |
|  | 超级表bind 单表多行单列的数据结构 | 插入bind 单表多行单列的数据结构 | 数据写入成功 | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt2_insert_super_table_simple_table_muti_rows_single_cols |
|  | 超级表bind 多表多行多列的数据结构 | 插入bind 多表多行多列的数据 | 数据写入成功 | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt_insert_super_table_muti_table_muti_rows_muti_cols |
|  | 超级表bind 多表单行多列的数据结构 | 插入bind 多表单行多列的数据结构 | 数据写入成功 | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt_insert_super_table_muti_table_single_rows_muti_cols |
|  | 超级表bind 多表多行单列的数据结构 | 插入bind 多表多行单列的数据结构 | 数据写入成功 | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt2_insert_super_table_muti_table_muti_rows_single_cols |
|  | 支持 STMT 复用，一个 STMT 实例可以多次 PREPARE 使用，同一个语句可以支持多个超级表写入 | 待补充脚本对应路径 | 多个超级表数据写入成功 | Y |  | 待补充 |
|  | stmt2对象初始化传入错误的sql，该stmt2对象不可服用 | 1. stmt2对象初始化传入错误的sql，绑定数据
1. stmt2在执行prepare，输入正确的sql | prepare报错 | Y |  |  |
|  | 绑定的数据列数量不一致 | 绑定的数据列数量不一致 | 明确的报错信息 | Y | Pass |  |
|  | 绑定的标签列数量不一致 | 先prepare，在绑定数据 | 明确的报错信息:Tags number not matched | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt2_tag_number_not_match |
|  | 绑定的标签列数量不一致 | 先绑定数据，在prepare | 明确的报错信息:Tags number not matched | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt2_tag_number_not_match |
|  | Insert语句拼写错误 | Insert语句拼写错误 | 明确的报错信息 | Y |  |  |
|  | stmt2不支持在sql中指定列值为固定值 | stmt2不支持在sql中指定列值为固定值 | insert into ? using {stablename} tags (?,?) values(?,?,1,2) | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt2_not_support_normal_value_in_sql |
|  | bindv 相关参数异常值NONE测试 | 待补充 | 明确的报错信息 | Y |  |  |
| 写入绑定参数的数据类型校验 | TIMESTAMP | 1.边界值校验
null
0
1000000
214748364700000
214591679900000
214591680000000
2.正常值校验
1626861392589111
1626861392590111
1695645296185376
1704067201685436
1682942496546787
3.异常值校验
'hello'
3.14
-3.14
100000000000000000000000
-100000000000000000000000 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y | Fail | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_timestamp_type

[TD-32477](https://jira.taosdata.com:18080/browse/TD-32477) 数据溢出无报错，timestamp 范围不明确。 |
|  | INT | 1.边界值校验
null
-2147483648
2147483647
0
1
-1
2.正常值校验
12345
-12345
999999
-999999
2147483646
3.异常值校验
'hello'
3.14
-3.14
2147483648
2147483648000788
-2147483649
-214748364923 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y | Pass | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_int_type |
|  | INT UNSIGNED | 1.边界值校验
null
0
1
2
4294967295
2147483647
2.正常值校验
123456
999999
1000000
4294967290
4294967294
3.异常值校验
'hello'
3.14
-3.14
-1
4294967296
-4294967296 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y | Pass | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_int_unsigned_type |
|  | BIGINT | 1.边界值校验
null
1
0
-1
9223372036854775807
-9223372036854775808
2.正常值校验
12345
-12345
999999
-999999
2147483646
3.异常值校验
'hello'
3.14
-3.14
9223372036854775808
92233720368547758082
-9223372036854775809
-92233720368547758091 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y | Pass | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_bigint_type |
|  | BIGINT UNSIGNED | 1.边界值校验
null
0
1
9223372036854775807
18446744073709551615
2.正常值校验
123456789012345
999999999999999
1000000000000000
18446744073709551614
18446744073709551600
3.异常值校验
'hello'
3.14
-3.14
-1
18446744073709551616
184467440737095516169090 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y | Pass | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_bigint_unsigned_type |
|  | FLOAT | 1.边界值校验
null
1.175494351e-38
3.402823466e38
0.0
-1.0
3.402823466e38 - 1.0
2.正常值校验
123.456
-123.456
1.23456789
-1.23456789
3.402823466e38 - 0.1
100
3.异常值校验
'hello'
3.402823466e39
3.402823466e39 + 1.0
-3.402823466e39
-3.402823466e39 - 1.0 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y | Pass | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_float_type |
|  | DOUBLE | 1.边界值校验
null
2.2250738585072014e-308
1.7976931348623157e+308
0.0
-1.0
1.7976931348623157e+308 - 1.0
2.正常值校验
123456789012.3456789
-123456789012.3456789
1.2345678901234567
-1.2345678901234567
1.7976931348623157e+308 - 0.1
123456789
3.异常值校验
'hello'
1.7976931348623157e+309
1.7976931348623157e+309 + 1.0
-1.7976931348623157e+309
-1.7976931348623157e+309 - 1.0 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y | Pass | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_double_type |
|  | BINARY | 1.边界值校验
null
''
'@@@@@!!!!!$$$$$#####'
'abcdefghijklmnopqrst'
2.正常值校验
'hello world'
'1234567890abcdef'
' \x7F\x80\x81\xFE'
'!@#$%^&*()_+{}\|:"<>?'
'abc1234567890xyz'
'\x00\x01\x02\x03abc'
3.异常值校验
100
-100
3.14
-3.14
'\x00' * 20
'\xFF' * 20
'1234567890abcdefghijkl'
'\xe4\xb8\xad\xe6\x96\x87' | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y | Fail | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_binary_type

[TD-32386](https://jira.taosdata.com:18080/browse/TD-32386) 长度溢出无报错 |
|  | SMALLINT | 1.边界值校验
null
-32768
32767
-32767
32766
0
2.正常值校验
12345
-12345
30000
-30000
100
3.异常值校验
'hello'
3.14
-3.14
32768
-32769 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y | Pass | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_smallint_type |
|  | SMALLINT UNSIGNED | 1.边界值校验
null
0
1
65534
65535
2.正常值校验
12345
60000
500
65530
65534
3.异常值校验
'hello'
3.14
-3.14
-1
65536 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y | Pass | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_smallint_unsigned_type |
|  | TINYINT | 1.边界值校验
null
-128
127
-127
126
0
2.正常值校验
50
-50
100
-100
10
3.异常值校验
'hello'
3.14
-3.14
-129
128 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y | Pass | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_tinyint_type |
|  | TINYINT UNSIGNED | 1.边界值校验
null
0
1
255
2.正常值校验
5
50
200
250
254
3.异常值校验
'hello'
3.14
-3.14
-1
256 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y | Pass | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_tinyint_unsigned_type |
|  | BOOL | 1.边界值校验
null
2.正常值校验
True
False
0
1
-1
5
-5
3.14
-3.14
3.异常值校验
''
'hello' | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y | Pass | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_bool_type |
|  | NCHAR | 1.边界值校验
null
''
'a'
'a' * 20
'a' * 19
'中' * 20
2.正常值校验
'测试字符串'
'测试abc123'
'!@# 测试'
'a ' * 10
'testvalue'* 2
'\'\'a\'\''
3.异常值校验
5
-5
3.14
-3.14
'a' * 21
'中' * 21 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y | Fail | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_nchar_type

[TD-32361](https://jira.taosdata.com:18080/browse/TD-32361) tag 长度溢出无报错 |
|  | JSON | 1.正常值校验
'{"name": "test", "age": 30}'
'{"is_valid": true, "count": 100, "score": 98.76}'
null
''
'{}'
'\t'
'{"": ""}'
'{"name": null}'
'{"name": "test", "name": "abc"}'
2.异常值校验
'hello'
True
False
5
3.14
'{"name": "test"'
'[1, 2, 3]'
'[1, 2, 3'
"{'key': 'value'}"
r'{key: value}'
'[{"name": "test1"}, {"name": "test2"}]'
r'{"user": {"id": 1, "info": {"name": "test", "age": 30}}' | 1.正常值不报错
2.异常值报错 | Y | Pass | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_json_type |
|  | VARCHAR | 1.边界值校验
null
''
'@@@@@!!!!!$$$$$#####'
'abcdefghijklmnopqrst'
2.正常值校验
'hello world'
'1234567890abcdef'
' \x7F\x80\x81\xFE'
'!@#$%^&*()_+{}\|:"<>?'
'abc1234567890xyz'
'\x00\x01\x02\x03abc'
3.异常值校验
100
-100
3.14
-3.14
'\x00' * 20
'\xFF' * 20
'1234567890abcdefghijkl'
'\xe4\xb8\xad\xe6\x96\x87' | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y | Fail | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_varchar_type

[TD-32361](https://jira.taosdata.com:18080/browse/TD-32361) tag 长度溢出无报错 |
|  | GEOMETRY | 1.正常值校验
'POINT(1.0 1.0)'
'POINT(123.456 789.012)'
'LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)'
'POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))'
'LINESTRING(0 0, 100 100, 200 200, 300 300)'
null
'POINT(1.0 1.0)'
'POINT EMPTY'
'LINESTRING(1.0 1.0, 2.0 2.0)'
'LINESTRING EMPTY'
'POLYGON((1.0 1.0, 2.0 2.0, 1.0 1.0))'
2.异常值校验
'hello'
5
-5
3.14
-3.14
'POINT(1.0)'
'LINESTRING(1.0 1.0)'
'POLYGON((1.0 1.0, 2.0 2.0, 3.0 3.0))'
'POLYGON((0 0, 4 0, 4 4))'
'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2))' | 1.正常值不报错
2.异常值报错 | Y | Pass | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_geometry_type |
|  | VARBINARY | 1.正常值校验
b'abc'
b'\x01\x02\x03\x04'
b'\xFA\xFE\xFD\xFC\xFB'
b'\x00' * 20
b'\x0A\x0B\x0C'
b'\x01' + b'string'
null
b''
b'\x00'
b'\x01' * 20
b'\x01' * 19
2.异常值校验
'hello'
0
1
-1
1000
-1000
3.14
-3.14
b'\x01' * 21
b'\x01' * 30 | 1.正常值不报错
2.异常值报错 | Y | Pass | [test_stmt2_data_type.py](http://test_stmt2_data_type.py)
test_stmt_varbinary_type |
| 查询绑定参数的数据类型校验 | TIMESTAMP | 1.边界值校验
2.正常值校验
3.异常值校验 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y |  |  |
|  | INT | 1.边界值校验
2.正常值校验
3.异常值校验 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y |  |  |
|  | INT UNSIGNED | 1.边界值校验
2.正常值校验
3.异常值校验 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y |  |  |
|  | BIGINT | 1.边界值校验
2.正常值校验
3.异常值校验 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y |  |  |
|  | BIGINT UNSIGNED | 1.边界值校验
2.正常值校验
3.异常值校验 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y |  |  |
|  | FLOAT | 1.边界值校验
2.正常值校验
3.异常值校验 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y |  |  |
|  | DOUBLE | 1.边界值校验
2.正常值校验
3.异常值校验 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y |  |  |
|  | BINARY | 1.边界值校验
2.正常值校验
3.异常值校验 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y |  |  |
|  | SMALLINT | 1.边界值校验
2.正常值校验
3.异常值校验 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y |  |  |
|  | SMALLINT UNSIGNED | 1.边界值校验
2.正常值校验
3.异常值校验 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y |  |  |
|  | TINYINT | 1.边界值校验
2.正常值校验
3.异常值校验 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y |  |  |
|  | TINYINT UNSIGNED | 1.边界值校验
2.正常值校验
3.异常值校验 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y |  |  |
|  | BOOL | 1.边界值校验
2.正常值校验
3.异常值校验 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y |  |  |
|  | NCHAR | 1.边界值校验
2.正常值校验
3.异常值校验 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y |  |  |
|  | JSON | 1.正常值校验
2.异常值校验 | 1.正常值不报错
2.异常值报错 | Y |  |  |
|  | VARCHAR | 1.边界值校验
2.正常值校验
3.异常值校验 | 1.超出范围的值报错
2.正常值不报错
3.异常值报错 | Y |  |  |
|  | GEOMETRY | 1.正常值校验
2.异常值校验 | 1.正常值不报错
2.异常值报错 | Y |  |  |
|  | VARBINARY | 1.正常值校验
2.异常值校验 | 1.正常值不报错
2.异常值报错 | Y |  |  |
| stmt2数据查询 | 子表查询 | 待补充脚本对应路径 | 支持
select * from child_table where ts = ? and v = ? | Y | Pass | [test_stmt2_query.py](http://test_stmt2_query.py)
test_query_stable_and_subtable |
|  | 普通表查询 | 待补充脚本对应路径 | 支持
select * from common_table where ts = ? and v = ? | Y | Pass | CI有少量覆盖

[test_stmt2_query.py](http://test_stmt2_query.py)
test_query_common_table |
|  | 超级表查询 | 待补充脚本对应路径 | 支持
select * from supper_table where ts = ? and v = ? | Y | Pass | [test_stmt2_query.py](http://test_stmt2_query.py)
test_query_stable_and_subtable |
|  | 查询绑定的内容只能是 SQL 语句中的常量或常量表达式 | 待补充脚本对应路径 | 功能支持 | Y | Pass | [test_stmt2_query.py](http://test_stmt2_query.py)
test_query_constants_and_expressions |
| statement2方法
（taos_stmt2_init / taos_stmt2_prepare） | 验证参数sql功能 | 1.初始化指定sql
2.初始化不指定sql | 无论是否配置sql，初始化都成功 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_statement2_param_sql |
|  | 验证参数TaosStmt2Option功能 | 不指定reqid | reqid为默认值0 | Y | Fail | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_statement2_param_option

待确认
[TD-32481](https://jira.taosdata.com:18080/browse/TD-32481) reqid 没有默认值 |
|  | 初始化失败 | 写一个错误的sql | 不报错 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_statement2_param_sql |
|  | 测试内部方法get_impl的功能 | 调用方法get_impl | 返回TaosStmt2OptionImpl 类对象 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_internal_func_get_impl |
|  | 测试高效写入 | stbInterlaceMode=true
singleTableBindOnce=true | 初始化成功 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_internal_func_get_impl |
|  |  | stbInterlaceMode=true
singleTableBindOnce=false | 初始化成功 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_internal_func_get_impl |
|  |  | stbInterlaceMode=false
singleTableBindOnce=true | 初始化成功 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_internal_func_get_impl |
|  |  | stbInterlaceMode=false
singleTableBindOnce=false | 初始化成功 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_internal_func_get_impl |
| prepare方法 | sql参数配置正确格式的sql | sql参数配置正确格式的sql | 仅记录sql | Y |  | 没有接口获取 prepare 保存的 sql |
|  | sql参数配置错误格式的sql | sql参数配置错误格式的sql | bind_param 时会报错 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_prepare |
|  | sql参数配置为空 | sql参数配置为空 | 返回 TSDB_CODE_INVALID_PARA | Y | Fail | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_prepare

[TD-32482](https://jira.taosdata.com:18080/browse/TD-32482) sql 参数为空没有报错 |
| bind_param方法 | tbnames为None | 数据写入，不绑定tbname，配置tbnames为None | 数据写入成功 | Y | Fail | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param

[TD-32456](https://jira.taosdata.com:18080/browse/TD-32456)
taos.error.StatementError: [0xffff]: all bind params is None. |
|  |  | 数据写入，绑定tbname，配置tbnames为None | 写入报错 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param |
|  |  | 数据查询，不绑定tbname，配置tbnames为None | 数据查询成功 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param |
|  |  | 数据查询，绑定tbname，配置tbnames为None | 查询报错 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param |
|  | tags为None | 数据写入，不绑定tags，配置tags为None | 数据写入成功 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param |
|  |  | 数据写入，绑定tags，配置tags为None | 写入报错 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param |
|  |  | 数据查询，不绑定tags，配置tags为None | 数据查询成功 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param |
|  |  | 数据查询，绑定tags，配置tags为None | 查询报错 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param |
|  | datas为None | 数据写入，不绑定datas，配置datas为None | 报错 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param

报错信息：taos.error.StatementError: [0x0200]: stmt bind param does not support normal value in sql |
|  |  | 数据写入，绑定datas，配置datas为None | 写入报错 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param |
|  |  | 数据查询，不绑定datas，配置datas为None | 数据查询成功 | Y | Fail | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param

待确认
taos.error.StatementError: [0x022a]: Stmt API usage error |
|  |  | 数据查询，绑定datas，配置datas为None | 查询报错 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param |
|  | tbnames, tags为None | 数据写入，不绑定tbnames和tags，配置为None | 数据写入成功 | Y | Fail | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param

[TD-32456](https://jira.taosdata.com:18080/browse/TD-32456)
taos.error.StatementError: [0xffff]: all bind params is None. |
|  |  | 数据查询，不绑定tbnames和tags，配置为None | 数据查询成功 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param |
|  | tbnames, tags, datas都为None | tbnames, tags, datas都为None | 直接报错 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param |
|  | prepare 和bind_param顺序 | 调用 prepare 接口在绑定数据 | 返回0 | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param |
|  |  | 未调用 prepare 接口就直接绑定 | 返回 StatementError: [0xffff]: stmt2 init failed taos.error.StatementError: [0x022a]: Stmt API usage error | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param |
| execute方法 | 未prepare，执行execute | 未prepare，执行execute | 返回 TSDB_CODE_TSC_STMT_API_ERROR | Y | Pass | [test_stmt2_func.py](http://test_stmt2_func.py)
test_func_bind_param |
|  | 正确流程执行execute | 按照正常流程执行execute | 1. 数据写入和查询成功
1. 返回值是受影响的行数 | Y | Pass | 在验证查询和写入的case中验证，无须再次验证 |
|  | 未绑定参数 | 未绑定参数，执行execute | 返回 TSDB_CODE_TSC_STMT_API_ERROR | Y |  | python连接器做了数据验证，无法验证c接口 |
| result方法 | 写入执行result方法 | 写入执行result方法 | 报错 | Y |  |  |
|  | 查询执行result方法 | 查询执行result方法 | 返回正确的查询结果 | Y |  |  |
| affected_rows属性 | 写入操作，调用affected_rows属性 | 写入操作，调用affected_rows属性 | 返回受影响的行数 | Y |  |  |
|  | 查询操作，调用affected_rows属性 | 查询操作，调用affected_rows属性 | 返回受影响的行数 | Y |  |  |
| close方法 | 执行close方法后statement2实例不可用 | 执行close方法 | statement2实例不可用 | Y |  |  |
| is_insert方法 | 写入操作，执行is_insert方法 | 写入操作，执行is_insert方法 | 返回True | Y |  |  |
|  | 查询操作，执行is_insert方法 | 查询操作，执行is_insert方法 | 返回False | Y |  |  |
| get_fields方法 | 写入操作，执行get_fields方法 | 写入操作，执行get_fields方法 | 返回当前待绑定参数的元数据信息 | Y |  |  |
|  | 查询操作，执行get_fields方法 | 查询操作，执行get_fields方法 | 返回当前待绑定参数的元数据信息 | Y |  |  |
|  | 超级表：prepare后可以调用get_fields方法 | prepare后可以调用get_fields方法 | get_fields方法返回对应的对象数据 | Y |  |  |
|  | 普通表：prepare后可以调用get_fields方法 | prepare后可以调用get_fields方法 | get_fields方法返回对应的对象数据 | Y |  |  |
|  | field_type为未知类型 | 配置field_type为未知类型 | 返回 StatementError("invalid field_type value: %d." % field_type) | Y |  |  |
| error | 当执行返回值不为0时，执行error方法 | 当执行返回值不为0时，执行error方法 | 返回最后一次错误的报错信息 | Y |  |  |
| 新增stmt2功能 | 支持异步调用 | 待补充脚本对应路径 | 功能支持 | Y |  |  |
| 问题回归验证 | [stmt bind 接口要求变长数据按定长分配空间，是否可以优化](https://jira.taosdata.com:18080/browse/TD-30355) | 待补充脚本对应路径 | 问题验证通过 | Y |  |  |
|  | [STMT 写入接口支持区分 NULL/None](https://jira.taosdata.com:18080/browse/TD-31428) | 待补充脚本对应路径 | 问题验证通过 | Y | Pass | [test_stmt2_insert.py](http://test_stmt2_insert.py):
test_stmt_td31647 |
|  | [stmt taos_stmt2_get_fields 支持获取子表名信息](https://jira.taosdata.com:18080/browse/TD-31647) | 待补充脚本对应路径 | 问题验证通过 | Y |  | 等待python支持该工能验证 |

### 9.2 可靠性

无

### 9.3 性能

#### 9.3.1 测试模型

| 模型名称 | 子表数 | 子表行数 | 步长 | 数据列数 | 标签数 | 优先级 | cachemodel | thread_count | num_of_records_per_req | thread_bind_vgroup | vgroups | replica | stt_trigger | cachesize(M) | wal_level | buffer(M) | duration | numOfCommitThreads | compressMsgSize |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 电表模型 | 100W | 288 | 1s | 3 | 2 | 0 | none | 32 | 1000 | yes | 32 | 1 | 2 | 100 | 1 | 256 | 14400m | 4 | -1 |

#### 9.3.2 测试范围

| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 性能测试 | 与上个发布版本对比，写入和查询性能无下降 | 通过taosBenchmark基于3.2.9.0和3.3.3.0版本进行同步数据写入性能对比 | 性能无下降 | Y |  |  |
|  |  | 通过taosBenchmark基于3.2.9.0和3.3.3.0版本进行异步数据查询性能对比 | 性能无下降 | Y |  |  |
|  | [设计实现 stmt 新接口，以提升 connector 对参数绑定写入的高效支持](https://jira.taosdata.com:18080/browse/TD-30813) | 跟stmt性能优化前的版本进行性能对比测试 | 性能有大幅提升 | Y |  | 之前stmt做过一次写入性能优化，因没有测试接入，目前还不清楚以哪个版本为基础进行性能对比测试 |
|  | stmt2 vs taosc写入性能对比 | 3.3.3.0的 stmt2 vs taosc | stmt2写入性能远优于taosc写入性能 | Y |  |  |
|  | stmt2 vs taosc查询性能对比 | 3.3.3.0的 stmt2 vs taosc | stmt2写入性能远优于taosc写入性能 | Y |  |  |


### 9.4 兼容性 

| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| stmt接口回归测试 | 验证stmt旧接口功能 | CI - test_stmt_set_tbname_tag.py | CI脚本测试通过 | Y |  |  |
|  |  | CI - [test_stmt_muti_insert_query.py](http://test_stmt_muti_insert_query.py) | CI脚本测试通过 | Y |  |  |
|  |  | CI - [stmt_error.py](http://stmt_error.py) | CI脚本测试通过 | Y |  |  |

## 10. 已知问题

1. 旧接口的退役规划
2. 未绑定表名时在调用bind_params或BindTable方法时还需要指定表名，不太合理，会在stmt2的下阶段优化该问题
3. prepare无法对sql进行校验，导致get_fields不可用
4. 指定表名但还绑定tag值，这种使用场景不太合理，需要如何处理

## 11. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: stmt2

## 12. 测试计划 

| 时间 | 关键事项 | 状态 | 备注 |
| --- | --- | --- | --- |
| 2024.8.23 | FS定稿 | 已完成 | [stmt2 功能规格](https://taosdata.feishu.cn/wiki/OHn7wgE38i6LiCkKFQec3wyKnOf) |
| 2024.9.6 | 开发stmt2的C接口 | 已完成 |  |
| 2024.9.10 | python连接器stmt2设计评审 | 已完成 | [Python 连接器 Native 支持 stmt2](https://taosdata.feishu.cn/wiki/HHcdwlfTpimKIukGBMWcOS2hnRg) |
| 2024.9.10-14 | python连接器适配stmt2 | 已完成 | 因为C接口有bug，延期提测，19日提测 |
| 2024.9.14-18 | 编写测试脚本 | 进行中 | 预留1天半编写脚本时间 |
| 2024.9.19 | 再次确认提测时间 | 已完成 | 1. 9.19日下班前python连接器提测（不包括异步接口） 1. taosBenchmark最晚24日提测 |
| 2024.9.19 | 推迟taosBenchmark提测时间 | 已完成 | ，因高优先级任务，推迟taosBenchmark的提测时间到27日 TD-31019 【2024.9.27】taosBenchmark提测时间推迟到10月10号 |
| 2024.9.23-10.10 | 功能&兼容性测试 | 未开始 | 【2024.9.20】python连接器 9月20日提测 【2024.9.27】因为高优先级基线性能测试，调整测试时间到10月10日完成 |
| 2024.10.11-10.18 | 性能对比测试 | 未开始 | 基于taosBenchmark支持stmt2的完成时间 |
| 2024.10.18 | 出测试报告 | 未开始 | 评估发版 |


## 13. 风险评估

8月初：目前看提测时间有风险，c的接口调整计划于2024.8.19号完成，后续python connector还要做接口适配工作，大概率python connector提测会在9月初。按计划9月第二周发版，在扣除测试通过后代码合并和回归测试等工作时间，预留给测试的时间可能会很紧张
9.14：C的接口已于9月6日提测，当前在开发python连接器，计划于9月14日提测，之后在开发taosBenchmark对stmt2接口的支持，预计3-5天，按照时间推算，功能测试最多留有3到5天时间，性能测试最早在9月18日启动，两项测试工作的是时间非常有限，若出现大问题，可能会影响到功能发版

## 14. 参考文档 

### 14.1 需求对应JIRA

TD-30813


TD-31949

### 14.2 设计文档

[stmt2 功能规格](https://taosdata.feishu.cn/wiki/OHn7wgE38i6LiCkKFQec3wyKnOf)
[参数绑定模块设计总结](https://taosdata.feishu.cn/docx/ULoZdtUsZokmryxoRT2cYHOynye)
[Python 连接器 Native 支持 stmt2](https://taosdata.feishu.cn/wiki/HHcdwlfTpimKIukGBMWcOS2hnRg)
