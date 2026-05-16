# [Test Report] TD-18769 New Data Type: VARBINARY (<=64KB)

### 1. 概述：

VARBINARY 是一种存储二进制数据的数据类型，类似postgre sql 的 bytea，VARBINARY类型的长度是可变的，在创建表时指定了最大的字节长度，其长度可以在0到最大长度之间，在这个最大值范围内使用多少就分配多少。

### 2. 测试环境：

102.168.1.35：

### 3. 测试用例：

| 用例名 | 测试步骤 | 期望结果 |
| --- | --- | --- |
| varbinary_insert | 1. 创建数据库，“create database varbinary_db;”, "use varbinary_db;" 1. 创建普通表，“create table t1(ts timestamp, c1 varbinary(64));” 1. 写入数据，“insert into t1 values(now, 'taosdata') （now+1s, '涛思数据'） （now+2s, null） (now+3s, '');” 1. 创建超级表，“create table st1(ts timestamp, c1 varbinary(128), c2 int) tags(groupid int, marks varbinary(64));” 1. 创建子表，‘create table ct1 using st1 tags(1, 'aa');', "create table ct2 using st1 tags(2, 'bb');" 1. 向子表写入数据，“insert into ct1 values(now, 'taosdata', 1) （now+1s, '涛思数据', 2） （now+2s, null, 3） (now+3s, '', 4) (now+4s, '~!@#$%^&*()_+?', 5);”， ‘insert into ct2 values(now, 'abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij12345678', 2)’; 1. 向子表写入数据超过最大长度， ‘insert into ct2 values(now,'abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij123456789', 2)’; | 1. 创建数据库正常 1. 创建表正常 1. 普通表写入数据正常 1. 创建超级表正常 1. 创建子表正常 1. 向子表写入数据正常 1. 写入数据失败，提示错误‘DB error: Value too long for column/tag: c1’ |
| varbinary_insert_boundary | 1. 创建普通表，使varbinary的长度为最大长度65517， “create table t2(ts timestamp, c1 varbinary(65517));” 1. 执行“desc t2”验证c1列长度为65517 1. 向t2写入最大长度数据 1. 创建超级表，使varbinary类型的tag为最大长度16382，“create table st2(ts timestamp, c1 int) tags(marks varbinary(16382));” 1. 执行“desc st2”验证tag列长度为16382 1. 创建st2子表，写入最大长度tag | 1. 创建表正常 1. c1列长度为65517 1. 写入最大长度数据正常 1. 创建超级表正常 1. tag列marks为最大长度16382 1. 写入最大长度tag正常 |
| varbinary_insert_schemaless | 1. schemaless写入如下数据： lines = [ '''st3,t1=b'taosdata' c1=2,c2=false,c3=b"\x98\xf4\x6e" 1685586804002''', '''st3,t1=b'taosdata' c1=2,c2=false,c3=b"test" 1685586804003''', '''st3,t1=b'taosdata' c1=2,c2=false,c3=b"涛思数据" 1685586804004''', '''st3,t1=b'taosdata' c1=2,c2=true 1685586804005''' ] 1. 执行“desc st3;”查询t1， c3的类型 | 1. schemaless写入数据正常 1. t1, c3的类型均为’VARBINARY‘(目前t1类型为NCHAR) |
| varbinary_udpate | 1. 修改普通表t1， c1的列长度，“alter table t1 modify column c1 varbinary(128);”, desc t1;查询修改后的列长度 1. 修改超级表st1， c1的列长度，“alter table st1 modify column c1 varbinary(256);” 1. 修改超级表st1， 标签marks的列长度，"alter table st1 modify tag marks varbinary(128);" 1. 执行“desc st1;”查询超级表c1列， marks标签的长度 1. 修改超级表st1, 改小c1列长度，“alter table st1 modify column c1 varbinary(128);” | 1. 修改varbinary列长度正常，修改后长度为128 1. 修改超级表列长度正常 1. 修改超级表标签长度正常 1. 修改后的超级表列长度为256， 标签列长度为128 1. 提示错误“DB error: Only binary/nchar/geometry column length could be modified, and the length can only be increased, not decreased”([TD-26039](https://jira.taosdata.com:18080/browse/TD-26039)) |
| varbinary_query_common | 1. 创建数据库、普通表、超级表及子表， 普通表及超级表均包含varbinary类型的列，超级表同时包含varbinary类型的tag列 1. 写入数据到普通表及子表，varbinary列包含null，重复值，中英文字符，数字等 1. 执行单列、多列查询 1. 执行条件查询，条件包括比较符，between..and..，is [not] null, in, order by, group by 1. 对varbinary列进行四则运算或位运算查询 1. 执行条件查询，条件包括【not】like，contains， match,nmatch | 1. 创建数据库，超级表及子表，普通表正常 1. 写入数据正常 1. 执行查询及查询结果正常 1. 执行查询及查询结果正常 1. 执行失败，提示错误“DB error: Invalid value type:” 1. 执行失败，提示错误“DB error: Invalid value type:” |
| varbinary_query_function | 1. 执行sql查询，包含如下函数：first/last/last_row/count/hyperloglog/sample/tail/mode/cast 1. 执行sql查询，包含max，min， abs, ltrim | 1. 执行正常 1. 执行失败，提示错误“DB error: Invalid value type:” |
| varbinary_query_join | 1. 执行sql“select * from st as st1 join st as st2 on st1.ts = st2.ts and st1.c2 <= st2.c2;” 1. 执行sql“select count(st1.c2) from st as st1 join st as st2 on st1.ts = st2.ts;” 1. 执行sql“select sum(st1.c2) from st as st1 join st as st2 on st1.ts = st2.ts;” | 1. 执行正常，结果显示正确 1. 执行正常，结果显示正确 1. 执行失败，提示错误“DB error: Invalid value type:” |
| varbinary_query_show_cmd | 1. 执行“show create table t;” 1. 执行“show create stable st；” | 1. 执行正常，无coredump，varbinary类型显示正常 1. 执行正常，无coredump，varbinary类型显示正常 |

### 4. 总结：

1. 新增varbinary类型写入普通表列、超级表列及标签正常，schemaless写入正常
2. 对普通列及标签列的边界值与预期相符
3. 对超出列宽度的写入有报错，提示信息中暂时未加入varbinary类型([TD-26039](https://jira.taosdata.com:18080/browse/TD-26039))
4. varbinary类型列、标签列支持udpate
5. 单列、多列查询，条件过滤查询正常；对不支持的操作符均提示错误
6. 支持的函数查询正常，first/last/last_row/count/hyperloglog/sample/tail/mode/cast，不支持的函数查询均有提示错误
7. join查询正常
8. show命令查询中，varbinary类型显示正常
