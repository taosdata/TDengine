# VARBINARY 数据类型

VARBINARY 是一种存储二进制数据的数据类型，类似postgre sql 的 bytea，VARBINARY类型的长度是可变的，在创建表时指定了最大的字节长度，其长度可以在0到最大长度之间，在这个最大值范围内使用多少就分配多少。
1. 长度限制。
  VARBINARY 数据列最大长度为65517个字节（约等于64KB），VARBINARY 标签列最大长度为16382个字节（约等于16KB）因为需要满足每行数据总长度不超过64KB，标签总长度不超过16KB的约束。
1. 建表 ( column和tag 都可以是varbinary类型)。
  
  ```sql
  create stable stb (ts timestamp, c1 nchar(32), c2 varbinary(16), c3 float) tags (t1 int, t2 binary(8), t3 varbinary(8))
  ```

说明：c2可以存储数据长度为在[0,16]个字节之间的二进制数据。
1. 插入数据。
   - 写入16进制表示的二进制数据，以 \x 开头，必须是16进制合法的字符，必须偶数个字符。
    ```sql
    insert into tb3 using stb tags (3, 'tb3_bin1', '\x7f8290') values (now + 2s, 'nchar1', '\x7f8290', 0.3)
    ```

   - 写入字符串，数据库里将存储字符串相应编码的二进制内容。
    ```sql
    insert into tb1 using stb tags (1, 'tb1_bin1', 'vart1') values (now, 'nchar1', 'varc1', 0.3)
    ```

   - 其他写入类型报错
  ```sql
  taos> insert into tb2 using stb tags (2, 'tb2_bin1', 093) values (now + 2s, 'nchar1', 892, 0.3);
  DB error: Invalid varbinary value: t3 (0.001733s)
  ```

  
1. 查询 
   - 查询结果以16进制 \x开头显示
  ```sql
  taos> select * from stb;
             ts            |               c1               |               c2               |          c3          |     t1      |     t2     |          t3          |
  =====================================================================================================================================================================
   2023-09-01 15:11:22.854 | nchar1                         | \x7F8290                       |            0.3000000 |           3 | tb3_bin1   | \x7F8290             |
   2023-09-01 15:11:23.856 | nchar1                         | \x7F829000                     |            0.3000000 |           3 | tb3_bin1   | \x7F8290             |
   2023-09-01 15:11:20.849 | nchar1                         | \x7661726331                   |            0.3000000 |           1 | tb1_bin1   | \x7661727431         |
   2023-09-01 15:11:21.852 | nchar2                         | NULL                           |            0.4000000 |           1 | tb1_bin1   | \x7661727431         |
   2023-09-01 15:11:24.858 | nchar1                         | \x                             |            0.3000000 |           2 | tb2_bin1   | \x                   |
   2023-09-01 15:11:25.860 | nchar1                         | \x00000000                     |            0.3000000 |           2 | tb2_bin1   | \x                   |
  Query OK, 6 row(s) in set (0.028570s)
  ```

1. VARBINARY  操作符和谓词支持
   - 不支持的操作符和谓词：
      - 算术运算符，位运算符，[NOT] LIKE/MATCH/NMATCH/->/CONTAINS
   - 支持的操作符和谓词：
      - 比较运算符（按字节从左到右比较，相同时更长的大）
      - IS [NOT] NULL/[NOT] BETWEEN AND/[NOT] IN。
2. VARBINARY 类型仅支持如下函数操作
   - first/last/last_row/count/hyperloglog/sample/tail/mode/cast。
   - cast函数不支持 VARBINARY 转成其他类型；其他类型只有 varchar 可以转换为VARBINARY（和写入一致）。
3. VARBINARY 类型支持 schemaless 方式写入。
   - 同sql写入一样，可以写入16进制表示的二进制数据，以 \x 开头，或者写入字符串
   - VARBINARY类型需以 b/B 开头。 
  ```sql
  vbin,t1=1 f1=283i32,f2=b"hello" 1632299372000
  vbin,t1=1 f2=B"\x98f46e",f1=106i32 1632299373000
  ```

1. VARBINARY 类型支持 stmt 方式写入
   - 具体使用方法可参考 https://github.com/taosdata/TDengine/blob/main/utils/test/c/varbinary_test.c
2. 链接器支持 @Adam Ji @霍立波 @Peng Sun @任新胜 @谭雪峰
   - VARBINARY的存储格式和binary类型一样，前两个字节是长度，后面是内容，只不过内容里是二进制类型，不一定是可打印字符。
   - TDenging里binary类型实际是varchar，是用来存储可打印字符的，和标准的binary类型不一样，由于历史原因保留而已，所以实际不建议用再用binary类型，用varchar代替。
