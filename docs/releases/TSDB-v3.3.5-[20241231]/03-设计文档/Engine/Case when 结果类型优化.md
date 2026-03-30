# Case when 结果类型优化

## 1. 背景

目前case when的结果类型是按照第一个then之后的类型来确定，同时允许then和else的类型不一致，后续then和else需要转换为第一个then之后的类型，带来的问题是当第一个then的类型长度较小时，其他then/else的值会被截断处理，造成用户无法理解输出结果，也可能出现两种类型不能转换的情况。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/10/16 | 0.1 | 李友豪 | Case when语句最终展示的结果类型 |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |

## 3. 定义

如下矩阵的行和列的标签表示的是需要显示的两种数据类型，共有20种不同的数据类型，行和列对应的表格中的数据(只需要看矩阵的上三角部分)表示的是上述需要显示的两种类型最终展示的结果类型。
|  | 0:NULL | 1:BOOL | 2:TINY | 3:SMAL | 4:INT | 5:BIGI | 6:FLOA | 7:DOUB | 8:VARC | 9:TIME | 10:NCHA | 11:UTINY | 12:USMA | 13:UINT | 14:UBIG | 15:JSON | 16:VARB | 17:DECI | 18:BLOB | 19:MEDB | 20:GEOM |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0:NULL | NULL | BOOL | TINY | SMAL | INT | BIGI | FLOA | DOUB | VARC | TIME | NCHA | UTINY | USMA | UINT | UBIG | JSON | VARB | -1 | -1 | -1 | GEOM |
| 1:BOOL | 0 | BOOL | TINY | SMAL | INT | BIGI | FLOA | DOUB | VARC | BIGI | NCHA | UTINY | USMA | UINT | UBIG | -1 | -1 | -1 | -1 | -1 | -1 |
| 2:TINY | 0 | 0 | TINY | SMAL | INT | BIGI | VARC | VARC | VARC | BIGI | NCHA | SMAL | INT | BIGI | VARC | -1 | -1 | -1 | -1 | -1 | -1 |
| 3:SMAL | 0 | 0 | 0 | SMAL | INT | BIGI | VARC | VARC | VARC | BIGI | NCHA | SMAL | INT | BIGI | VARC | -1 | -1 | -1 | -1 | -1 | -1 |
| 4:INT | 0 | 0 | 0 | 0 | INT | BIGI | VARC | VARC | VARC | BIGI | NCHA | INT | INT | BIGI | VARC | -1 | -1 | -1 | -1 | -1 | -1 |
| 5:BIGI | 0 | 0 | 0 | 0 | 0 | BIGI | VARC | VARC | VARC | BIGI | NCHA | BIGI | BIGI | BIGI | VARC | -1 | -1 | -1 | -1 | -1 | -1 |
| 6:FLOA | 0 | 0 | 0 | 0 | 0 | 0 | FLOA | DOUB | VARC | VARC | NCHA | VARC | VARC | VARC | VARC | -1 | -1 | -1 | -1 | -1 | -1 |
| 7:DOUB | 0 | 0 | 0 | 0 | 0 | 0 | 0 | DOUB | VARC | VARC | NCHA | VARC | VARC | VARC | VARC | -1 | -1 | -1 | -1 | -1 | -1 |
| 8:VARC | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | VARC | VARC | NCHA | VARC | VARC | VARC | VARC | -1 | VARB | -1 | -1 | -1 | -1 |
| 9:TIME | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | TIME | NCHA | BIGI | BIGI | BIGI | VARC | -1 | -1 | -1 | -1 | -1 | -1 |
| 10:NCHA | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | NCHA | NCHA | NCHA | NCHA | NCHA | -1 | -1 | -1 | -1 | -1 | -1 |
| 11:UTINY | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | UTINY | USMA | UINT | UBIG | -1 | -1 | -1 | -1 | -1 | -1 |
| 12:USMA | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | USMA | UINT | UBIG | -1 | -1 | -1 | -1 | -1 | -1 |
| 13:UINT | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | UINT | UBIG | -1 | -1 | -1 | -1 | -1 | -1 |
| 14:UBIG | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | UBIG | -1 | -1 | -1 | -1 | -1 | -1 |
| 15:JSON | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | JSON | -1 | -1 | -1 | -1 | -1 |
| 16:VARB | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | VARB | -1 | -1 | -1 | -1 |
| 17:DECI | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | -1 | -1 | -1 | -1 |
| 18:BLOB | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | -1 | -1 | -1 |
| 19:MEDB | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | -1 | -1 |
| 20:GEOM | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | GEOM |

## 4. 行为说明

```sql

create stable test_stable_json (ts TIMESTAMP,c_int INT,c_uint INT UNSIGNED, c_bigint BIGINT, c_ubigint BIGINT UNSIGNED, c_float FLOAT, c_double DOU
BLE, c_binary BINARY(20), c_smallint SMALLINT, c_usmallint SMALLINT UNSIGNED, c_tinyint TINYINT,c_utinyint TINYINT UNSIGNED,c_bool BOOL,c_nchar NCHAR(20)
, c_varchar VARCHAR(20), c_varbinary VARBINARY(20), c_geometry GEOMETRY(50)) tags(tag_id JSON);

create table t_test using test_stable_json tags('{\"tag1\":5}');

insert into t_test values ('2022-09-30 15:15:01',123,456,1234567890,9876543210,123.45,678.90,'binary_val',32767,65535,127,255,true,'涛思数据',
'varchar_val', '1101', 'point(10 10)');
```

- 对于两种类型可以显示为其中的一种的情况（如int和bigint显示的最终结果类型为int，int和double显示的结果类型为double）不做过多说明，下文只列出两种类型不能显示为其中的一种的情况。
- TSDB_DATA_TYPE_NULL : 和其大于TSDB_DATA_TYPE_NULL（0）类型的显示类型为其它类型
  ```sql
  select case when 1 then 'abc' end from t_test;  
  // 此时只有一个then，结果类型即为该then的类型varchar
  ```

- TSDB_DATA_TYPE_BOOL ： 和其余大于TSDB_DATA_TYPE_BOOL（1）的类型t，最终显示类型为类型t（和JSON例外，显示为VARCHAR）
  ```sql
  select case when 0 then c_bool else c_int end from t_test;//结果类型为int，
  ```

- TSDB_DATA_TYPE_TINYINT：和其它大于TSDB_DATA_TYPE_TINYINT（2）的类型t，最终显示为类型t；其它的例外情况如下
  ```sql
  select case when 0 then c_tinyint else ts end from t_test; //结果类型为BIGINT
  select case when 0 then c_tinyint else c_utinyint end from t_test; //结果类型为smallint
  select case when 0 then c_tinyint else c_usmallint end from t_test; //结果类型为int
  select case when 0 then c_tinyint else c_uint end from t_test; //结果类型为BIGINT
  select case when 0 then c_tinyint else c_ubigint end from t_test;//结果类型为varchar
  ```

- TSDB_DATA_TYPE_SMALLINT : 和其它大于TSDB_DATA_TYPE_SMALLINT（3）的类型t，显示结果为类型t；其它的例外情况如下：
  ```cpp
  select case when 0 then c_smallint else ts end from t_test; //结果类型为BIGINT
  select case when 0 then c_smallint else c_utinyint end from t_test; //结果类型为SAMLLINT
  select case when 0 then c_smallint else c_usmallint end from t_test;//结果类型为INT
  select case when 0 then c_smallint else c_uint end from t_test;//结果类型为BIGINT
  select case when 0 then c_smallint else c_ubigint end from t_test;//结果类型为VARCHAR
  ```

- TSDB_DATA_TYPE_INT： 和其它大于TSDB_DATA_TYPE_INT（4）的类型t，显示结果类型为t；其它例外情况如下：
  ```sql
  select case when 0 then c_int else ts end from t_test; //结果类型为BIGINT
  select case when 0 then c_int else c_utinyint end from t_test; //结果类型为INT
  select case when 0 then c_int else c_usmallint end from t_test; //结果类型为INT
  select case when 0 then c_int else c_uint end from t_test; //结果类型为BIGINT
  select case when 0 then c_int else c_ubigint end from t_test; //结果类型为VARCHAR
  ```

- TSDB_DATA_TYPE_BIGINT : 
  ```sql
  select case when 0 then c_bigint else c_ubigint end from t_test; //结果类型为VARCHAR
  select case when 0 then c_bigint else tag_id end from t_test; //结果类型为VARCHAR
  ```

- TSDB_DATA_TYPE_FLOAT:
  ```sql
  select case when 0 then c_float else ts end from t_test; //结果类型为VARCHAR
  select case when 0 then c_float else c_ubigint end from t_test; //结果类型为VARCHAR
  select case when 0 then c_float else tag_id end from t_test; //结果类型为VARCHAR
  ```

- TSDB_DATA_TYPE_DOUBLE:
  ```sql
  select case when 0 then c_double else ts end from t_test; //结果类型为VARCHAR
  select case when 0 then c_double else tag_id end from t_test; //结果类型为VARCHAR
  select case when 0 then c_double else c_ubigint end from t_test; //结果类型为VARCHAR
  ```

- TSDB_DATA_TYPE_VARCHAR: 与其它类型一块均显示为VARCHAR，和NCHAR一块则显示为NCHAR
  ```sql
  select case when 0 then c_char else c_nchar end from t_test; //结果类型为NCHAR
  ```

- TSDB_DATA_TYPE_TIMESTAMP:
  ```sql
  select case when 0 then ts else c_nchar end from t_test; //结果类型为NCHAR
  select case when 0 then ts else c_utinyint end from t_test; //结果类型为BIGINT
  select case when 0 then ts else c_usmallint end from t_test; //结果类型为BIGINT
  select case when 0 then ts else c_uint end from t_test; //结果类型为BIGINT
  select case when 0 then ts else c_ubigint end from t_test; //结果类型为VARCHAR
  select case when 0 then ts else tag_id end from t_test; //结果类型为VARCHAR
  ```

- TSDB_DATA_TYPE_NCHAR ： 和其它类型的显示结果均为NCHAR
- TSDB_DATA_TYPE_UTINYINT：
  ```sql
  select case when 0 then c_utinyint else tag_id end from t_test; //结果类型为VARCHAR
  ```

- TSDB_DATA_TYPE_USMALLINT :
  ```sql
  select case when 0 then c_usmallint else tag_id end from t_test; //结果类型为VARCHAR
  ```

- TSDB_DATA_TYPE_UINT:
  ```sql
  select case when 0 then c_uint else tagid end from t_test; //结果类型为VARCHAR
  ```

- TSDB_DATA_TYPE_UBIGINT : 
- TSDB_DATA_TYPE_JSON：由于目前cast函数对于json还不支持，所以不能和其它类型选择最终的结果类型。
- TSDB_DATA_TYPE_VARBINARY : 由于该类型和其它类型无法进行cast，所以不能和其它类型选择最终的结果类型。
- TSDB_DATA_TYPE_GEOMETRY :  由于目前cast函数对于geometry还不支持，所以不能和其它类型选择最终的结果类型。

## 5. 性能

无

## 6. 兼容性

使用case when语句时，taosc显示的结果有变化；

## 7. 运维

无

## 8. 使用场景

使用到case when的所有语句

## 9. 约束和限制

无

## 10. 常见错误和排查

- 由于VARBINARY无法类型强转为其它类型，所以VARBINARY只能和VARBINARY做显示兼容，和其它类型则返回类型强转失败的错误码：TSDB_CODE_SCALAR_CONVERT_ERROR
- 如果输入类型为TDengine暂时还不支持的类型或者非法的类型码（<0)，则返回错误码：TSDB_CODE_INVALID_PARA

## 11. 可观测性

所有可以执行case when 语句的地方，如在case_when.py中，

## 12. 安装和卸载

无

## 13. 文档

不需要

## 14. 参考文档

无

## 15. 附录

无
