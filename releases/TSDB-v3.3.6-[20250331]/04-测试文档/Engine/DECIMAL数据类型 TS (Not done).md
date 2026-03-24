# DECIMAL数据类型 TS (Not done)

## 1. 测试目标

- DECIMAL 类型支持写入、读取、更新和删除
- DECIMAL 类型支持数值类型的函数
- DECIMAL 类型支持订阅
- DECIMAL 类型支持UDF
- DECIMAL 类型支持流计算
- taosBenchmark 支持 DECIMAL 类型定义
- 数据写入、查询性能，性能衰减应在 50% 以内

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.510 | 1.0.0 | 翟坤 | 未完成，第二轮的设计评审还未开始 |
|  |  |  |  |
|  |  |  |  |

## 3. 测试结论

## 4. 开发质量报告

结论：本特性/优化的开发质量是（优，良，一般，差，很差）

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 （测试阻塞，无法进行） | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 0 |
| 严重 Bug 总数 | 0 |

## 5. 已知问题和限制

这里用于记录产品使用上的一些限制，包括不支持的场景等，以及在发版时没有解决的 minor issues.
- 暂无
- 

## 6. 测试资源及环境

   测试平台：Linux x64
   测试资源：192.168.0.215

## 7. 测试范围及重点

## 8. 测试用例

### 8.1 功能测试用例

测试用例中所有涉及到decimal数据类型数据内容的测试点，都要覆盖字符串和数值两种数据方式，并且两种方式的行为应该完全一致

#### 8.1.1 基本语法测试用例

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | DECIMAL大小写校验 | Decimal、decimal、DeCimal |  |  |  |
| 2 | DECIMAL拼写错误校验 | Decimall、deci mal、decimla |  |  |  |
| 3 | 缺少括号 | decimal（4,2 和decimal 4,2 ） |  |  |  |
| 4 | 多余括号 | decimal（4,2））、decimal（（4,2）） |  |  |  |
| 5 | 缺少PRECISION | decimal（,2） |  |  |  |
| 6 | 正确格式 | Decimal decimal（4.2） decimal（4） |  |  |  |
| 7 | 0<MAX_PRECISION<=38 | 1. 不报错：decimal（38）、decimal（38，0）、decimal（38，4）、decimal（8，4） 1. 报错：decimal（39）、decimal（39,5）、decimal（0）、decimal（0，5） |  |  | 具体的边界值需要最后确定 |
| 8 | 0<=MAX_SCALE<=20 | decimal（38，20）、decimal（20，20） |  |  | 具体的边界值需要最后确定 |
| 9 | 输入字符类型，报错 | '123b'、'12'、'12.1'、'ab'、'True'、‘鬼眼狂刀’、'None'、'null'、'now'、' '、"1*10" |  |  |  |
| 10 | 输入浮点类型，报错 | 12.3、0.1、-1E-100 |  |  |  |
| 11 | 输入bool类型，报错 | true、True、TRUE、false |  |  |  |
| 12 | 输入GEOMETRY类型，报错 | 'PONIT(1 1 )'、'LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)' |  |  |  |
| 13 | 输入二进制类型，报错 | '\x7f8290'、'\x' |  |  |  |
| 14 | 输入none，报错 | none、NONE、None |  |  |  |
| 15 | 输入null，报错 | null、NULL、Null |  |  |  |
| 16 | 输入空字符，报错 | ''、"" |  |  |  |
| 17 | 输入科学计数法，报错 | 1. 整型，不报错："1e1", ，'+1E3+2' 1. 小数，报错："-0.1e-10" |  |  |  |
| 18 | 输入json，报错 | '{"k1": "v1"}' |  |  |  |
| 19 | 输入函数，报错 | now()、today |  |  |  |
| 20 | 输入timetamp，报错 | "2024-02-01 00:00:01.001-08:00" |  |  |  |
| 21 | 输入整型，不报错 | 12 |  |  |  |
| 22 | PRECISION 小于 SCALE，报错 | decimal（3，4）、decimal（19，20） |  |  |  |
| 23 | 不设置SCALE | decimal（25）等价于 decimal（25，0） |  |  |  |
| 24 | 不设置PRECISION和SCALE | decimal等价于PRECISION和SCALE长度随着输入数据的长度而适配 |  |  |  |
| 25 | SCALE=0 | 1. 不报错decimal（3，0） 1. 报错decimal（0，0） |  |  |  |

#### 8.1.2 数据写入测试用例

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 小数部分>SCALE，保留到SCALE，不做进位操作 | 1. decimal(8,2):输入123456.125，存储123456.12 1. decimal(8):输入-12345678.625，存储12345678 1. decimal(18,0):输入5678.625，存储5678 1. decimal(4,4):输入-0.62588，存储0.6258 1. decimal(4,3):输入0.62508，存储0.625 1. decimal(4,2):输入-23.660，存储23.66 1. decimal(4,2):输入23.009，存储23.00 1. decimal(38,20):输入23.9999999999999999999999，存储23.99999999999999999999 |  |  |  |
| 2 | 整数部分 > PRECISION - SCALE，报错Decimal field overflow | 1. decimal(8,2):输入-12345678.125，报错 1. decimal(3,2):输入12.125，报错 1. decimal(3,2):输入02.125，报不报错？？ |  |  |  |
| 边界值校验 | 3 | 写入定义范围内的边界值与0值 | 1. decimal(3,1):输入 -99.9和99.9，写入成功 1. decimal(10):输入 -9999999999和9999999999，写入成功 1. decimal(10,5):输入 -0和0.0，写入成功 |  |  |  |
| 4 | 输入非法字符，报错 | '123b'、'ab'、'True'、‘鬼眼狂刀’、'None'、'now'、' '、"" |  |  |  |
| 5 | 输入GEOMETRY类型，报错 | 'PONIT(1 1 )'、'LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)' |  |  |  |
| 6 | 输入二进制类型，报错 | '\x7f8290'、'\x' |  |  |  |
| 7 | 输入none，报错 | none、NONE、None |  |  |  |
| 8 | 输入json，报错 | '{"k1": "v1"}' |  |  |  |
| 9 | 输入函数，报错 | now()、today |  |  |  |
| 10 | 输入timetamp，报错 | "2024-02-01 00:00:01.001-08:00" |  |  |  |
| 11 | 输入bool类型， 报错 | true、True、TRUE、false |  |  |  |
| 12 | 输入整型，不报错 | 12 |  |  |  |
| 13 | 输入浮点类型，不报错 | 12.3、0.1 |  |  |  |
| 14 | 输入null，不报错 | null、NULL、Null |  |  |  |
| 15 | 输入科学计数法，不报错 | "1e1", "-0.1e-10"，'+1E3+2' |  |  |  |
| 16 | 子表 |  |  |  |  |
| 17 | 普通表 |  |  |  |  |
| 18 | 超级表 |  |  |  |  |
| 19 | influxDB 行协议 |  |  |  |  |
| 20 | OpenTSDB行协议 |  |  |  |  |
| 21 | OpenTSDB JSON格式协议 |  |  |  |  |
| 22 | bind_param单行插入 |  |  |  |  |
| 23 | bind_param_batch多行批量插入 |  |  |  |  |
| 24 | Decimal to decimal |  |  |  |  |
| 25 | Int to decimal |  |  |  |  |
| 26 | Float to decimal |  |  |  |  |
|  | Timestamp to decimial，报错 |  |  |  |  |
|  | Binary to decimal，报错 |  |  |  |  |
|  | Bool to decimal，报错？ |  |  |  |  |
|  | Nchar to decimal，报错 |  |  |  |  |
|  | Json to decimal，报错 |  |  |  |  |
|  | Varchar to decimal，报错 |  |  |  |  |
|  | Geometry to decimal，报错 |  |  |  |  |
|  | Varbinary to decimal，报错 |  |  |  |  |
|  | decimal to int |  |  |  |  |
|  | decimal to tinyint，当decimal超过tinyint范围时报错？做截断？ |  |  |  |  |
|  | decimal to float |  |  |  |  |
|  | decimial to timestamp，报错 |  |  |  |  |
|  | decimal to binary，报错 |  |  |  |  |
|  | decimal to bool，报错? |  |  |  |  |
|  | decimal to nchar，不报错 |  |  |  |  |
|  | decimal to json，报错 |  |  |  |  |
|  | decimal to varchar，不报错 |  |  |  |  |
|  | decimal to Geometry，报错 |  |  |  |  |
|  | decimal to Varbinary，报错 |  |  |  |  |
| 列和tag类型为decimal |  | 设计一个多个列和tag定义为decimal的超级表，然后插入数据 |  |  |  |  |
|  | 指定decimal列插入数据 |  |  |  |  |
|  | 指定列中未包含定义为decimla的其他列，插入数据 |  |  |  |  |

#### 8.1.3 数据查询测试用例

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 数据查询测试 | 1 | 数值常量在decimal定义范围内 | [decimal] = 12345678.98567 |  |  |  |
|  | 2 | 数值常量整数位=38 | 1. [decimal] = 9999999999999999999999999999999999999.000，保留整数位 1. [decimal] = 9999999999999999999999999999999999999.9999999999999999999，保留整数位 |  |  |  |
|  | 3 | 数值常量整数位>38 | 抛出异常信息overflow：1000000000000000000000000000000000000.00001 |  |  |  |
|  | 4 | 数值常量小数位=PRECISION和SCALE |  |  |  |  |
|  | 数值比较 |  | Decimal vs decimal |  |  |  |  |
|  |  |  | Decimal vs int（是否需要覆盖所有整数类型？） |  |  |  |  |
|  |  |  | Decimal vs float |  |  |  |  |
|  |  |  | Decimal vs double |  |  |  |  |

#### 8.1.4 数据删除测试用例

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
|  | 超级表 |  |  |  |  |
|  | 子表 |  |  |  |  |
|  | 普通表 |  |  |  |  |
|  | 超级表 |  |  |  |  |
|  | 子表 |  |  |  |  |
|  | 普通表 |  |  |  |  |
| 删除大数量数据 |  | 一次性删除包含decimal列的1亿行数据 |  |  |  |  |

#### 8.1.5 数值类函数测试用例

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
|  | 数值常量 | Select ABS(9999.3243) from t0; |  |  |  |
|  | decimal列 | Select ABS(col_decimal) from t0; |  |  |  |
|  | decimal列运算 | Select ABS(col_decimal1 + col_decimal2) from t0; |  |  |  |
|  |  | decimal列+int列运算 | Select ABS(col_decimal + col_int) from t0; |  |  |  |
| ACOS |  | 数值常量 | Select ACOS(9999.3243) from t0; |  |  |  |
|  |  | decimal列 | Select ACOS(col_decimal) from t0; |  |  |  |
|  |  | decimal列运算 | Select ACOS(col_decimal1 + col_decimal2) from t0; |  |  |  |
|  |  | decimal列+int列运算 | Select ACOS(col_decimal + col_int) from t0; |  |  |  |
| ASIN |  | 数值常量 | Select ASIN(9999.3243) from t0; |  |  |  |
|  |  | decimal列 | Select ASIN(col_decimal) from t0; |  |  |  |
|  |  | decimal列运算 | Select ASIN(col_decimal1 + col_decimal2) from t0; |  |  |  |
|  |  | decimal列+int列运算 | Select ASIN(col_decimal + col_int) from t0; |  |  |  |
| ATAN |  | 数值常量 | Select ATAN(9999.3243) from t0; |  |  |  |
|  |  | decimal列 | Select ATAN(col_decimal) from t0; |  |  |  |
|  |  | decimal列运算 | Select ATAN(col_decimal1 + col_decimal2) from t0; |  |  |  |
|  |  | decimal列+int列运算 | Select ATAN(col_decimal + col_int) from t0; |  |  |  |
| CEIL |  | 数值常量 | Select CEIL(9999.3243) from t0; |  |  |  |
|  |  | decimal列 | Select CEIL(col_decimal) from t0; |  |  |  |
|  |  | decimal列运算 | Select CEIL(col_decimal1 + col_decimal2) from t0; |  |  |  |
|  |  | decimal列+int列运算 | Select CEIL(col_decimal + col_int) from t0; |  |  |  |
| COS |  | 数值常量 | Select COS(9999.3243) from t0; |  |  |  |
|  |  | decimal列 | Select COS(col_decimal) from t0; |  |  |  |
|  |  | decimal列运算 | Select COS(col_decimal1 + col_decimal2) from t0; |  |  |  |
|  |  | decimal列+int列运算 | Select COS(col_decimal + col_int) from t0; |  |  |  |
| FLOOR |  | 数值常量 | Select FLOOR(col_float) from t0; |  |  |  |
|  |  | decimal列 | Select FLOOR(col_decimal) from t0; |  |  |  |
|  |  | decimal列运算 | Select FLOOR(col_decimal1 + col_decimal2) from t0; |  |  |  |
|  |  | decimal列+int列运算 | Select FLOOR(col_decimal + col_int) from t0; |  |  |  |
| LOG |  | 数值常量 | Select LOG(col_float,col_float) from t0; |  |  |  |
|  |  | decimal列 | Select LOG(col_decimal,col_decimal) from t0; |  |  |  |
|  |  | decimal列运算 | Select LOG(col_decimal1 + col_decimal2, col_decimal) from t0; |  |  |  |
|  |  | decimal列+int列运算 | Select LOG(col_decimal + col_int, col_decimal) from t0; |  |  |  |
| POW |  | 数值常量 | Select POW(col_float,col_float) from t0; |  |  |  |
|  |  | decimal列 | Select POW(col_decimal,col_decimal) from t0; |  |  |  |
|  |  | decimal列运算 | Select POW(col_decimal1 + col_decimal2, col_decimal) from t0; |  |  |  |
|  |  | decimal列+int列运算 | Select POW(col_decimal + col_int, col_decimal) from t0; |  |  |  |
| ROUND |  | 数值常量 | Select ROUND(col_float) from t0; |  |  |  |
|  |  | decimal列 | Select ROUND(col_decimal) from t0; |  |  |  |
|  |  | decimal列运算 | Select ROUND(col_decimal1 + col_decimal2) from t0; |  |  |  |
|  |  | decimal列+int列运算 | Select ROUND(col_decimal + col_int) from t0; |  |  |  |
| SIN |  | 数值常量 | Select SIN(9999.3243) from t0; |  |  |  |
|  |  | decimal列 | Select SIN(col_decimal) from t0; |  |  |  |
|  |  | decimal列运算 | Select SIN(col_decimal1 + col_decimal2) from t0; |  |  |  |
|  |  | decimal列+int列运算 | Select SIN(col_decimal + col_int) from t0; |  |  |  |
| SQRT |  | 数值常量 | Select SQRT(9999.3243) from t0; |  |  |  |
|  |  | decimal列 | Select SQRT(col_decimal) from t0; |  |  |  |
|  |  | decimal列运算 | Select SQRT(col_decimal1 + col_decimal2) from t0; |  |  |  |
|  |  | decimal列+int列运算 | Select SQRT(col_decimal + col_int) from t0; |  |  |  |
| TAN |  | 数值常量 | Select TAN(9999.3243) from t0; |  |  |  |
|  |  | decimal列 | Select TAN(col_decimal) from t0; |  |  |  |
|  |  | decimal列运算 | Select TAN(col_decimal1 + col_decimal2) from t0; |  |  |  |
|  |  | decimal列+int列运算 | Select TAN(col_decimal + col_int) from t0; |  |  |  |
| CAST |  | Select CAST('3.1415926' as decimal) from t0; |  |  |  |
|  |  | Select CAST('3333333333333333333333333333333333333.1415926' as decimal) from t0; - 报错 |  |  |  |
|  |  | Select CAST('3.1415926' as decimal(6.5)) from t0; |  |  |  |
|  |  | Select CAST('3.1415926' as decimal(38.20)) from t0; |  |  |  |
|  |  | Decimal to varchar |  |  |  |  |
|  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |
| APERCENTILE |  |  |  |  |  |  |
| AVG |  | 数值常量 | Select TAN(9999.3243) from t0; |  |  |  |
|  |  | decimal列 | Select TAN(col_decimal) from t0; |  |  |  |
|  |  | decimal列运算 | Select TAN(col_decimal1 + col_decimal2) from t0; |  |  |  |
|  |  | decimal列+int列运算 | Select TAN(col_decimal + col_int) from t0; |  |  |  |
| COUNT |  |  |  |  |  |  |
| LEASTSQUARES |  |  |  |  |  |  |
| SPREAD |  |  |  |  |  |  |
| STDDEV |  |  |  |  |  |  |
| SUM |  |  |  |  |  |  |
| HYPERLOGLOG |  |  |  |  |  |  |
| HISTOGRAM |  |  |  |  |  |  |
| PERCENTILE |  |  |  |  |  |  |
| BOTTOM |  |  |  |  |  |  |
| FIRST |  |  |  |  |  |  |
| INTERP |  |  |  |  |  |  |
| LAST |  |  |  |  |  |  |
| LAST_ROW |  |  |  |  |  |  |
| MAX |  |  |  |  |  |  |
| MIN |  |  |  |  |  |  |
| MODE |  |  |  |  |  |  |
| SAMPLE |  |  |  |  |  |  |
| TAIL |  |  |  |  |  |  |
| TOP |  |  |  |  |  |  |
| UNIQUE |  |  |  |  |  |  |
| CSUM |  |  |  |  |  |  |
| DERIVATIVE |  |  |  |  |  |  |
| diff |  |  |  |  |  |  |
| IRATE |  |  |  |  |  |  |
| MAVG |  |  |  |  |  |  |
| STATECOUNT |  |  |  |  |  |  |
| STATEDURATION |  |  |  |  |  |  |
| TWA |  |  |  |  |  |  |
| distinct |  |  |  |  |  |  |
| unique |  |  |  |  |  |  |
| Join场景测试（冯超） | LEFT JOIN |  |  |  |  |  |  |
|  | ASOF JOIN |  |  |  |  |  |  |
|  | Window JOIN |  |  |  |  |  |  |
| 订阅（浩然） |  |  |  |  |  |  |  |
| UDF（段宽军） | 不支持 |  |  |  |  |  |  |
| 流计算（贾靖斌） |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |

#### 8.1.6 Join测试用例

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Join场景测试（冯超） | LEFT JOIN |  |  |  |  |  |  |
|  | ASOF JOIN |  |  |  |  |  |  |
|  | Window JOIN |  |  |  |  |  |  |

#### 8.1.7 订阅测试用例

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 订阅（浩然） |  |  |  |  |  |  |  |
| UDF（段宽军） | 不支持 |  |  |  |  |  |  |
| 流计算（贾靖斌） |  |  |  |  |  |  |  |

#### 8.1.8 UDF测试用例

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| UDF（段宽军） | 不支持UDF |  |  |  |  |  |  |

#### 8.1.9 流计算测试用例

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 流计算（贾靖斌） |  |  |  |  |  |  |  |

#### 8.1.10 客户端显示测试用例

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| TAOSC数据展示测试 |  |  |  |  |  |  |  |

### 8.2 性能测试用例

## 9. 问题(Optional)

这里用于记录需要讨论的问题：
- 暂无

## 10. 遗留Jira

## 11. 测试备忘 (Optional)

这里用于记录测试过程中发现的，与产品行为相关的一些重要信息。

## 12. 参考文档 (Optional)

- Jira信息：
  TS-4244

- Requirement Doc：[需求说明：Decimal](https://taosdata.feishu.cn/wiki/CI6KwADzEiaOrjkLL4Uc2yhUneb)
- Functional Spec：[DECIMAL数据类型](https://taosdata.feishu.cn/wiki/RQcswXCNXiNQamkMKWucmVrWnUc)
