# taosx OPC UA/DA 测试报告

## 一、测试概述

文档来源：[OPC Connector 用户手册](https://taosdata.feishu.cn/wiki/wikcnxrfiyN7mkyvAocGz6oets7) 

## 二、测试环境

| IP | 操作系统 | 运行软件 | 版本 |
| --- | --- | --- | --- |
| 192.168.0.196 | Windows 10 | OPC UA/DA 模拟器，taosX, OPC connector | taosx: main OPC connector: enh/sunpeng/TD-22293-opc-phase-2 |
| 192.168.1.96 | Ubuntu 20.04 | taosX, OPC connector, TDengine | taosx: main OPC connector: enh/sunpeng/TD-22293-opc-phase-2 TDengine: main |

## 三、测试用例

### OPC UA

| OPC UA 数据类型 | TDengine 数据类型 | 预期结果 | 原生连接 (6030) | ws 连接 (6041) |
| --- | --- | --- | --- | --- |
| Float | 数据写入正确 | 符合预期 | 符合预期 |
| Double | 数据写入正确 | 不符合预期 | 不符合预期 |
| Int / Int unsigned | 数据无法写入 | 符合预期 | 符合预期 |
| Double | Float | 数据无法写入 | 符合预期 | 符合预期 |
|  | Double | 数据写入正确 | 符合预期 | 符合预期 |
|  | Int / Int unsigned | 数据无法写入 | 符合预期 | 符合预期 |
| SByte | TINYINT | 数据写入正确 | 符合预期 | 符合预期 |
|  | SMALLINT | 数据写入正确 | 符合预期 | 符合预期 |
|  | INT | 数据写入正确 | 符合预期 | 符合预期 |
|  | BIGINT | 数据写入正确 | 符合预期 | 符合预期 |
|  | FLOAT | 数据无法写入 | 符合预期 | 符合预期 |
|  | DOUBLE | 数据无法写入 | 符合预期 | 符合预期 |
|  | TINYINT UNSIGNED/ SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED | 数据无法写入 | 符合预期 | 符合预期 |
| Int16 | TINYINT | 数据无法写入 | 符合预期 | 符合预期 |
|  | SMALLINT | 数据写入正确 | 符合预期 | 符合预期 |
|  | INT | 数据写入正确 | 符合预期 | 符合预期 |
|  | BIGINT | 数据写入正确 | 符合预期 | 符合预期 |
|  | FLOAT | 数据无法写入 | 符合预期 | 符合预期 |
|  | DOUBLE | 数据无法写入 | 符合预期 | 符合预期 |
|  | TINYINT UNSIGNED/ SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED | 数据无法写入 | 符合预期 | 符合预期 |
| Int32 | TINYINT | 数据无法写入 | 符合预期 | 符合预期 |
|  | SMALLINT | 数据无法写入 | 符合预期 | 符合预期 |
|  | INT | 数据写入正确 | 符合预期 | 符合预期 |
|  | BIGINT | 数据写入正确 | 符合预期 | 符合预期 |
|  | FLOAT | 数据无法写入 | 符合预期 | 符合预期 |
|  | DOUBLE | 数据无法写入 | 符合预期 | 符合预期 |
|  | TINYINT UNSIGNED/ SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED | 数据无法写入 | 符合预期 | 符合预期 |
| Int64 | TINYINT | 数据无法写入 | 符合预期 | 符合预期 |
|  | SMALLINT | 数据无法写入 | 符合预期 | 符合预期 |
|  | INT | 数据无法写入 | 符合预期 | 符合预期 |
|  | BIGINT | 数据写入正确 | 符合预期 | 符合预期 |
|  | FLOAT | 数据无法写入 | 符合预期 | 符合预期 |
|  | DOUBLE | 数据无法写入 | 符合预期 | 符合预期 |
|  | TINYINT UNSIGNED/ SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED | 数据无法写入 | 符合预期 | 符合预期 |
| TINYINT |  |  |  |
| SMALLINT |  |  |  |
| INT |  |  |  |
| BIGINT |  |  |  |
| FLOAT |  |  |  |
| DOUBLE |  |  |  |
| TINYINT UNSIGNED/ SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED |  |  |  |
| TINYINT |  |  |  |
| SMALLINT |  |  |  |
| INT |  |  |  |
| BIGINT |  |  |  |
| FLOAT |  |  |  |
| DOUBLE |  |  |  |
| TINYINT UNSIGNED/ SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED |  |  |  |
| TINYINT |  |  |  |
| SMALLINT |  |  |  |
| INT |  |  |  |
| BIGINT |  |  |  |
| FLOAT |  |  |  |
| DOUBLE |  |  |  |
| TINYINT UNSIGNED/ SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED |  |  |  |
| TINYINT |  |  |  |
| SMALLINT |  |  |  |
| INT |  |  |  |
| BIGINT |  |  |  |
| FLOAT |  |  |  |
| DOUBLE |  |  |  |
| TINYINT UNSIGNED/ SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED |  |  |  |
| Boolean | Bool |  |  |  |
| String | Nchar |  |  |  |
| String | Varchar |  |  |  |

### OPC DA

### CSV

csv 导入测试没有问题

### 异常测试

字符串超过定义长度无法写入：符合预期

## 四、测试发现的问题：

1. 阻塞 Windows 环境 OPC UA/DA 原生连接测试
  TD-23408

1. 
  TD-23377

1. 
  TD-23523

1. OPC 连接器内存泄漏，已修复

##
