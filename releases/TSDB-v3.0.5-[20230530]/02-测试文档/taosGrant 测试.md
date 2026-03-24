# taosGrant 测试

## 一、测试结论

## 二、测试概述

企业版许可证机制升级，增加对连接器的支持，详细信息参考：[Connector Licensing Mechanism](https://taosdata.feishu.cn/wiki/wikcnOJwxAzuwtP4MuiO41Dh4Zd) 

## 三、测试环境

- 软件环境： TDinternal 3.0 分支
- 硬件环境： 192.168.1.96 

## 四、测试场景

1. 授权码生成验证
2. 单机测试
   - 升级
   - 授权码更新后间隔指定的时间后可以通过 show grants 命令查看更新
   - 连接器 （OPC UA/DA,  PI, Kafka, influxdb, mqtt）- **blocked**
3. 集群测试
   - 滚动升级
   - 取并集测试
   - 连接器 （OPC UA/DA,  PI, Kafka, influxdb, mqtt）- **blocked**
4. 异常测试
   - 非企业版
   - 无效输入

  | cmd | 符合预期？ |
| --- | --- |
| ./taosGrant_linux64 -k wKaSZ38xsZtAx5ekH9O2kmFU -c_app OPC | 是 |
| ./taosGrant_linux64 -k wKaSZ38xsZtAx5ekH9O2kmFU -c_app OPC_UA -c_expire 100 -c_number -100 | 是 |
| ./taosGrant_linux64 -k wKaSZ38xsZtAx5ekH9O2kmFU -c_app OPC_UA -c_expire -1 | 是 |
| ./taosGrant_linux64 -k wKaSZ38xsZtAx5ekH9O2kmFU -c_app OPC_UA -c_expire 100 -c_number 100 -c_speed -10 | 是 |
| ./taosGrant_linux64 -k wKaSZ38xsZtAx5ekH9O2kmFU -c_app OPC_UA, OPC_DA -c_expire 10 | TD-24259 |
|  |  |

  1. 

## 五、测试发现的问题

TD-24259
