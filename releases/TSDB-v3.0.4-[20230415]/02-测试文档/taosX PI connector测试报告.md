# taosX PI connector测试报告

## 1. 测试概述

文档编写来源：[PI  Connector](https://taosdata.feishu.cn/wiki/wikcnDUSdhAHpJtbUDcN39zRIXe) 
任务来源：
TD-22894

taosX命令行格式：
```sql
taosx run \
    -f "pi://WIN-2OA23UM12TN/Met1&TemplateForPIPoint=template1,template2&TemplateForAFElement=template3,template4" \
    -t "taos://tdengine:6030/pi"
```

测试时需要注意的事项：
（1）taosx任务参数：
WIN-2OA23UM12TN 为PI系统的hosts；
Met1为PI系统的数据库名；
TemplateForPIPoint和TemplateForAFElement分别为PIPoint方式和AFElement模式，“=”号后接具体的templateID
（2）目标库需要提前创建
（3）taos侧支持native和websocket，对应的测试用例则有2组
（4）支持通过explorer启动任务
1.From pi to taos
2.from pi to taos+ws
（5）根据PI connector中描述的数据类型对应关系，构建覆盖所有PI数据类型，验证数据类型转换的一致性。

## 2. 遗留的问题

TD-23428


## 3. 测试环境

采用2台机器，分别模拟PI源库和TDengine目标库，taosXPI库部署在相同机器上。

| IP | 操作系统 | 运行软件 |
| --- | --- | --- |
| 192.168.0.34 | winserver2022 | PI系统、PI connector、taosX |
| 192.168.1.41 | linux | TDengine（main） |

## 4. 正常测试

|  |
|  |
| from | to |
| PI | taos | 符合预期 |
| PI | taos+ws | 符合预期 |
| 可正常写入数据，数据类型转换正确 | TD-23516 |

数据类型转换正确性验证：
| Point Type | TDengine Column Type |
| --- | --- |
| Digital | NCHAR |
| Int16 | INT |
| Int32 | INT |
| Int64  | BIGINT |
| Float16 | FLOAT |
| Float32 | FLOAT |
| Float64 | DOUBLE |
| String | NCHAR |
| Timestamp | TIMESTAMP |


| Point Type in PI | Point Type in TDengine | 是否符合预期 |
| --- | --- | --- |
| Digital | nchar | TD-23484 |
| int16 | int | TD-23482 |
| int32 | int | 符合预期 |
| int64 | bigint | TD-23482 |
| float16 | float | 符合预期 |
| float32 | float | 符合预期 |
| float64 | double | 符合预期 |
| string | nchar | TD-23484 |
| timestamp | timestamp | TD-23481 |
| boolean | bool | 符合预期 |
| guid | nchar | TD-23484 |

## 5. 异常测试

| 测试用例描述 | 预期结果 | 是否符合预期 |
| --- | --- | --- |
| To native方式，任务执行过程中断开TDengine链接后重连，断开过程中无数据写入；重新使TDengine上线后再写入数据 | taosX进程不受影响，链接恢复后能够将新的数据继续写入 | 符合预期 |
| To native方式，任务执行过程中断开TDengine链接后重连，断开过程中通过PI发送新的数据 | taosX任务报错并自动终止 | TD-23428 |
| To ws方式，任务执行过程中断开TDengine链接后重连，断开过程中无数据写入；恢复链接后通过PI写入新数据 | taosX进程不受影响，链接恢复后能够将新的数据继续写入 | 符合预期 |
| To ws方式，任务执行过程中断开TDengine链接后重连，断开过程中通过PI写入新数据 | taosX任务报错并自动终止 | TD-23428 |
| To ws方式，任务执行过程中断开TDengine侧的taosadapter链接后重连，断开过程中无数据写入；恢复数据链接后通过PI写入新数据 | taosX进程不受影响，链接恢复后能够将新的数据继续写入 | 符合预期 |
| To ws方式，任务执行过程中断开TDengine侧的taosadapter链接后重连，断开过程中通过PI写入新数据 | 命令行下 taosX 报错退出。 | TD-23428 |
