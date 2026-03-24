#  Euler 操作系统 + Hygon CPU CI测试 Test Spec

## 1. 测试目标

本次测试通过运行完整的CI测试用例集，主要验证TDengine在Euler 操作系统 + Hygon CPU平台上各个基础功能。

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-06-05 | 1.0 | Charles |  |

## 3. 测试结论

运行所有CI测试用例: Pass 1365, Failed 24
用例问题： 4个，已修改，修改后，用例pass
配置问题：6个，S3内网服务器连接失败；python V3.9.9 引用kafka库V1.3.5 报错；编译taospyudf报错；jdbc连接报错
资源不足：1个， No enough memory
Jira相关：tmq用例 3个（jira TD-30365); tsim用例 10个 （jira TD-30367)
引用库问题：
所有失败CI用例如下：

| No. | 用例名称 | 失败原因 |
| --- | --- | --- |
| 1 | python3 ./test.py -f enterprise/s3/s3Basic.py -N 3 | 连接S3内网服务器失败 |
| 2 | python3 ./test.py -f community/insert/insert_basic.py -N 3 | 用例问题，修改用例后，用例pass |
| 3 | python3 ./test.py -f 2-query/td-28068.py | 用例问题，修改用例后，用例pass |
| 4 | python3 ./test.py -f 7-tmq/subscribeDb3.py |
| 5 | python3 ./test.py -f 7-tmq/tmqDropStbCtb.py |
| 6 | python3 test.py -f 7-tmq/tmqVnodeSplit-stb-select.py -N 2 -n 1 |
| 7 | python3 ./test.py -f 0-others/compatibility.py | 用例中用到的是社区版，不支持openEuler系统，手动修改脚本参数为企业版，用例通过 |
| 8 | python3 ./test.py -f 0-others/udfpy_main.py | 编译taospyudf报错：fatal error: Python.h: No such file or directory （python版本： V3.9.9） |
| 9 | python3 ./test.py -f 1-insert/insert_double.py | 用例问题，修改用例后，用例pass |
| 10 | python3 ./test.py -f 1-insert/alter_database.py | Create db buffer: No enough memory |
| 11 | ./test.sh -f tsim/query/udfpy.sim | 编译taospyudf报错：fatal error: Python.h: No such file or directory （python版本： V3.9.9） |
| 12 | ./test.sh -f tsim/stream/basic0.sim -g |
| 13 | ./test.sh -f tsim/tmq/snapshot1.sim |
| 14 | ./test.sh -f tsim/valgrind/checkError1.sim |
| 15 | ./test.sh -f tsim/valgrind/checkError2.sim |
| 16 | ./test.sh -f tsim/valgrind/checkError3.sim |
| 17 | ./test.sh -f tsim/valgrind/checkError4.sim |
| 18 | ./test.sh -f tsim/valgrind/checkError5.sim |
| 19 | ./test.sh -f tsim/valgrind/checkError6.sim |
| 20 | ./test.sh -f tsim/valgrind/checkError7.sim |
| 21 | ./test.sh -f tsim/valgrind/checkError8.sim |
| 22 | ,,n,docs-examples-test,bash python.sh | 引用的kafka（V1.3.5) 库有错误 |
| 23 | ,,n,docs-examples-test,bash jdbc.sh | The POM for com.taosdata.jdbc:taos-jdbcdriver:jar:3.2.7-SNAPSHOT is missing |
| 24 | ,,n,docs-examples-test,bash test_R.sh | Error in dbConnect(driver, "jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata") : Unable to connect JDBC to jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata JDBC ERROR: JNI ERROR (0x2354): Unable to establish connection |

## 4. 已知问题和限制

无

## 5. 测试资源及环境

测试平台：Linux x64
测试资源：47.98.158.20 (root/tbase125!)
- openEuler 22.03 LTS-SP1
- Hygon CPU 8 Core
- Memory 32G
- Disk 100G
测试版本：3.0分之最新代码

## 6. 测试用例

CI测试用例集

## 7. 问题

| Id | Title | Commen |
| --- | --- | --- |
| TD-30367 | TD-30367 |  |
| TD-30365 | TD-30365 |  |

## 8. 测试计划 

2024-05-29 -- 2024-05-31

## 9. 测试备忘 

带Asan编译报错，使用不带Asan版本进行测试

## 10. 参考文档

[需求说明：支持 Euler 操作系统](https://taosdata.feishu.cn/wiki/EiYmwcMkLijQELk0tm6cYfDfnac)
