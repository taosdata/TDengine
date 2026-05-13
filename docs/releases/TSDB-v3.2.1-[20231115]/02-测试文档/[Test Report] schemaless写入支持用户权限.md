# [Test Report] schemaless写入支持用户权限

### 1. 概述：

schemaless写入数据支持用户权限验证，由于此功能主要与数据写入相关，数据订阅权限主要与读权限相关，不在此次测试范围; 同时schemaless写入不支持普通表写入，不考虑普通表权限场景。用户权限与schemaless写入划分如下：

| 用户权限 | schemaless写入 |
| --- | --- |
| db.*/db write | 可以自动建超级表，自动建子表写数据 |
| db read | 对db有read权限, 不可以自动建超级表，自动建子表写数据, 写入数据报错 |
| Stable write | 对stable有write权限, 可以自动建子表写数据 |
| Stable read | 对stable有read权限 , 不可以自动建子表写数据，写入数据报错 |
| Child table write | 对子表有write权限, 可以写数据到子表 |
| Child table read | 对子表有read权限, 不可以写数据到子表 |

### 2. 测试环境： {folded="true"}

192.168.1.35：
CPU: Intel(R) Xeon(R) CPU E5-2630 v2 @ 2.60GHz （2）24核
Mem: DDR3  32 GB * 2
Disk: 2792GB

### 3. 测试用例：

| 用例类型 | 用例名称 | 用例描述 | 期望结果 | 测试结果 |
| --- | --- | --- | --- | --- |
| 数据库写权限，schemaless数据写入 | 1. 安装部署企业版3.0最新代码 1. 创建用户test 1. 使用root用户创建数据库db，授权用户test数据库写权限 1. 使用test用户schemaless写入数据 1. 使用root用户插销test用户对db的写权限 1. 删除用户test | 1. 安装部署正常 1. 创建用户test正常 1. 创建数据库db、且用户test拥有数据库写权限 1. schemaless写入数据正常 1. 撤销用户test对db的写权限正常 1. 删除用户test正常 | pass |
| 超级表写权限，schemaless数据写入 | 1. 安装部署企业版3.0最新代码 1. 创建用户test 1. 使用root用户创建数据库db，超级表st， 授权用户test超级表st写权限 1. 使用test用户schemaless写入数据 1. 使用root用户插销test用户对st的写权限 1. 删除用户test | 1. 安装部署正常 1. 创建用户test正常 1. 创建数据库db、超级表st正常，且拥有st的写权限 1. schemaless写入数据正常 1. 撤销用户test对st的写权限正常 1. 删除用户test正常 | pass |
| 子表写权限，schemaless数据写入 | 1. 安装部署企业版3.0最新代码 1. 创建用户test 1. 使用root用户创建数据库db，超级表st，子表ct，授权用户test超级子表ct写权限 1. 使用test用户schemaless写入数据到子表ct 1. 使用root用户插销test用户对ct的写权限 1. 删除用户test | 1. 安装部署正常 1. 创建用户test正常 1. 创建数据库db、超级表st正常，且拥有st的读权限 1. schemaless写入数据到子表ct正常 1. 撤销用户test对子表ct写权限正常 1. 删除用户test正常 | pass |
| 数据库读权限，schemaless数据写入报错 | 1. 安装部署企业版3.0最新代码 1. 创建用户test 1. 使用root用户创建数据库db，超级表st， 授权用户test数据库db读权限 1. 使用test用户schemaless写入数据 1. 使用root用户插销test用户对数据库db的读权限 1. 删除用户test | 1. 安装部署正常 1. 创建用户test正常 1. 创建数据库db、超级表st正常，且拥有db的读权限 1. schemaless写入数据报错 1. 撤销用户test对db的读权限正常 1. 删除用户test正常 | pass |
| 超级表读权限，schemaless数据写入报错 | 1. 安装部署企业版3.0最新代码 1. 创建用户test 1. 使用root用户创建数据库db，超级表st， 授权用户test超级表st读权限 1. 使用test用户schemaless写入数据 1. 使用root用户插销test用户对超级表st的读权限 1. 删除用户test | 1. 安装部署正常 1. 创建用户test正常 1. 创建数据库db、超级表st正常，且拥有st的读权限 1. schemaless写入数据报错 1. 撤销用户test对st的读权限正常 1. 删除用户test正常 | pass |
| 子表读权限，schemaless数据写入报错 | 1. 安装部署企业版3.0最新代码 1. 创建用户test 1. 使用root用户创建数据库db，超级表st，子表ct，授权用户test子表ct读权限 1. 使用test用户schemaless写入数据到子表ct 1. 使用root用户插销test用户对子表ct的读权限 1. 删除用户test | 1. 安装部署正常 1. 创建用户test正常 1. 创建数据库db、超级表st、子表ct正常，且拥有ct的读权限 1. schemaless写入数据到ct报错 1. 撤销用户test对ct的读权限正常 1. 删除用户test正常 | pass |

### 4. 总结：

经测试，schemaless写入对用户数据库、超级表、及子表的读写权限与预期相符，满足概述中需求描述。
