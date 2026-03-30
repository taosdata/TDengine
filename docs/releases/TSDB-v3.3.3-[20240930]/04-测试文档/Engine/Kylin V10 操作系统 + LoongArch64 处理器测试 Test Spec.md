# Kylin V10 操作系统 + LoongArch64 处理器测试 Test Spec

## 1. 测试目标

验证在 Kylin V10 操作系统 +LoongArch64 处理器环境下 TDengine 的所有组件都能正常启动运行，验证 CI 测试用例都可以成功通过

TS-5115

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 07/17/2024 | 1.0 | Ping Xiao | draft |
| 08/07/2024 | 1.1 | Ping Xiao | 更新测试结论 |

## 3. 测试结论

- 安装包：TDengine-enterprise-3.3.2.6.0807-Linux-Loongarch64.tar.gz
- 测试结果：CI TSIM 用例出去 valgrind 检测到 rockersdb 的问题外，其他用例都可以成功通过；python 用例因为环境的原因未执行
- 结论：TDengine-enterprise-3.3.2.6.0807-Linux-Loongarch64.tar.gz 安装包在 Kylin V10 操作系统 +LoongArch64 处理器环境下完全适配

## 4. 测试内容

CI 用例测试
TSIM: Pass 456, Fail 27, 失败用例如下

| 失败用例 | 失败原因 |
| --- | --- |
| tsim/valgrind/checkError1.sim tsim/valgrind/checkError2.sim tsim/valgrind/checkError3.sim tsim/valgrind/checkError4.sim tsim/valgrind/checkError5.sim tsim/valgrind/checkError7.sim tsim/valgrind/checkError6.sim tsim/valgrind/checkError8.sim | Rocksdb valgrind 检测报错 |
| tsim/stream/event2.sim | 机器太慢导致，增加 case 中重试次数就可以通过 |
| tsim/stream/basic0.sim tsim/tag/3.sim tsim/tag/4.sim tsim/tag/5.sim tsim/tag/6.sim tsim/tag/bigint.sim tsim/tag/binary_binary.sim tsim/tag/binary.sim tsim/tag/bool_binary.sim tsim/tag/bool_int.sim tsim/tag/bool.sim tsim/tag/double.sim tsim/tag/float.sim tsim/tag/int_binary.sim tsim/tag/int_float.sim tsim/tag/int.sim tsim/tag/smallint.sim tsim/tag/tinyint.sim | 重跑后都可以通过 |


| 类型 | 项目 | 验证 | 备注 |
| --- | --- | --- | --- |
| 安装包 | 安装 | Pass | 没有 upx 压缩，安装包较大 |
|  | 服务启停 | Pass |  |
|  | 卸载 | Pass |  |

## 5. 已知问题和限制

- LoongArch64 平台软件支持不够友好，python 3 从 3.7.9 版本升级到 3.8.0 版本导致 yum 命令无法使用，最后重置系统；
- upx 软件没有适配 LoongArch64 平台，导致生成的二进制文件无法压缩，安装包从 320 M 增加到 550 M;
- 今后打包脚本如果要支持 LoongArch64 平台，打包脚本还需要做一定的调整

## 6. 测试资源及环境

测试平台：华为云 Kylin V10 + LoongArch64 CPU
测试资源：
http://111.207.111.194:20080/core/auth/login/
用户名：loongson12
密码：loongson12

## 7. 测试内容

CI 测试用例集

## 8. Jira 列表

TD-30907


TD-30910


TD-31010


TD-31266


TD-31161

## 9. 测试计划 

2024-07-05 -- 2024-08-07

## 10. 测试备忘 

软件更新需要从官方网站 http://www.loongnix.cn/zh/ 下载，最好不要自己在其他网站下载后编译

## 11. 参考文档

http://www.loongnix.cn/zh/
