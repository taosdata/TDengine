# Kylin V10 操作系统 + 鲲鹏处理器测试 Test Spec

## 1. 测试目标

验证在 Kylin V10 操作系统 + 鲲鹏处理器环境下已有的 arm 安装包可以正常启动运行，验证 CI 测试用例都可以成功通过

TS-4856

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 06/04/2024 | 1.0 | Ping Xiao |  |

## 3. 测试结论

1. 安装包：**TDengine-enterprise-3.3.0.6-Linux-arm64.tar.gz**
2. 测试结果：CI 用例除去环境原因以及 case 设计缺陷导致的问题外都可以通过 （其中 python 测试用例缺陷 [TD-30354](https://jira.taosdata.com:18080/browse/TD-30354) 已修复，sml_test 用例缺陷 [TD-30360](https://jira.taosdata.com:18080/browse/TD-30360) 导致的问题待修复）arm 已有安装包可以在 Kylin V10 操作系统 + 鲲鹏处理器的环境下正常使用
3. 结论：标准 Linux/ARM64 安装包可以在 Kylin V10 + 鲲鹏处理器环境下直接使用

## 4. 测试内容

| 类型 | 项目 | 验证 | 备注 |
| --- | --- | --- | --- |
| 安装包 | 安装 | Pass |  |
|  | 服务启停 | Pass |  |
|  | 卸载 | Pass |  |
| CI 用例 | TSIM 用例 | Pass 470, Failed 11 |
|  | Python 用例 | Pass 877, Failed 60 |

## 5. 已知问题和限制

无

## 6. 测试资源及环境

测试平台：华为云 Kylin V10 + KunPeng CPU
测试资源：[123.60.179.114](http://123.60.179.114) root/tbase125!

## 7. 测试内容

CI 测试用例集

## 8. Jira 列表

TD-30354


TD-30360

## 9. 测试计划 

2024-05-29 -- 2024-05-31

## 10. 测试备忘 

## 11. 参考文档
