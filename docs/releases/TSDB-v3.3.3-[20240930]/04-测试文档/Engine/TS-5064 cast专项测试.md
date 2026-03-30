# TS-5064 cast专项测试

## 1. 测试目的

针对 jira TS-5064 [宁德新能源] 出现taosd crash，且cast为使用频率较高的函数，所以对cast进行全面测试，旨在发现暴露更多的问题，增加更多的测试场景和回归用例。

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024/07/18 | 0.1 | @智勇 |  |

## 3. 测试结论

测试通过

## 4. 已知问题和限制

无

## 5. 测试资源及环境

测试平台：Linux x64
测试资源：192.168.0.174
测试版本：
```plaintext
TDengine Enterprise Edition
taosd version: 3.3.3.0.alpha compatible_version: 3.0.0.0
git: 0c3dc2bef43b35f917cf53190034f631f42c36d0
gitOfInternal: 3917a123ef0af701706a32764e76180af641eb05
build: Linux-x64 2024-07-12 09:35:44 +0800
```

## 6. 测试范围及方法

### 6.1 测试范围

Cast 函数的使用

### 6.2 测试方法

在已有的 cast case 的基础上，增加之前缺失的使用场景、类型转换及边界测试，同时形成集成到 CI 流程中。

## 7. 测试用例

通过 SQL 插入数据或者直接在 select 语句中构造，详见https://github.com/taosdata/TDengine/blob/3.0/tests/system-test/2-query/cast.py https://github.com/taosdata/TDengine/blob/3.0/tests/army/query/function/cast.py

## 8. 发现问题

TD-30948


TD-31023

## 9. 参考文档
