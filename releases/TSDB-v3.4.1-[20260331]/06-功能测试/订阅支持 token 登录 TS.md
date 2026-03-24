# 订阅支持 token 登录 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-31 | - | 0.1 | 王明明 | 新建 |
|  |  |  |  |  |

## 2. 测试目标

本次测试的主要目标：测试订阅支持 token 登录的逻辑

## 3. 参考文档

JIRA: 
FS：[订阅支持 token 登录 FS](https://taosdata.feishu.cn/wiki/MRRFw2gPPirkAYkUpFZc0JgCnLg)

## 4. 测试结论

功能测试通过。

## 5. 测试环境

- OS: Linux

## 6. 功能测试

主要测试覆盖内容包括：
1. 测试 token 异常时（disable，invalid 等)， tmq_consumer_new 报错。
2. 测试 token 正常时，tmq_consumer_new 正常，并且可以正常poll 消费数据。
3. 动态将token 设置为 disable，poll 为NULL，taos_errstr(NULL)  获取到对应的错误信息
4. 动态将token 设置为 enable，poll 到正常数据
5. 动态删除 token，poll 为NULL，taos_errstr(NULL)  获取到对应的错误信息
以上所有测试在 ASAN 模式下测试通过。

## 7. 易用性测试

不涉及

## 8. 长期稳定性测试

无

## 9. 性能测试

无

## 10. 安全性测试

无

## 11. 兼容性测试

不涉及兼容性测试。

## 12. 已知问题和限制

无
