# 客户端注册实例 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-11-19 | - | 0.1 | 彭荣坤 | 新建 |
| 2025-11-28 | 2025-11-28 | 1.0 | 关胜亮 | 审核后发布 |

## 2. 测试目标

本次测试的主要目标：测试客户端注册实例的功能和性能是否满足需求

## 3. 参考文档

JIRA: [TS-7431](https://jira.taosdata.com:18080/browse/TS-7431)
RS：[外部实例注册 RS](https://taosdata.feishu.cn/wiki/CGzFw7t8EiiMC0knynkcmyFOnkd)
FS：[客户端注册实例 FS](https://taosdata.feishu.cn/wiki/DoJ2wT1ZsicAgakZg84cCAGSnGd)
原始需求：[客户端版本兼容性解决方案](https://taosdata.feishu.cn/wiki/VTEuwbf6DiDIHCkAsxRcH0t7nUg) 第 4.3 节

## 4. 测试结论

功能符合预期，测试通过。

## 5. 测试环境

- OS: Windows, Linux, macOS

## 6. 功能测试

通过单元测试source/client/test/instanceTest.cpp实现下列测试场景：

| 分类 | 测试场景 | 编号 | 测试用例 | 预期行为 | 测试结果 | 说明 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 正常使用taos_register_instance注册功能 | 成功注册之后，能查询到相应的结果 | 通过 |  |
| 2 | 过期用例自动清除 | 注册超过设置的expire之后，查询不到结果 | 通过 |  |
| 3 | taos_register_instance删除指定id实例 | 指定id被正常删除 | 通过 | expire=-1 |
| 4 | taos_register_instance制定不过期实例 | 实例永远不过期 | 通过 | expire=0 |
| 5 | taos_list_instances按照type过滤查询 | 返回指定type的id数组 | 通过 |  |
| 6 | taos_list_instances不指定type查询 | 返回id全集 | 通过 |  |
| 7 | taos_list_instances查询不存在的type | 返回list为空 | 通过 |  |
| 8 | 通过系统表查询实例performance_schema.perf_instances | 能返回全列id,type,desc,firest_reg_time,last_reg_time,expire | 通过 | select * from performance_schema.perf_instances; |
| 9 | 查询系统表id结果和taos_list_instances函数查询结果对比 | 查询结果相同 | 通过 |  |
| 10 | 用SHOW INSTANCES [LIKE 'pattern']对id进行模糊查询 | 返回id符合pattern的集合 | 通过 | show instances |
| 11 | 实例更新，两次调用taos_register_instance，查看`last_reg_time` 是否有变化 | `last_reg_time` 被更新 | 通过 |  |
|  |  | 12 | mndoe切主 | 用例全部清空 | 通过 |  |
| 内存检测 |  | 1 | 上述用例全部用asan跑一遍 | 没有内存报错 | 通过 |  |
|  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |

## 7. 易用性测试

不涉及

## 8. 长期稳定性测试

无

## 9. 性能测试

#### 9.0.1 系统表性能

 环境：
本地环境macos Darwin Kernel Version 23.6.0    16G

| 测试指标 | 平均响应时间 | 预期时间 |
| --- | --- | --- |
| taos_register_instance注册响应时间 | 0.7ms | <20ms |
| taos_register_instance删除响应时间 | 1ms | <20ms |
| taos_list_instances查询响应时间（存量10条） | 0.5ms | <50ms |
| 查询系统表perf_instances响应时间（存量10条） | 7ms | <50ms |

#### 9.0.2 读写性能

1. 用taosBenchmark写入同时用taos_register_instance进行注册
结果：性能波动<3%
1. 用taosBenchmark查询同时用taos_register_instance进行注册
结果：性能波动<3%

## 10. 安全性测试

访问权限测试：
1. root用户show instances，可以查询到结果
2. 创建角色sysinfo 0，show instances
```c {wrap}
taos> show instances;

DB error: Insufficient privilege for operation [0x80000303] (0.007045s)
```

1. 授予权限sysinfo 1，之后再show instances，可以查询到结果

## 11. 兼容性测试

不涉及兼容性测试。

## 12. 已知问题和限制

无
