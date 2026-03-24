# TD-21963  event window for batch processing功能的测试

## 一、测试概述

任务来源：
TD-21963

用户手册：[Event Window](https://taosdata.feishu.cn/wiki/wikcnYfolqvThTHzqhm0hYpCb9b) 

### 语法

```sql
window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)] [FILL(fill_mod_and_val)]
  | EVENT_WINDOW START WITH start_trigger_condition END WITH end_trigger_condition
}
```

## 二、测试结论

测试正常，可以发布。

## 三、测试方案

### 1.测试准备

a、提取和封装start_trigger_condition和end_trigger_condition，可以随机组合。覆盖点：

| 整形类型 | <、=、>、!=、>=、<=、 | 整数、小数 |  |
| --- | --- | --- | --- |
| 浮点类型 | 同上 | 整形，小数，浮点数 |  |
| bool类型 | ==、！= | true、false、0、1 |  |
| 字符类型 | like（_ , %）,match，nmatch,正则 |  |  |
| ts类型 | <、=、>、!=、>=、<=、 |  |  |
| 标量函数 | 支持 | 非标量函数 | 不支持 |

同时根据start_trigger_condition和end_trigger_condition，要组合验证：

| start | end | 测试结论 |
| --- | --- | --- |
| 正常打开 | 正常关闭 | 正常 |
| 正常打开 | 无法关闭 | 正常 |
| 无法打开 | 正常关闭 | 正常 |
| 无法打开 | 无法关闭 | 正常 |


b、因为语法上属于窗口子句的一部分，所以用例的新增和修改都是在窗口用例内扩充。需要新增下面的组合：
event_window。(重点***)---支持，下面的不支持
session + event_window。(重点**)
state_window + event_window。(重点**)
interval + event_window。(重点**)
session + state_window +interval + event_window。(重点*，包括任意3种的组合，和全部4种的组合，主要验证语句是否会core掉)
c、要验证QueryPolicy = 1、2、3、4的情况组合。

### 2.测试环境

（1）单机

| 软件项 | IP |
| --- | --- |
| TDengine | 192.168.1.44：6030 |
| taostest测试框架 | 192.168.1.44 |

（2）3节点集群

| 软件项 | IP |
| --- | --- |
| 192.168.0.203:ceph01 |
| 192.168.0.203:node221 |
| 192.168.0.203:node222 |
| taostest测试框架 | 192.168.0.203 |

### 3.测试用例

#### （1）单机场景

前置条件
FirstEP：u1-44：6030
fqdn：u1-44
serverport：6030


| 编号 | 测试内容 | 预期结果 | 是否符合预期 | 备注 |
| --- | --- | --- | --- | --- |
| 1(重点***) | 调试时以count函数进行调试和验证 | 脚本通过，数据检查通过 | 符合 | 0228版本完成 |
| 2(重点**) | 用max、min、last、last_row、first、avg、sum函数进行同步验证 | 同上，为了检测重点函数的计算正确性 | 符合 | 0228版本完成 |
| 3(重点*) | 其余剩余函数进行同步验证 | 同上，优先级会低一些 |  | 0228以后才能完成 |

#### （2）三节点三副本3mnode场景

前置条件
dnode1（mnode）
FirstEP：SingleQueryExt3.0_203.common_cluster_30.yamlnet_ceph01
dnode2（mnode）
FirstEP：SingleQueryExt3.0_203.common_cluster_30.yamlnet_node221
dnode3（mnode）
FirstEP：SingleQueryExt3.0_203.common_cluster_30.yamlnet_node222

client
FirstEP：SingleQueryExt3.0_203.common_cluster_30.yamlnet_taostest

脚本会复用单机场景的，直接在全量上进行多副本的验证。
