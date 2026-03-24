# drop dnode xx force 测试报告

## 一、测试结论

测试已通过，测试过程中遇到的一些 corner case:
1. 有两个 mnode 的时候，mnode所以在的 dnode offline的情况下无法使用 drop dnode xx force 命令删除，但是有3个 mnode 的时候如果有 1 个 mnode 所在的 dnode 可以使用 drop dnode xx force 命令删除其中一个 mnode
2. 3 节点 3 副本的情况下，某个节点 offline 的时候无法使用 drop dnode xx force 命令删除，因为 drop dnode 的过程中有副本恢复的操作，恢复的操作中，不允许报一个 vnode 的 2 个副本放在同一的 dnode 上

## 二、测试概述

对于一些已经离线，或离线且无法恢复的节点，可以进行强制删除：[Force Drop Dnode用户手册](https://taosdata.feishu.cn/wiki/wikcnSyY3Ex78KUZt3mx7aYuuwd) 

## 三、测试环境

- 软件环境： TDinternal 3.0 分支
- 硬件环境： 192.168.1.96 

## 四、测试场景

1. 5节点集群

  | **测试场景** |
| --- |
|  | 预期结果 | 实际结果 | 预期结果 | 实际结果 | 预期结果 | 符合预期 |
| mnode 单副本，删除 mnode 所在的 dnode | 错误提示 | ✅ | 错误提示 | ✅ | 错误提示 | ✅ |
| mnode 三副本，删除 mnode leader 所在的 dnode | 错误提示 | ✅ | 错误提示 | ✅ | 错误提示 | ✅ |
| mnode 三副本，删除 mnode follower 所在的 dnode (online) | 错误提示 | ✅ | 错误提示 | ✅ | 错误提示 | ✅ |
| mnode 三副本，删除 mnode follower 所在的 dnode (offline) | 正常删除？ | ✅ | Unsafe 提示 | ✅ | 正常删除 | ✅ |
| 删除 qnode 所在的 dnode (online) | 错误提示 | ✅ | 错误提示 | ✅ | 错误提示 | ✅ |
| 删除 qnode 所在的 dnode (offline) | 可以删除？ | ✅ | Unsafe 提示 | ✅ | 正常删除 | ✅ |
| 删除 qnode 所在的 dnode 同时也是 mnode (offline) |  |  | 无法删除 | ✅ |  |  |
| 删除非 mnode/qnode 所在的 dnode (online) | 错误提示 | ✅ | 错误提示 | ✅ | 错误提示 | ✅ |
| 删除非 mnode/qnode 所在的 dnode (offline) | 可以删除？ | ✅ | Unsafe 提示 | ✅ | 正常删除 | ✅ |

1. 3节点集群

  | **测试场景** |
| --- |
|  | 预期结果 | 实际结果 | 预期结果 | 实际结果 | 预期结果 | 符合预期 |
| mnode 单副本，删除 mnode 所在的 dnode | 错误提示 | ✅ | 错误提示 | ✅ | 错误提示 | ✅ |
| mnode 三副本，删除 mnode leader 所在的 dnode | 错误提示 | ✅ | 错误提示 | ✅ | 错误提示 | ✅ |
| mnode 三副本，删除 mnode follower 所在的 dnode (online) | 错误提示 | ✅ | 错误提示 | ✅ | 错误提示 | ✅ |
| mnode 三副本，删除 mnode follower 所在的 dnode (offline) | 正常删除？ | ✅ | Unsafe 提示 | ✅ | 无法删除 | ✅ |
| 删除 qnode 所在的 dnode (online) | 错误提示 | ✅ | 错误提示 | ✅ | 错误提示 | ✅ |
| 删除 qnode 所在的 dnode (offline) | 可以删除？ | ✅ | Unsafe 提示 | ✅ | 无法删除 | ✅ |
| 删除非 mnode/qnode 所在的 dnode (online) | 错误提示 | ✅ | 错误提示 | ✅ | 错误提示 | ✅ |
| 删除非 mnode/qnode 所在的 dnode (offline) | 可以删除？ | ✅ | Unsafe 提示 | ✅ | 无法删除 | ✅ |

## 五、测试发现的问题

TD-24419


TD-24498
