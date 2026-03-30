# TS-3134：Allow user to reset root password

## 一、测试结论

已验证，基本功能没有问题

## 二、测试概述

允许用户在忘记数据库密码时使用数据库并修改密码：参考： [Reset Root Password](https://taosdata.feishu.cn/wiki/wikcnL6qCcXp2AaBSchGKko6Umc) 

## 三、测试环境

- 软件环境： TDengine 3.0 分支
- 硬件环境： 192.168.1.63

## 四、测试场景

1. 单节点
   - skipGrant = 1, 不重启数据库，root 用户无法登陆
   - skipGrant = 1, 不重启数据库，非 root 用户无法登陆
   - skipGrant = 1, 重启数据库，root 用户可以登陆
   - skipGrant = 1, 重启数据库，非 root 用户可以登陆
2. 集群
   - skipGrant = 1, 1 个 mnode, 不重启 mnode, root 用户无法登陆
   - skipGrant = 1, 1 个 mnode, 不重启 mnode, 非 root 用户无法登陆
   - skipGrant = 1, 3 个 mnode, 只重启 mnode leader, root 用户可以登陆
   - skipGrant = 1, 3 个 mnode, 只重启 mnode leader, 非 root 用户可以登陆
   - skipGrant = 1, 3 个 mnode, 只重启 mnode follower, root 用户无法登陆
   - skipGrant = 1, 3 个 mnode, 只重启 mnode follower, 非 root 用户无法登陆

## 五、测试发现的问题

无
