# Explorer host 显示问题解决方法讨论

## 1. 问题背景

[TD-30280](https://jira.taosdata.com:18080/browse/TD-30280)

### 1.1 问题根源

在 概览 grafana / `编程`各个子页面 / `工具`各个子页面 中会显示当前TDengine 的 host 连接信息，也就是 `ip:port`，方便复制后即可使用。
Explorer 目前是基于配置的 cluster 提取 ip:port 展示，但是可能存在以下两个场景，造成显示数据不准，从而误导用户。
1. Docker 部署，ip 、port 被映射，但是显示的 ip:port 为容器 ip 或者主机名，无法正确展示。
2. Cluster 被配置为 native 连接，显示的 port 是 native 连接 port，外部第三方服务无法使用。

### 1.2 解决目标

期望显示可用的 ip:port，能够直接复制使用。
目前展示的 host 可分为三类:

| 类别 | 示例 | 可用 |
| --- | --- | --- |
| http链接 | http://ip:port | adapter / explore |
| dsn | taos://root:taosdata@ip:port | adapter |
| jdbc | jdbc:TAOS-RS://ip:port?useSSL=false | adapter |

## 2. 解决方案

### 2.1 方案一：使用 taos-adpater 地址 6041

Explorer toml 配置文件中，cluster 始终配置 taos adapter host；cluster-native 单独配置。Explorer 系统在生成 data task 时，优先使用 native 创建 task；如果没有配置 cluster-native，则使用 cluster 创建任务。
优势：当前对外暴露的就是 taos-adapter 接口，对外比较稳定；

@张元湃 帮忙确认一下，docker 安装时，Explorer toml 配置的 cluster 是否是映射后的 ip 和 port ?


### 2.2 方案二：使用 Explorer 地址 6060

对第三方暴露 Explorer 服务 host。
优势：Explorer 内部实现不用做改动；
缺点：可能对第三方服务适用性无法保障，需要一一验证。
