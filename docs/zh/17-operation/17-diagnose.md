---
title: 诊断及其他
description: 一些常见问题的诊断技巧
---

## 网络连接诊断

当出现客户端应用无法访问服务端时，需要确认客户端与服务端之间网络的各端口连通情况，以便有针对性地排除故障。

目前网络连接诊断支持在：Linux/Windows/macOS 之间进行诊断测试。

诊断步骤：

1. 如拟诊断的端口范围与服务器 taosd 实例的端口范围相同，须先停掉 taosd 实例
2. 服务端命令行输入：`taos -n server -P <port> -l <pktlen>` 以服务端身份启动对端口 port 为基准端口的监听
3. 客户端命令行输入：`taos -n client -h <fqdn of server> -P <port> -l <pktlen>` 以客户端身份启动对指定的服务器、指定的端口发送测试包

-l <pktlen\>： 测试网络包的大小（单位：字节）。最小值是 11、最大值是 64000，默认值为 1000。
注：两端命令行中指定的测试包长度必须一致，否则测试显示失败。

服务端运行正常的话会输出以下信息：

```bash
# taos -n server -P 6030 -l 1000
network test server is initialized, port:6030
request is received, size:1000
request is received, size:1000
...
...
...
request is received, size:1000
request is received, size:1000
```

客户端运行正常会输出以下信息：

```bash
# taos -n client -h 172.27.0.7 -P 6000
taos -n client -h v3s2 -P 6030 -l 1000
network test client is initialized, the server is v3s2:6030
request is sent, size:1000
response is received, size:1000
request is sent, size:1000
response is received, size:1000
...
...
...
request is sent, size:1000
response is received, size:1000
request is sent, size:1000
response is received, size:1000

total succ:  100/100	cost:   16.23 ms	speed:    5.87 MB/s
```

仔细阅读打印出来的错误信息，可以帮助管理员找到原因，以解决问题。

## 服务端日志

taosd 服务端日志文件标志位 debugflag 默认为 131，在 debug 时往往需要将其提升到 135 或 143 。

一旦设定为 135 或 143，日志文件增长很快，特别是写入、查询请求量较大时，增长速度惊人。请注意日志文件目录所在磁盘的空间大小。

## 客户端日志

每个独立运行的客户端（一个进程）生成一个独立的客户端日志，其命名方式采用 taoslog+<序号> 的方式命名。文件标志位 debugflag 默认为 131，在 debug 时往往需要将其提升到 135 或 143 。

- taoslog 客户端（driver）生成的日志，默认记录客户端 INFO/ERROR/WARNING 级别日志，还根据设置的日志输出级别，记录 DEBUG（日志级别 135）、TRACE（日志级别是 143）。

其中，日志文件最大长度由 numOfLogLines 来进行配置，一个 taosd 实例最多保留两个文件。

taosd 服务端日志采用异步落盘写入机制，优点是可以避免硬盘写入压力太大，对性能造成很大影响。缺点是，在极端情况下，存在少量日志行数丢失的可能。当问题分析需要的时候，可以考虑将 参数 asynclog 设置成 0，修改为同步落盘写入机制，保证日志不会丢失。
