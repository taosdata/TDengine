---
title: 诊断及其他
---

## 网络连接诊断

当出现客户端应用无法访问服务端时，需要确认客户端与服务端之间网络的各端口连通情况，以便有针对性地排除故障。

目前网络连接诊断支持在：Linux 与 Linux，Linux 与 Windows 之间进行诊断测试。

诊断步骤：

1. 如拟诊断的端口范围与服务器 taosd 实例的端口范围相同，须先停掉 taosd 实例
2. 服务端命令行输入：`taos -n server -P <port> -l <pktlen>` 以服务端身份启动对端口 port 为基准端口的监听
3. 客户端命令行输入：`taos -n client -h <fqdn of server> -P <port> -l <pktlen>` 以客户端身份启动对指定的服务器、指定的端口发送测试包
 
-l <pktlen\>： 测试网络包的大小（单位：字节）。最小值是 11、最大值是 64000，默认值为 1000。
注：两端命令行中指定的测试包长度必须一致，否则测试显示失败。

服务端运行正常的话会输出以下信息：

```bash
# taos -n server -P 6000
12/21 14:50:13.522509 0x7f536f455200 UTL work as server, host:172.27.0.7 startPort:6000 endPort:6011 pkgLen:1000

12/21 14:50:13.522659 0x7f5352242700 UTL TCP server at port:6000 is listening
12/21 14:50:13.522727 0x7f5351240700 UTL TCP server at port:6001 is listening
...
...
...
12/21 14:50:13.523954 0x7f5342fed700 UTL TCP server at port:6011 is listening
12/21 14:50:13.523989 0x7f53437ee700 UTL UDP server at port:6010 is listening
12/21 14:50:13.524019 0x7f53427ec700 UTL UDP server at port:6011 is listening
12/21 14:50:22.192849 0x7f5352242700 UTL TCP: read:1000 bytes from 172.27.0.8 at 6000
12/21 14:50:22.192993 0x7f5352242700 UTL TCP: write:1000 bytes to 172.27.0.8 at 6000
12/21 14:50:22.237082 0x7f5351a41700 UTL UDP: recv:1000 bytes from 172.27.0.8 at 6000
12/21 14:50:22.237203 0x7f5351a41700 UTL UDP: send:1000 bytes to 172.27.0.8 at 6000
12/21 14:50:22.237450 0x7f5351240700 UTL TCP: read:1000 bytes from 172.27.0.8 at 6001
12/21 14:50:22.237576 0x7f5351240700 UTL TCP: write:1000 bytes to 172.27.0.8 at 6001
12/21 14:50:22.281038 0x7f5350a3f700 UTL UDP: recv:1000 bytes from 172.27.0.8 at 6001
12/21 14:50:22.281141 0x7f5350a3f700 UTL UDP: send:1000 bytes to 172.27.0.8 at 6001
...
...
...
12/21 14:50:22.677443 0x7f5342fed700 UTL TCP: read:1000 bytes from 172.27.0.8 at 6011
12/21 14:50:22.677576 0x7f5342fed700 UTL TCP: write:1000 bytes to 172.27.0.8 at 6011
12/21 14:50:22.721144 0x7f53427ec700 UTL UDP: recv:1000 bytes from 172.27.0.8 at 6011
12/21 14:50:22.721261 0x7f53427ec700 UTL UDP: send:1000 bytes to 172.27.0.8 at 6011
```

客户端运行正常会输出以下信息：

```bash
# taos -n client -h 172.27.0.7 -P 6000
12/21 14:50:22.192434 0x7fc95d859200 UTL work as client, host:172.27.0.7 startPort:6000 endPort:6011 pkgLen:1000

12/21 14:50:22.192472 0x7fc95d859200 UTL server ip:172.27.0.7 is resolved from host:172.27.0.7
12/21 14:50:22.236869 0x7fc95d859200 UTL successed to test TCP port:6000
12/21 14:50:22.237215 0x7fc95d859200 UTL successed to test UDP port:6000
...
...
...
12/21 14:50:22.676891 0x7fc95d859200 UTL successed to test TCP port:6010
12/21 14:50:22.677240 0x7fc95d859200 UTL successed to test UDP port:6010
12/21 14:50:22.720893 0x7fc95d859200 UTL successed to test TCP port:6011
12/21 14:50:22.721274 0x7fc95d859200 UTL successed to test UDP port:6011
```

仔细阅读打印出来的错误信息，可以帮助管理员找到原因，以解决问题。

## 启动状态及 RPC 诊断

`taos -n startup -h <fqdn of server>`

判断 taosd 服务端是否成功启动，是数据库管理员经常遇到的一种情形。特别当若干台服务器组成集群时，判断每个服务端实例是否成功启动就会是一个重要问题。除检索 taosd 服务端日志文件进行问题定位、分析外，还可以通过 `taos -n startup -h <fqdn of server>` 来诊断一个 taosd 进程的启动状态。

针对多台服务器组成的集群，当服务启动过程耗时较长时，可通过该命令行来诊断每台服务器的 taosd 实例的启动状态，以准确定位问题。

`taos -n rpc -h <fqdn of server>`

该命令用来诊断已经启动的 taosd 实例的端口是否可正常访问。如果 taosd 程序异常或者失去响应，可以通过 `taos -n rpc -h <fqdn of server>` 来发起一个与指定 fqdn 的 rpc 通信，看看 taosd 是否能收到，以此来判定是网络问题还是 taosd 程序异常问题。

## sync 及 arbitrator 诊断

```
taos -n sync -P 6040 -h <fqdn of server>
taos -n sync -P 6042 -h <fqdn of server>
```

用来诊断 sync 端口是否工作正常，判断服务端 sync 模块是否成功工作。另外，-P 6042 用来诊断 arbitrator 是否配置正常，判断指定服务器的 arbitrator 是否能正常工作。

## 网络速度诊断

`taos -n speed -h <fqdn of server> -P 6030 -N 10 -l 10000000 -S TCP`

从 2.2.0.0 版本开始，taos 工具新提供了一个网络速度诊断的模式，可以对一个正在运行中的 taosd 实例或者 `taos -n server` 方式模拟的一个服务端实例，以非压缩传输的方式进行网络测速。这个模式下可供调整的参数如下：

-n：设为“speed”时，表示对网络速度进行诊断。
-h：所要连接的服务端的 FQDN 或 ip 地址。如果不设置这一项，会使用本机 taos.cfg 文件中 FQDN 参数的设置作为默认值。
-P：所连接服务端的网络端口。默认值为 6030。
-N：诊断过程中使用的网络包总数。最小值是 1、最大值是 10000，默认值为 100。
-l：单个网络包的大小（单位：字节）。最小值是 1024、最大值是 1024 `*` 1024 `*` 1024，默认值为 1024。
-S：网络封包的类型。可以是 TCP 或 UDP，默认值为 TCP。

## FQDN 解析速度诊断

`taos -n fqdn -h <fqdn of server>`

从 2.2.0.0 版本开始，taos 工具新提供了一个 FQDN 解析速度的诊断模式，可以对一个目标 FQDN 地址尝试解析，并记录解析过程中所消耗的时间。这个模式下可供调整的参数如下：

-n：设为“fqdn”时，表示对 FQDN 解析进行诊断。
-h：所要解析的目标 FQDN 地址。如果不设置这一项，会使用本机 taos.cfg 文件中 FQDN 参数的设置作为默认值。

## 服务端日志

taosd 服务端日志文件标志位 debugflag 默认为 131，在 debug 时往往需要将其提升到 135 或 143 。

一旦设定为 135 或 143，日志文件增长很快，特别是写入、查询请求量较大时，增长速度惊人。如合并保存日志，很容易把日志内的关键信息（如配置信息、错误信息等）冲掉。为此，服务端将重要信息日志与其他日志分开存放：

- taosinfo 存放重要信息日志, 包括：INFO/ERROR/WARNING 级别的日志信息。不记录 DEBUG、TRACE 级别的日志。
- taosdlog 服务器端生成的日志，记录 taosinfo 中全部信息外，还根据设置的日志输出级别，记录 DEBUG（日志级别 135）、TRACE（日志级别是 143）。

## 客户端日志

每个独立运行的客户端（一个进程）生成一个独立的客户端日志，其命名方式采用 taoslog+<序号> 的方式命名。文件标志位 debugflag 默认为 131，在 debug 时往往需要将其提升到 135 或 143 。

- taoslog 客户端（driver）生成的日志，默认记录客户端 INFO/ERROR/WARNING 级别日志，还根据设置的日志输出级别，记录 DEBUG（日志级别 135）、TRACE（日志级别是 143）。

其中，日志文件最大长度由 numOfLogLines 来进行配置，一个 taosd 实例最多保留两个文件。

taosd 服务端日志采用异步落盘写入机制，优点是可以避免硬盘写入压力太大，对性能造成很大影响。缺点是，在极端情况下，存在少量日志行数丢失的可能。
