---
title: 常见问题及反馈
description: 一些常见问题的解决方法汇总
---

## 问题反馈

如果 FAQ 中的信息不能够帮到您，需要 TDengine 技术团队的技术支持与协助，请将以下两个目录中内容打包：

1. /var/log/taos （如果没有修改过默认路径）
2. /etc/taos（如果没有指定其他配置文件路径）

附上必要的问题描述，包括使用的 TDengine 版本信息、平台环境信息、发生该问题的执行操作、出现问题的表征及大概的时间，在 [GitHub](https://github.com/taosdata/TDengine) 提交 issue。

为了保证有足够的 debug 信息，如果问题能够重复，请修改/etc/taos/taos.cfg 文件，最后面添加一行“debugFlag 135"(不带引号本身），然后重启 taosd, 重复问题，然后再递交。也可以通过如下 SQL 语句，临时设置 taosd 的日志级别。

```
  alter dnode <dnode_id> 'debugFlag' '135';
```

其中 dnode_id 请从 show dnodes; 命令输出中获取。

但系统正常运行时，请一定将 debugFlag 设置为 131，否则会产生大量的日志信息，降低系统效率。

## 常见问题列表

### 1. TDengine3.0 之前的版本升级到 3.0 及以上的版本应该注意什么？

3.0 版在之前版本的基础上，进行了完全的重构，配置文件和数据文件是不兼容的。在升级之前务必进行如下操作：

1. 删除配置文件，执行 `sudo rm -rf /etc/taos/taos.cfg`
2. 删除日志文件，执行 `sudo rm -rf /var/log/taos/`
3. 确保数据已经不再需要的前提下，删除数据文件，执行 `sudo rm -rf /var/lib/taos/`
4. 安装最新3.0稳定版本的 TDengine
5. 如果需要迁移数据或者数据文件损坏，请联系涛思数据官方技术支持团队，进行协助解决

### 2. Windows 平台下 JDBCDriver 找不到动态链接库，怎么办？

请看为此问题撰写的 [技术博客](https://www.taosdata.com/blog/2019/12/03/950.html)。

### 3. 如何让 TDengine crash 时生成 core 文件？

请看为此问题撰写的 [技术博客](https://www.taosdata.com/blog/2019/12/06/974.html)。

### 4. 遇到错误“Unable to establish connection” 怎么办？

客户端遇到连接故障，请按照下面的步骤进行检查：

1. 检查网络环境

 - 云服务器：检查云服务器的安全组是否打开 TCP/UDP 端口 6030/6041 的访问权限
 - 本地虚拟机：检查网络能否 ping 通，尽量避免使用`localhost` 作为 hostname
 - 公司服务器：如果为 NAT 网络环境，请务必检查服务器能否将消息返回值客户端

2. 确保客户端与服务端版本号是完全一致的，开源社区版和企业版也不能混用

3. 在服务器，执行 `systemctl status taosd` 检查*taosd*运行状态。如果没有运行，启动*taosd*

4. 确认客户端连接时指定了正确的服务器 FQDN (Fully Qualified Domain Name —— 可在服务器上执行 Linux/macOS 命令 hostname -f 获得），FQDN 配置参考：[一篇文章说清楚 TDengine 的 FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html)。

5. ping 服务器 FQDN，如果没有反应，请检查你的网络，DNS 设置，或客户端所在计算机的系统 hosts 文件。如果部署的是 TDengine 集群，客户端需要能 ping 通所有集群节点的 FQDN。

6. 检查防火墙设置（Ubuntu 使用 ufw status，CentOS 使用 firewall-cmd --list-port），确保集群中所有主机在端口 6030/6041 上的 TCP/UDP 协议能够互通。

7. 对于 Linux 上的 JDBC（ODBC, Python, Go 等接口类似）连接, 确保*libtaos.so*在目录*/usr/local/taos/driver*里, 并且*/usr/local/taos/driver*在系统库函数搜索路径*LD_LIBRARY_PATH*里

8. 对于 macOS 上的 JDBC（ODBC, Python, Go 等接口类似）连接, 确保*libtaos.dylib*在目录*/usr/local/lib*里, 并且*/usr/local/lib*在系统库函数搜索路径*LD_LIBRARY_PATH*里

9. 对于 Windows 上的 JDBC, ODBC, Python, Go 等连接，确保*C:\TDengine\driver\taos.dll*在你的系统库函数搜索目录里 (建议*taos.dll*放在目录 _C:\Windows\System32_)

10. 如果仍不能排除连接故障

 - Linux/macOS 系统请使用命令行工具 nc 来分别判断指定端口的 TCP 和 UDP 连接是否通畅
   检查 UDP 端口连接是否工作：`nc -vuz {hostIP} {port} `
   检查服务器侧 TCP 端口连接是否工作：`nc -l {port}`
   检查客户端侧 TCP 端口连接是否工作：`nc {hostIP} {port}`

 - Windows 系统请使用 PowerShell 命令 Test-NetConnection -ComputerName {fqdn} -Port {port} 检测服务段端口是否访问

11. 也可以使用 taos 程序内嵌的网络连通检测功能，来验证服务器和客户端之间指定的端口连接是否通畅：[诊断及其他](../../operation/diagnose/)。

### 5. 遇到错误 Unable to resolve FQDN” 怎么办？

产生这个错误，是由于客户端或数据节点无法解析 FQDN(Fully Qualified Domain Name)导致。对于 TAOS Shell 或客户端应用，请做如下检查：

1. 请检查连接的服务器的 FQDN 是否正确，FQDN 配置参考：[一篇文章说清楚 TDengine 的 FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html)
2. 如果网络配置有 DNS server，请检查是否正常工作
3. 如果网络没有配置 DNS server，请检查客户端所在机器的 hosts 文件，查看该 FQDN 是否配置，并是否有正确的 IP 地址
4. 如果网络配置 OK，从客户端所在机器，你需要能 Ping 该连接的 FQDN，否则客户端是无法连接服务器的
5. 如果服务器曾经使用过 TDengine，且更改过 hostname，建议检查 data 目录的 dnode.json 是否符合当前配置的 EP，路径默认为/var/lib/taos/dnode。正常情况下，建议更换新的数据目录或者备份后删除以前的数据目录，这样可以避免该问题。
6. 检查/etc/hosts 和/etc/hostname 是否是预配置的 FQDN

### 6. 最有效的写入数据的方法是什么？

批量插入。每条写入语句可以一张表同时插入多条记录，也可以同时插入多张表的多条记录。

### 7. Windows 系统下插入的 nchar 类数据中的汉字被解析成了乱码如何解决？

Windows 下插入 nchar 类的数据中如果有中文，请先确认系统的地区设置成了中国（在 Control Panel 里可以设置），这时 cmd 中的`taos`客户端应该已经可以正常工作了；如果是在 IDE 里开发 Java 应用，比如 Eclipse， IntelliJ，请确认 IDE 里的文件编码为 GBK（这是 Java 默认的编码类型），然后在生成 Connection 时，初始化客户端的配置，具体语句如下：

```JAVA
Class.forName("com.taosdata.jdbc.TSDBDriver");
Properties properties = new Properties();
properties.setProperty(TSDBDriver.LOCALE_KEY, "UTF-8");
Connection = DriverManager.getConnection(url, properties);
```

### 8.  Windows 系统下客户端无法正常显示中文字符？

Windows 系统中一般是采用 GBK/GB18030 存储中文字符，而 TDengine 的默认字符集为 UTF-8 ，在 Windows 系统中使用 TDengine 客户端时，客户端驱动会将字符统一转换为 UTF-8 编码后发送到服务端存储，因此在应用开发过程中，调用接口时正确配置当前的中文字符集即可。

在 Windows 10 环境下运行 TDengine 客户端命令行工具 taos 时，若无法正常输入、显示中文，可以对客户端 taos.cfg 做如下配置：

```
locale C 
charset UTF-8
```

### 9. 表名显示不全

由于 TDengine CLI 在终端中显示宽度有限，有可能比较长的表名显示不全，如果按照显示的不全的表名进行相关操作会发生 Table does not exist 错误。解决方法可以是通过修改 taos.cfg 文件中的设置项 maxBinaryDisplayWidth， 或者直接输入命令 set max_binary_display_width 100。或者在命令结尾使用 \G 参数来调整结果的显示方式。

### 10. 如何进行数据迁移？

TDengine 是根据 hostname 唯一标志一台机器的，对于3.0版本，将数据文件从机器 A 移动机器 B 时，需要重新配置机器 B 的 hostname 为机器 A 的 hostname。

注：3.x 和 之前的1.x、2.x 版本的存储结构不兼容，需要使用迁移工具或者自己开发应用导出导入数据。

### 11. 如何在命令行程序 taos 中临时调整日志级别

为了调试方便，命令行程序 taos 新增了与日志记录相关的指令：

```sql
ALTER LOCAL local_option
 
local_option: {
    'resetLog'
  | 'rpcDebugFlag' 'value'
  | 'tmrDebugFlag' 'value'
  | 'cDebugFlag' 'value'
  | 'uDebugFlag' 'value'
  | 'debugFlag' 'value'
}
```

其含义是，在当前的命令行程序下，清空本机所有客户端生成的日志文件(resetLog)，或修改一个特定模块的日志记录级别（只对当前命令行程序有效，如果 taos 命令行程序重启，则需要重新设置）：

 - value 的取值可以是：131（输出错误和警告日志），135（ 输出错误、警告和调试日志），143（ 输出错误、警告、调试和跟踪日志）。

### 12. go 语言编写组件编译失败怎样解决？

TDengine 3.0版本包含一个使用 go 语言开发的 taosAdapter 独立组件，需要单独运行，提供restful接入功能以及支持多种其他软件（Prometheus、Telegraf、collectd、StatsD 等）的数据接入功能。
使用最新 develop 分支代码编译需要先 `git submodule update --init --recursive` 下载 taosAdapter 仓库代码后再编译。

go 语言版本要求 1.14 以上，如果发生 go 编译错误，往往是国内访问 go mod 问题，可以通过设置 go 环境变量来解决：

```sh
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
```

### 13. 如何查询数据占用的存储空间大小？

默认情况下，TDengine 的数据文件存储在 /var/lib/taos ，日志文件存储在 /var/log/taos 。

若想查看所有数据文件占用的具体大小，可以执行 Shell 指令：`du -sh /var/lib/taos/vnode --exclude='wal'` 来查看。此处排除了 WAL 目录，因为在持续写入的情况下，这里大小几乎是固定的，并且每当正常关闭 TDengine 让数据落盘后，WAL 目录都会清空。

若想查看单个数据库占用的大小，可在命令行程序 taos 内指定要查看的数据库后执行 `show vgroups;` ，通过得到的 VGroup id 去 /var/lib/taos/vnode 下查看包含的文件夹大小。

### 14. 客户端连接串如何保证高可用？

请看为此问题撰写的 [技术博客](https://www.taosdata.com/blog/2021/04/16/2287.html)

### 15. 时间戳的时区信息是怎样处理的？

TDengine 中时间戳的时区总是由客户端进行处理，而与服务端无关。具体来说，客户端会对 SQL 语句中的时间戳进行时区转换，转为 UTC 时区（即 Unix 时间戳——Unix Timestamp）再交由服务端进行写入和查询；在读取数据时，服务端也是采用 UTC 时区提供原始数据，客户端收到后再根据本地设置，把时间戳转换为本地系统所要求的时区进行显示。

客户端在处理时间戳字符串时，会采取如下逻辑：

1. 在未做特殊设置的情况下，客户端默认使用所在操作系统的时区设置。
2. 如果在 taos.cfg 中设置了 timezone 参数，则客户端会以这个配置文件中的设置为准。
3. 如果在 C/C++/Java/Python 等各种编程语言的 Connector Driver 中，在建立数据库连接时显式指定了 timezone，那么会以这个指定的时区设置为准。例如 Java Connector 的 JDBC URL 中就有 timezone 参数。
4. 在书写 SQL 语句时，也可以直接使用 Unix 时间戳（例如 `1554984068000`）或带有时区的时间戳字符串，也即以 RFC 3339 格式（例如 `2013-04-12T15:52:01.123+08:00`）或 ISO-8601 格式（例如 `2013-04-12T15:52:01.123+0800`）来书写时间戳，此时这些时间戳的取值将不再受其他时区设置的影响。

### 16. TDengine 3.0 都会用到哪些网络端口？

使用到的网络端口请看文档：[serverport](../../reference/config/#serverport)

需要注意，文档上列举的端口号都是以默认端口 6030 为前提进行说明，如果修改了配置文件中的设置，那么列举的端口都会随之出现变化，管理员可以参考上述的信息调整防火墙设置。

### 17. 为什么 RESTful 接口无响应、Grafana 无法添加 TDengine 为数据源、TDengineGUI 选了 6041 端口还是无法连接成功？

这个现象可能是因为 taosAdapter 没有被正确启动引起的，需要执行：```systemctl start taosadapter``` 命令来启动 taosAdapter 服务。

需要说明的是，taosAdapter 的日志路径 path 需要单独配置，默认路径是 /var/log/taos ；日志等级 logLevel 有 8 个等级，默认等级是 info ，配置成 panic 可关闭日志输出。请注意操作系统 / 目录的空间大小，可通过命令行参数、环境变量或配置文件来修改配置，默认配置文件是 /etc/taos/taosadapter.toml 。

有关 taosAdapter 组件的详细介绍请看文档：[taosAdapter](../../reference/taosadapter/)

### 18. 发生了 OOM 怎么办？

OOM 是操作系统的保护机制，当操作系统内存(包括 SWAP )不足时，会杀掉某些进程，从而保证操作系统的稳定运行。通常内存不足主要是如下两个原因导致，一是剩余内存小于 vm.min_free_kbytes ；二是程序请求的内存大于剩余内存。还有一种情况是内存充足但程序占用了特殊的内存地址，也会触发 OOM 。

TDengine 会预先为每个 VNode 分配好内存，每个 Database 的 VNode 个数受 建库时的vgroups参数影响，每个 VNode 占用的内存大小受 buffer参数 影响。要防止 OOM，需要在项目建设之初合理规划内存，并合理设置 SWAP ，除此之外查询过量的数据也有可能导致内存暴涨，这取决于具体的查询语句。TDengine 企业版对内存管理做了优化，采用了新的内存分配器，对稳定性有更高要求的用户可以考虑选择企业版。

### 19. 在macOS上遇到Too many open files怎么办？

taosd日志文件报错Too many open file，是由于taosd打开文件数超过系统设置的上限所致。
解决方案如下：
1. 新建文件 /Library/LaunchDaemons/limit.maxfiles.plist，写入以下内容(以下示例将limit和maxfiles改为10万，可按需修改)：
```
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
"http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
<key>Label</key>
  <string>limit.maxfiles</string>
<key>ProgramArguments</key>
<array>
  <string>launchctl</string>
  <string>limit</string>
  <string>maxfiles</string>
  <string>100000</string>
  <string>100000</string>
</array>
<key>RunAtLoad</key>
  <true/>
<key>ServiceIPC</key>
  <false/>
</dict>
</plist>
```
2. 修改文件权限
```
sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
sudo chmod 644 /Library/LaunchDaemons/limit.maxfiles.plist
```
3. 加载 plist 文件 (或重启系统后生效。launchd在启动时会自动加载该目录的 plist)
```
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
```
4.确认更改后的限制
```
launchctl limit maxfiles
```
### 20 建库时提示 Out of dnodes
该提示是创建 db 的 vnode 数量不够了，需要的 vnode 不能超过了 dnode 中 vnode 的上限。因为系统默认是一个 dnode 中有 CPU 核数两倍的 vnode，也可以通过配置文件中的参数 supportVnodes 控制。
正常调大 taos.cfg 中 supportVnodes 参数即可。

### 21 在服务器上的使用 taos-CLI 能查到指定时间段的数据，但在客户端机器上查不到？
这种情况是因为客户端与服务器上设置的时区不一致导致的，调整客户端与服务器的时区一致即可解决。

### 22 表名确认是存在的，但在写入或查询时返回表名不存在，什么原因？ 
TDengine 中的所有名称，包括数据库名、表名等都是区分大小写的，如果这些名称在程序或 taos-CLI 中没有使用反引号（`）括起来使用，即使你输入的是大写的，引擎也会转化成小写来使用，如果名称前后加上了反引号，引擎就不会再转化成小写，会保持原样来使用。

### 23 在 taos-CLI 中查询，字段内容不能完全显示出来怎么办？
可以使用 \G 参数来竖式显示，如 show databases\G; （为了输入方便，在"\"后加 TAB 键，会自动补全后面的内容）

### 24 使用 taosBenchmark 测试工具写入数据查询很快，为什么我写入的数据查询非常慢？
TDengine 在写入数据时如果有很严重的乱序写入问题，会严重影响查询性能，所以需要在写入前解决乱序的问题。如果业务是从 kafka 消费写入，请合理设计消费者，尽可能的一个子表数据由一个消费者去消费并写入，避免由设计产生的乱序。

### 25 我想统计下前后两条写入记录之间的时间差值是多少？
使用 DIFF 函数，可以查看时间列或数值列前后两条记录的差值，非常方便，详细说明见 SQL手册->函数->DIFF
