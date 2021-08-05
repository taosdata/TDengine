# 常见问题

## 0. 怎么报告问题？

如果 FAQ 中的信息不能够帮到您，需要 TDengine 技术团队的技术支持与协助，请将以下两个目录中内容打包：
1. /var/log/taos （如果没有修改过默认路径）
2. /etc/taos

附上必要的问题描述，包括使用的 TDengine 版本信息、平台环境信息、发生该问题的执行操作、出现问题的表征及大概的时间，在 [GitHub](https://github.com/taosdata/TDengine) 提交Issue。

为了保证有足够的debug信息，如果问题能够重复，请修改/etc/taos/taos.cfg文件，最后面添加一行“debugFlag 135"(不带引号本身），然后重启taosd, 重复问题，然后再递交。也可以通过如下SQL语句，临时设置taosd的日志级别。
```
    alter dnode <dnode_id> debugFlag 135;
```
但系统正常运行时，请一定将debugFlag设置为131，否则会产生大量的日志信息，降低系统效率。

## 1. TDengine2.0之前的版本升级到2.0及以上的版本应该注意什么？☆☆☆

2.0版在之前版本的基础上，进行了完全的重构，配置文件和数据文件是不兼容的。在升级之前务必进行如下操作：

1. 删除配置文件，执行 `sudo rm -rf /etc/taos/taos.cfg`
2. 删除日志文件，执行 `sudo rm -rf /var/log/taos/`
3. 确保数据已经不再需要的前提下，删除数据文件，执行 `sudo rm -rf /var/lib/taos/`
4. 安装最新稳定版本的 TDengine
5. 如果需要迁移数据或者数据文件损坏，请联系涛思数据官方技术支持团队，进行协助解决

## 2. Windows平台下JDBCDriver找不到动态链接库，怎么办？

请看为此问题撰写的[技术博客](https://www.taosdata.com/blog/2019/12/03/950.html)

## 3. 创建数据表时提示more dnodes are needed

请看为此问题撰写的[技术博客](https://www.taosdata.com/blog/2019/12/03/965.html)

## 4. 如何让TDengine crash时生成core文件？

请看为此问题撰写的[技术博客](https://www.taosdata.com/blog/2019/12/06/974.html)

## 5. 遇到错误“Unable to establish connection”, 我怎么办？

客户端遇到连接故障，请按照下面的步骤进行检查：

1. 检查网络环境
    * 云服务器：检查云服务器的安全组是否打开TCP/UDP 端口6030-6042的访问权限
    * 本地虚拟机：检查网络能否ping通，尽量避免使用`localhost` 作为hostname
    * 公司服务器：如果为NAT网络环境，请务必检查服务器能否将消息返回值客户端

2. 确保客户端与服务端版本号是完全一致的，开源社区版和企业版也不能混用

3. 在服务器，执行 `systemctl status taosd` 检查*taosd*运行状态。如果没有运行，启动*taosd*

4. 确认客户端连接时指定了正确的服务器FQDN (Fully Qualified Domain Name(可在服务器上执行Linux命令hostname -f获得)）,FQDN配置参考：[一篇文章说清楚TDengine的FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html)。

5. ping服务器FQDN，如果没有反应，请检查你的网络，DNS设置，或客户端所在计算机的系统hosts文件。如果部署的是TDengine集群，客户端需要能ping通所有集群节点的FQDN。

6. 检查防火墙设置（Ubuntu 使用 ufw status，CentOS 使用 firewall-cmd --list-port），确认TCP/UDP 端口6030-6042 是打开的

7. 对于Linux上的JDBC（ODBC, Python, Go等接口类似）连接, 确保*libtaos.so*在目录*/usr/local/taos/driver*里, 并且*/usr/local/taos/driver*在系统库函数搜索路径*LD_LIBRARY_PATH*里

8. 对于Windows上的JDBC, ODBC, Python, Go等连接，确保*C:\TDengine\driver\taos.dll*在你的系统库函数搜索目录里 (建议*taos.dll*放在目录 *C:\Windows\System32*)

9. 如果仍不能排除连接故障

   * Linux 系统请使用命令行工具nc来分别判断指定端口的TCP和UDP连接是否通畅
     检查UDP端口连接是否工作：`nc -vuz {hostIP} {port} `
     检查服务器侧TCP端口连接是否工作：`nc -l {port}`
     检查客户端侧TCP端口连接是否工作：`nc {hostIP} {port}`

   * Windows 系统请使用 PowerShell 命令 Net-TestConnection -ComputerName {fqdn} -Port {port} 检测服务段端口是否访问

10. 也可以使用taos程序内嵌的网络连通检测功能，来验证服务器和客户端之间指定的端口连接是否通畅（包括TCP和UDP）：[TDengine 内嵌网络检测工具使用指南](https://www.taosdata.com/blog/2020/09/08/1816.html)。

## 6. 遇到错误“Unexpected generic error in RPC”或者“Unable to resolve FQDN”，我怎么办？

产生这个错误，是由于客户端或数据节点无法解析FQDN(Fully Qualified Domain Name)导致。对于TAOS Shell或客户端应用，请做如下检查：

1. 请检查连接的服务器的FQDN是否正确,FQDN配置参考：[一篇文章说清楚TDengine的FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html)。
2. 如果网络配置有DNS server, 请检查是否正常工作
3. 如果网络没有配置DNS server, 请检查客户端所在机器的hosts文件，查看该FQDN是否配置，并是否有正确的IP地址。
4. 如果网络配置OK，从客户端所在机器，你需要能Ping该连接的FQDN，否则客户端是无法连接服务器的

## 7. 虽然语法正确，为什么我还是得到 "Invalid SQL" 错误

如果你确认语法正确，2.0之前版本，请检查SQL语句长度是否超过64K。如果超过，也会返回这个错误。

## 8. 是否支持validation queries?

TDengine还没有一组专用的validation queries。然而建议你使用系统监测的数据库”log"来做。

<a class="anchor" id="update"></a>
## 9. 我可以删除或更新一条记录吗？

TDengine 目前尚不支持删除功能，未来根据用户需求可能会支持。

从 2.0.8.0 开始，TDengine 支持更新已经写入数据的功能。使用更新功能需要在创建数据库时使用 UPDATE 1 参数，之后可以使用 INSERT INTO 命令更新已经写入的相同时间戳数据。UPDATE 参数不支持 ALTER DATABASE 命令修改。没有使用 UPDATE 1 参数创建的数据库，写入相同时间戳的数据不会修改之前的数据，也不会报错。

另需注意，在 UPDATE 设置为 0 时，后发送的相同时间戳的数据会被直接丢弃，但并不会报错，而且仍然会被计入 affected rows （所以不能利用 INSERT 指令的返回信息进行时间戳查重）。这样设计的主要原因是，TDengine 把写入的数据看做一个数据流，无论时间戳是否出现冲突，TDengine 都认为产生数据的原始设备真实地产生了这样的数据。UPDATE 参数只是控制这样的流数据在进行持久化时要怎样处理——UPDATE 为 0 时，表示先写入的数据覆盖后写入的数据；而 UPDATE 为 1 时，表示后写入的数据覆盖先写入的数据。这种覆盖关系如何选择，取决于对数据的后续使用和统计中，希望以先还是后生成的数据为准。

## 10. 我怎么创建超过1024列的表？

使用2.0及其以上版本，默认支持1024列；2.0之前的版本，TDengine最大允许创建250列的表。但是如果确实超过限值，建议按照数据特性，逻辑地将这个宽表分解成几个小表。

## 11. 最有效的写入数据的方法是什么？

批量插入。每条写入语句可以一张表同时插入多条记录，也可以同时插入多张表的多条记录。

## 12. Windows系统下插入的nchar类数据中的汉字被解析成了乱码如何解决？

Windows下插入nchar类的数据中如果有中文，请先确认系统的地区设置成了中国（在Control Panel里可以设置），这时cmd中的`taos`客户端应该已经可以正常工作了；如果是在IDE里开发Java应用，比如Eclipse， Intellij，请确认IDE里的文件编码为GBK（这是Java默认的编码类型），然后在生成Connection时，初始化客户端的配置，具体语句如下：
```JAVA
Class.forName("com.taosdata.jdbc.TSDBDriver");
Properties properties = new Properties();
properties.setProperty(TSDBDriver.LOCALE_KEY, "UTF-8");
Connection = DriverManager.getConnection(url, properties);
```

## 13.JDBC报错： the excuted SQL is not a DML or a DDL？

请更新至最新的JDBC驱动
```xml
<dependency>
  <groupId>com.taosdata.jdbc</groupId>
  <artifactId>taos-jdbcdriver</artifactId>
  <version>2.0.27</version>
</dependency>
```

## 14. taos connect failed, reason&#58; invalid timestamp

常见原因是服务器和客户端时间没有校准，可以通过和时间服务器同步的方式（Linux 下使用 ntpdate 命令，Windows 在系统时间设置中选择自动同步）校准。

## 15. 表名显示不全

由于 taos shell 在终端中显示宽度有限，有可能比较长的表名显示不全，如果按照显示的不全的表名进行相关操作会发生 Table does not exist 错误。解决方法可以是通过修改 taos.cfg 文件中的设置项 maxBinaryDisplayWidth， 或者直接输入命令 set max_binary_display_width 100。或者在命令结尾使用 \G 参数来调整结果的显示方式。

## 16. 如何进行数据迁移？

TDengine是根据hostname唯一标志一台机器的，在数据文件从机器A移动机器B时，注意如下两件事：

- 2.0.0.0 至 2.0.6.x 的版本，重新配置机器B的hostname为机器A的hostname
- 2.0.7.0 及以后的版本，到/var/lib/taos/dnode下，修复dnodeEps.json的dnodeId对应的FQDN，重启。确保机器内所有机器的此文件是完全相同的。
- 1.x 和 2.x 版本的存储结构不兼容，需要使用迁移工具或者自己开发应用导出导入数据。

## 17. 如何在命令行程序 taos 中临时调整日志级别

为了调试方便，从 2.0.16 版本开始，命令行程序 taos 新增了与日志记录相关的两条指令：

```mysql
ALTER LOCAL flag_name flag_value;
```

其含义是，在当前的命令行程序下，修改一个特定模块的日志记录级别（只对当前命令行程序有效，如果 taos 命令行程序重启，则需要重新设置）：
- flag_name 的取值可以是：debugFlag，cDebugFlag，tmrDebugFlag，uDebugFlag，rpcDebugFlag
- flag_value 的取值可以是：131（输出错误和警告日志），135（ 输出错误、警告和调试日志），143（ 输出错误、警告、调试和跟踪日志）

```mysql
ALTER LOCAL RESETLOG;
```

其含义是，清空本机所有由客户端生成的日志文件。

<a class="anchor" id="timezone"></a>
## 18. 时间戳的时区信息是怎样处理的？

TDengine 中时间戳的时区总是由客户端进行处理，而与服务端无关。具体来说，客户端会对 SQL 语句中的时间戳进行时区转换，转为 UTC 时区（即 Unix 时间戳——Unix Timestamp）再交由服务端进行写入和查询；在读取数据时，服务端也是采用 UTC 时区提供原始数据，客户端收到后再根据本地设置，把时间戳转换为本地系统所要求的时区进行显示。

客户端在处理时间戳字符串时，会采取如下逻辑：
1. 在未做特殊设置的情况下，客户端默认使用所在操作系统的时区设置。
2. 如果在 taos.cfg 中设置了 timezone 参数，则客户端会以这个配置文件中的设置为准。
3. 如果在 C/C++/Java/Python 等各种编程语言的 Connector Driver 中，在建立数据库连接时显式指定了 timezone，那么会以这个指定的时区设置为准。例如 Java Connector 的 JDBC URL 中就有 timezone 参数。
4. 在书写 SQL 语句时，也可以直接使用 Unix 时间戳（例如 `1554984068000`）或带有时区的时间戳字符串，也即以 RFC 3339 格式（例如 `2013-04-12T15:52:01.123+08:00`）或 ISO-8601 格式（例如 `2013-04-12T15:52:01.123+0800`）来书写时间戳，此时这些时间戳的取值将不再受其他时区设置的影响。

<a class="anchor" id="port"></a>
## 19. TDengine 都会用到哪些网络端口？

在 TDengine 2.0 版本中，会用到以下这些网络端口（以默认端口 6030 为前提进行说明，如果修改了配置文件中的设置，那么这里列举的端口都会出现变化），管理员可以参考这里的信息调整防火墙设置：

| 协议 | 默认端口   | 用途说明                         | 修改方法                        |
| :--- | :-------- | :---------------------------------- | :------------------------------- |
| TCP | 6030      | 客户端与服务端之间通讯。            | 由配置文件设置 serverPort 决定。 |
| TCP | 6035      | 多节点集群的节点间通讯。            | 随 serverPort 端口变化。        |
| TCP | 6040      | 多节点集群的节点间数据同步。        | 随 serverPort 端口变化。         |
| TCP | 6041      | 客户端与服务端之间的 RESTful 通讯。 | 随 serverPort 端口变化。        |
| TCP | 6042      | Arbitrator 的服务端口。           | 因 Arbitrator 启动参数设置变化。 |
| TCP | 6060      | 企业版内 Monitor 服务的网络端口。   |                               |
| UDP | 6030-6034 | 客户端与服务端之间通讯。            | 随 serverPort 端口变化。        |
| UDP | 6035-6039 | 多节点集群的节点间通讯。            | 随 serverPort 端口变化。        |
