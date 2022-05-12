---
title: 常见问题及反馈
---

## 问题反馈

如果 FAQ 中的信息不能够帮到您，需要 TDengine 技术团队的技术支持与协助，请将以下两个目录中内容打包：

1. /var/log/taos （如果没有修改过默认路径）
2. /etc/taos

附上必要的问题描述，包括使用的 TDengine 版本信息、平台环境信息、发生该问题的执行操作、出现问题的表征及大概的时间，在 [GitHub](https://github.com/taosdata/TDengine) 提交 issue。

为了保证有足够的 debug 信息，如果问题能够重复，请修改/etc/taos/taos.cfg 文件，最后面添加一行“debugFlag 135"(不带引号本身），然后重启 taosd, 重复问题，然后再递交。也可以通过如下 SQL 语句，临时设置 taosd 的日志级别。

```
  alter dnode <dnode_id> debugFlag 135;
```

但系统正常运行时，请一定将 debugFlag 设置为 131，否则会产生大量的日志信息，降低系统效率。

## 常见问题列表

**1. TDengine2.0 之前的版本升级到 2.0 及以上的版本应该注意什么？☆☆☆**

    2.0 版在之前版本的基础上，进行了完全的重构，配置文件和数据文件是不兼容的。在升级之前务必进行如下操作：

    1. 删除配置文件，执行 `sudo rm -rf /etc/taos/taos.cfg`
    2. 删除日志文件，执行 `sudo rm -rf /var/log/taos/`
    3. 确保数据已经不再需要的前提下，删除数据文件，执行 `sudo rm -rf /var/lib/taos/`
    4. 安装最新稳定版本的 TDengine
    5. 如果需要迁移数据或者数据文件损坏，请联系涛思数据官方技术支持团队，进行协助解决

**2. Windows 平台下 JDBCDriver 找不到动态链接库，怎么办？**

    请看为此问题撰写的[技术博客](https://www.taosdata.com/blog/2019/12/03/950.html)。

**3. 创建数据表时提示 more dnodes are needed**

    请看为此问题撰写的[技术博客](https://www.taosdata.com/blog/2019/12/03/965.html)。

**4. 如何让 TDengine crash 时生成 core 文件？**

    请看为此问题撰写的[技术博客](https://www.taosdata.com/blog/2019/12/06/974.html)。

**5. 遇到错误“Unable to establish connection”, 我怎么办？**

    客户端遇到连接故障，请按照下面的步骤进行检查：

    1. 检查网络环境

     - 云服务器：检查云服务器的安全组是否打开 TCP/UDP 端口 6030-6042 的访问权限
     - 本地虚拟机：检查网络能否 ping 通，尽量避免使用`localhost` 作为 hostname
     - 公司服务器：如果为 NAT 网络环境，请务必检查服务器能否将消息返回值客户端

    2. 确保客户端与服务端版本号是完全一致的，开源社区版和企业版也不能混用

    3. 在服务器，执行 `systemctl status taosd` 检查*taosd*运行状态。如果没有运行，启动*taosd*

    4. 确认客户端连接时指定了正确的服务器 FQDN (Fully Qualified Domain Name —— 可在服务器上执行 Linux 命令 hostname -f 获得），FQDN 配置参考：[一篇文章说清楚 TDengine 的 FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html)。

    5. ping 服务器 FQDN，如果没有反应，请检查你的网络，DNS 设置，或客户端所在计算机的系统 hosts 文件。如果部署的是 TDengine 集群，客户端需要能 ping 通所有集群节点的 FQDN。

    6. 检查防火墙设置（Ubuntu 使用 ufw status，CentOS 使用 firewall-cmd --list-port），确认 TCP/UDP 端口 6030-6042 是打开的

    7. 对于 Linux 上的 JDBC（ODBC, Python, Go 等接口类似）连接, 确保*libtaos.so*在目录*/usr/local/taos/driver*里, 并且*/usr/local/taos/driver*在系统库函数搜索路径*LD_LIBRARY_PATH*里

    8. 对于 Windows 上的 JDBC, ODBC, Python, Go 等连接，确保*C:\TDengine\driver\taos.dll*在你的系统库函数搜索目录里 (建议*taos.dll*放在目录 _C:\Windows\System32_)

    9. 如果仍不能排除连接故障

     - Linux 系统请使用命令行工具 nc 来分别判断指定端口的 TCP 和 UDP 连接是否通畅
       检查 UDP 端口连接是否工作：`nc -vuz {hostIP} {port} `
       检查服务器侧 TCP 端口连接是否工作：`nc -l {port}`
       检查客户端侧 TCP 端口连接是否工作：`nc {hostIP} {port}`

     - Windows 系统请使用 PowerShell 命令 Net-TestConnection -ComputerName {fqdn} -Port {port} 检测服务段端口是否访问

    10. 也可以使用 taos 程序内嵌的网络连通检测功能，来验证服务器和客户端之间指定的端口连接是否通畅（包括 TCP 和 UDP）：[TDengine 内嵌网络检测工具使用指南](https://www.taosdata.com/blog/2020/09/08/1816.html)。

**6. 遇到错误“Unexpected generic error in RPC”或者“Unable to resolve FQDN”，我怎么办？**

    产生这个错误，是由于客户端或数据节点无法解析 FQDN(Fully Qualified Domain Name)导致。对于 TAOS Shell 或客户端应用，请做如下检查：

    1. 请检查连接的服务器的 FQDN 是否正确，FQDN 配置参考：[一篇文章说清楚 TDengine 的 FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html)
    2. 如果网络配置有 DNS server，请检查是否正常工作
    3. 如果网络没有配置 DNS server，请检查客户端所在机器的 hosts 文件，查看该 FQDN 是否配置，并是否有正确的 IP 地址
    4. 如果网络配置 OK，从客户端所在机器，你需要能 Ping 该连接的 FQDN，否则客户端是无法连接服务器的
    5. 如果服务器曾经使用过 TDengine，且更改过 hostname，建议检查 data 目录的 dnodeEps.json 是否符合当前配置的 EP，路径默认为/var/lib/taos/dnode。正常情况下，建议更换新的数据目录或者备份后删除以前的数据目录，这样可以避免该问题。
    6. 检查/etc/hosts 和/etc/hostname 是否是预配置的 FQDN

**7. 虽然语法正确，为什么我还是得到 "Invalid SQL" 错误**

    如果你确认语法正确，2.0 之前版本，请检查 SQL 语句长度是否超过 64K。如果超过，也会返回这个错误。

**8. 是否支持 validation queries？**

    TDengine 还没有一组专用的 validation queries。然而建议你使用系统监测的数据库”log"来做。

<a class="anchor" id="update"></a>

**9. 我可以删除或更新一条记录吗？**

    TDengine 目前尚不支持删除功能，未来根据用户需求可能会支持。

    从 2.0.8.0 开始，TDengine 支持更新已经写入数据的功能。使用更新功能需要在创建数据库时使用 UPDATE 1 参数，之后可以使用 INSERT INTO 命令更新已经写入的相同时间戳数据。UPDATE 参数不支持 ALTER DATABASE 命令修改。没有使用 UPDATE 1 参数创建的数据库，写入相同时间戳的数据不会修改之前的数据，也不会报错。

    另需注意，在 UPDATE 设置为 0 时，后发送的相同时间戳的数据会被直接丢弃，但并不会报错，而且仍然会被计入 affected rows （所以不能利用 INSERT 指令的返回信息进行时间戳查重）。这样设计的主要原因是，TDengine 把写入的数据看做一个数据流，无论时间戳是否出现冲突，TDengine 都认为产生数据的原始设备真实地产生了这样的数据。UPDATE 参数只是控制这样的流数据在进行持久化时要怎样处理——UPDATE 为 0 时，表示先写入的数据覆盖后写入的数据；而 UPDATE 为 1 时，表示后写入的数据覆盖先写入的数据。这种覆盖关系如何选择，取决于对数据的后续使用和统计中，希望以先还是后生成的数据为准。

    此外，从 2.1.7.0 版本开始，支持将 UPDATE 参数设为 2，表示“支持部分列更新”。也即，当 UPDATE 设为 1 时，如果更新一个数据行，其中某些列没有提供取值，那么这些列会被设为 NULL；而当 UPDATE 设为 2 时，如果更新一个数据行，其中某些列没有提供取值，那么这些列会保持原有数据行中的对应值。

**10. 我怎么创建超过 1024 列的表？**

    使用 2.0 及其以上版本，默认支持 1024 列；2.0 之前的版本，TDengine 最大允许创建 250 列的表。但是如果确实超过限值，建议按照数据特性，逻辑地将这个宽表分解成几个小表。（从 2.1.7.0 版本开始，表的最大列数增加到了 4096 列。）

**11. 最有效的写入数据的方法是什么？**

    批量插入。每条写入语句可以一张表同时插入多条记录，也可以同时插入多张表的多条记录。

**12. Windows 系统下插入的 nchar 类数据中的汉字被解析成了乱码如何解决？**

    Windows 下插入 nchar 类的数据中如果有中文，请先确认系统的地区设置成了中国（在 Control Panel 里可以设置），这时 cmd 中的`taos`客户端应该已经可以正常工作了；如果是在 IDE 里开发 Java 应用，比如 Eclipse， Intellij，请确认 IDE 里的文件编码为 GBK（这是 Java 默认的编码类型），然后在生成 Connection 时，初始化客户端的配置，具体语句如下：

    ```JAVA
    Class.forName("com.taosdata.jdbc.TSDBDriver");
    Properties properties = new Properties();
    properties.setProperty(TSDBDriver.LOCALE_KEY, "UTF-8");
    Connection = DriverManager.getConnection(url, properties);
    ```

**13.JDBC 报错： the excuted SQL is not a DML or a DDL？**

    请更新至最新的 JDBC 驱动

    ```xml
    <dependency>
      <groupId>com.taosdata.jdbc</groupId>
      <artifactId>taos-jdbcdriver</artifactId>
      <version>2.0.27</version>
    </dependency>
    ```

**14. taos connect failed, reason&#58; invalid timestamp**

    常见原因是服务器和客户端时间没有校准，可以通过和时间服务器同步的方式（Linux 下使用 ntpdate 命令，Windows 在系统时间设置中选择自动同步）校准。

**15. 表名显示不全**

    由于 taos shell 在终端中显示宽度有限，有可能比较长的表名显示不全，如果按照显示的不全的表名进行相关操作会发生 Table does not exist 错误。解决方法可以是通过修改 taos.cfg 文件中的设置项 maxBinaryDisplayWidth， 或者直接输入命令 set max_binary_display_width 100。或者在命令结尾使用 \G 参数来调整结果的显示方式。

**16. 如何进行数据迁移？**

    TDengine 是根据 hostname 唯一标志一台机器的，在数据文件从机器 A 移动机器 B 时，注意如下两件事：

     - 2.0.0.0 至 2.0.6.x 的版本，重新配置机器 B 的 hostname 为机器 A 的 hostname。
     - 2.0.7.0 及以后的版本，到/var/lib/taos/dnode 下，修复 dnodeEps.json 的 dnodeId 对应的 FQDN，重启。确保机器内所有机器的此文件是完全相同的。
     - 1.x 和 2.x 版本的存储结构不兼容，需要使用迁移工具或者自己开发应用导出导入数据。

**17. 如何在命令行程序 taos 中临时调整日志级别**

    为了调试方便，从 2.0.16 版本开始，命令行程序 taos 新增了与日志记录相关的两条指令：

    ```sql
    ALTER LOCAL flag_name flag_value;
    ```

    其含义是，在当前的命令行程序下，修改一个特定模块的日志记录级别（只对当前命令行程序有效，如果 taos 命令行程序重启，则需要重新设置）：

     - flag_name 的取值可以是：debugFlag，cDebugFlag，tmrDebugFlag，uDebugFlag，rpcDebugFlag
     - flag_value 的取值可以是：131（输出错误和警告日志），135（ 输出错误、警告和调试日志），143（ 输出错误、警告、调试和跟踪日志）

    ```sql
    ALTER LOCAL RESETLOG;
    ```

    其含义是，清空本机所有由客户端生成的日志文件。

<a class="anchor" id="timezone"></a>

**18. go 语言编写组件编译失败怎样解决？**

    新版本 TDengine 2.3.0.0 包含一个使用 go 语言开发的 taosAdapter 独立组件，需要单独运行，取代之前 taosd 内置的 httpd ，提供包含原 httpd 功能以及支持多种其他软件（Prometheus、Telegraf、collectd、StatsD 等）的数据接入功能。
    使用最新 develop 分支代码编译需要先 `git submodule update --init --recursive` 下载 taosAdapter 仓库代码后再编译。

    目前编译方式默认自动编译 taosAdapter。go 语言版本要求 1.14 以上，如果发生 go 编译错误，往往是国内访问 go mod 问题，可以通过设置 go 环境变量来解决：

    ```sh
    go env -w GO111MODULE=on
    go env -w GOPROXY=https://goproxy.cn,direct
    ```

    如果希望继续使用之前的内置 httpd，可以关闭 taosAdapter 编译，使用
    `cmake .. -DBUILD_HTTP=true` 使用原来内置的 httpd。
