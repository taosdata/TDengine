---
title: 常见问题及反馈
description: 一些常见问题的解决方法汇总
---

## 问题反馈

如果 FAQ 中的信息不能够帮到您，需要 TDengine TSDB 技术团队的技术支持与协助，请将以下两个目录中内容打包：

1. `/var/log/taos` （如果没有修改过默认路径）
2. `/etc/taos` （如果没有指定其他配置文件路径）

附上必要的问题描述，包括使用的 TDengine TSDB 版本信息、平台环境信息、发生该问题的执行操作、出现问题的表征及大概的时间，在 [GitHub](https://github.com/taosdata/TDengine) 提交 issue。

为了保证有足够的 debug 信息，如果问题能够重复，请修改 `/etc/taos/taos.cfg` 文件，最后面添加一行 `debugFlag 135`，然后重启 taosd, 重复问题，然后再递交。也可以通过如下 SQL 语句，临时设置 taosd 的日志级别。

```sql
  alter dnode <dnode_id> 'debugFlag' '135';
```

其中 dnode_id 请从 `show dnodes` 命令输出中获取。

但系统正常运行时，请一定将 debugFlag 设置为 `131`，否则会产生大量的日志信息，降低系统效率。

<!-- markdownlint-disable MD051 -->
## 常见问题列表

- [1. 安装与部署](#安装与部署)
- [2. 连接](#连接)
- [3. 数据写入](#数据写入)
- [4. 数据查询](#数据查询)
- [5. 数据订阅（TMQ）](#数据订阅)
- [6. 运维与监控](#运维与监控)
- [7. 升级与迁移](#升级与迁移)
- [8. 客户端与工具](#客户端与工具)
<!-- markdownlint-enable MD051 -->

## 1. 安装与部署 {#安装与部署}

### 1.1 Windows 平台运行 TDengine TSDB 出现丢失 MSVCP1400.DLL 解决方法？

1. 重新安装 Microsoft Visual C++ Redistributable‌：由于 msvcp140.dll 是 Microsoft Visual C++ Redistributable 的一部分，重新安装这个包通常可以解决大部分问题。可以从 Microsoft 官方网站下载相应的版本进行安装‌
2. 手动上网下载并替换 msvcp140.dll 文件‌：可以从可靠的源下载 msvcp140.dll 文件，并将其复制到系统的相应目录下。确保下载的文件与您的系统架构（32 位或 64 位）相匹配，并确保来源的安全性‌

### 1.2 go 语言编写组件编译失败怎样解决？

TDengine TSDB 3.0 版本包含一个使用 go 语言开发的 taosAdapter 独立组件，需要单独运行，提供 restful 接入功能以及支持多种其他软件（Prometheus、Telegraf、collectd、StatsD 等）的数据接入功能。
使用最新 develop 分支代码编译需要先 `git submodule update --init --recursive` 下载 taosAdapter 仓库代码后再编译。

go 语言版本要求 1.14 以上，如果发生 go 编译错误，往往是国内访问 go mod 问题，可以通过设置 go 环境变量来解决：

```sh
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
```

### 1.3 如果从 Docker Hub 拉取 TDengine 镜像失败，应该如何解决？{#docker-hub-failure}

如果无法正常访问 Docker Hub 官方仓库（hub.docker.com），可以尝试以下方法解决：

- 检查网络连接是否正常。
- 从 [TDengine 下载中心](https://www.taosdata.com/download-center) 下载镜像文件，然后使用 `docker load` 命令加载镜像，用法详见下载页面的使用说明。
- 尝试使用其他镜像源，例如：[CNIX Internal Container Registry Mirror](https://m.ixdev.cn/)，该镜像源与涛思数据无关，如果失效，请尝试更换网络上的其它镜像源，用法详见镜像源的使用说明。

### 1.4 内网环境如何获取 JDBC 驱动的所有依赖 jar？

问题描述：
私有化部署的内网环境无法访问 Maven 中央仓库，需要将 JDBC 驱动及其所有依赖 jar 上传至内网私服。

问题解决：
下载 TDengine JDBC 驱动源码后，执行以下命令将所有编译期依赖的 jar 导出到 `./lib` 目录：

```bash
mvn dependency:copy-dependencies -DoutputDirectory=./lib -DincludeScope=compile
```

然后将 `./lib` 目录下的所有 jar 上传至内网私服即可。

## 2. 连接 {#连接}

### 2.1 遇到错误 "Unable to establish connection" 怎么办？

客户端遇到连接故障，请按照下面的步骤进行检查：

1. 检查网络环境

- 云服务器：检查云服务器的安全组是否打开 TCP/UDP 端口 6030/6041 的访问权限
- 本地虚拟机：检查网络能否 ping 通，尽量避免使用 `localhost` 作为 hostname
- 公司服务器：如果为 NAT 网络环境，请务必检查服务器能否将消息返回值客户端

2. 确保客户端与服务端版本号是完全一致的，开源社区版和企业版也不能混用

3. 在服务器，执行 `systemctl status taosd` 检查 *taosd* 运行状态。如果没有运行，启动 *taosd*

4. 确认客户端连接时指定了正确的服务器 FQDN (Fully Qualified Domain Name —— 可在服务器上执行 Linux/macOS 命令 `hostname -f` 获得），FQDN 配置参考 [一篇文章说清楚 TDengine 的 FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html)。

5. ping 服务器 FQDN，如果没有反应，请检查你的网络，DNS 设置，或客户端所在计算机的系统 hosts 文件。如果部署的是 TDengine 集群，客户端需要能 ping 通所有集群节点的 FQDN。

6. 检查防火墙设置（Ubuntu 使用 ufw status，CentOS 使用 firewall-cmd --list-port），确保集群中所有主机在端口 6030/6041 上的 TCP/UDP 协议能够互通。

7. 对于 Linux 上的 JDBC（ODBC、Python、Go 等接口类似）连接，确保 *libtaos.so* 在目录 */usr/local/taos/driver* 里，并且 */usr/local/taos/driver* 在系统库函数搜索路径 *LD_LIBRARY_PATH* 里

8. 对于 macOS 上的 JDBC（ODBC、Python、Go 等接口类似）连接，确保 *libtaos.dylib* 在目录 */usr/local/lib* 里，并且 */usr/local/lib* 在系统库函数搜索路径 *LD_LIBRARY_PATH* 里

9. 对于 Windows 上的 JDBC、ODBC、Python、Go 等连接，确保 *C:\TDengine\driver\taos.dll* 在你的系统库函数搜索目录里 (建议 *taos.dll* 放在目录 *C:\Windows\System32*)

10. 如果仍不能排除连接故障

- Linux/macOS 系统请使用命令行工具 nc 来分别判断指定端口的 TCP 和 UDP 连接是否通畅
   检查 UDP 端口连接是否工作：`nc -vuz {hostIP} {port}`
   检查服务器侧 TCP 端口连接是否工作：`nc -l {port}`
   检查客户端侧 TCP 端口连接是否工作：`nc {hostIP} {port}`

- Windows 系统请使用 PowerShell 命令 `Test-NetConnection -ComputerName \{fqdn} -Port \{port}` 检测服务段端口是否访问

11. 也可以使用 taos 程序内嵌的网络连通检测功能，来验证服务器和客户端之间指定的端口连接是否通畅：[运维指南](../../operation)。

### 2.2 遇到错误 "Unable to resolve FQDN" 怎么办？

产生这个错误，是由于客户端或数据节点无法解析 FQDN(Fully Qualified Domain Name) 导致。对于 TDengine TSDB CLI 或客户端应用，请做如下检查：

1. 请检查连接的服务器的 FQDN 是否正确，FQDN 配置参考：[一篇文章说清楚 TDengine 的 FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html)
2. 如果网络配置有 DNS server，请检查是否正常工作
3. 如果网络没有配置 DNS server，请检查客户端所在机器的 hosts 文件，查看该 FQDN 是否配置，并是否有正确的 IP 地址
4. 如果网络配置 OK，从客户端所在机器，你需要能 Ping 该连接的 FQDN，否则客户端是无法连接服务器的
5. 如果服务器曾经使用过 TDengine TSDB，且更改过 hostname，建议检查 data 目录的 dnode.json 是否符合当前配置的 EP，路径默认为/var/lib/taos/dnode。正常情况下，建议更换新的数据目录或者备份后删除以前的数据目录，这样可以避免该问题。
6. 检查 /etc/hosts 和/etc/hostname 是否是预配置的 FQDN

### 2.3 为什么 RESTful 接口无响应、Grafana 无法添加 TDengine TSDB 为数据源、TDengine TSDB GUI 选了 6041 端口还是无法连接成功？

这个现象可能是因为 taosAdapter 没有被正确启动引起的，需要执行：```systemctl start taosadapter``` 命令来启动 taosAdapter 服务。

需要说明的是，taosAdapter 的日志路径 path 需要单独配置，默认路径是 /var/log/taos；日志等级 logLevel 有 8 个等级，默认等级是 info，配置成 panic 可关闭日志输出。请注意操作系统 `/` 目录的空间大小，可通过命令行参数、环境变量或配置文件来修改配置，默认配置文件是 /etc/taos/taosadapter.toml。

有关 taosAdapter 组件的详细介绍请看文档：[taosAdapter](../../reference/components/taosadapter/)

### 2.4 客户端连接串如何保证高可用？

请看为此问题撰写的 [技术博客](https://www.taosdata.com/blog/2021/04/16/2287.html)

### 2.5 遇到报错 "DND ERROR Version not compatible, client: 3000700, server: 3020300"

说明客户端和服务端版本不兼容，这里 client 的版本是 3.0.7.0，server 版本是 3.2.3.0。目前的兼容策略是前三位一致，client 和 server 才能兼容。

### 2.6 修改 database 的 root 密码后，启动 taos 遇到报错 "failed to connect to server, reason: Authentication failure"

默认情况，启动 taos 服务会使用系统默认的用户名（root）和密码尝试连接 taosd，在 root 密码修改后，启用 taos 连接就需要指明用户名和密码，例如 `taos -h xxx.xxx.xxx.xxx -u root -p`，然后输入新密码进行连接。修改密码后，您还需要相应地修改 taosKeeper 组件的配置文件（默认位于 /etc/taos/taoskeeper.toml），修改其访问 TDengine TSDB 的密码后重启服务。

如果是容器化部署，请参考部署文档中 Docker 部署章节的“[自定义密码、升级与健康检查](../../operation/deployment#custom-passwords-upgrades-and-health-checks)”说明。

其中，`3.3.6.6` 版本开始支持 `TAOS_ROOT_PASSWORD`，`3.3.8.8` 及以上版本支持 `TAOS_ROOT_PASSWORD_FILE` 并可直接升级，`3.4.1.0` 及以上版本支持 `taos-check startup` 和 `taos-check service`。

### 2.7 遇到报错 "some vnode/qnode/mnode(s) out of service" 怎么办？

客户端未配置所有服务端的 FQDN 解析。比如服务端有 3 个节点，客户端只配置了 1 个节点的 FQDN 解析。FQDN 配置参考：[一篇文章说清楚 TDengine 的 FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html)

### 2.8 第一次连接集群时遇到 "Sync leader is unreachable" 怎么办？

报这个错，说明第一次向集群的连接是成功的，但第一次访问的 IP 不是 mnode 的 leader 节点，客户端试图与 leader 建立连接时发生错误。客户端通过 EP，也就是指定的 fqdn 与端口号寻找 leader 节点，常见的报错原因有两个：

- 集群中其他节点的端口没有打开
- 客户端的 hosts 未正确配置

因此用户首先要检查服务端，集群的所有端口（原生连接默认 6030，http 连接默认 6041）有无打开；其次是客户端的 hosts 文件中是否配置了集群所有节点的 fqdn 与 IP 信息。
如仍无法解决，则需要联系涛思技术人员支持。

### 2.9 加载动态库失败，报错 "No such file or directory" 或 "failed to load libtaosws.so" 怎么办？

问题描述：
在使用 TDengine TSDB 客户端应用（taos-CLI、taosBenchmark、taosdump 等）或客户端连接器（如 Java、Python、Go 等）时，可能会遇到加载动态链接库`libtaosnative.so`或`libtaosws.so`失败的错误。
如：failed to load libtaosws.so since No such file or directory [0x80FF0002]

问题原因：
这是由于客户端无法找到所需的动态链接库文件，可能是因为文件未正确安装，或系统的库路径未正确配置等原因。

问题解决：

- **检查文件**：检查系统共享库目录下是否存在 `libtaosnative.so` 或 `libtaosws.so` 软链文件及相应实体文件也完整，如软链或实体文件已不存在，在 TDengine TSDB 客户端或服务器安装包中均包含这些文件，请重新安装。
- **检查环境变量**：确保系统加载共享库目录环境变量`LD_LIBRARY_PATH` 包含 `libtaosnative.so` 或 `libtaosws.so` 文件所在目录，若未包含，可通过 `export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:<new_path>` 添加。
- **检查权限**：确保当前用户对 `libtaosnative.so` 或 `libtaosws.so` 软链及其实体文件具有读取和执行权限。
- **检查文件损坏**：可以通过 `readelf -h 库文件` 检查库文件是否完整。
- **检查文件依赖**：可以通过 `ldd 库文件` 查看库文件的依赖项是否完整，确保所有依赖项均已正确安装且可访问。

### 2.10 Windows 平台下 JDBCDriver 找不到动态链接库，怎么办？

请看为此问题撰写的 [技术博客](https://www.taosdata.com/blog/2019/12/03/950.html)。

### 2.11 JDBC 原生连接报错 "no taos in java.library.path" 或 "UnsatisfiedLinkError" 怎么办？

问题描述：
使用 JDBC 原生连接时，遇到类似 `java.lang.UnsatisfiedLinkError: no taos in java.library.path` 的报错。

问题原因：
客户端无法找到 libtaos 动态库，通常是因为未安装 taosc 客户端，或 `java.library.path` 未包含库所在目录。

问题解决：

1. 检查是否已安装 TDengine 客户端（taosc）。
2. 检查 `java.library.path` 是否包含 libtaos 所在目录（Linux/macOS 一般为 `/usr/local/taos/driver`，Windows 为 `C:\TDengine\driver`），若未包含，可通过 JVM 启动参数 `-Djava.library.path=<path>` 指定，或将目录加入系统 `LD_LIBRARY_PATH`（Linux/macOS）/ `PATH`（Windows）环境变量。
3. macOS 用户若版本较低，建议升级到新版 TDengine 客户端。

### 2.12 JDBC 原生连接报错 "Operation not permitted" 怎么办？

问题描述：
使用 JDBC 原生连接时，遇到 `Operation not permitted` 报错。

问题原因：
当前用户没有写日志文件的权限，导致客户端初始化失败。

问题解决：
检查 TDengine 日志目录（默认 `/var/log/taos`）的权限，确保运行 Java 应用的用户对该目录具有写权限，或通过 `logDir` 配置项指定一个有写权限的目录。

### 2.13 JDBC WebSocket 连接超时，报错 "can't create connection with server within: 60000 milliseconds"（错误码 0x231d）怎么办？

问题描述：
调用栈类似：

```plaintext
java.sql.SQLException: ERROR (0x231d): can't create connection with server within: 60000 milliseconds
        at com.taosdata.jdbc.ws.Transport.checkConnection(Transport.java:393)
```

问题原因：
网络不通、端口未开放、Nginx 转发配置错误或 jar 包冲突均可能导致此问题。

问题解决：

1. 检查网络连通性，确认 taosAdapter 所在主机的端口（默认 6041）可访问：

   ```bash
   ping <adapterIp>
   telnet <host> 6041
   ```

2. 若已安装 taos 客户端，可用以下命令直接测试 Adapter 连接：

   ```bash
   taos -Z 1 -h <host> -P 6041
   ```

3. 若未安装 taos 客户端，可用 curl 验证 Adapter HTTP 端口是否正常：

   ```bash
   curl -L -H "Authorization: Basic cm9vdDp0YW9zZGF0YQ==" \
     -d "show databases;" \
     <host>:6041/rest/sql
   ```

4. 若配置了 Nginx 转发，请检查 Nginx 是否正确支持 WebSocket（见问题 2.14）。
5. 若以上均正常，请执行 `mvn dependency:tree` 排查是否存在 JSON 解析库冲突（如 jackson、fastjson 版本冲突）。

### 2.14 JDBC 通过 Nginx 转发时报错 "WebSocket handshake error, code: 400 Bad Request" 怎么办？

问题描述：
JDBC 通过 Nginx 反向代理连接 taosAdapter 时，报错 `WebSocket handshake error, code: 400 Bad Request`。

问题原因：
Nginx 未配置 WebSocket 升级所需的 HTTP 头信息。

问题解决：
在 Nginx 配置中添加 WebSocket 支持：

```nginx
location /ws {
    proxy_pass http://<taosadapter>:6041;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "Upgrade";
    proxy_set_header Host $host;
}
```

### 2.15 JDBC 使用 Nginx 转发时连接频繁断开怎么办？

问题描述：
通过 Nginx 转发使用 JDBC WebSocket 连接，一段时间后连接会自动断开。

问题原因：
Nginx 的 `proxy_send_timeout` 和 `proxy_read_timeout` 设置过短，导致长连接被提前关闭。

问题解决：
在 Nginx 配置中适当调大超时参数：

```nginx
proxy_send_timeout 3600s;
proxy_read_timeout 3600s;
```

### 2.16 JDBC 连接池应该如何配置（以 HikariCP 为例）？

建议配置如下，无需配置 `validationQuery`（JDBC 3.7.5 及以上版本对 `isValid` 接口已做缓存优化）：

```java
config.setMinimumIdle(10);           // 最小空闲连接数
config.setMaximumPoolSize(10);       // 连接池最大连接数
config.setConnectionTimeout(30000);  // 获取连接的最大等待时间（毫秒）
config.setMaxLifetime(0);            // 连接最大存活时间，0 表示无限制
config.setIdleTimeout(0);            // 空闲连接回收时间，0 表示无限制
```

可通过 `show connections;` 命令查看当前连接数，验证连接池配置是否生效。

## 3. 数据写入 {#数据写入}

### 3.1 最有效的写入数据的方法是什么？

批量插入。每条写入语句可以一张表同时插入多条记录，也可以同时插入多张表的多条记录。

### 3.2 Windows 系统下插入的 nchar 类数据中的汉字被解析成了乱码如何解决？

Windows 下插入 nchar 类的数据中如果有中文，请先确认系统的地区设置成了中国（在 Control Panel 里可以设置），这时 cmd 中的 `taos` 客户端应该已经可以正常工作了；如果是在 IDE 里开发 Java 应用，比如 Eclipse，IntelliJ，请确认 IDE 里的文件编码为 GBK（这是 Java 默认的编码类型），然后在生成 Connection 时，初始化客户端的配置，具体语句如下：

```JAVA
Class.forName("com.taosdata.jdbc.TSDBDriver");
Properties properties = new Properties();
properties.setProperty(TSDBDriver.LOCALE_KEY, "UTF-8");
Connection = DriverManager.getConnection(url, properties);
```

### 3.3 使用 taosBenchmark 测试工具写入数据查询很快，为什么我写入的数据查询非常慢？

TDengine TSDB 在写入数据时如果有很严重的乱序写入问题，会严重影响查询性能，所以需要在写入前解决乱序的问题。如果业务是从 Kafka 消费写入，请合理设计消费者，尽可能的一个子表数据由一个消费者去消费并写入，避免由设计产生的乱序。

### 3.4 数据库升级，从 2.6 升到 3.3，数据迁移同时业务还有数据在写入，会产生严重乱序吗？

这种情况通常不会产生乱序，首先我们来解释下 TDengine TSDB 中乱序是指什么？TDengine TSDB 中的乱序是指从时间戳为 0 开始按数据库设置的 Duration 参数（默认是 10 天）切割成时间窗口，在每个时间窗口中写入的数据不按顺序时间写入导致的现象为乱序现象，只要保证同一窗口是顺序写入的，即使窗口之间写入并非顺序，也不会产生乱序。

再看上面场景，补旧数据和新数据同时写入，新旧数据之间一般会存在较大距离，不会落在同一窗口中，只要保证新老数据都是顺序写的，即不会产生乱序现象。

### 3.5 JDBC 写入报错 "Invalid message"（错误码 0x115）怎么办？

问题描述：
使用 JDBC 自动建表写入时，遇到 `(0x115): Invalid message` 报错。

问题原因：
写入时指定的子表已在其他超级表下存在，导致冲突。

问题解决：
检查子表所属的超级表是否与写入语句中指定的超级表一致，确保不向错误的超级表写入数据。

### 3.6 Java 应用写入 TDengine 出现进程假死、QPS 接近 0 怎么办？

问题描述：
Java 写入应用运行一段时间后进程假死，几乎没有写入 QPS。

问题原因：
通常由 JVM GC 问题导致，而非 JDBC 驱动内存泄漏。

问题解决：

1. 执行 `top -Hp <进程ID>` 检查是否有大量 GC 线程占用 CPU。
2. 执行 `jstat -gcutil <进程ID> 3000 100` 观察 Young GC 是否停止、Full GC 是否占满所有时间。
3. 若确认是 GC 问题，排查应用代码中的内存分配，优化对象创建频率。

### 3.7 JDBC 写入性能低，如何排查和优化？

常见排查点：

1. **物理资源**：确认服务器使用的是 SSD 和万兆网络，本机压测时注意排除 WiFi 等低速网络的干扰。
2. **服务端压力**：通过监控查看 taosd 的 CPU、内存、网络、磁盘使用情况，若 CPU 很低说明请求未有效到达服务端。
3. **VGROUP 配置**：建库时默认 VGROUP 数为 2，若写入并发较高，需适当增大 VGROUP 数。
4. **Stmt 对象复用**：频繁创建 `PreparedStatement`（Stmt）对象会严重影响性能。正确做法是在应用初始化时创建好 Stmt 对象并持续复用，可参考[高效写入示例](https://docs.taosdata.com/develop/high/)。可通过 `show queries;` 验证是否存在长时间持续运行的参数绑定语句，以确认 Stmt 已被复用。
5. **ORM 框架**：使用 MyBatis 等 ORM 框架时，注意是否退化为逐条提交，可参考 [MyBatis 写入示例](https://github.com/taosdata/TDengine/blob/main/docs/examples/JDBC/mybatisplus-demo/src/test/java/com/taosdata/example/mybatisplusdemo/mapper/MetersMapperTest.java)。
6. **子表已存在时不传 TAG**：若确认子表已存在，可使用以下格式的 SQL 省略 TAG，提升性能：

   ```sql
   INSERT INTO meters (tbname, ts, current, voltage, phase) VALUES(?, ?, ?, ?, ?)
   ```

## 4. 数据查询 {#数据查询}

### 4.1 时间戳的时区信息是怎样处理的？

TDengine TSDB 中时间戳的时区总是由客户端进行处理，而与服务端无关。具体来说，客户端会对 SQL 语句中的时间戳进行时区转换，转为 UTC 时区（即 Unix 时间戳——Unix Timestamp）再交由服务端进行写入和查询；在读取数据时，服务端也是采用 UTC 时区提供原始数据，客户端收到后再根据本地设置，把时间戳转换为本地系统所要求的时区进行显示。

客户端在处理时间戳字符串时，会采取如下逻辑：

1. 在未做特殊设置的情况下，客户端默认使用所在操作系统的时区设置。
2. 如果在 taos.cfg 中设置了 timezone 参数，则客户端会以这个配置文件中的设置为准。
3. 如果在 C/C++/Java/Python 等各种编程语言的 Connector Driver 中，在建立数据库连接时显式指定了 timezone，那么会以这个指定的时区设置为准。例如 Java Connector 的 JDBC URL 中就有 timezone 参数。
4. 在书写 SQL 语句时，也可以直接使用 Unix 时间戳（例如 `1554984068000`）或带有时区的时间戳字符串，也即以 RFC 3339 格式（例如 `2013-04-12T15:52:01.123+08:00`）或 ISO-8601 格式（例如 `2013-04-12T15:52:01.123+0800`）来书写时间戳，此时这些时间戳的取值将不再受其他时区设置的影响。

### 4.2 在服务器上的使用 TDengine TSDB CLI 能查到指定时间段的数据，但在客户端机器上查不到？

这种情况是因为客户端与服务器上设置的时区不一致导致的，调整客户端与服务器的时区一致即可解决。

### 4.3 表名确认是存在的，但在写入或查询时返回表名不存在，什么原因？

TDengine TSDB 中的所有名称，包括数据库名、表名等都是区分大小写的，如果这些名称在程序或 TDengine TSDB CLI 中没有使用反引号（`）括起来使用，即使你输入的是大写的，引擎也会转化成小写来使用，如果名称前后加上了反引号，引擎就不会再转化成小写，会保持原样来使用。

### 4.4 我想统计下前后两条写入记录之间的时间差值是多少？

使用 DIFF 函数，可以查看时间列或数值列前后两条记录的差值，非常方便，详细说明见 SQL 手册->函数->DIFF

### 4.5 超级表带 TAG 过滤查子查数据与直接查子表哪个快？

直接查子表更快。超级表带 TAG 过滤查询子查数据是为满足查询方便性，同时可对多个子表中数据进行过滤，如果目的是追求性能并已明确查询子表，直接从子表查性能更高

## 5. 数据订阅（TMQ） {#数据订阅}

### 5.1 TMQ 订阅报错 "Unknown error: 65534"（错误码 0xfffe）怎么办？

问题描述：
使用 TMQ 订阅时，遇到 `subscribe topic error, code: (0xfffe), message: Unknown error: 65534` 报错。

问题原因：
订阅时 `Properties` 中传入了 TDengine 不支持的自定义属性。

问题解决：
检查创建 `TaosConsumer` 时传入的 `Properties`，移除 TDengine 不支持的属性键，仅保留 TDengine 文档中明确支持的配置项。

### 5.2 JDBC 订阅超级表时如何在消费数据中获取子表名？

如果订阅数据库或超级表，可在创建消费者时将 `value.deserializer` 设置为 `com.taosdata.jdbc.tmq.MapEnhanceDeserializer`，然后使用 `TaosConsumer<TMQEnhMap>` 类型创建消费者。这样每行数据可以反序列化为包含子表名和字段值的 `Map` 对象。

## 6. 运维与监控 {#运维与监控}

### 6.1 TDengine TSDB 3.0 都会用到哪些网络端口？

使用到的网络端口请看文档：[运维指南](../../operation)

需要注意，文档上列举的端口号都是以默认端口 6030 为前提进行说明，如果修改了配置文件中的设置，那么列举的端口都会随之出现变化，管理员可以参考上述的信息调整防火墙设置。

### 6.2 发生了 OOM 怎么办？

OOM 是操作系统的保护机制，当操作系统内存 (包括 SWAP) 不足时，会杀掉某些进程，从而保证操作系统的稳定运行。通常内存不足主要是如下两个原因导致，一是剩余内存小于 vm.min_free_kbytes；二是程序请求的内存大于剩余内存。还有一种情况是内存充足但程序占用了特殊的内存地址，也会触发 OOM。

TDengine TSDB 会预先为每个 VNode 分配好内存，每个 Database 的 VNode 个数受 建库时的 vgroups 参数影响，每个 VNode 占用的内存大小受 buffer 参数 影响。要防止 OOM，需要在项目建设之初合理规划内存，并合理设置 SWAP，除此之外查询过量的数据也有可能导致内存暴涨，这取决于具体的查询语句。TDengine TSDB 企业版对内存管理做了优化，采用了新的内存分配器，对稳定性有更高要求的用户可以考虑选择企业版。

### 6.3 在 macOS 上遇到 Too many open files 怎么办？

taosd 日志文件报错 Too many open file，是由于 taosd 打开文件数超过系统设置的上限所致。
解决方案如下：

1. 新建文件 /Library/LaunchDaemons/limit.maxfiles.plist，写入以下内容(以下示例将 limit 和 maxfiles 改为 10 万，可按需修改)：

```xml
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

```bash
sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
sudo chmod 644 /Library/LaunchDaemons/limit.maxfiles.plist
```

3. 加载 plist 文件 (或重启系统后生效。launchd 在启动时会自动加载该目录的 plist)

```bash
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
```

4. 确认更改后的限制

```bash
launchctl limit maxfiles
```

### 6.4 建库时提示 Out of dnodes 或者建表时提示 Vnodes exhausted

该提示是创建 db 的 vnode 数量不够了，需要的 vnode 不能超过了 dnode 中 vnode 的上限。因为系统默认是一个 dnode 中有 CPU 核数两倍的 vnode，也可以通过配置文件中的参数 supportVnodes 控制。
正常调大 taos.cfg 中 supportVnodes 参数即可。

### 6.5 如何查询数据占用的存储空间大小？

默认情况下，TDengine TSDB 的数据文件存储在 /var/lib/taos，日志文件存储在 /var/log/taos。

若想查看所有数据文件占用的具体大小，可以执行 Shell 指令：`du -sh /var/lib/taos/vnode --exclude='wal'` 来查看。此处排除了 WAL 目录，因为在持续写入的情况下，这里大小几乎是固定的，并且每当正常关闭 TDengine TSDB 让数据落盘后，WAL 目录都会清空。

若想查看单个数据库占用的大小，可在命令行程序 taos 内指定要查看的数据库后执行 `show vgroups;` ，通过得到的 VGroup id 去 /var/lib/taos/vnode 下查看包含的文件夹大小。

### 6.6 如何查看数据库的数据压缩率和磁盘占用指标？

TDengine TSDB 3.3.5.0 之前的版本，只提供以表为统计单位的压缩率，数据库及整体还未提供，查看命令是在客户端 TDengine TSDB CLI 中执行 `SHOW TABLE DISTRIBUTED table_name;` 命令，table_name 为要查看压缩率的表，可以为超级表、普通表及子表，详细可 [查看此处](https://docs.taosdata.com/reference/taos-sql/show/#show-table-distributed)

TDengine TSDB 3.3.5.0 及以上的版本，还提供了数据库整体压缩率和磁盘空间占用统计。查看数据库整体的数据压缩率和磁盘空间占用的命令为 `SHOW db_name.disk_info;`，查看数据库各个模块的磁盘空间占用的命令为 `SELECT * FROM INFORMATION_SCHEMA.INS_DISK_USAGE WHERE db_name='db_name';`，db_name 为要查看的数据库名称。详细可 [查看此处](https://docs.taosdata.com/reference/taos-sql/database/#%E6%9F%A5%E7%9C%8B%E6%95%B0%E6%8D%AE%E5%BA%93%E7%9A%84%E7%A3%81%E7%9B%98%E7%A9%BA%E9%97%B4%E5%8D%A0%E7%94%A8)

### 6.7 短时间内，通过 systemd 重启 taosd 超过一定次数后重启失败，报错：start-limit-hit

问题描述：
TDengine TSDB 3.3.5.1 及以上的版本，taosd.service 的 systemd 配置文件中，StartLimitInterval 参数从 60 秒调整为 900 秒。若在 900 秒内 taosd 服务重启达到 3 次，后续通过 systemd 启动 taosd 服务时会失败，执行 `systemctl status taosd.service` 显示错误：Failed with result 'start-limit-hit'。

问题原因：
TDengine TSDB 3.3.5.1 之前的版本，StartLimitInterval 为 60 秒。若在 60 秒内无法完成 3 次重启（例如，因从 WAL（预写式日志）中恢复大量数据导致启动时间较长），则下一个 60 秒周期内的重启会重新计数，导致系统持续不断地重启 taosd 服务。为避免无限重启问题，将 StartLimitInterval 由 60 秒调整为 900 秒。因此，在使用 systemd 短时间内多次启动 taosd 时遇到 start-limit-hit 错误的机率增多。

问题解决：
1）通过 systemd 重启 taosd 服务：推荐方法是先执行命令 `systemctl reset-failed taosd.service` 重置失败计数器，然后再通过 `systemctl restart taosd.service` 重启；若需长期调整，可手动修改 /etc/systemd/system/taosd.service 文件，将 StartLimitInterval 调小或将 StartLimitBurst 调大 (注：重新安装 taosd 会重置该参数，需要重新修改)，执行 `systemctl daemon-reload` 重新加载配置，然后再重启。2）也可以不通过 systemd 而是通过 taosd 命令直接重启 taosd 服务，此时不受 StartLimitInterval 和 StartLimitBurst 参数限制。

### 6.8 我确认修改了配置文件中参数但并没有生效？

问题描述：
TDengine TSDB 3.4.0.0 及以上的版本，有些用户可能会遇到一个问题：我在 `taos.cfg` 中修改了某个配置参数，但是重启后发现并没有生效，查看日志也找不到任何报错。

问题原因：
这是由于 TDengine TSDB 3.4.0.0 及以上版本，为了进一步提升 TDengine TSDB 的安全等级，防止恶意篡改配置文件，TDengine TSDB 禁止通过修改配置文件来改变配置参数，请您使用 ALTER 命令，通过 SQL 的方式修改配置参数的值。

### 6.9 如何让 TDengine TSDB crash 时生成 core 文件？

请看为此问题撰写的 [技术博客](https://www.taosdata.com/blog/2019/12/06/974.html)。

### 6.10 如何在命令行程序 taos 中临时调整日志级别

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

其含义是，在当前的命令行程序下，清空本机所有客户端生成的日志文件 (resetLog)，或修改一个特定模块的日志记录级别（只对当前命令行程序有效，如果 taos 命令行程序重启，则需要重新设置）：

- value 的取值可以是：131（输出错误和警告日志）、135（输出错误、警告和调试日志）、143（输出错误、警告、调试和跟踪日志）。

### 6.11 修改 database 的 root 密码后，Grafana 监控插件 TDinsight 无数据展示

TDinsight 插件中展示的数据是通过 taosKeeper 和 taosAdapter 服务收集并存储于 TD 的 log 库中，在 root 密码修改后，需要同步更新 taosKeeper 和 taosAdapter 配置文件中对应的密码信息，然后重启 taosKeeper 和 taosAdapter 服务（注：若是集群需要重启每个节点上的对应服务）。

### 6.12 为什么开源版 TDengine TSDB 的主进程会建立一个与公网的连接？

这个连接只会上报不涉及任何用户数据的最基本信息，用于官方了解产品在世界范围内的分布情况，进而优化产品，提升用户体验，具体采集项目为：集群名、操作系统版本、cpu 信息等。

该特性为可选配置项，在开源版中默认开启，具体参数为 telemetryReporting，在官方文档中有做说明，链接如下：[参数简介](https://docs.taosdata.com/reference/components/taosd/#%E7%9B%91%E6%8E%A7%E7%9B%B8%E5%85%B3)

您可以随时关闭该参数，只需要在 taos.cfg 中修改 telemetryReporting 为 0，然后重启数据库服务即可。

代码位于：[点击此处](https://github.com/taosdata/TDengine/blob/62e609c558deb764a37d1a01ba84bc35115a85a4/source/dnode/mnode/impl/src/mndTelem.c)

此外，对于安全性要求极高的企业版 TDengine TSDB Enterprise 来说，此参数不会工作。

### 6.13 同一台服务器，数据库的数据目录 dataDir 不变，为什么原有数据库丢失且集群 ID 发生了变化？

背景知识：TDengine TSDB 服务端进程（taosd）在启动时，若数据目录（dataDir，该目录在配置文件 taos.cfg 中指定）下不存在有效的数据文件子目录（如 mnode、dnode 和 vnode 等），则会自动创建这些目录。在创建新的 mnode 目录的同时，会分配一个新的集群 ID，从而产生一个新的集群。

原因分析：taosd 的数据目录 dataDir 可以指向多个不同的挂载点。如果这些挂载点未在 fstab 文件中配置自动挂载，服务器重启后，dataDir 将仅作为一个本地磁盘的普通目录存在，而未能按预期指向挂载的磁盘。此时，若 taosd 服务启动，它将在 dataDir 下新建目录，从而产生一个新的集群。

问题影响：服务器重启后，原有数据库丢失（注：并非真正丢失，只是原有的数据磁盘未挂载，暂时看不到）且集群 ID 发生变化，导致无法访问原有数据库。对于企业版用户，如果已针对集群 ID 进行授权，还会发现集群服务器的机器码未变，但原有的授权已失效。如果未针对该问题进行监控或者未及时发现并进行处理，则用户不会注意到原有数据库已经丢失，从而造成损失，增加运维成本。

问题解决：应在 fstab 文件中配置 dataDir 目录的自动挂载，确保 dataDir 始终指向预期的挂载点和目录，此时，再重启服务器，会找回原有的数据库和集群。在后续的版本中，我们将开发一个功能，使 taosd 在检测到启动前后 dataDir 发生变化时，在启动阶段退出，同时提供相应的错误提示。

## 7. 升级与迁移 {#升级与迁移}

### 7.1 TDengine TSDB 3.0 之前的版本升级到 3.0 及以上的版本应该注意什么？

3.0 版在之前版本的基础上，进行了完全的重构，配置文件和数据文件是不兼容的。在升级之前务必进行如下操作：

1. 删除配置文件，执行 `sudo rm -rf /etc/taos/taos.cfg`
2. 删除日志文件，执行 `sudo rm -rf /var/log/taos/`
3. 确保数据已经不再需要的前提下，删除数据文件，执行 `sudo rm -rf /var/lib/taos/`
4. 安装最新 3.0 稳定版本的 TDengine TSDB
5. 如果需要迁移数据或者数据文件损坏，请联系涛思数据官方技术支持团队，进行协助解决

### 7.2 如何进行数据迁移？

TDengine TSDB 是根据 hostname 唯一标志一台机器的，对于 3.0 版本，将数据文件从机器 A 移动机器 B 时，需要重新配置机器 B 的 hostname 为机器 A 的 hostname。

注：3.x 和 之前的 1.x、2.x 版本的存储结构不兼容，需要使用迁移工具或者自己开发应用导出导入数据。

## 8. 客户端与工具 {#客户端与工具}

### 8.1 Windows 系统下客户端无法正常显示中文字符？

Windows 系统中一般是采用 GBK/GB18030 存储中文字符，而 TDengine TSDB 的默认字符集为 UTF-8，在 Windows 系统中使用 TDengine TSDB 客户端时，客户端驱动会将字符统一转换为 UTF-8 编码后发送到服务端存储，因此在应用开发过程中，调用接口时正确配置当前的中文字符集即可。

在 Windows 10 环境下运行 TDengine TSDB 客户端命令行工具 taos 时，若无法正常输入、显示中文，可以对客户端 taos.cfg 做如下配置：

```bash
locale C
charset UTF-8
```

### 8.2 表名显示不全

由于 TDengine TSDB CLI 在终端中显示宽度有限，有可能比较长的表名显示不全，如果按照显示的不全的表名进行相关操作会发生 Table does not exist 错误。解决方法可以是通过修改 taos.cfg 文件中的设置项 maxBinaryDisplayWidth，或者直接输入命令 set max_binary_display_width 100。或者在命令结尾使用 \G 参数来调整结果的显示方式。

### 8.3 在 TDengine TSDB CLI 中查询，字段内容不能完全显示出来怎么办？

可以使用 `\G` 参数来竖式显示，如 `show databases\G;` （为了输入方便，在"\"后加 TAB 键，会自动补全后面的内容）

### 8.4 DBeaver 连接 TDengine 时中文或字符串显示乱码怎么办？

问题描述：
通过 DBeaver 查询 TDengine 中的字符串数据时，显示为乱码。

问题原因：
JDBC 驱动因历史原因将 `varchar` 类型当作 `binary` 处理，导致编码识别异常。

问题解决：
升级到较新版本的 JDBC 驱动，并在 DBeaver 的 JDBC 连接 URL 中添加相应参数。详细配置方法参考[官方文档](https://docs.taosdata.com/third-party/tool/dbeaver/)。
