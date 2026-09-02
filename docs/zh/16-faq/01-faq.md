---
sidebar_label: 常见问题及反馈
title: 常见问题及反馈
description: 一些常见问题的解决方法汇总
---

## 问题反馈

如果 FAQ 中的信息无法解决你的问题，需要 TDengine 技术团队协助，请将以下两个目录的内容打包：

1. `/var/log/taos` （如果没有修改过默认路径）
2. `/etc/taos` （如果没有指定其他配置文件路径）

附上必要的问题描述，包括使用的 TDengine 版本信息、平台环境信息、复现该问题的操作步骤、问题现象及大致时间，并在 [GitHub](https://github.com/taosdata/TDengine) 提交 issue。

为获取足够的调试信息，如果问题可复现，请在 `/etc/taos/taos.cfg` 末尾添加一行 `debugFlag 135`，重启 `taosd` 后复现问题再提交。也可通过如下 SQL 临时设置 `taosd` 的日志级别。

```sql
ALTER DNODE <dnode_id> 'debugFlag' '135';
```

其中 `dnode_id` 可从 `SHOW DNODES` 的输出中获取。

系统正常运行时，须将 `debugFlag` 设为 `131`，否则会产生大量日志并降低系统效率。

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

### 1.1 Windows 平台运行 TDengine 出现丢失 `msvcp140.dll` 如何解决？

1. 重新安装 Microsoft Visual C++ Redistributable：`msvcp140.dll` 属于该运行库，重新安装通常可解决问题。请从 Microsoft 官网下载对应版本安装。
2. 手动下载并替换 `msvcp140.dll`：从可靠来源下载后复制到系统相应目录。请确认文件与系统架构（32 位或 64 位）匹配，并核验来源安全性。

### 1.2 Go 语言编写组件编译失败如何解决？

TDengine `v3.0` 起包含使用 Go 语言开发的独立组件 `taosAdapter`，需单独运行，提供 RESTful 接入，并支持 Prometheus、Telegraf、collectd、StatsD 等多种软件的数据接入。
使用最新 `develop` 分支编译时，须先执行 `git submodule update --init --recursive` 拉取 `taosAdapter` 仓库代码后再编译。

Go 语言版本要求 1.14 及以上。若出现 Go 编译错误，常见原因是国内访问 Go module 受限，可通过设置 Go 环境变量解决：

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
- 公司服务器：如果为 NAT 网络环境，请务必检查服务器能否将消息返回给客户端

2. 确保客户端与服务端版本号完全一致，TDengine TSDB-OSS 与 TDengine TSDB-Enterprise 也不能混用

3. 在服务器上执行 `systemctl status taosd` 检查 `taosd` 运行状态。如果没有运行，请启动 `taosd`

4. 确认客户端连接时指定了正确的服务器 FQDN（Fully Qualified Domain Name，可在服务器上执行 Linux/macOS 命令 `hostname -f` 获得）。FQDN 配置可参考 [一篇文章说清楚 TDengine 的 FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html)。

5. ping 服务器 FQDN，如果没有反应，请检查你的网络，DNS 设置，或客户端所在计算机的系统 hosts 文件。如果部署的是 TDengine 集群，客户端需要能 ping 通所有集群节点的 FQDN。

6. 检查防火墙设置（Ubuntu 使用 ufw status，CentOS 使用 firewall-cmd --list-port），确保集群中所有主机在端口 6030/6041 上的 TCP/UDP 协议能够互通。

7. 对于 Linux 上的 JDBC（ODBC、Python、Go 等接口类似）连接，确保 `libtaos.so` 位于 `/usr/local/taos/driver`，且该目录已加入系统库搜索路径 `LD_LIBRARY_PATH`

8. 对于 macOS 上的 JDBC（ODBC、Python、Go 等接口类似）连接，确保 `libtaos.dylib` 位于 `/usr/local/lib`，且该目录已加入系统库搜索路径 `LD_LIBRARY_PATH`

9. 对于 Windows 上的 JDBC、ODBC、Python、Go 等连接，确保 `C:\TDengine\driver\taos.dll` 位于系统库搜索目录（建议将 `taos.dll` 放到 `C:\Windows\System32`）

10. 如果仍不能排除连接故障

- Linux/macOS 系统请使用命令行工具 nc 来分别判断指定端口的 TCP 和 UDP 连接是否通畅
   检查 UDP 端口连接是否工作：`nc -vuz {hostIP} {port}`
   检查服务器侧 TCP 端口连接是否工作：`nc -l {port}`
   检查客户端侧 TCP 端口连接是否工作：`nc {hostIP} {port}`

- Windows 系统请使用 PowerShell 命令 `Test-NetConnection -ComputerName \{fqdn} -Port \{port}` 检测服务端端口是否可访问

11. 也可使用 `taos shell` 内嵌的网络连通检测功能，验证服务器与客户端之间指定端口是否通畅，详见 [运维指南](../12-operations-and-tooling/02-operations/index.md)。

### 2.2 遇到错误 "Unable to resolve FQDN" 怎么办？

该错误由客户端或数据节点无法解析 FQDN（Fully Qualified Domain Name）导致。对于 `taos` shell 或客户端应用，请做如下检查：

1. 请检查连接的服务器的 FQDN 是否正确，FQDN 配置参考：[一篇文章说清楚 TDengine 的 FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html)
2. 如果网络配置有 DNS server，请检查是否正常工作
3. 如果网络没有配置 DNS server，请检查客户端所在机器的 hosts 文件，查看该 FQDN 是否配置，并是否有正确的 IP 地址
4. 如果网络配置 OK，从客户端所在机器，你需要能 Ping 该连接的 FQDN，否则客户端是无法连接服务器的
5. 如果服务器曾经使用过 TDengine，且更改过 hostname，建议检查 data 目录下的 `dnode.json` 是否符合当前配置的 EP，路径默认为 `/var/lib/taos/dnode`。正常情况下，建议更换新的数据目录，或备份后删除旧数据目录，以避免该问题。
6. 检查 `/etc/hosts` 和 `/etc/hostname` 是否为预配置的 FQDN

### 2.3 为什么 RESTful 接口无响应、Grafana 无法添加 TDengine 为数据源、TDengine GUI 选了 6041 端口还是无法连接成功？

该现象可能是因为 `taosAdapter` 未正确启动，可执行 `systemctl start taosadapter` 启动服务。

说明：`taosAdapter` 的日志路径 `path` 需单独配置，默认路径为 `/var/log/taos`；日志等级 `logLevel` 共 8 级，默认 `info`，配置为 `panic` 可关闭日志输出。请注意操作系统根目录 `/` 的磁盘空间。可通过命令行参数、环境变量或配置文件修改，默认配置文件为 `/etc/taos/taosadapter.toml`。

有关 `taosAdapter` 的详细说明，详见 [taosAdapter 参考手册](../12-operations-and-tooling/03-components/03-taosadapter.md)

### 2.4 客户端连接串如何保证高可用？

详见为此问题撰写的 [技术博客](https://www.taosdata.com/blog/2021/04/16/2287.html)

### 2.5 遇到报错 "DND ERROR Version not compatible, client: 3000700, server: 3020300"

说明客户端与服务端版本不兼容。此处 client 版本为 `v3.0.7.0`，server 版本为 `v3.2.3.0`。当前兼容策略为版本号前三位一致时，客户端与服务端才可兼容。

### 2.6 修改 database 的 root 密码后，启动 taos 遇到报错 "failed to connect to server, reason: Authentication failure"

默认情况下，启动 `taos` shell 会使用默认用户名（`root`）和密码尝试连接 `taosd`。修改 `root` 密码后，连接时须显式指定用户名和密码，例如 `taos -h xxx.xxx.xxx.xxx -u root -p`，再输入新密码。修改密码后，还须同步更新 `taosKeeper` 配置文件（默认位于 `/etc/taos/taoskeeper.toml`）中访问 TDengine 的密码，并重启服务。

若为容器化部署，请参阅 Docker 部署章节中的 [自定义密码、升级与健康检查](../12-operations-and-tooling/02-operations/03-deployment/02-docker.md#custom-passwords-upgrades-and-health-checks)。

其中，`v3.3.6.6` 起支持 `TAOS_ROOT_PASSWORD`，`v3.3.8.8` 及以上支持 `TAOS_ROOT_PASSWORD_FILE` 并可直接升级，`v3.4.1.0` 及以上支持 `taos-check startup` 和 `taos-check service`。

### 2.7 遇到报错 "some vnode/qnode/mnode(s) out of service" 怎么办？

客户端未配置所有服务端的 FQDN 解析。比如服务端有 3 个节点，客户端只配置了 1 个节点的 FQDN 解析。FQDN 配置参考：[一篇文章说清楚 TDengine 的 FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html)

### 2.8 第一次连接集群时遇到 "Sync leader is unreachable" 怎么办？

报这个错，说明第一次向集群的连接是成功的，但第一次访问的 IP 不是 mnode 的 leader 节点，客户端试图与 leader 建立连接时发生错误。客户端通过 EP，也就是指定的 fqdn 与端口号寻找 leader 节点，常见的报错原因有两个：

- 集群中其他节点的端口没有打开
- 客户端的 hosts 未正确配置

因此请先检查服务端：集群所有端口（原生连接默认 `6030`，HTTP 连接默认 `6041`）是否已开放；再检查客户端 `hosts` 是否配置了集群全部节点的 FQDN 与 IP。
如仍无法解决，请联系涛思数据技术支持。

### 2.9 加载动态库失败，报错 "No such file or directory" 或 "failed to load libtaosws.so" 怎么办？

问题描述：
在使用 TDengine 客户端应用（`taos` shell、taosBenchmark、taosdump 等）或客户端连接器（如 Java、Python、Go 等）时，可能会遇到加载动态链接库 `libtaosnative.so` 或 `libtaosws.so` 失败的错误。
例如：`failed to load libtaosws.so since No such file or directory [0x80FF0002]`

问题原因：
这是由于客户端无法找到所需的动态链接库文件，可能是因为文件未正确安装，或系统的库路径未正确配置等原因。

问题解决：

- **检查文件**：检查系统共享库目录下是否存在 `libtaosnative.so` 或 `libtaosws.so` 软链文件及相应实体文件也完整，如软链或实体文件已不存在，在 TDengine 客户端或服务器安装包中均包含这些文件，请重新安装。
- **检查环境变量**：确保共享库搜索路径环境变量 `LD_LIBRARY_PATH` 包含 `libtaosnative.so` 或 `libtaosws.so` 所在目录；若未包含，可通过 `export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:<new_path>` 添加。
- **检查权限**：确保当前用户对 `libtaosnative.so` 或 `libtaosws.so` 软链及其实体文件具有读取和执行权限。
- **检查文件损坏**：可以通过 `readelf -h 库文件` 检查库文件是否完整。
- **检查文件依赖**：可以通过 `ldd 库文件` 查看库文件的依赖项是否完整，确保所有依赖项均已正确安装且可访问。

### 2.10 Windows 平台下 JDBCDriver 找不到动态链接库，怎么办？

详见为此问题撰写的 [技术博客](https://www.taosdata.com/blog/2019/12/03/950.html)。

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

Windows 下向 `NCHAR` 类型写入中文时，请先确认系统区域设置为中国（可在 Control Panel 中设置），此时命令提示符中的 `taos` shell 通常可正常工作；若在 IDE（如 Eclipse、IntelliJ）中开发 Java 应用，请确认 IDE 文件编码为 GBK（Java 默认编码类型），并在创建 Connection 时初始化客户端配置，示例如下：

```java
Class.forName("com.taosdata.jdbc.TSDBDriver");
Properties properties = new Properties();
properties.setProperty(TSDBDriver.LOCALE_KEY, "UTF-8");
Connection = DriverManager.getConnection(url, properties);
```

### 3.3 使用 taosBenchmark 测试工具写入数据查询很快，为什么我写入的数据查询非常慢？

TDengine 在写入时若存在严重乱序，会显著影响查询性能，因此须在写入前消除乱序。若业务从 Kafka 消费写入，请合理设计消费者，尽量保证一个子表的数据由同一消费者写入，避免设计引入乱序。

### 3.4 数据库从 `v2.6` 升级到 `v3.3`，迁移同时业务仍在写入，会产生严重乱序吗？

通常不会。TDengine 中的乱序是指：从时间戳 0 起按数据库 `DURATION` 参数（默认 10 天）划分时间窗口后，同一窗口内写入时间戳未按顺序写入。只要同一窗口内顺序写入，即使窗口之间的写入顺序交错，也不算乱序。

在上述场景中，补旧数据与新数据一般时间间隔较大，通常不落在同一窗口；只要新旧数据各自顺序写入，即不会产生乱序。

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
4. **Stmt 对象复用**：频繁创建 `PreparedStatement`（Stmt）对象会严重影响性能。正确做法是在应用初始化时创建好 Stmt 对象并持续复用，可参考 [高效写入](../10-developer-guide/05-high-throughput.md)。可通过 `SHOW QUERIES;` 验证是否存在长时间持续运行的参数绑定语句，以确认 Stmt 已被复用。
5. **ORM 框架**：使用 MyBatis 等 ORM 框架时，注意是否退化为逐条提交，可参考 [MyBatis 写入示例](https://github.com/taosdata/TDengine/blob/main/docs/examples/JDBC/mybatisplus-demo/src/test/java/com/taosdata/example/mybatisplusdemo/mapper/MetersMapperTest.java)。
6. **子表已存在时不传 TAG**：若确认子表已存在，可使用以下格式的 SQL 省略 TAG，提升性能：

   ```sql
   INSERT INTO meters (tbname, ts, current, voltage, phase) VALUES(?, ?, ?, ?, ?)
   ```

## 4. 数据查询 {#数据查询}

### 4.1 时间戳的时区信息如何处理？

TDengine 中时间戳的时区总是由客户端进行处理，而与服务端无关。具体来说，客户端会对 SQL 语句中的时间戳进行时区转换，转为 UTC 时区（即 Unix 时间戳——Unix Timestamp）再交由服务端进行写入和查询；在读取数据时，服务端也是采用 UTC 时区提供原始数据，客户端收到后再根据本地设置，把时间戳转换为本地系统所要求的时区进行显示。

客户端在处理时间戳字符串时，会采取如下逻辑：

1. 在未做特殊设置的情况下，客户端默认使用所在操作系统的时区设置。
2. 如果在 `taos.cfg` 中设置了 `timezone` 参数，则客户端以该配置为准。
3. 如果在 C/C++/Java/Python 等连接器建立连接时显式指定了 `timezone`，则以该指定时区为准。例如 Java 连接器的 JDBC URL 中有 `timezone` 参数。
4. 在书写 SQL 语句时，也可以直接使用 Unix 时间戳（例如 `1554984068000`）或带有时区的时间戳字符串，也即以 RFC 3339 格式（例如 `2013-04-12T15:52:01.123+08:00`）或 ISO-8601 格式（例如 `2013-04-12T15:52:01.123+0800`）来书写时间戳，此时这些时间戳的取值将不再受其他时区设置的影响。

### 4.2 在服务器上使用 `taos` shell 能查到指定时间段的数据，但在客户端机器上查不到？

这种情况是因为客户端与服务器上设置的时区不一致导致的，调整客户端与服务器的时区一致即可解决。

### 4.3 表名确认是存在的，但在写入或查询时返回表名不存在，什么原因？

TDengine 中的所有名称（包括数据库名、表名等）均区分大小写。若在程序或 `taos` shell 中未使用反引号括起名称，即便输入为大写，引擎也会转为小写；若使用了反引号，则保持原样。

### 4.4 如何统计前后两条写入记录之间的时间差值？

使用 `DIFF` 函数可查看时间列或数值列前后两条记录的差值，详见 [DIFF](../05-tdengine-sql/04-data-query/03-function.md#diff)。

### 4.5 超级表带 TAG 过滤查询子表数据与直接查子表哪个更快？

直接查询子表更快。使用 TAG 过滤查询超级表便于同时过滤多张子表的数据；若已明确目标子表且更看重性能，直接查询子表通常更快。

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

### 6.1 TDengine 3.0 都会用到哪些网络端口？

使用到的网络端口详见 [容量规划 · 网络端口要求](../12-operations-and-tooling/02-operations/01-planning.md#网络端口要求)。

说明：文档所列端口以默认端口 `6030` 为前提；若修改了配置文件中的端口相关设置，实际端口会随之变化，管理员可据此调整防火墙规则。

### 6.2 发生了 OOM 怎么办？

OOM 是操作系统的保护机制，当操作系统内存 (包括 SWAP) 不足时，会杀掉某些进程，从而保证操作系统的稳定运行。通常内存不足主要是如下两个原因导致，一是剩余内存小于 vm.min_free_kbytes；二是程序请求的内存大于剩余内存。还有一种情况是内存充足但程序占用了特殊的内存地址，也会触发 OOM。

TDengine 会预先为每个 vnode 分配内存。每个数据库的 vnode 数量受建库时 `vgroups` 参数影响，每个 vnode 占用的内存大小受 `buffer` 等参数影响。为防止 OOM，须在建设初期合理规划内存并配置 SWAP；查询过量数据也可能导致内存暴涨，具体取决于查询语句。TDengine TSDB-Enterprise 对内存管理做了优化并采用新的内存分配器，对稳定性要求更高的用户可考虑选用企业版。

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

该提示表示创建数据库所需的 vnode 数量不足，所需 vnode 数不能超过 dnode 的 vnode 上限。系统默认每个 dnode 可支持的 vnode 数为 CPU 核数的两倍，也可通过配置参数 `supportVnodes` 控制。通常调大 `taos.cfg` 中的 `supportVnodes` 即可。

### 6.5 如何查询数据占用的存储空间大小？

默认情况下，TDengine 的数据文件位于 `/var/lib/taos`，日志文件位于 `/var/log/taos`。

若需查看全部数据文件占用空间，可执行：`du -sh /var/lib/taos/vnode --exclude='wal'`。此处排除了 WAL 目录：持续写入时该目录大小通常较稳定，且正常关闭 TDengine 使数据落盘后 WAL 目录会被清空。

若需查看单个数据库占用空间，可在 `taos` shell 中指定数据库后执行 `SHOW VGROUPS;`，再根据得到的 vgroup id 到 `/var/lib/taos/vnode` 下查看对应目录大小。

### 6.6 如何查看数据库的数据压缩率和磁盘占用指标？

`v3.3.5.0` 之前的版本仅提供以表为统计单位的压缩率，尚未提供数据库级整体统计。可在客户端 `taos` shell 中执行 `SHOW TABLE DISTRIBUTED table_name;`，其中 `table_name` 可为超级表、普通表或子表。详见 [`SHOW TABLE DISTRIBUTED`](../05-tdengine-sql/09-system-info/03-show.md#show-table-distributed)。

`v3.3.5.0` 及以上版本还提供数据库整体压缩率与磁盘占用统计。查看整体压缩率与磁盘占用可执行 `SHOW db_name.DISK_INFO;`；查看各模块磁盘占用可执行 `SELECT * FROM INFORMATION_SCHEMA.INS_DISK_USAGE WHERE db_name='db_name';`，其中 `db_name` 为数据库名。详见 [查看数据库的磁盘空间占用](../05-tdengine-sql/02-ddl/01-database.md#查看数据库的磁盘空间占用)。

### 6.7 WAL 对存储空间和表观压缩率的影响

WAL（Write-Ahead Log，预写式日志）是 TDengine 保证数据可靠性的核心机制。所有写入请求在落盘到数据文件之前，均以 **原始未压缩格式** 写入 WAL。因此，在写入初期，WAL 尚未按策略清理时，其大小可能超过已压缩的数据文件，导致观察到的总存储占用 **暂时大于压缩后的数据量**，这是正常现象。随着数据持续写入，历史 WAL 会按配置自动清理，总存储占用将趋于稳定并趋近于实际压缩数据的大小。

### 6.8 短时间内，通过 systemd 重启 taosd 超过一定次数后重启失败，报错：start-limit-hit

问题描述：
`v3.3.5.1` 及以上版本中，`taosd.service` 的 systemd 配置将 `StartLimitInterval` 从 60 秒调整为 900 秒。若在 900 秒内 `taosd` 重启达到 3 次，后续通过 systemd 启动会失败，`systemctl status taosd.service` 可能显示：`Failed with result 'start-limit-hit'`。

问题原因：
`v3.3.5.1` 之前，`StartLimitInterval` 为 60 秒。若 60 秒内无法完成 3 次重启（例如从 WAL 恢复大量数据导致启动较慢），下一周期会重新计数，可能造成持续重启。为避免无限重启，将该参数调整为 900 秒，因此短时间内多次通过 systemd 启动更容易触发 `start-limit-hit`。

问题解决：

1. 通过 systemd 重启：推荐先执行 `systemctl reset-failed taosd.service` 重置失败计数，再执行 `systemctl restart taosd.service`。若需长期调整，可修改 `/etc/systemd/system/taosd.service`，减小 `StartLimitInterval` 或增大 `StartLimitBurst`（重新安装 `taosd` 会重置该文件，须再次修改），然后执行 `systemctl daemon-reload` 并重启。
2. 也可不经 systemd，直接运行 `taosd` 启动服务，此时不受 `StartLimitInterval` / `StartLimitBurst` 限制。

### 6.9 我确认修改了配置文件中参数但并没有生效？

问题描述：
`v3.4.0.0` 及以上版本中，部分用户可能遇到：已在 `taos.cfg` 中修改配置参数，但重启后未生效，日志中也无明显报错。

问题原因：
自 `v3.4.0.0` 起，为提升安全等级并防止恶意篡改配置文件，TDengine 不再允许通过修改配置文件变更运行时配置参数。请使用 `ALTER` 语句通过 SQL 修改配置参数。

### 6.10 如何让 TDengine crash 时生成 core 文件？

详见为此问题撰写的 [技术博客](https://www.taosdata.com/blog/2019/12/06/974.html)。

### 6.11 如何在 `taos` shell 中临时调整日志级别

为便于调试，`taos` shell 提供与日志记录相关的语句：

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

其含义是：在当前 `taos` shell 会话中清空本机客户端日志文件（`resetLog`），或修改特定模块的日志级别（仅对当前会话有效；重启 `taos` shell 后须重新设置）：

- value 的取值可以是：131（输出错误和警告日志）、135（输出错误、警告和调试日志）、143（输出错误、警告、调试和跟踪日志）。

### 6.12 修改 database 的 root 密码后，Grafana 监控插件 TDinsight 无数据展示

TDinsight 插件展示的数据由 `taosKeeper` 与 `taosAdapter` 收集并写入 TDengine 的 `log` 库。修改 `root` 密码后，须同步更新二者配置文件中的密码，并重启 `taosKeeper` 与 `taosAdapter`（集群场景下须重启每个节点上的对应服务）。

### 6.13 为什么开源版 TDengine 的主进程会建立与公网的连接？

该连接仅上报不涉及用户数据的基本信息（如集群名、操作系统版本、CPU 信息等），供官方了解产品全球分布情况，以优化产品与体验。

该特性为可选配置，在开源版中默认开启，对应参数为 `telemetryReporting`，详见 [taosd · 监控相关](../12-operations-and-tooling/03-components/01-taosd.md#监控相关)。

可随时关闭：在 `taos.cfg` 中将 `telemetryReporting` 设为 `0`，然后重启数据库服务。

相关代码见：[mndTelem.c](https://github.com/taosdata/TDengine/blob/62e609c558deb764a37d1a01ba84bc35115a85a4/source/dnode/mnode/impl/src/mndTelem.c)。

此外，对安全性要求极高的 TDengine TSDB-Enterprise 不会启用该参数。

### 6.14 同一台服务器，数据库的数据目录 dataDir 不变，为什么原有数据库丢失且集群 ID 发生了变化？

背景知识：TDengine 服务端进程（`taosd`）启动时，若数据目录（`dataDir`，在 `taos.cfg` 中指定）下不存在有效的数据文件子目录（如 `mnode`、`dnode`、`vnode` 等），会自动创建这些目录。创建新的 `mnode` 目录时会分配新的集群 ID，从而形成新集群。

原因分析：`dataDir` 可指向多个挂载点。若这些挂载点未在 `fstab` 中配置自动挂载，服务器重启后 `dataDir` 可能只是本地普通目录，未指向预期磁盘。此时启动 `taosd` 会在该目录下新建目录，从而形成新集群。

问题影响：服务器重启后，原有数据库看似丢失（实际多因数据盘未挂载而暂时不可见），集群 ID 也会变化，导致无法访问原库。对企业版用户，若已按集群 ID 授权，可能出现机器码未变但授权失效的情况。若未监控或未及时处理，可能造成数据不可见与运维成本上升。

问题解决：应在 `fstab` 中配置 `dataDir` 自动挂载，确保始终指向预期挂载点与目录，再重启服务器即可找回原有数据库与集群。后续版本计划在检测到启动前后 `dataDir` 变化时于启动阶段退出并给出明确错误提示。

## 7. 升级与迁移 {#升级与迁移}

### 7.1 从 TDengine `v3.0` 之前的版本升级到 `v3.0` 及以上应注意什么？

`v3.0` 相对此前版本做了全面重构，配置文件与数据文件均不兼容。升级前务必执行：

1. 删除配置文件：`sudo rm -rf /etc/taos/taos.cfg`
2. 删除日志文件：`sudo rm -rf /var/log/taos/`
3. 在确认数据不再需要的前提下，删除数据文件：`sudo rm -rf /var/lib/taos/`
4. 安装最新的 `v3.0` 稳定版 TDengine
5. 若需迁移数据或数据文件已损坏，请联系涛思数据官方技术支持

### 7.2 如何进行数据迁移？

TDengine 以 hostname 唯一标识一台机器。对于 `v3.0`，将数据文件从机器 A 迁移到机器 B 时，须将机器 B 的 hostname 配置为与机器 A 相同。

说明：`v3.x` 与此前的 `v1.x`、`v2.x` 存储结构不兼容，须使用迁移工具或自行开发应用导出导入数据。

## 8. 客户端与工具 {#客户端与工具}

### 8.1 Windows 系统下客户端无法正常显示中文字符？

Windows 系统一般以 GBK/GB18030 存储中文，而 TDengine 默认字符集为 UTF-8。在 Windows 上使用 TDengine 客户端时，驱动会将字符统一转为 UTF-8 再发送到服务端；应用开发时在调用接口处正确配置当前中文字符集即可。

在 Windows 10 上运行 `taos` shell 时，若无法正常输入或显示中文，可在客户端 `taos.cfg` 中配置：

```bash
locale C
charset UTF-8
```

### 8.2 表名显示不全

由于 `taos` shell 在终端中显示宽度有限，较长表名可能显示不全；若按截断后的表名操作，可能报 `Table does not exist`。可通过修改 `taos.cfg` 中的 `maxBinaryDisplayWidth`，或执行 `set max_binary_display_width 100`，或在命令末尾使用 `\G` 调整显示方式。

### 8.3 在 `taos` shell 中查询时字段内容无法完整显示怎么办？

可使用 `\G` 竖式显示，例如 `SHOW DATABASES\G;`（输入 `\` 后按 Tab 可自动补全）。

### 8.4 DBeaver 连接 TDengine 时中文或字符串显示乱码怎么办？

问题描述：
通过 DBeaver 查询 TDengine 中的字符串数据时，显示为乱码。

问题原因：
JDBC 驱动因历史原因将 `varchar` 类型当作 `binary` 处理，导致编码识别异常。

问题解决：
升级到较新版本的 JDBC 驱动，并在 DBeaver 的 JDBC 连接 URL 中添加相应参数。详细配置方法详见 [与 DBeaver 的集成](../13-ecosystem-integrations/04-tool/01-dbeaver.md)。
