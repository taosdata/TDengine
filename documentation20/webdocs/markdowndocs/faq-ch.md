#常见问题

#### 1. TDengine2.0之前的版本升级到2.0及以上的版本应该注意什么？☆☆☆

2.0版本在之前版本的基础上，进行了完全的重构，配置文件和数据文件是不兼容的。在升级之前务必进行如下操作：

1. 删除配置文件，执行 <code> sudo rm -rf /etc/taos/taos.cfg </code>
2. 删除日志文件，执行 <code> sudo rm -rf /var/log/taos/ </code>
3. 确保数据已经不再需要的前提下，删除数据文件，执行 <code> sudo rm -rf /var/lib/taos/ </code>
4. 安装最新稳定版本的TDengine
5. 如果数据需要迁移数据或者数据文件损坏，请联系涛思数据官方技术支持团队，进行协助解决

#### 2. Windows平台下JDBCDriver找不到动态链接库，怎么办？
请看为此问题撰写的<a href='blog/2019/12/03/jdbcdriver找不到动态链接库/'>技术博客 </a>

#### 3. 创建数据表时提示more dnodes are needed
请看为此问题撰写的<a href='blog/2019/12/03/创建数据表时提示more-dnodes-are-needed/'>技术博客</a>

#### 4. 如何让TDengine crash时生成core文件？
请看为此问题撰写的<a href='blog/2019/12/06/tdengine-crash时生成core文件的方法/'>技术博客</a>

#### 5. 遇到错误"failed to connect to server", 我怎么办？

客户端遇到链接故障，请按照下面的步骤进行检查：

1. 确保客户端与服务端版本号是完全一致的，开源社区版和企业版也不能混用
2. 在服务器，执行 `systemctl status taosd` 检查*taosd*运行状态。如果没有运行，启动*taosd*
3. 确认客户端连接时指定了正确的服务器IP地址
4. ping服务器IP，如果没有反应，请检查你的网络
5. 检查防火墙设置，确认TCP/UDP 端口6030-6039 是打开的
6. 对于Linux上的JDBC（ODBC, Python, Go等接口类似）连接, 确保*libtaos.so*在目录*/usr/local/lib/taos*里, 并且*/usr/local/lib/taos*在系统库函数搜索路径*LD_LIBRARY_PATH*里 
7. 对于windows上的JDBC, ODBC, Python, Go等连接，确保*driver/c/taos.dll*在你的系统搜索目录里 (建议*taos.dll*放在目录 *C:\Windows\System32*)
8. 如果仍不能排除连接故障，请使用命令行工具nc来分别判断指定端口的TCP和UDP连接是否通畅
   检查UDP端口连接是否工作：`nc -vuz {hostIP} {port} `
   检查服务器侧TCP端口连接是否工作：`nc -l {port}`
   检查客户端侧TCP端口链接是否工作：`nc {hostIP} {port}`


#### 6. 虽然语法正确，为什么我还是得到 "Invalid SQL" 错误

如果你确认语法正确，2.0之前版本，请检查SQL语句长度是否超过64K。如果超过，也会返回这个错误。

#### 7. 是否支持validation queries?

TDengine还没有一组专用的validation queries。然而建议你使用系统监测的数据库”log"来做。

#### 8. 我可以删除或更新一条记录吗？

不能。因为TDengine是为联网设备采集的数据设计的，不容许修改。但TDengine提供数据保留策略，只要数据记录超过保留时长，就会被自动删除。

#### 10. 我怎么创建超过250列的表？

使用2.0及其以上版本，默认支持1024列；2.0之前的版本，TDengine最大允许创建250列的表。但是如果确实超过限值，建议按照数据特性，逻辑地将这个宽表分解成几个小表。

#### 10. 最有效的写入数据的方法是什么？

批量插入。每条写入语句可以一张表同时插入多条记录，也可以同时插入多张表的记录。

#### 11. 最有效的写入数据的方法是什么？windows系统下插入的nchar类数据中的汉字被解析成了乱码如何解决？

windows下插入nchar类的数据中如果有中文，请先确认系统的地区设置成了中国（在Control Panel里可以设置），这时cmd中的`taos`客户端应该已经可以正常工作了；如果是在IDE里开发Java应用，比如Eclipse， Intellij，请确认IDE里的文件编码为GBK（这是Java默认的编码类型），然后在生成Connection时，初始化客户端的配置，具体语句如下：

​      Class.forName("com.taosdata.jdbc.TSDBDriver");

​      Properties properties = new Properties();

​      properties.setProperty(TSDBDriver.LOCALE_KEY, "UTF-8");

​      Connection = DriverManager.getConnection(url, properties);

#### 12.TDengine GO windows驱动的如何编译？
请看为此问题撰写的<a href='blog/2020/01/06/tdengine-go-windows驱动的编译/'>技术博客 



