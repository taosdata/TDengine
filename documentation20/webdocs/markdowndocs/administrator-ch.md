# TDengine的运营与维护

## 容量规划

使用TDengine来搭建一个物联网大数据平台，计算资源、存储资源需要根据业务场景进行规划。下面分别讨论系统运行所需要的内存、CPU以及硬盘空间。

### 内存需求

每个DB可以创建固定数目的vnode，默认与CPU核数相同，可通过maxVgroupsPerDb配置；每个vnode会占用固定大小的内存（大小与数据库的配置参数blocks和cache有关)；每个Table会占用与标签总长度有关的内存；此外，系统会有一些固定的内存开销。因此，每个DB需要的系统内存可通过如下公式计算：

```
Memory Size = maxVgroupsPerDb * (blocks * cache + 10Mb) + numOfTables * (tagSizePerTable + 0.5Kb)
```

示例：假设是4核机器，cache是缺省大小16M, blocks是缺省值6，假设有10万张表，标签总长度是256字节，则总的内存需求为：4\*(16\*6+10) + 100000*(0.25+0.5)/1000 = 499M。

实际运行的系统往往会根据数据特点的不同，将数据存放在不同的DB里。因此做规划时，也需要考虑。

如果内存充裕，可以加大Blocks的配置，这样更多数据将保存在内存里，提高查询速度。

### CPU需求

CPU的需求取决于如下两方面：

* __数据插入__ TDengine单核每秒能至少处理一万个插入请求。每个插入请求可以带多条记录，一次插入一条记录与插入10条记录，消耗的计算资源差别很小。因此每次插入，条数越大，插入效率越高。如果一个插入请求带200条以上记录，单核就能达到每秒插入100万条记录的速度。但对前端数据采集的要求越高，因为需要缓存记录，然后一批插入。
* __查询需求__ TDengine提供高效的查询，但是每个场景的查询差异很大，查询频次变化也很大，难以给出客观数字。需要用户针对自己的场景，写一些查询语句，才能确定。

因此仅对数据插入而言，CPU是可以估算出来的，但查询所耗的计算资源无法估算。在实际运营过程中，不建议CPU使用率超过50%，超过后，需要增加新的节点，以获得更多计算资源。

### 存储需求

TDengine相对于通用数据库，有超高的压缩比，在绝大多数场景下，TDengine的压缩比不会低于5倍，有的场合，压缩比可达到10倍以上，取决于实际场景的数据特征。压缩前的原始数据大小可通过如下方式计算：

```
Raw DataSize = numOfTables * rowSizePerTable * rowsPerTable
```

示例：1000万台智能电表，每台电表每15分钟采集一次数据，每次采集的数据128字节，那么一年的原始数据量是：10000000\*128\*24\*60/15*365 = 44.8512T。TDengine大概需要消耗44.851/5=8.97024T空间。

用户可以通过参数keep，设置数据在磁盘中的最大保存时长。为进一步减少存储成本，TDengine还提供多级存储，最冷的数据可以存放在最廉价的存储介质上，应用的访问不用做任何调整，只是读取速度降低了。

为提高速度，可以配置多块硬盘，这样可以并发写入或读取数据。需要提醒的是，TDengine采取多副本的方式提供数据的高可靠，因此不再需要采用昂贵的磁盘阵列。

### 物理机或虚拟机台数

根据上面的内存、CPU、存储的预估，就可以知道整个系统需要多少核、多少内存、多少存储空间。如果数据副本数不为1，总需求量需要再乘以副本数。

因为TDengine具有很好的水平扩展能力，根据总量，再根据单个物理机或虚拟机的资源，就可以轻松决定需要购置多少台物理机或虚拟机了。

**立即计算CPU、内存、存储，请参见：<a href='https://www.taosdata.com/config/config.html'>资源估算方法</a>**

## 容错和灾备

### 容错

TDengine支持**WAL**（Write Ahead Log）机制，实现数据的容错能力，保证数据的高可用。

TDengine接收到应用的请求数据包时，先将请求的原始数据包写入数据库日志文件，等数据成功写入数据库数据文件后，再删除相应的WAL。这样保证了TDengine能够在断电等因素导致的服务重启时从数据库日志文件中恢复数据，避免数据的丢失。

涉及的系统配置参数有两个：

- walLevel：WAL级别，0：不写wal; 1：写wal, 但不执行fsync; 2：写wal, 而且执行fsync。
- fsync：当walLevel设置为2时，执行fsync的周期。设置为0，表示每次写入，立即执行fsync。

如果要100%的保证数据不丢失，需要将walLevel设置为2，fsync设置为0。这时写入速度将会下降。但如果应用侧启动的写数据的线程数达到一定的数量(超过50)，那么写入数据的性能也会很不错，只会比fsync设置为3000毫秒下降30%左右。

### 灾备

TDengine的集群通过多个副本的机制，来提供系统的高可用性，实现灾备能力。

TDengine集群是由mnode负责管理的，为保证mnode的高可靠，可以配置多个mnode副本，副本数由系统配置参数numOfMnodes决定，为了支持高可靠，需要设置大于1。为保证元数据的强一致性，mnode副本之间通过同步方式进行数据复制，保证了元数据的强一致性。

TDengine集群中的时序数据的副本数是与数据库关联的，一个集群里可以有多个数据库，每个数据库可以配置不同的副本数。创建数据库时，通过参数replica 指定副本数。为了支持高可靠，需要设置副本数大于1。

TDengine集群的节点数必须大于等于副本数，否则创建表时将报错。

当TDengine集群中的节点部署在不同的物理机上，并设置多个副本数时，就实现了系统的高可靠性，无需再使用其他软件或工具。TDengine企业版还可以将副本部署在不同机房，从而实现异地容灾。

## 服务端配置

TDengine系统后台服务由taosd提供，可以在配置文件taos.cfg里修改配置参数，以满足不同场景的需求。配置文件的缺省位置在/etc/taos目录，可以通过taosd命令行执行参数-c指定配置文件目录。比如taosd -c /home/user来指定配置文件位于/home/user这个目录。

另外可以使用 “-C” 显示当前服务器配置参数：

```
taosd -C
```

下面仅仅列出一些重要的配置参数，更多的参数请看配置文件里的说明。各个参数的详细介绍及作用请看前述章节，而且这些参数的缺省配置都是工作的，一般无需设置。**注意：配置修改后，需要重启*taosd*服务才能生效。**

- firstEp: taosd启动时，主动连接的集群中首个dnode的end point, 默认值为localhost:6030。
- fqdn：数据节点的FQDN，缺省为操作系统配置的第一个hostname。如果习惯IP地址访问，可设置为该节点的IP地址。
- serverPort：taosd启动后，对外服务的端口号，默认值为6030。
- httpPort: RESTful服务使用的端口号，所有的HTTP请求（TCP）都需要向该接口发起查询/写入请求, 默认值为6041。
- dataDir: 数据文件目录，所有的数据文件都将写入该目录。默认值：/var/lib/taos。
- logDir：日志文件目录，客户端和服务器的运行日志文件将写入该目录。默认值：/var/log/taos。
- arbitrator：系统中裁决器的end point, 缺省值为空。
- role：dnode的可选角色。0-any; 既可作为mnode，也可分配vnode；1-mgmt;只能作为mnode，不能分配vnode；2-dnode;不能作为mnode，只能分配vnode
- debugFlag：运行日志开关。131（输出错误和警告日志），135（ 输出错误、警告和调试日志），143（ 输出错误、警告、调试和跟踪日志）。默认值：131或135（不同模块有不同的默认值）。
- numOfLogLines：单个日志文件允许的最大行数。默认值：10,000,000行。
- logKeepDays：日志文件的最长保存时间。大于0时，日志文件会被重命名为taosdlog.xxx，其中xxx为日志文件最后修改的时间戳，单位为秒。默认值：0天。
- maxSQLLength：单条SQL语句允许最长限制。默认值：65380字节。
- telemetryReporting: 是否允许 TDengine 采集和上报基本使用信息，0表示不允许，1表示允许。 默认值：1。
- stream: 是否启用连续查询（流计算功能），0表示不允许，1表示允许。 默认值：1。
- queryBufferSize: 为所有并发查询占用保留的内存大小。计算规则可以根据实际应用可能的最大并发数和表的数字相乘，再乘 170 。单位为字节。
- ratioOfQueryCores: 设置查询线程的最大数量。最小值0 表示只有1个查询线程；最大值2表示最大建立2倍CPU核数的查询线程。默认为1，表示最大和CPU核数相等的查询线程。该值可以为小数，即0.5表示最大建立CPU核数一半的查询线程。

**注意：**对于端口，TDengine会使用从serverPort起13个连续的TCP和UDP端口号，请务必在防火墙打开。因此如果是缺省配置，需要打开从6030到6042共13个端口，而且必须TCP和UDP都打开。

不同应用场景的数据往往具有不同的数据特征，比如保留天数、副本数、采集频次、记录大小、采集点的数量、压缩等都可完全不同。为获得在存储上的最高效率，TDengine提供如下存储相关的系统配置参数：

- days：一个数据文件存储数据的时间跨度，单位为天，默认值：10。
- keep：数据库中数据保留的天数，单位为天，默认值：3650。
- minRows: 文件块中记录的最小条数，单位为条，默认值：100。
- maxRows: 文件块中记录的最大条数，单位为条，默认值：4096。
- comp: 文件压缩标志位，0：关闭，1:一阶段压缩，2:两阶段压缩。默认值：2。
- walLevel：WAL级别。1：写wal, 但不执行fsync; 2：写wal, 而且执行fsync。默认值：1。
- fsync：当wal设置为2时，执行fsync的周期。设置为0，表示每次写入，立即执行fsync。单位为毫秒，默认值：3000。
- cache: 内存块的大小，单位为兆字节（MB），默认值：16。
- blocks: 每个VNODE（TSDB）中有多少cache大小的内存块。因此一个VNODE的用的内存大小粗略为（cache * blocks）。单位为块，默认值：4。
- replica：副本个数，取值范围：1-3。单位为个，默认值：1
- precision：时间戳精度标识，ms表示毫秒，us表示微秒。默认值：ms

对于一个应用场景，可能有多种数据特征的数据并存，最佳的设计是将具有相同数据特征的表放在一个库里，这样一个应用有多个库，而每个库可以配置不同的存储参数，从而保证系统有最优的性能。TDengine允许应用在创建库时指定上述存储参数，如果指定，该参数就将覆盖对应的系统配置参数。举例，有下述SQL：

```
 create database demo days 10 cache 32 blocks 8 replica 3;
```

该SQL创建了一个库demo, 每个数据文件存储10天数据，内存块为32兆字节，每个VNODE占用8个内存块，副本数为3，而其他参数与系统配置完全一致。

TDengine集群中加入一个新的dnode时，涉及集群相关的一些参数必须与已有集群的配置相同，否则不能成功加入到集群中。会进行校验的参数如下：

- numOfMnodes：系统中管理节点个数。默认值：3。
- balance：是否启动负载均衡。0：否，1：是。默认值：1。
- mnodeEqualVnodeNum: 一个mnode等同于vnode消耗的个数。默认值：4。
- offlineThreshold: dnode离线阈值，超过该时间将导致该dnode从集群中删除。单位为秒，默认值：86400*10（即10天）。
- statusInterval: dnode向mnode报告状态时长。单位为秒，默认值：1。
- maxTablesPerVnode: 每个vnode中能够创建的最大表个数。默认值：1000000。
- maxVgroupsPerDb: 每个数据库中能够使用的最大vnode个数。
- arbitrator: 系统中裁决器的end point，缺省为空。
- timezone、locale、charset 的配置见客户端配置。

为方便调试，可通过SQL语句临时调整每个dnode的日志配置，系统重启后会失效：

```mysql
ALTER DNODE <dnode_id> <config>
```

- dnode_id: 可以通过SQL语句"SHOW DNODES"命令获取
- config: 要调整的日志参数，在如下列表中取值
    > resetlog 截断旧日志文件，创建一个新日志文件
    > debugFlag  < 131 | 135 | 143 > 设置debugFlag为131、135或者143

例如：
```
    alter dnode 1 debugFlag 135;
```

## 客户端配置

TDengine系统的前台交互客户端应用程序为taos，以及应用驱动，它与taosd共享同一个配置文件taos.cfg。运行taos时，使用参数-c指定配置文件目录，如taos -c /home/cfg，表示使用/home/cfg/目录下的taos.cfg配置文件中的参数，缺省目录是/etc/taos。更多taos的使用方法请见<a href="https://www.taosdata.com/cn/documentation/administrator/#_TDengine_Shell命令行程序">Shell命令行程序</a>。本节主要说明 taos 客户端应用在配置文件 taos.cfg 文件中使用到的参数。

**2.0.10.0 之后版本支持命令行以下参数显示当前客户端参数的配置**

```bash
taos -C  或  taos --dump-config
```

客户端配置参数

- firstEp: taos启动时，主动连接的集群中第一个taosd实例的end point, 缺省值为 localhost:6030。

- secondEp: taos 启动时，如果 firstEp 连不上，将尝试连接 secondEp。

- locale

    默认值：系统中动态获取，如果自动获取失败，需要用户在配置文件设置或通过API设置

    TDengine为存储中文、日文、韩文等非ASCII编码的宽字符，提供一种专门的字段类型nchar。写入nchar字段的数据将统一采用UCS4-LE格式进行编码并发送到服务器。需要注意的是，编码正确性是客户端来保证。因此，如果用户想要正常使用nchar字段来存储诸如中文、日文、韩文等非ASCII字符，需要正确设置客户端的编码格式。

    客户端的输入的字符均采用操作系统当前默认的编码格式，在Linux系统上多为UTF-8，部分中文系统编码则可能是GB18030或GBK等。在docker环境中默认的编码是POSIX。在中文版Windows系统中，编码则是CP936。客户端需要确保正确设置自己所使用的字符集，即客户端运行的操作系统当前编码字符集，才能保证nchar中的数据正确转换为UCS4-LE编码格式。

    在 Linux 中 locale 的命名规则为: <语言>_<地区>.<字符集编码> 如：zh_CN.UTF-8，zh代表中文，CN代表大陆地区，UTF-8表示字符集。字符集编码为客户端正确解析本地字符串提供编码转换的说明。Linux系统与 Mac OSX 系统可以通过设置locale来确定系统的字符编码，由于Windows使用的locale中不是POSIX标准的locale格式，因此在Windows下需要采用另一个配置参数charset来指定字符编码。在Linux 系统中也可以使用charset来指定字符编码。

- charset

    默认值：系统中动态获取，如果自动获取失败，需要用户在配置文件设置或通过API设置

    如果配置文件中不设置charset，在Linux系统中，taos在启动时候，自动读取系统当前的locale信息，并从locale信息中解析提取charset编码格式。如果自动读取locale信息失败，则尝试读取charset配置，如果读取charset配置也失败，则中断启动过程。

    在Linux系统中，locale信息包含了字符编码信息，因此正确设置了Linux系统locale以后可以不用再单独设置charset。例如：
    ```
    locale zh_CN.UTF-8
    ```
    在Windows系统中，无法从locale获取系统当前编码。如果无法从配置文件中读取字符串编码信息，taos默认设置为字符编码为CP936。其等效在配置文件中添加如下配置：
    ```
    charset CP936
    ```
    如果需要调整字符编码，请查阅当前操作系统使用的编码，并在配置文件中正确设置。

    在Linux系统中，如果用户同时设置了locale和字符集编码charset，并且locale和charset的不一致，后设置的值将覆盖前面设置的值。
    ```
    locale zh_CN.UTF-8
    charset GBK
    ```
    则charset的有效值是GBK。
    ```
    charset GBK
    locale zh_CN.UTF-8
    ```
    charset的有效值是UTF-8。

    日志的配置参数，与server 的配置参数完全一样。

- timezone

    默认值：从系统中动态获取当前的时区设置

    客户端运行系统所在的时区。为应对多时区的数据写入和查询问题，TDengine 采用 Unix 时间戳(Unix Timestamp)来记录和存储时间戳。Unix 时间戳的特点决定了任一时刻不论在任何时区，产生的时间戳均一致。需要注意的是，Unix时间戳是在客户端完成转换和记录。为了确保客户端其他形式的时间转换为正确的 Unix 时间戳，需要设置正确的时区。

    在Linux系统中，客户端会自动读取系统设置的时区信息。用户也可以采用多种方式在配置文件设置时区。例如：
    ```
    timezone UTC-8
    timezone GMT-8
    timezone Asia/Shanghai
    ```
    均是合法的设置东八区时区的格式。

    时区的设置对于查询和写入SQL语句中非Unix时间戳的内容（时间戳字符串、关键词now的解析）产生影响。例如：
    ```sql
    SELECT count(*) FROM table_name WHERE TS<'2019-04-11 12:01:08';
    ```
    在东八区，SQL语句等效于
    ```sql
    SELECT count(*) FROM table_name WHERE TS<1554955268000;
    ```
    在UTC时区，SQL语句等效于
    ```sql
    SELECT count(*) FROM table_name WHERE TS<1554984068000;
    ```
    为了避免使用字符串时间格式带来的不确定性，也可以直接使用Unix时间戳。此外，还可以在SQL语句中使用带有时区的时间戳字符串，例如：RFC3339格式的时间戳字符串，2013-04-12T15:52:01.123+08:00或者ISO-8601格式时间戳字符串2013-04-12T15:52:01.123+0800。上述两个字符串转化为Unix时间戳不受系统所在时区的影响。

    启动taos时，也可以从命令行指定一个taosd实例的end point，否则就从taos.cfg读取。

- maxBinaryDisplayWidth

    Shell中binary 和 nchar字段的显示宽度上限，超过此限制的部分将被隐藏。默认值：30。可在 shell 中通过命令 set max_binary_display_width nn 动态修改此选项。

## 用户管理

系统管理员可以在CLI界面里添加、删除用户，也可以修改密码。CLI里SQL语法如下：

```sql
CREATE USER <user_name> PASS <'password'>;
```

创建用户，并指定用户名和密码，密码需要用单引号引起来,单引号为英文半角

```sql
DROP USER <user_name>;
```

删除用户，限root用户使用

```sql
ALTER USER <user_name> PASS <'password'>;
```

修改用户密码, 为避免被转换为小写，密码需要用单引号引用,单引号为英文半角

```sql
ALTER USER <user_name> PRIVILEGE <super|write|read>;
```

修改用户权限为：super/write/read，不需要添加单引号

```mysql
SHOW USERS;
```

显示所有用户

**注意：**SQL 语法中，< >表示需要用户输入的部分，但请不要输入< >本身

## 数据导入

TDengine提供多种方便的数据导入功能，一种按脚本文件导入，一种按数据文件导入，一种是taosdump工具导入本身导出的文件。

**按脚本文件导入**

TDengine的shell支持source filename命令，用于批量运行文件中的SQL语句。用户可将建库、建表、写数据等SQL命令写在同一个文件中，每条命令单独一行，在shell中运行source命令，即可按顺序批量运行文件中的SQL语句。以‘#’开头的SQL语句被认为是注释，shell将自动忽略。

**按数据文件导入**

TDengine也支持在shell对已存在的表从CSV文件中进行数据导入。CSV文件只属于一张表且CSV文件中的数据格式需与要导入表的结构相同, 在导入的时候，其语法如下

```mysql
insert into tb1 file 'path/data.csv';
```
注意：如果CSV文件首行存在描述信息，请手动删除后再导入

例如，现在存在一个子表d1001, 其表结构如下：

```mysql
taos> DESCRIBE d1001
             Field              |        Type        |   Length    |    Note    |
=================================================================================
 ts                             | TIMESTAMP          |           8 |            |
 current                        | FLOAT              |           4 |            |
 voltage                        | INT                |           4 |            |
 phase                          | FLOAT              |           4 |            |
 location                       | BINARY             |          64 | TAG        |
 groupid                        | INT                |           4 | TAG        |
```
要导入的data.csv的格式如下：

```csv
'2018-10-04 06:38:05.000',10.30000,219,0.31000
'2018-10-05 06:38:15.000',12.60000,218,0.33000
'2018-10-06 06:38:16.800',13.30000,221,0.32000
'2018-10-07 06:38:05.000',13.30000,219,0.33000
'2018-10-08 06:38:05.000',14.30000,219,0.34000
'2018-10-09 06:38:05.000',15.30000,219,0.35000
'2018-10-10 06:38:05.000',16.30000,219,0.31000
'2018-10-11 06:38:05.000',17.30000,219,0.32000
'2018-10-12 06:38:05.000',18.30000,219,0.31000
```
那么可以用如下命令导入数据

```mysql
taos> insert into d1001 file '~/data.csv';
Query OK, 9 row(s) affected (0.004763s)
```

**taosdump工具导入**

TDengine提供了方便的数据库导入导出工具taosdump。用户可以将taosdump从一个系统导出的数据，导入到其他系统中。具体使用方法，请参见博客：<a href='https://www.taosdata.com/blog/2020/03/09/1334.html'>TDengine DUMP工具使用指南</a>

## 数据导出

为方便数据导出，TDengine提供了两种导出方式，分别是按表导出和用taosdump导出。

**按表导出CSV文件**

如果用户需要导出一个表或一个STable中的数据，可在shell中运行

```mysql
select * from <tb_name> >> data.csv;
```

这样，表tb_name中的数据就会按照CSV格式导出到文件data.csv中。

**用taosdump导出数据**

TDengine提供了方便的数据库导出工具taosdump。用户可以根据需要选择导出所有数据库、一个数据库或者数据库中的一张表,所有数据或一时间段的数据，甚至仅仅表的定义。具体使用方法，请参见博客：<a href='https://www.taosdata.com/blog/2020/03/09/1334.html'>TDengine DUMP工具使用指南</a>

## 系统连接、任务查询管理

系统管理员可以从CLI查询系统的连接、正在进行的查询、流式计算，并且可以关闭连接、停止正在进行的查询和流式计算。CLI里SQL语法如下：

```mysql
SHOW CONNECTIONS;
```

显示数据库的连接，其中一列显示ip:port, 为连接的IP地址和端口号。

```mysql
KILL CONNECTION <connection-id>;
```

强制关闭数据库连接，其中的connection-id是SHOW CONNECTIONS中显示的第一列的数字。

```mysql
SHOW QUERIES;
```

显示数据查询，其中第一列显示的以冒号隔开的两个数字为query-id，为发起该query应用连接的connection-id和查询次数。

```mysql
KILL QUERY <query-id>;
```

强制关闭数据查询，其中query-id是SHOW QUERIES中显示的 connection-id:query-no字串，如“105:2”，拷贝粘贴即可。

```mysql
SHOW STREAMS;
```

显示流式计算，其中第一列显示的以冒号隔开的两个数字为stream-id, 为启动该stream应用连接的connection-id和发起stream的次数。

```mysql
KILL STREAM <stream-id>;
```

强制关闭流式计算，其中的中stream-id是SHOW STREAMS中显示的connection-id:stream-no字串，如103:2，拷贝粘贴即可。

## 系统监控

TDengine启动后，会自动创建一个监测数据库log，并自动将服务器的CPU、内存、硬盘空间、带宽、请求数、磁盘读写速度、慢查询等信息定时写入该数据库。TDengine还将重要的系统操作（比如登录、创建、删除数据库等）日志以及各种错误报警信息记录下来存放在log库里。系统管理员可以从CLI直接查看这个数据库，也可以在WEB通过图形化界面查看这些监测信息。

这些监测信息的采集缺省是打开的，但可以修改配置文件里的选项enableMonitor将其关闭或打开。

## 文件目录结构

安装TDengine后，默认会在操作系统中生成下列目录或文件：

| 目录/文件                 | 说明                                                         |
| ------------------------- | :----------------------------------------------------------- |
| /usr/local/taos/bin       | TDengine可执行文件目录。其中的执行文件都会软链接到/usr/bin目录下。 |
| /usr/local/taos/connector | TDengine各种连接器目录。                                     |
| /usr/local/taos/driver    | TDengine动态链接库目录。会软链接到/usr/lib目录下。           |
| /usr/local/taos/examples  | TDengine各种语言应用示例目录。                               |
| /usr/local/taos/include   | TDengine对外提供的C语言接口的头文件。                        |
| /etc/taos/taos.cfg        | TDengine默认[配置文件]                                       |
| /var/lib/taos             | TDengine默认数据文件目录,可通过[配置文件]修改位置.           |
| /var/log/taos             | TDengine默认日志文件目录,可通过[配置文件]修改位置            |

**可执行文件**

TDengine的所有可执行文件默认存放在 _/usr/local/taos/bin_ 目录下。其中包括：

- _taosd_：TDengine服务端可执行文件
- _taos_： TDengine Shell可执行文件
- _taosdump_：数据导入导出工具
- remove.sh：卸载TDengine的脚本, 请谨慎执行，链接到/usr/bin目录下的rmtaos命令。会删除TDengine的安装目录/usr/local/taos，但会保留/etc/taos、/var/lib/taos、/var/log/taos。

您可以通过修改系统配置文件taos.cfg来配置不同的数据目录和日志目录。

## TDengine参数限制与保留关键字

- 数据库名：不能包含“.”以及特殊字符，不能超过32个字符
- 表名：不能包含“.”以及特殊字符，与所属数据库名一起，不能超过192个字符
- 表的列名：不能包含特殊字符，不能超过64个字符
- 表的列数：不能超过1024列
- 记录的最大长度：包括时间戳8 byte，不能超过16KB
- 单条SQL语句默认最大字符串长度：65480 byte
- 数据库副本数：不能超过3
- 用户名：不能超过23个byte
- 用户密码：不能超过15个byte
- 标签(Tags)数量：不能超过128个
- 标签的总长度：不能超过16Kbyte
- 记录条数：仅受存储空间限制
- 表的个数：仅受节点个数限制
- 库的个数：仅受节点个数限制
- 单个库上虚拟节点个数：不能超过64个

目前TDengine有将近200个内部保留关键字，这些关键字无论大小写均不可以用作库名、表名、STable名、数据列名及标签列名等。这些关键字列表如下：

| 关键字列表 |             |              |            |           |
| ---------- | ----------- | ------------ | ---------- | --------- |
| ABLOCKS    | CONNECTION  | GT           | MINUS      | SHOW      |
| ABORT      | CONNECTIONS | ID           | MNODES     | SLASH     |
| ACCOUNT    | COPY        | IF           | MODULES    | SLIDING   |
| ACCOUNTS   | COUNT       | IGNORE       | NCHAR      | SMALLINT  |
| ADD        | CREATE      | IMMEDIATE    | NE         | SPREAD    |
| AFTER      | CTIME       | IMPORT       | NONE       | STAR      |
| ALL        | DATABASE    | IN           | NOT        | STATEMENT |
| ALTER      | DATABASES   | INITIALLY    | NOTNULL    | STDDEV    |
| AND        | DAYS        | INSERT       | NOW        | STREAM    |
| AS         | DEFERRED    | INSTEAD      | OF         | STREAMS   |
| ASC        | DELIMITERS  | INTEGER      | OFFSET     | STRING    |
| ATTACH     | DESC        | INTERVAL     | OR         | SUM       |
| AVG        | DESCRIBE    | INTO         | ORDER      | TABLE     |
| BEFORE     | DETACH      | IP           | PASS       | TABLES    |
| BEGIN      | DIFF        | IS           | PERCENTILE | TAG       |
| BETWEEN    | DIVIDE      | ISNULL       | PLUS       | TAGS      |
| BIGINT     | DNODE       | JOIN         | PRAGMA     | TBLOCKS   |
| BINARY     | DNODES      | KEEP         | PREV       | TBNAME    |
| BITAND     | DOT         | KEY          | PRIVILEGE  | TIMES     |
| BITNOT     | DOUBLE      | KILL         | QUERIES    | TIMESTAMP |
| BITOR      | DROP        | LAST         | QUERY      | TINYINT   |
| BOOL       | EACH        | LE           | RAISE      | TOP       |
| BOTTOM     | END         | LEASTSQUARES | REM        | TRIGGER   |
| BY         | EQ          | LIKE         | REPLACE    | UMINUS    |
| CACHE      | EXISTS      | LIMIT        | REPLICA    | UPLUS     |
| CASCADE    | EXPLAIN     | LINEAR       | RESET      | USE       |
| CHANGE     | FAIL        | LOCAL        | RESTRICT   | USER      |
| CLOG       | FILL        | LP           | ROW        | USERS     |
| CLUSTER    | FIRST       | LSHIFT       | ROWS       | USING     |
| COLON      | FLOAT       | LT           | RP         | VALUES    |
| COLUMN     | FOR         | MATCH        | RSHIFT     | VARIABLE  |
| COMMA      | FROM        | MAX          | SCORES     | VGROUPS   |
| COMP       | GE          | METRIC       | SELECT     | VIEW      |
| CONCAT     | GLOB        | METRICS      | SEMI       | WAVG      |
| CONFIGS    | GRANTS      | MIN          | SET        | WHERE     |
| CONFLICT   | GROUP       |              |            |           |