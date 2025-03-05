---
title: "taosd 参考手册"
sidebar_label: "taosd"
toc_max_heading_level: 4
---

taosd 是 TDengine 数据库引擎的核心服务，其配置文件默认是 `/etc/taos/taos.cfg` 但也可以指定不同路径的配置文件。本节详细介绍 taosd 的命令行参数及配置文件中的配置参数。

## 命令行参数

taosd 命令行参数如下
- -a `<json file>`：指定一个 JSON 文件，其中包含服务启动时的各项配置参数，其格式形如 `{"fqdn":"td1"}`，关于配置参数的细节请参考下一节
- -c `<directory>`：指定配置文件所在目录
- -s：打印 SDB 信息
- -C: 打印配置信息
- -e: 指定环境变量的字符串，例如：`-e 'TAOS_FQDN=td1'`
- -E: 指定环境变量的文件路径，默认是 `./.env`，.env 文件中的内容可以是 `TAOS_FQDN=td1`
- -o: 指定日志输入方式，可选 `stdout`, `stderr`, `/dev/null`, `<directory>`,` <directory>/<filename>`, `<filename>`
- -k: 获取机器码
- -dm: 启用内存调度
- -V: 打印版本信息

## 配置参数

:::note
配置文件参数修改后，需要重启*taosd*服务，或客户端应用才能生效
:::

### 连接相关

#### firstEp
- 说明：taosd 启动时，主动连接的集群中首个 dnode 的 end point
- 类型：endpoint
- 默认值：localhost:6030
- 动态修改：不支持
- 支持版本：从 v3.0.0.0 版本开始引入

#### secondEp
- 说明：taosd 启动时，如果 firstEp 连接不上，尝试连接集群中第二个 dnode 的 endpoint
- 类型：endpoint
- 默认值：无
- 动态修改：不支持
- 支持版本：从 v3.0.0.0 版本开始引入

#### fqdn
- 说明：taosd 监听的服务地址
- 类型：fqdn
- 默认值：所在服务器上配置的第一个 hostname
- 动态修改：不支持
- 支持版本：从 v3.0.0.0 版本开始引入

#### serverPort
- 说明：taosd 监听的端口
- 类型：整数
- 默认值：6030
- 最小值：1
- 最大值：65056
- 动态修改：不支持
- 支持版本：从 v3.0.0.0 版本开始引入

#### compressMsgSize
- 说明：是否对 RPC 消息进行压缩
- 类型：整数；-1：所有消息都不压缩；0：所有消息都压缩；N (N>0)：只有大于 N 个字节的消息才压缩
- 默认值：-1
- 最小值：-1
- 最大值：100000000
- 动态修改：支持通过 SQL 修改，重启后生效
- 支持版本：从 v3.0.0.0 版本开始引入

#### shellActivityTimer
- 说明：客户端向 mnode 发送心跳的时长
- 类型：整数
- 单位：秒
- 默认值：3
- 最小值：1
- 最大值：120
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.0.0.0 版本开始引入

#### numOfRpcSessions
- 说明：RPC 支持的最大连接数
- 类型：整数
- 默认值：30000
- 最小值：100
- 最大值：100000
- 动态修改：支持通过 SQL 修改，重启后生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### numOfRpcThreads
- 说明：RPC 收发数据的线程数目
- 类型：整数
- 默认值：CPU 核数的一半
- 最小值：1
- 最大值：50
- 动态修改：支持通过 SQL 修改，重启后生效
- 支持版本：从 v3.0.0.0 版本开始引入

#### numOfTaskQueueThreads
- 说明：RPC 处理消息的线程数目
- 类型：整数
- 默认值：CPU 核数的一半
- 最小值：4
- 最大值：16
- 动态修改：支持通过 SQL 修改，重启后生效
- 支持版本：从 v3.0.0.0 版本开始引入

#### rpcQueueMemoryAllowed
- 说明：dnode 已经收到并等待处理的 RPC 消息占用内存的最大值
- 类型：整数
- 单位：byte
- 默认值：服务器内存的 1/10
- 最小值：104857600
- 最大值：INT64_MAX
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.0.0.0 版本开始引入

#### resolveFQDNRetryTime
- 说明：FQDN 解析失败时的重试次数
- 类型：整数
- 默认值：100
- 最小值：1
- 最大值：10240
- 动态修改：不支持
- 支持版本：v3.3.4.0 版本之后取消

#### timeToGetAvailableConn
- 说明：获得可用连接的最长等待时间
- 类型：整数
- 单位：毫秒
- 默认值：500000
- 最小值：20
- 最大值：1000000
- 动态修改：不支持
- 支持版本：v3.3.4.0 版本之后取消

#### maxShellConns
- 说明：允许创建的最大链接数
- 类型：整数
- 默认值：50000
- 最小值：10
- 最大值：50000000
- 动态修改：不支持
- 支持版本：v3.3.4.0 版本之后取消

#### maxRetryWaitTime
- 说明：重连最大超时时间, 从重试时候开始计算
- 类型：整数
- 单位：毫秒
- 默认值：10000
- 最小值：0
- 最大值：86400000
- 动态修改：支持通过 SQL 修改，重启后生效
- 支持版本：从 v3.3.4.0 版本开始引入

#### shareConnLimit
- 说明：一个链接可以共享的请求的数目
- 类型：整数
- 默认值：10
- 最小值：1
- 最大值：512
- 动态修改：支持通过 SQL 修改，重启后生效
- 支持版本：从 v3.3.4.0 版本开始引入

#### readTimeout
- 说明：单个请求最小超时时间
- 类型：整数
- 单位：秒
- 默认值：900
- 最小值：64
- 最大值：604800
- 动态修改：支持通过 SQL 修改，重启后生效
- 支持版本：从 v3.3.4.0 版本开始引入

### 监控相关

#### monitor
- 说明：是否收集监控数据并上报
- 类型：整数；0：关闭；1：打开
- 默认值：0
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.0.0.0 版本开始引入

#### monitorFqdn
- 说明：taosKeeper 服务所在服务器的地址
- 类型：fqdn
- 默认值：无
- 动态修改：支持通过 SQL 修改，重启后生效
- 支持版本：从 v3.0.0.0 版本开始引入

#### monitorPort
- 说明：taosKeeper 服务所监听的端口号
- 类型：整数
- 默认值：6043
- 最小值：1
- 最大值：65056
- 动态修改：支持通过 SQL 修改，重启后生效
- 支持版本：从 v3.0.0.0 版本开始引入

#### monitorInterval
- 说明：监控数据库记录系统参数（CPU/内存）的时间间隔
- 类型：整数
- 单位：秒
- 默认值：30
- 最小值：1
- 最大值：200000
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.0.0.0 版本开始引入

#### monitorMaxLogs
- 说明：缓存的待上报日志条数
- 类型：整数
- 默认值：100
- 最小值：1
- 最大值：1000000
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.0.0.0 版本开始引入

#### monitorComp
- 说明：是否采用压缩方式上报监控日志
- 类型：整数
- 默认值：0
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，重启后生效
- 支持版本：从 v3.0.0.0 版本开始引入

#### monitorLogProtocol
- 说明：是否打印监控日志
- 类型：整数
- 默认值：0
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.3.0.0 版本开始引入

#### monitorForceV2
- 说明：是否使用 V2 版本协议上报日志
- 类型：整数
- 默认值：1
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.3.0.0 版本开始引入

#### telemetryReporting
- 说明：是否上传 telemetry
- 类型：整数；0：关闭；1：打开
- 默认值：1
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.0.0.0 版本开始引入

#### telemetryServer
- 说明：telemetry 服务器地址
- 类型：fqdn
- 默认值：telemetry.taosdata.com
- 动态修改：不支持
- 支持版本：从 v3.0.0.0 版本开始引入

#### telemetryPort
- 说明：telemetry 服务器端口号
- 类型：整数
- 默认值：80
- 最小值：1
- 最大值：65056
- 动态修改：不支持
- 支持版本：从 v3.0.0.0 版本开始引入

#### telemetryInterval
- 说明：telemetry 上传时间间隔
- 类型：整数
- 单位：秒
- 默认值：86400
- 最小值：1
- 最大值：200000
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.0.0.0 版本开始引入

#### crashReporting
- 说明：是否使用 V2 版本协议上报日志
- 类型：整数；0：不上传；1：上传
- 默认值：1
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

### 查询相关

#### countAlwaysReturnValue
- 说明：count/hyperloglog 函数在输入数据为空或者 NULL 的情况下是否返回值
- 类型：整数；0：返回空行，1：如果查询中含有 INTERVAL 子句或者该查询使用了 TSMA，且相应的组或窗口内数据为空或者 NULL，对应的组或窗口将不返回查询结果
- 默认值：1
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.0.0.0 版本开始引入

#### tagFilterCache
- 说明：是否缓存标签过滤结果
- 类型：整数；0：不缓存，1：缓存
- 默认值：0
- 最小值：0
- 最大值：1
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### queryBufferSize
- 说明：查询可用的缓存大小
- 类型：整数；-1 表示不限制
- 单位：MB
- 默认值：-1
- 最小值：-1
- 最大值：500000000000
- 动态修改：支持通过 SQL 修改，重启后效
- 支持版本：预留参数，当前版本尚不支持

#### queryRspPolicy
- 说明：查询响应策略
- 类型：整数；0：正常模式；1：快速响应模式，服务端收到查询消息后立即返回响应，而不是有查询结果后才响应
- 默认值：0
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### queryUseMemoryPool
- 说明：查询是否使用内存池管理内存
- 类型：整数；0：关闭；1：打开
- 默认值：1
- 最小值：0
- 最大值：1
- 动态修改：不支持
- 支持版本：从 v3.3.5.0 版本开始引入

#### minReservedMemorySize
- 说明：最小预留的系统可用内存数量，除预留外的内存都可以被用于查询
- 类型：整数
- 单位：MB
- 默认值：系统物理内存的 20%
- 最小值：1024
- 最大值：1000000000
- 动态修改：不支持
- 支持版本：从 v3.3.5.0 版本开始引入

#### singleQueryMaxMemorySize
- 说明：单个查询在单个节点(dnode)上可以使用的内存上限，超过该上限将返回错
- 类型：整数；0：无上限
- 单位：MB
- 默认值：0
- 最小值：0
- 最大值：1000000000
- 动态修改：不支持
- 支持版本：从 v3.3.5.0 版本开始引入

#### filterScalarMode
- 说明：强制使用标量过滤模式
- 类型：整数；0：关闭；1：开启
- 默认值：0
- 最小值：0
- 最大值：1
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### queryNoFetchTimeoutSec
- 说明：查询中当应用长时间不 FETCH 数据时的超时时间，从最后一次响应起计时，超时自动清除任务 `内部参数`
- 类型：整数；0：关闭；1：开启
- 默认值：18000
- 最小值：60
- 最大值：1000000000
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### queryPlannerTrace
- 说明：查询计划是否输出详细日志 `内部参数`
- 类型：整数；0：关闭；1：开启
- 默认值：0
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### queryNodeChunkSize
- 说明：查询计划的块大小 `内部参数`
- 类型：整数
- 单位：byte
- 默认值：32 * 1024
- 最小值：1024
- 最大值：128 * 1024
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### queryUseNodeAllocator
- 说明：查询计划的分配方法 `内部参数`
- 类型：整数；0：关闭；1：开启
- 默认值：1
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### queryMaxConcurrentTables
- 说明：查询计划的分配方法 `内部参数`
- 类型：整数
- 默认值：200
- 最小值：INT64_M
- 最大值：INT64_MAX
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### queryRsmaTolerance
- 说明：查询计划的分配方法 `内部参数`
- 类型：整数
- 默认值：1000
- 最小值：0
- 最大值：900000
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### enableQueryHb
- 说明：是否发送查询心跳消息 `内部参数`
- 类型：整数；0：关闭；1：开启
- 默认值：1
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### pqSortMemThreshold
- 说明：排序使用的内存阈值 `内部参数`
- 类型：整数
- 单位：MB
- 默认值：16
- 最小值：1
- 最大值：10240
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

### 区域相关
#### timezone
- 说明：时区
- 默认值：从系统中动态获取当前的时区设置
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### locale
- 说明：系统区位信息及编码格式
- 默认值：从系统中获取
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### charset
- 说明：字符集编码
- 默认值：从系统中获取
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

:::info
#### 区域相关参数说明
1. 为应对多时区的数据写入和查询问题，TDengine 采用 Unix 时间戳(Unix Timestamp)来记录和存储时间戳。Unix 时间戳的特点决定了任一时刻不论在任何时区，产生的时间戳均一致。需要注意的是，Unix 时间戳是在客户端完成转换和记录。为了确保客户端其他形式的时间转换为正确的 Unix 时间戳，需要设置正确的时区。

在 Linux/macOS 中，客户端会自动读取系统设置的时区信息。用户也可以采用多种方式在配置文件设置时区。例如：

```
timezone UTC-8
timezone GMT-8
timezone Asia/Shanghai
```

均是合法的设置东八区时区的格式。但需注意，Windows 下并不支持 `timezone Asia/Shanghai` 这样的写法，而必须写成 `timezone UTC-8`。

时区的设置对于查询和写入 SQL 语句中非 Unix 时间戳的内容（时间戳字符串、关键词 now 的解析）产生影响。例如：

```sql
SELECT count(*) FROM table_name WHERE TS<'2019-04-11 12:01:08';
```

在东八区，SQL 语句等效于

```sql
SELECT count(*) FROM table_name WHERE TS<1554955268000;
```

在 UTC 时区，SQL 语句等效于

```sql
SELECT count(*) FROM table_name WHERE TS<1554984068000;
```

为了避免使用字符串时间格式带来的不确定性，也可以直接使用 Unix 时间戳。此外，还可以在 SQL 语句中使用带有时区的时间戳字符串，例如：RFC3339 格式的时间戳字符串，2013-04-12T15:52:01.123+08:00 或者 ISO-8601 格式时间戳字符串 2013-04-12T15:52:01.123+0800。上述两个字符串转化为 Unix 时间戳不受系统所在时区的影响。


2. TDengine 为存储中文、日文、韩文等非 ASCII 编码的宽字符，提供一种专门的字段类型 nchar。写入 nchar 字段的数据将统一采用 UCS4-LE 格式进行编码并发送到服务器。需要注意的是，编码正确性是客户端来保证。因此，如果用户想要正常使用 nchar 字段来存储诸如中文、日文、韩文等非 ASCII 字符，需要正确设置客户端的编码格式。

客户端的输入的字符均采用操作系统当前默认的编码格式，在 Linux/macOS 系统上多为 UTF-8，部分中文系统编码则可能是 GB18030 或 GBK 等。在 docker 环境中默认的编码是 POSIX。在中文版 Windows 系统中，编码则是 CP936。客户端需要确保正确设置自己所使用的字符集，即客户端运行的操作系统当前编码字符集，才能保证 nchar 中的数据正确转换为 UCS4-LE 编码格式。

在 Linux/macOS 中 locale 的命名规则为：\<语言>_\<地区>.\<字符集编码> 如：zh_CN.UTF-8，zh 代表中文，CN 代表大陆地区，UTF-8 表示字符集。字符集编码为客户端正确解析本地字符串提供编码转换的说明。Linux/macOS 可以通过设置 locale 来确定系统的字符编码，由于 Windows 使用的 locale 中不是 POSIX 标准的 locale 格式，因此在 Windows 下需要采用另一个配置参数 charset 来指定字符编码。在 Linux/macOS 中也可以使用 charset 来指定字符编码。


3. 如果配置文件中不设置 charset，在 Linux/macOS 中，taos 在启动时候，自动读取系统当前的 locale 信息，并从 locale 信息中解析提取 charset 编码格式。如果自动读取 locale 信息失败，则尝试读取 charset 配置，如果读取 charset 配置也失败，则中断启动过程。

在 Linux/macOS 中，locale 信息包含了字符编码信息，因此正确设置了 Linux/macOS 的 locale 以后可以不用再单独设置 charset。例如：

```
locale zh_CN.UTF-8
```

在 Windows 系统中，无法从 locale 获取系统当前编码。如果无法从配置文件中读取字符串编码信息，taos 默认设置为字符编码为 CP936。其等效在配置文件中添加如下配置：

```
charset CP936
```

如果需要调整字符编码，请查阅当前操作系统使用的编码，并在配置文件中正确设置。

在 Linux/macOS 中，如果用户同时设置了 locale 和字符集编码 charset，并且 locale 和 charset 的不一致，后设置的值将覆盖前面设置的值。

```
locale zh_CN.UTF-8
charset GBK
```

则 charset 的有效值是 GBK。

```
charset GBK
locale zh_CN.UTF-8
```

charset 的有效值是 UTF-8。

:::

### 存储相关

#### dataDir
- 说明：数据文件目录，所有的数据文件都将写入该目录
- 类型：字符串
- 默认值：/var/lib/taos
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### diskIDCheckEnabled
- 说明：在重启 dnode 时增加了检查 dataDir 所在磁盘 id 是否发生改变
- 类型：整数；0:进行检查，1:不进行检查
- 默认值：1
- 最小值：0
- 最大值：1
- 支持版本：从 v3.3.5.0 版本开始引入

#### tempDir
- 说明：指定所有系统运行过程中的临时文件生成的目录
- 类型：字符串
- 默认值：/tmp
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### minimalDataDirGB
- 说明：dataDir 指定的时序数据存储目录所需要保留的最小空间
- 类型：浮点数
- 单位：GB
- 默认值：2
- 最小值：0.001f
- 最大值：10000000
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### minimalTmpDirGB
- 说明：tempDir 所指定的临时文件目录所需要保留的最小空间
- 类型：浮点数
- 单位：GB
- 默认值：1
- 最小值：0.001f
- 最大值：10000000
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### minDiskFreeSize
- 说明：当某块磁盘上的可用空间小于等于这个阈值时，该磁盘将不再被选择用于生成新的数据文件 `企业版参数`
- 类型：整数
- 单位：byte
- 默认值：52428800
- 最小值：52428800
- 最大值：1073741824
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### s3MigrateIntervalSec
- 说明：本地数据文件自动上传 S3 的触发周期 `企业版参数`
- 类型：整数
- 单位：秒
- 默认值：3600
- 最小值：600
- 最大值：100000
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.3.4.3 版本开始引入

#### s3MigrateEnabled
- 说明：是否自动进行 S3 迁移 `企业版参数`
- 类型：整数；0:关闭，1:开启
- 默认值：0
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.3.4.3 版本开始引入

#### s3Accesskey
- 说明：冒号分隔的用户 SecretId:SecretKey `企业版参数`
- 示例：AKIDsQmwsfKxTo2A6nGVXZN0UlofKn6JRRSJ:lIdoy99ygEacU7iHfogaN2Xq0yumSm1E
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.3.4.3 版本开始引入

#### s3Endpoint
- 说明：用户所在地域的 COS 服务域名，支持 http 和 https，bucket 的区域需要与 endpoint 保持一致，否则无法访问 `企业版参数`
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.3.4.3 版本开始引入

#### s3BucketName
- 说明：存储桶名称，减号后面是用户注册 COS 服务的 AppId，其中 AppId 是 COS 特有，AWS 和阿里云都没有，配置时需要作为 bucket name 的一部分，使用减号分隔；参数值均为字符串类型，但不需要引号 `企业版参数`
- 示例：test0711-1309024725
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.3.4.3 版本开始引入

#### s3PageCacheSize
- 说明：S3 page cache 缓存页数目 `企业版参数`
- 类型：整数
- 单位：页
- 默认值：4096
- 最小值：4
- 最大值：1048576
- 示例：test0711-1309024725
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.3.4.3 版本开始引入

#### s3UploadDelaySec
- 说明：data 文件持续多长时间不再变动后上传至 S3 `企业版参数`
- 类型：整数
- 单位：秒
- 默认值：60
- 最小值：1
- 最大值：2592000
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.3.4.3 版本开始引入

#### cacheLazyLoadThreshold
- 说明：缓存的装载策略 `内部参数`
- 类型：整数
- 默认值：500
- 最小值：0
- 最大值：100000
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

### 集群相关

#### supportVnodes
- 说明：dnode 支持的最大 vnode 数目 
- 类型：整数
- 默认值：CPU 核数的 2 倍 + 5
- 最小值：0
- 最大值：4096
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### numOfCommitThreads
- 说明：落盘线程的最大数量
- 类型：整数
- 默认值：4
- 最小值：1
- 最大值：1024
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### numOfCompactThreads
- 说明：合并线程的最大数量
- 类型：整数
- 默认值：2
- 最小值：1
- 最大值：16
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### numOfMnodeReadThreads
- 说明：mnode 的 Read 线程数目
- 类型：整数
- 默认值：CPU 核数的四分之一（不超过 4）
- 最小值：0
- 最大值：1024
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### numOfVnodeQueryThreads
- 说明：vnode 的 Query 线程数目
- 类型：整数
- 默认值：CPU 核数的两倍（不超过 16）
- 最小值：0
- 最大值：1024
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### numOfVnodeFetchThreads
- 说明：vnode 的 Fetch 线程数目
- 类型：整数
- 默认值：CPU 核数的四分之一（不超过 4）
- 最小值：0
- 最大值：1024
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### numOfVnodeRsmaThreads
- 说明：vnode 的 Rsma 线程数目
- 类型：整数
- 默认值：CPU 核数的四分之一（不超过 4）
- 最小值：0
- 最大值：1024
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### numOfQnodeQueryThreads
- 说明：qnode 的 Query 线程数目
- 类型：整数
- 默认值：CPU 核数的两倍（不超过 16）
- 最小值：0
- 最大值：1024
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### numOfSnodeSharedThreads
- 说明：snode 的共享线程数目
- 类型：整数
- 默认值：CPU 核数的四分之一（不小于 2，不超过 4）
- 最小值：0
- 最大值：1024
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### numOfSnodeUniqueThreads
- 说明：snode 的独占线程数目
- 类型：整数
- 默认值：CPU 核数的四分之一（不小于 2，不超过 4）
- 最小值：0
- 最大值：1024
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### ratioOfVnodeStreamThreads
- 说明：流计算使用 vnode 线程的比例
- 类型：浮点数
- 默认值：0.5
- 最小值：0.01
- 最大值：4
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### ttlUnit
- 说明：ttl 参数的单位
- 类型：整数
- 单位：秒
- 默认值：86400
- 最小值：1
- 最大值：31572500
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### ttlPushInterval
- 说明：ttl 检测超时频率
- 类型：整数
- 单位：秒
- 默认值：10
- 最小值：1
- 最大值：100000
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### ttlChangeOnWrite
- 说明：ttl 到期时间是否伴随表的修改操作改变
- 类型：整数；0：不改变，1：改变
- 默认值：0
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### ttlBatchDropNum
- 说明：ttl 一批删除子表的数目
- 类型：整数
- 默认值：10000
- 最小值：0
- 最大值：2147483647
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### retentionSpeedLimitMB
- 说明：数据在不同级别硬盘上迁移时的速度限制
- 类型：整数
- 单位：MB
- 默认值：0，表示不限制
- 最小值：0
- 最大值：1024
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### maxTsmaNum
- 说明：集群内可创建的TSMA个数
- 类型：整数
- 默认值：0
- 最小值：0
- 最大值：3
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### tmqMaxTopicNum
- 说明：订阅最多可建立的 topic 数量
- 类型：整数
- 默认值：20
- 最小值：1
- 最大值：10000
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### tmqRowSize
- 说明：订阅数据块的最大记录条数
- 类型：整数
- 默认值：4096
- 最小值：1
- 最大值：1000000
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### audit
- 说明：审计功能开关；`企业版参数`
- 类型：整数；0:关闭，1：开启
- 默认值：1
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### auditInterval
- 说明：审计数据上报的时间间隔；`企业版参数`
- 类型：整数
- 默认值：5000
- 最小值：500
- 最大值：200000
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### auditCreateTable
- 说明：是否针对创建子表开启申计功能；`企业版参数`
- 类型：整数；0:关闭，1：开启
- 默认值：1
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### encryptAlgorithm
- 说明：数据加密算法；`企业版参数`
- 类型：字符串
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### encryptScope
- 说明：加密范围；`企业版参数`
- 类型：字符串
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### enableWhiteList
- 说明：白名单功能开关；`企业版参数`
- 类型：整数；0:关闭，1：开启
- 默认值：0
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### syncLogBufferMemoryAllowed
- 说明：一个 dnode 允许的 sync 日志缓存消息占用的内存最大值
- 类型：整数
- 单位：bytes
- 默认值：服务器内存的 1/10
- 最小值：104857600
- 最大值：9223372036854775807
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：v3.1.3.2/v3.3.2.13 版本开始生效

#### syncElectInterval
- 说明：用于同步模块调试,`内部参数`
- 类型：整数
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### syncHeartbeatInterval
- 说明：用于同步模块调试,`内部参数`
- 类型：整数
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### syncHeartbeatTimeout
- 说明：用于同步模块调试,`内部参数`
- 类型：整数
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### syncSnapReplMaxWaitN
- 说明：用于同步模块调试,`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### arbHeartBeatIntervalSec
- 说明：用于同步模块调试,`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### arbCheckSyncIntervalSec
- 说明：用于同步模块调试,`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### arbSetAssignedTimeoutSec
- 说明：用于同步模块调试,`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### arbSetAssignedTimeoutSec
- 说明：用于 mnode 模块调试,`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### mndLogRetention
- 说明：用于 mnode 模块调试,`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### skipGrant
- 说明：用于授权检查,`内部参数`
- 类型：整数
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### trimVDbIntervalSec
- 说明：用于删除过期数据,`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### ttlFlushThreshold
- 说明：ttl 定时器的频率,`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### compactPullupInterval
- 说明：数据重整定时器的频率,`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### walFsyncDataSizeLimit
- 说明：WAL 进行 FSYNC 的阈值`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### transPullupInterval
- 说明：mnode 执行事务的重试间`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### mqRebalanceInterval
- 说明：消费者再平衡的时间间隔`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### uptimeInterval
- 说明：用于记录系统启动时间`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### timeseriesThreshold
- 说明：用于统计用量`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### udf
- 说明：是否启动 UDF 服务
- 类型：整数；0：不启动，1：启动
- 默认值：0
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，重启生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### udfdResFuncs
- 说明：用于统计用量`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，重启生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### udfdLdLibPath
- 说明：用于统计用量`内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，重启生效 
- 支持版本：从 v3.1.0.0 版本开始引入

### 流计算参数

#### disableStream
- 说明：流计算的启动开关
- 类型：整数；0：启动，1：关闭
- 默认值：0
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，重启生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### streamBufferSize
- 说明：控制内存中窗口状态缓存的大小
- 类型：整数
- 单位：bytes
- 默认值：128 * 1024 * 1024
- 最小值：0
- 最大值：9223372036854775807
- 动态修改：支持通过 SQL 修改，重启生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### streamAggCnt
- 说明：并发进行聚合计算的数目 `内部参数`
- 类型：整数
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### checkpointInterval
- 说明：checkponit 同步间隔 `内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，重启生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### concurrentCheckpoint
- 说明：是否并发检查 checkpoint `内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### maxStreamBackendCache
- 说明：流计算使用的最大缓存 `内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### streamSinkDataRate
- 说明：用于控制流计算结果的写入速度 `内部参数`
- 类型：整数
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### streamNotifyMessageSize
- 说明：用于控制事件通知的消息大小 `内部参数`
- 类型：整数
- 单位：KB
- 默认值：8192
- 最小值：8
- 最大值：1048576
- 动态修改：不支持
- 支持版本：从 v3.3.6.0 版本开始引入

#### streamNotifyFrameSize
- 说明：用于控制事件通知消息发送时底层的帧大小 `内部参数`
- 类型：整数
- 单位：KB
- 默认值：256
- 最小值：8
- 最大值：1048576
- 动态修改：不支持
- 支持版本：从 v3.3.6.0 版本开始引入

### 日志相关

#### logDir
- 说明：日志文件目录，运行日志将写入该目录
- 类型：字符串
- 默认值：/var/log/taos
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### minimalLogDirGB
- 说明：日志文件夹所在磁盘可用空间大小小于该值时，停止写日志
- 类型：浮点数
- 单位：GB
- 默认值：1
- 最小值：0.001f
- 最大值：10000000
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### numOfLogLines
- 说明：单个日志文件允许的最大行数
- 类型：整数
- 默认值：10,000,000
- 最小值：1000
- 最大值：2000000000
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### asyncLog
- 说明：日志写入模式
- 类型：整数；0：同步，1：异步
- 默认值：1
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### logKeepDays
- 说明：日志文件的最长保存时间，小于等于0意味着只有两个日志文件相互切换保存日志，超过两个文件保存数量的日志会被删除；当设置为大于 0 的值时，当日志文件大小达到设置的上限时会被重命名为 taosdlog.yyy，其中 yyy 为日志文件最后修改的时间戳，并滚动产生新的日志文件
- 类型：整数；0
- 单位：天
- 默认值：0
- 最小值：-365000
- 最大值：365000
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### slowLogThreshold
- 说明：慢查询门限值，大于等于门限值认为是慢查询
- 类型：整数
- 单位：秒
- 默认值：3
- 最小值：1
- 最大值：2147483647
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.3.0.0 版本开始引入

#### slowLogMaxLen
- 说明：慢查询日志最大长度
- 类型：整数
- 默认值：4096
- 最小值：1
- 最大值：16384
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.3.0.0 版本开始引入

#### slowLogScope
- 说明：慢查询记录类型
- 取值范围：ALL/QUERY/INSERT/OTHERS/NONE
- 默认值：QUERY
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.3.0.0 版本开始引入

#### slowLogExceptDb
- 说明：指定的数据库不上报慢查询，仅支持配置换一个数据库
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.3.0.0 版本开始引入

#### debugFlag
- 说明：运行日志开关，该参数的设置会影响所有模块的开关，后设置的参数起效
- 类型：整数
- 取值范围：131（输出错误和警告日志），135（输出错误、警告和调试日志），143（输出错误、警告、调试和跟踪日志）
- 默认值：131 或 135 （取决于不同模块）
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### tmrDebugFlag
- 说明：定时器模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### uDebugFlag
- 说明：共用功能模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### rpcDebugFlag
- 说明：rpc 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### qDebugFlag
- 说明：query 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### dDebugFlag
- 说明：dnode 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### vDebugFlag
- 说明：vnode 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### mDebugFlag
- 说明：mnode 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### azDebugFlag
- 说明：S3 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.3.4.3 版本开始引入

#### sDebugFlag
- 说明：sync 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### tsdbDebugFlag
- 说明：tsdb 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### tqDebugFlag
- 说明：tq 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入


#### fsDebugFlag
- 说明：fs 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### udfDebugFlag
- 说明：udf 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### smaDebugFlag
- 说明：sma 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### idxDebugFlag
- 说明：index 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### tdbDebugFlag
- 说明：tdb 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### metaDebugFlag
- 说明：meta 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### stDebugFlag
- 说明：stream 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### sndDebugFlag
- 说明：snode 模块的日志开关
- 类型：整数
- 取值范围：同上
- 默认值：131
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入


### 调试相关

#### enableCoreFile
- 说明：crash 时是否生成 core 文件
- 类型：整数；0：不生成，1：生成；
- 默认值：1
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### configDir
- 说明：配置文件所在目录
- 类型：字符串
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### forceReadConfig
- 说明：配置文件所在目录
- 类型：整数；0:使用持久化的配置参数，1:使用配置文件中的配置参数；
- 默认值：0
- 最小值：0
- 最大值：1
- 动态修改：不支持
- 支持版本：从 v3.3.5.0 版本开始引入

#### scriptDir
- 说明：测试工具的脚本目录 `内部参数`
- 类型：字符串
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### assert
- 说明：断言控制开关
- 类型：整数；0:关闭，1：开启
- 默认值：0
- 最小值：0
- 最大值：1
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### randErrorChance
- 说明：用于随机失败测试 `内部参数`
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### randErrorDivisor
- 说明：用于随机失败测试 `内部参数`
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### randErrorScope
- 说明：用于随机失败测试 `内部参数`
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### safetyCheckLevel
- 说明：用于随机失败测试 `内部参数`
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### experimental
- 说明：用于一些实验特性 `内部参数`
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### simdEnable
- 说明：用于测试 SIMD 加速 `内部参数`
- 动态修改：不支持
- 支持版本：从 v3.3.4.3 版本开始引入

#### AVX512Enable
- 说明：用于测试 AVX512 加速 `内部参数`
- 动态修改：不支持
- 支持版本：从 v3.3.4.3 版本开始引入

#### rsyncPort
- 说明：用于调试流计算 `内部参数`
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### snodeAddress
- 说明：用于调试流计算 `内部参数`
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### checkpointBackupDir
- 说明：用于恢复 snode 数据 `内部参数`
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本开始引入

#### enableAuditDelete
- 说明：用于测试审计功能 `内部参数`
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### slowLogThresholdTest
- 说明：用于测试慢日志 `内部参数`
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本开始引入

#### bypassFlag
- 说明：配置文件所在目录
- 类型：整数；
- 取值范围：0：正常写入，1：写入消息在 taos 客户端发送 RPC 消息前返回，2：写入消息在 taosd 服务端收到 RPC 消息后返回，4：写入消息在 taosd 服务端写入内存缓存前返回，8：写入消息在 taosd 服务端数据落盘前返回
- 默认值：0
- 动态修改：支持通过 SQL 修改，立即生效
- 支持版本：从 v3.3.4.5 版本开始引入

### 压缩参数

#### fPrecision
- 说明：设置 float 类型浮点数压缩精度, 小于此值的浮点数尾数部分将被截断
- 类型：浮点数
- 默认值：0.00000001
- 最小值：0.00000001
- 最大值：0.1
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### dPrecision
- 说明：设置 double 类型浮点数压缩精度，小于此值的浮点数尾数部分将被截取
- 类型：浮点数
- 默认值：0.0000000000000001
- 最小值：0.0000000000000001
- 最大值：0.1
- 动态修改：支持通过 SQL 修改，立即生效 
- 支持版本：从 v3.1.0.0 版本开始引入

#### lossyColumn
- 说明：对 float 和/或 double 类型启用 TSZ 有损压缩
- 取值范围：float/double/none
- 默认值：none，表示关闭无损压缩
- 动态修改：不支持
- 支持版本：从 v3.1.0.0 版本引入，v3.3.0.0 以后废弃

#### ifAdtFse
- 说明：在启用 TSZ 有损压缩时，使用 FSE 算法替换 HUFFMAN 算法，FSE 算法压缩速度更快，但解压稍慢，追求压缩速度可选用此算法
- 类型：整数：0：关闭，1：打开
- 默认值：0
- 最小值：0
- 最大值：1
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本引入，v3.3.0.0 以后废弃

#### maxRange
- 说明：用于有损压缩设置 `内部参数`
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本引入，v3.3.0.0 以后废弃

#### curRange
- 说明：用于有损压缩设置 `内部参数`
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本引入，v3.3.0.0 以后废弃

#### compressor
- 说明：用于有损压缩设置 `内部参数`
- 动态修改：支持通过 SQL 修改，重启生效
- 支持版本：从 v3.1.0.0 版本引入，v3.3.0.0 以后废弃

**补充说明**
1. 在 3.3.5.0 之后，所有配置参数都将被持久化到本地存储，重启数据库服务后，将默认使用持久化的配置参数列表；如果您希望继续使用 config 文件中配置的参数，需设置 forceReadConfig 为 1。
2. 在 3.2.0.0 ~ 3.3.0.0（不包含）版本生效，启用该参数后不能回退到升级前的版本
3. TSZ 压缩算法是通过数据预测技术完成的压缩，所以更适合有规律变化的数据
4. TSZ 压缩时间会更长一些，如果您的服务器 CPU 空闲多，存储空间小的情况下适合选用
5. 示例：对 float 和 double 类型都启用有损压缩
```shell
lossyColumns     float|double
```
6. 配置需重启服务生效，重启如果在 taosd 日志中看到以下内容，表明配置已生效：
```sql
   02/22 10:49:27.607990 00002933 UTL  lossyColumns     float|double
```


## taosd 监控指标

taosd 会将监控指标上报给 taosKeeper，这些监控指标会被 taosKeeper 写入监控数据库，默认是 `log` 库，可以在 taoskeeper 配置文件中修改。以下是这些监控指标的详细介绍。

### taosd\_cluster\_basic 表

`taosd_cluster_basic` 表记录集群基础信息。

| field                | type      | is\_tag | comment                         |
| :------------------- | :-------- | :------ | :------------------------------ |
| ts                   | TIMESTAMP |         | timestamp                       |
| first\_ep            | VARCHAR   |         | 集群 first ep                   |
| first\_ep\_dnode\_id | INT       |         | 集群 first ep 的 dnode id       |
| cluster_version      | VARCHAR   |         | tdengine version。例如：3.0.4.0 |
| cluster\_id          | VARCHAR   | TAG     | cluster id                      |

### taosd\_cluster\_info 表

`taosd_cluster_info` 表记录集群信息。

| field                    | type      | is\_tag | comment                                          |
| :----------------------- | :-------- | :------ | :----------------------------------------------- |
| \_ts                     | TIMESTAMP |         | timestamp                                        |
| cluster_uptime           | DOUBLE    |         | 当前 master 节点的uptime。单位：秒               |
| dbs\_total               | DOUBLE    |         | database 总数                                    |
| tbs\_total               | DOUBLE    |         | 当前集群 table 总数                              |
| stbs\_total              | DOUBLE    |         | 当前集群 stable 总数                             |
| dnodes\_total            | DOUBLE    |         | 当前集群 dnode 总数                              |
| dnodes\_alive            | DOUBLE    |         | 当前集群 dnode 存活总数                          |
| mnodes\_total            | DOUBLE    |         | 当前集群 mnode 总数                              |
| mnodes\_alive            | DOUBLE    |         | 当前集群 mnode 存活总数                          |
| vgroups\_total           | DOUBLE    |         | 当前集群 vgroup 总数                             |
| vgroups\_alive           | DOUBLE    |         | 当前集群 vgroup 存活总数                         |
| vnodes\_total            | DOUBLE    |         | 当前集群 vnode 总数                              |
| vnodes\_alive            | DOUBLE    |         | 当前集群 vnode 存活总数                          |
| connections\_total       | DOUBLE    |         | 当前集群连接总数                                 |
| topics\_total            | DOUBLE    |         | 当前集群 topic 总数                              |
| streams\_total           | DOUBLE    |         | 当前集群 stream 总数                             |
| grants_expire\_time      | DOUBLE    |         | 认证过期时间，企业版有效，社区版为 DOUBLE 最大值 |
| grants_timeseries\_used  | DOUBLE    |         | 已用测点数                                       |
| grants_timeseries\_total | DOUBLE    |         | 总测点数，开源版本为 DOUBLE 最大值               |
| cluster\_id              | VARCHAR   | TAG     | cluster id                                       |

### taosd\_vgroups\_info 表

`taosd_vgroups_info` 表记录虚拟节点组信息。

| field          | type      | is\_tag | comment                                        |
| :------------- | :-------- | :------ | :--------------------------------------------- |
| \_ts           | TIMESTAMP |         | timestamp                                      |
| tables\_num    | DOUBLE    |         | vgroup 中 table 数量                           |
| status         | DOUBLE    |         | vgroup 状态, 取值范围 unsynced = 0, ready = 1 |
| vgroup\_id     | VARCHAR   | TAG     | vgroup id                                      |
| database\_name | VARCHAR   | TAG     | vgroup 所属的 database 名字                    |
| cluster\_id    | VARCHAR   | TAG     | cluster id                                     |

### taosd\_dnodes\_info 表

`taosd_dnodes_info` 记录 dnode 信息。

| field             | type      | is\_tag | comment                                                                                           |
| :---------------- | :-------- | :------ | :------------------------------------------------------------------------------------------------ |
| \_ts              | TIMESTAMP |         | timestamp                                                                                         |
| uptime            | DOUBLE    |         | dnode uptime，单位：秒                                                                            |
| cpu\_engine       | DOUBLE    |         | taosd cpu 使用率，从 `/proc/<taosd_pid>/stat` 读取                                                |
| cpu\_system       | DOUBLE    |         | 服务器 cpu 使用率，从 `/proc/stat` 读取                                                           |
| cpu\_cores        | DOUBLE    |         | 服务器 cpu 核数                                                                                   |
| mem\_engine       | DOUBLE    |         | taosd 内存使用率，从 `/proc/<taosd_pid>/status` 读取                                              |
| mem\_free         | DOUBLE    |         | 服务器可用内存，单位 KB                                                                           |
| mem\_total        | DOUBLE    |         | 服务器内存总量，单位 KB                                                                           |
| disk\_used        | DOUBLE    |         | data dir 挂载的磁盘使用量，单位 bytes                                                             |
| disk\_total       | DOUBLE    |         | data dir 挂载的磁盘总容量，单位 bytes                                                             |
| system\_net\_in   | DOUBLE    |         | 网络吞吐率，从 `/proc/net/dev` 中读取的 received bytes。单位 byte/s                               |
| system\_net\_out  | DOUBLE    |         | 网络吞吐率，从 `/proc/net/dev` 中读取的 transmit bytes。单位 byte/s                               |
| io\_read          | DOUBLE    |         | io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 rchar 与上次数值计算之后，计算得到速度。单位 byte/s |
| io\_write         | DOUBLE    |         | io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 wchar 与上次数值计算之后，计算得到速度。单位 byte/s |
| io\_read\_disk    | DOUBLE    |         | 磁盘 io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 read_bytes。单位 byte/s                        |
| io\_write\_disk   | DOUBLE    |         | 磁盘 io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 write_bytes。单位 byte/s                       |
| vnodes\_num       | DOUBLE    |         | dnode 上 vnodes 数量                                                                              |
| masters           | DOUBLE    |         | dnode 上 master node 数量                                                                         |
| has\_mnode        | DOUBLE    |         | dnode 是否包含 mnode，取值范围 包含=1,不包含=0                                                   |
| has\_qnode        | DOUBLE    |         | dnode 是否包含 qnode，取值范围 包含=1,不包含=0                                                   |
| has\_snode        | DOUBLE    |         | dnode 是否包含 snode，取值范围 包含=1,不包含=0                                                   |
| has\_bnode        | DOUBLE    |         | dnode 是否包含 bnode，取值范围 包含=1,不包含=0                                                   |
| error\_log\_count | DOUBLE    |         | error 总数                                                                                        |
| info\_log\_count  | DOUBLE    |         | info 总数                                                                                         |
| debug\_log\_count | DOUBLE    |         | debug 总数                                                                                        |
| trace\_log\_count | DOUBLE    |         | trace 总数                                                                                        |
| dnode\_id         | VARCHAR   | TAG     | dnode id                                                                                          |
| dnode\_ep         | VARCHAR   | TAG     | dnode endpoint                                                                                    |
| cluster\_id       | VARCHAR   | TAG     | cluster id                                                                                        |

### taosd\_dnodes\_status 表

`taosd_dnodes_status` 表记录 dnode 状态信息。

| field       | type      | is\_tag | comment                                  |
| :---------- | :-------- | :------ | :--------------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                                |
| status      | DOUBLE    |         | dnode 状态,取值范围 ready=1，offline =0 |
| dnode\_id   | VARCHAR   | TAG     | dnode id                                 |
| dnode\_ep   | VARCHAR   | TAG     | dnode endpoint                           |
| cluster\_id | VARCHAR   | TAG     | cluster id                               |

### taosd\_dnodes\_log\_dir 表

`taosd_dnodes_log_dir` 表记录 log 目录信息。

| field       | type      | is\_tag | comment                             |
| :---------- | :-------- | :------ | :---------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                           |
| avail       | DOUBLE    |         | log 目录可用空间。单位 byte         |
| used        | DOUBLE    |         | log 目录已使用空间。单位 byte       |
| total       | DOUBLE    |         | log 目录空间。单位 byte             |
| name        | VARCHAR   | TAG     | log 目录名，一般为 `/var/log/taos/` |
| dnode\_id   | VARCHAR   | TAG     | dnode id                            |
| dnode\_ep   | VARCHAR   | TAG     | dnode endpoint                      |
| cluster\_id | VARCHAR   | TAG     | cluster id                          |

### taosd\_dnodes\_data\_dir 表

`taosd_dnodes_data_dir` 表记录 data 目录信息。

| field       | type      | is\_tag | comment                           |
| :---------- | :-------- | :------ | :-------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                         |
| avail       | DOUBLE    |         | data 目录可用空间。单位 byte      |
| used        | DOUBLE    |         | data 目录已使用空间。单位 byte    |
| total       | DOUBLE    |         | data 目录空间。单位 byte          |
| level       | VARCHAR   | TAG     | 0、1、2 多级存储级别              |
| name        | VARCHAR   | TAG     | data 目录，一般为 `/var/lib/taos` |
| dnode\_id   | VARCHAR   | TAG     | dnode id                          |
| dnode\_ep   | VARCHAR   | TAG     | dnode endpoint                    |
| cluster\_id | VARCHAR   | TAG     | cluster id                        |

### taosd\_mnodes\_info 表

`taosd_mnodes_info` 表记录 mnode 角色信息。

| field       | type      | is\_tag | comment                                                                                                  |
| :---------- | :-------- | :------ | :------------------------------------------------------------------------------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                                                                                                |
| role        | DOUBLE    |         | mnode 角色， 取值范围 offline = 0,follower = 100,candidate = 101,leader = 102,error = 103,learner = 104 |
| mnode\_id   | VARCHAR   | TAG     | master node id                                                                                           |
| mnode\_ep   | VARCHAR   | TAG     | master node endpoint                                                                                     |
| cluster\_id | VARCHAR   | TAG     | cluster id                                                                                               |

### taosd\_vnodes\_role 表

`taosd_vnodes_role` 表记录虚拟节点角色信息。

| field          | type      | is\_tag | comment                                                                                                 |
| :------------- | :-------- | :------ | :------------------------------------------------------------------------------------------------------ |
| \_ts           | TIMESTAMP |         | timestamp                                                                                               |
| vnode\_role    | DOUBLE    |         | vnode 角色，取值范围 offline = 0,follower = 100,candidate = 101,leader = 102,error = 103,learner = 104 |
| vgroup\_id     | VARCHAR   | TAG     | dnode id                                                                                                |
| dnode\_id      | VARCHAR   | TAG     | dnode id                                                                                                |
| database\_name | VARCHAR   | TAG     | vgroup 所属的 database 名字                                                                             |
| cluster\_id    | VARCHAR   | TAG     | cluster id                                                                                              |

### taosd\_sql\_req 表

`taosd_sql_req` 记录服务端 sql 请求信息。

| field       | type      | is\_tag | comment                                  |
| :---------- | :-------- | :------ | :--------------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                                |
| count       | DOUBLE    |         | sql 数量                                 |
| result      | VARCHAR   | TAG     | sql的执行结果，取值范围 Success, Failed |
| username    | VARCHAR   | TAG     | 执行sql的user name                       |
| sql\_type   | VARCHAR   | TAG     | sql类型，取值范围 inserted_rows         |
| dnode\_id   | VARCHAR   | TAG     | dnode id                                 |
| dnode\_ep   | VARCHAR   | TAG     | dnode endpoint                           |
| vgroup\_id  | VARCHAR   | TAG     | dnode id                                 |
| cluster\_id | VARCHAR   | TAG     | cluster id                               |

### taos\_sql\_req 表

`taos_sql_req` 记录客户端 sql 请求信息。

| field       | type      | is\_tag | comment                                   |
| :---------- | :-------- | :------ | :---------------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                                 |
| count       | DOUBLE    |         | sql 数量                                  |
| result      | VARCHAR   | TAG     | sql的执行结果，取值范围 Success, Failed  |
| username    | VARCHAR   | TAG     | 执行sql的user name                        |
| sql\_type   | VARCHAR   | TAG     | sql类型，取值范围 select, insert，delete |
| cluster\_id | VARCHAR   | TAG     | cluster id                                |

### taos\_slow\_sql 表

`taos_slow_sql` 记录客户端慢查询信息。

| field       | type      | is\_tag | comment                                               |
| :---------- | :-------- | :------ | :---------------------------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                                             |
| count       | DOUBLE    |         | sql 数量                                              |
| result      | VARCHAR   | TAG     | sql的执行结果，取值范围 Success, Failed              |
| username    | VARCHAR   | TAG     | 执行sql的user name                                    |
| duration    | VARCHAR   | TAG     | sql执行耗时，取值范围 3-10s,10-100s,100-1000s,1000s- |
| cluster\_id | VARCHAR   | TAG     | cluster id                                            |

### taos\_slow\_sql\_detail 表

`taos_slow_sql_detail` 记录客户端慢查询详细信息。子表名规则为 `{user}_{db}_{ip}_clusterId_{cluster_id}`

| field          | type      | is\_tag | comment                                               |
| :------------- | :-------- | :------ | :---------------------------------------------------- |
| start\_ts      | TIMESTAMP |         | sql 开始执行的客户端时间，单位ms，主键                     |
| request\_id    | UINT64_T  |         | sql 请求的 request id，为 hash 生产的随机值              |
| query\_time    | INT32_T   |         | sql 执行耗时, 单位ms                                   |
| code           | INT32_T   |         | sql 执行返回码，0表示成功                               |
| error\_info    | VARCHAR   |         | sql 执行失败时，记录的错误信息                           |
| type           | INT8_T    |         | sql 语句的类型（1-查询，2-写入，4-其他）                  |
| rows\_num      | INT64_T   |         | sql 执行结果的记录数目                                   |
| sql            | VARCHAR   |         | sql 语句的字符串                                       |
| process\_name  | VARCHAR   |         | 进程名称                                              |
| process\_id    | VARCHAR   |         | 进程 id                                              |
| db             | VARCHAR   | TAG     | 执行 sql 所属数据库                                    |
| user           | VARCHAR   | TAG     | 执行 sql 语句的用户                                    |
| ip             | VARCHAR   | TAG     | 记录执行 sql 语句的 client 的 ip 地址                   |
| cluster\_id    | VARCHAR   | TAG     | cluster id                                           |

