# 数据接入适配工具-Design Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-07 | 2025-01-07 | 1.0 | 谭雪峰 | 安可送测第一版 |
| 2025-11-28 | 2025-11-28 | 1.1 | 霍琳贺 | 1. 添加架构设计 1. 添加安全考虑 1. 其他修改 |

## 2. 引言

### 2.1 目的

本设计文档旨在详细描述 taosAdapter 的设计目标、技术架构和实现细节，为开发、部署及维护 taosAdapter 提供指导。同时，本文档将为后续的功能扩展和性能优化提供设计依据，确保 taosAdapter 能持续高效地支持 TDengine 生态系统。

### 2.2 范围

taosAdapter 是一个为 TDengine 提供支持的桥接工具，主要用于：
1. 实现与数据采集代理（如 Telegraf、StatsD 和 collectd）的无缝集成。
2. 提供兼容 InfluxDB 和 OpenTSDB 协议的数据写入接口。
3. 优化数据从第三方系统到 TDengine 的传输效率。

### 2.3 受众

本设计文档的目标读者包括：
1. **开发人员**：负责实现和优化 taosAdapter 的工程师。
2. **系统架构师**：需要理解 taosAdapter 的整体架构和技术决策。
3. **运维工程师**：负责部署和维护 taosAdapter 的人员。

## 3. 术语

1. **TDengine****：** 一种开源的时序数据库，专为处理物联网、大数据和实时分析场景下的大规模数据采集、存储和查询而设计。
2. **taosAdapter****：** 为 TDengine 提供桥接功能的适配器工具，支持与第三方数据采集代理和主流协议的集成。
3. **HTTP****：** 一种应用层协议，用于分布式超媒体信息系统的通信，是 Web 服务和接口通信的基础。
4. **RESTful API****：** 一种基于 HTTP 的架构风格，用于构建分布式系统，强调无状态通信和资源的统一表示。
5. **WebSocket****：** 一种全双工通信协议，用于在客户端和服务器之间建立持久连接，适合低延迟和实时数据传输的场景。
6. **Basic 验证****：** 一种 HTTP 身份验证机制，通过在请求头中携带用户名和密码进行简单的身份验证。
7. **JSON****：** 一种轻量级的数据交换格式，易于人类阅读和编写，同时便于机器解析和生成，广泛用于 RESTful 接口的数据传输。

## 4. 概述

1. 架构：描述整体架，可能包括类图、组件图或系统的其他结构表示
![](./images/img_NoTvb5q2uoasPXxwNlycx8bxnIf.png)

1. 技术：
   - 开发语言：Go
   - 调用动态库：cgo
   - HTTP 框架: gin（https://gin-gonic.com/）
   - WebSocket 框架：websocket（github.com/gorilla/websocket）
   - 日志库：logrus（github.com/sirupsen/logrus）
   - 日志切割库：file-rotatelogs（github.com/taosdata/file-rotatelogs）
   - 配置解析：viper（github.com/spf13/viper）
   - JSON 库：标准库（json）、json-iterator（github.com/json-iterator/go）
2. 依赖项：列出所有依赖项
   - TDengine 客户端动态库
   - cgo 使用 gcc （Windows 上使用 msys2 和 mingw）

## 5. 设计考虑

1. 假设
   - 数据采集代理（如 Telegraf、StatsD 和 collectd）已正确配置并能够稳定运行。
   - 使用 taosAdapter 的系统运行在高可靠性的网络环境中。
   - TDengine 的实例已正常部署，且拥有足够的存储和计算能力。
2. 限制
   - 部署的 taosAdapter 版本与 TDengine 版本相对应。
   - 单独部署 taosAdapter 需要安装 TDengine 客户端。
   - 被弃用的的接口只为兼容做保留不在其上做新功能。
3. 设计模式和原则
   - 适配器模式
   - 单例模式
   - 策略模式
4. 设计原则:
   - 模块化设计: 各功能模块分离，便于扩展和维护。
   - 接口隔离原则: 各模块之间通过明确的接口交互，减少耦合。
   - 高内聚**低耦合**: 各模块专注于自身的功能，减少对其他模块的依赖。
5. 风险和缓解措施：识别潜在风险和缓解策略。
   - 风险：由于查询结果大导致 HTTP 查询结果在内存缓存。
      - 缓解措施：使用 HTTP 分块传输和 JSON 流式拼接方案分块返回结果。
   - 风险：C 函数执行时间长导致 go 创建大量线程。
      - 缓解措施：
         - 使用 channel 来模拟信号量控制 C 函数并发度，对于执行时间短的 C 函数不进行控制。
         - 针对订阅这种高频调用且大部分为串行操作场景，每个连接创建一个 C 线程，C 函数调用在这个线程上进行避免运行时创建新线程，并且在调用前加锁保证线程安全。

## 6. 架构设计

taosAdapter 采用插件化设计，核心为一个基于 Gin 框架的 HTTP 服务器，通过 CGO 调用 TDengine C 客户端库（libtaos）与 TDengine 交互。

### 6.1 分层架构

![](./images/wb_Sdmcwf8tahtjEFbowvJcadgynve.png)

### 6.2 核心组件

##### 6.2.0.1 Controller 层

- **REST Controller**：处理 RESTful SQL 请求
- **WS Controller**：处理 WebSocket 连接与消息
  - Query: SQL 查询/写入
  - Stmt: 预编译语句
  - Schemaless: 无模式写入
  - TMQ: 消息队列订阅
- **Ping Controller**：健康检查
- **Metrics Controller**：Prometheus 指标暴露

##### 6.2.0.2 Plugin 层

插件通过实现 `Plugin` 接口注册，统一生命周期管理：
```go
type Plugin interface {
    Init(r gin.IRouter) error
    Start() error
    Stop() error
    String() string
    Version() string
}
```

已实现插件：
- InfluxDB / OpenTSDB / StatsD / collectd
- Prometheus / node_exporter / OpenMetrics
- Authentication

##### 6.2.0.3 数据库访问层

- **commonpool**：连接池管理，支持白名单过滤
- **syncinterface**：同步接口（查询、DDL/DML）
- **async**：异步接口（大批量写入）
- **asynctmq**：消息队列

##### 6.2.0.4 CGO 封装层

- 使用信号量（Semaphore）控制 C 方法并发
- 支持同步（`SyncLocker`）与异步（`AsyncLocker`）限流
- 支持 taosc 客户端白名单回调机制

## 7. 详细设计

### 7.1 配置

1. 组件设计：
   - 支持配置文件、环境变量和命令行参数三种配置方式，命令行参数优先于环境变量优先于配置文件。
   - 默认配置文件是 `/etc/taos/taosadapter.toml`，windows 上默认配置文件 `C:\TDengine\cfg\taosadapter.toml`。
   - 使用 `viper.SetDefault` 设置默认值， `viper.BindEnv` 设置配置项与环境变量对应，`pflag.Bool` `pflag.Int` `pflag.Duration` 等方法设置命令行参数与配置项对应关系。
   - 对应的代码路径在 `config` 文件夹下以及 `plugin` 文件夹下各个插件的 `config.go`。
2. 列出系统中的关键数据结构：
   - 全局配置
    ```go
    type Config struct {
        // 跨域设置
        Cors                CorsConfig
        // TDengine 配置文件夹
        TaosConfigDir       string
        // 是否开启 Debug 模式（pprof）
        Debug               bool
        // HTTP 和 WebSocket 对外端口
        Port                int
        // 日志级别
        LogLevel            string
        // HTTP 查询返回的行数
        RestfulRowLimit     int
        // 失败是否返回 http 错误码，具体见 FS
        HttpCodeServerError bool
        // schemaless 是否自动创建 DB
        SMLAutoCreateDB     bool
        // 日志相关配置
        Log                 Log
        // 连接池相关配置
        Pool                Pool
        // 监控相关配置
        Monitor             Monitor
        // 上报给 taosKeeper 相关配置
        UploadKeeper        UploadKeeper
        // TMQ 相关配置（已废弃）
        TMQ                 TMQ
    }
    ```

   - 跨域设置，为简化配置配置文件中只保留 `AllowAllOrigins` 且默认为 `true`
    ```go
    type CorsConfig struct {
            // AllowAllOrigins 表示是否允许所有来源进行跨域请求。
            AllowAllOrigins bool
    
            // AllowOrigins 列出允许的特定来源（域名）进行跨域请求。
            AllowOrigins []string
    
            // AllowHeaders 列出允许的请求头。
            AllowHeaders []string
    
            // ExposeHeaders 列出可以暴露给浏览器的响应头。
            ExposeHeaders []string
    
            // AllowCredentials 表示是否允许跨域请求中携带认证信息（如 Cookie）。
            AllowCredentials bool
    
            // AllowWebSockets 表示是否允许 WebSocket 连接的跨域请求。
            AllowWebSockets bool
    }
    ```

   - 日志配置
    ```go
    type Log struct {
            // Path 表示日志文件的存储路径。
            Path string
    
            // RotationCount 表示日志文件轮换的最大数量，达到该数量后会删除最旧的日志文件。
            RotationCount uint
    
            // RotationTime 表示日志文件轮换的时间间隔。
            RotationTime time.Duration
    
            // RotationSize 表示日志文件轮换的大小限制，达到该大小后会轮换日志文件。
            RotationSize uint
    
            // EnableRecordHttpSql 表示是否记录 HTTP 请求中的 SQL 查询。
            EnableRecordHttpSql bool
    
            // SqlRotationCount 表示 SQL 日志文件的轮换数量限制。
            SqlRotationCount uint
    
            // SqlRotationTime 表示 SQL 日志文件的轮换时间间隔。
            SqlRotationTime time.Duration
    
            // SqlRotationSize 表示 SQL 日志文件的轮换大小限制。
            SqlRotationSize uint
    }
    
    ```

   - 连接池配置
    ```go
    type Pool struct {
            // MaxConnect 表示连接池中最大连接数。
            MaxConnect int
    
            // MaxIdle 表示连接池中最大空闲连接数。
            MaxIdle int
    
            // IdleTimeout 表示连接池中空闲连接的超时时间（此项已被废弃为兼容保留）。
            IdleTimeout time.Duration
    }
    ```

   - 监控配置
    ```go
    type Monitor struct {
            // 表示是否禁用监控功能，（已废弃，指标上报给 taosKeeper）。
            Disable bool
    
            // 表示收集监控数据的时间间隔。
            CollectDuration time.Duration
    
            // 表示是否在 cgroup 环境中运行。
            InCGroup bool
    
            // 表示taosAdapter内存占比超过此阈值时，通过 HTTP 查询请求将返回错误。
            PauseQueryMemoryThreshold float64
    
            // 表示taosAdapter内存占比超过此阈值时，通过 HTTP 查询和写入请求将返回错误。
            PauseAllMemoryThreshold float64
    
            // 表示监控实例的标识符，用于区分不同的监控实例（上报给 taosKeeper 时用到此项）。
            Identity string
    }
    
    type Monitor struct {
        // 表示是否禁用监控功能（建议禁用，使用上传到 taosKeeper）
        Disable                   bool
        // 表示收集监控数据的时间间隔
        CollectDuration           time.Duration
        // 是否禁用客户端ip维度（如果客户端特别多会占用大量内存）
        DisableClientIP           bool
        // 表示是否在 cgroup 环境中运行
        InCGroup                  bool
        // 表示taosAdapter内存占比超过此阈值时，通过 HTTP 查询请求将返回错误。
        PauseQueryMemoryThreshold float64
        // 表示taosAdapter内存占比超过此阈值时，通过 HTTP 查询和写入请求将返回错误。
        PauseAllMemoryThreshold   float64
        // 表示监控实例的标识符，用于区分不同的监控实例（上报给 taosKeeper 时用到此项）。
        Identity                  string
        // 是否写入到 TDengine(建议此项不使用，将指标上报到 taosKeeper）
        WriteToTD                 bool
        // 写入到 TDengine 使用的用户名
        User                      string
        // 写入到 TDengine 使用的密码
        Password                  string
        // 写入 TDengine 间隔
        WriteInterval             time.Duration
    }
    
    ```

   - 指标上报 taoskeeper 配置项
    ```go
    type UploadKeeper struct {
            // Enable 表示是否启用上传功能，启用时会执行上传操作。
            Enable bool
    
            // Url 表示上传的目标 URL 地址。
            Url string
    
            // Interval 表示上传操作的执行间隔时间。
            Interval time.Duration
    
            // Timeout 表示上传操作的超时时间，超过该时间没有完成上传则认为失败。
            Timeout time.Duration
    
            // RetryTimes 表示上传失败后的最大重试次数。
            RetryTimes uint
    
            // RetryInterval 表示上传失败后，重试操作的间隔时间。
            RetryInterval time.Duration
    }
    
    ```

1. 组件设计：
   - 设置配置项
      - 设置默认值使用 `viper.SetDefault`，例如设置日志保留30份
      ```go
      viper.SetDefault("log.rotationCount", 30)
      ```

      - 设置环境变量使用 `viper.BindEnv`，例如设置`log.rotationCount`对应环境变量`TAOS_ADAPTER_LOG_ROTATION_COUNT`
      ```go
      viper.BindEnv("log.rotationCount", "TAOS_ADAPTER_LOG_ROTATION_COUNT")
      ```

      - 设置命令行使用 pflag 相关接口，例如：
      ```go {wrap}
      pflag.Uint("log.rotationCount", 30, `log rotation count. Env "TAOS_ADAPTER_LOG_ROTATION_COUNT"`)
      // 设置一字符别名
      pflag.BoolP("version", "V", false, "Print the version and exit")
      ```

   - 读取配置项
    使用 viper 的 Get 相关接口，例如读取日志配置
    ```go {wrap}
    type Log struct {
        Path          string
        RotationCount uint
        RotationTime  time.Duration
        RotationSize  uint
    
        EnableRecordHttpSql bool
        SqlRotationCount    uint
        SqlRotationTime     time.Duration
        SqlRotationSize     uint
    }
    
    func (l *Log) setValue() {
        l.Path = viper.GetString("log.path")
        l.RotationCount = viper.GetUint("log.rotationCount")
        l.RotationTime = viper.GetDuration("log.rotationTime")
        l.RotationSize = viper.GetSizeInBytes("log.rotationSize")
        l.EnableRecordHttpSql = viper.GetBool("log.enableRecordHttpSql")
        l.SqlRotationCount = viper.GetUint("log.sqlRotationCount")
        l.SqlRotationTime = viper.GetDuration("log.sqlRotationTime")
        l.SqlRotationSize = viper.GetSizeInBytes("log.sqlRotationSize")
    }
    ```

   - 特殊命令行参数
      - -V 或 --version 打印版本并推出。
      - --help 打印全部命令行参数。

### 7.2 日志

1. 组件设计：
   - 使用结构化日志框架 logrus。
   - 自定义日志输出格式：
      - 时间戳，精确到微秒，不含年份，格式统一为 mm/dd HH:MM:SS.000000。
      - 进程 ID。
      - 固定字符串 "taos_ADAPTER"。
      - 日志级别：分别使用 trace/debug/info/warning/error/fatal/panic。
      - 日志消息体，消息体中如果有 key=value  形式，使用等号分隔 key=value，使用空格分割 key-value 对。
   - 使用 hook 方式将日志同时输出到 stdout 与配置文件，前台运行时可以通过命令行看到日志，后台运行时可以通过配置文件看到日志。
   - 达到以下条件时日志写入文件：
      - 缓存 5 秒。
      - 缓存超过 1024 字节。
      - Fatal 和 Panic 日志立即记录。
      - 程序退出。
   - 请求结束时打印 info 级别日志、记录 http 响应码、请求持续时间、客户端 ip、请求方法、请求的 uri。
   - HTTP SQL 记录时间、进程ID、请求ID、sql 内容。
   - 对应代码在 `log`。
2. 列出系统中的关键数据结构
   - 日志格式化结构
  ```go {wrap}
  type TaosLogFormatter struct {
  }
  // 实现 logrus.Formatter,自定义格式化方法
  func (t *TaosLogFormatter) Format(entry *logrus.Entry) ([]byte, error)
  ```

   - 日志写文件钩子
  ```go {wrap}
  type FileHook struct {
      // 格式化
      formatter logrus.Formatter
      // 文件 writer
      writer    io.Writer
      // 缓存
      buf       *bytes.Buffer
      sync.Mutex
  }
  // 实现 logrus.Hook 接口，将日志输出到其他地方
  func (f *FileHook) Levels() []logrus.Level
  func (f *FileHook) Fire(entry *logrus.Entry) error
  ```


1. 使用几种类型的图表来解释设计
   - 日志处理流程
  <diagram type="1"/>

### 7.3 连接池

由于 HTTP 请求是无状态的，因此需要使用数据库连接池来减少每次请求建立新数据库连接所带来的延迟。
1. 组件设计：
   - 需要针对每个不同的用户名创建连接池，使用 map 管理，key 为用户名，value 为连接池。
   - 连接池 map 使用场景为读多写少，使用 sync.Map 进行优化。
   - 如果短时间内来许多尝试创建连接池的操作，使用 singleflight 来对同一用户名和密码操作进行合并。
   - 所有创建出来的连接无超时时间，只要创建出来就一直复用直到连接池释放。
   - 配置项有获取连接超时和做多等待数限制，当获取连接超时或当前等待获取连接池的请求超过限制则直接返回失败。
   - 当连接池释放时所有等待中的请求返回错误，空闲连接释放，当使用后连接放回时释放连接。
   - 当连接池创建成功之后获取一个连接注册用户删除、修改密码、白名单变动事件。
      - 当接收到用户删除和修改密码的事件后释放当前连接池并将当前连接池从 map 移除。
      - 当白名单（企业版功能）变动之后重新获取白名单。
   - 相关代码在 `db/commonpool` 目录和 `tools/connectpool`目录，`tools/connectpool` 负责单个连接池的操作，`db/commonpool` 负责连接池 map 管理，应用代码调用 `db/commonpool`内接口，不可调用 `tools/connectpool` 内接口。
2. 列出系统中的关键数据结构
   - 单个连接池 `tools/connectpool`
  ```go {wrap}
  // 配置项
  type Config struct {
          // 初始化创建连接池
          InitialCap  int
          // 最大数量
          MaxCap      int
          // 最大等待数
          MaxWait     int
          // 等待超时时间
          WaitTimeout time.Duration
          // 创建连接方法
          Factory     func() (unsafe.Pointer, error)
          // 释放连接方法
          Close       func(pointer unsafe.Pointer)
  }
  
  // 连接池
  type ConnectPool struct {
      // 锁
      mu           sync.RWMutex
      // 空闲连接队列
      conns        chan unsafe.Pointer
      // 创建连接方法
      factory      func() (unsafe.Pointer, error)
      // 释放连接方法
      close        func(pointer unsafe.Pointer)
      // 最大数量
      maxActive    int
      // 正在使用中的连接数
      openingConns int
      // 最大等待数
      maxWait      int
      // 等待超时时间
      waitTimeout  time.Duration
      // 请求等待队列
      connReqs     []chan connReq
      // 连接池释放标志位
      released     bool
      // 保证只执行一次的同步，保证只释放一次
      releasedOnce sync.Once
  }
  
  // 构造方法
  func NewConnectPool(poolConfig *Config) (*ConnectPool, error)
  // 获取连接
  func (c *ConnectPool) Get() (unsafe.Pointer, error) 
  // 放回连接
  func (c *ConnectPool) Put(conn unsafe.Pointer) error
  // 释放连接池
  func (c *ConnectPool) Release()
  ```

   - `db/commonpool` 封装连接池
  ```go {wrap}
  type ConnectorPool struct {
      // 密码修改通知管道
      changePassChan        chan int32
      // 白名单变动通知管道
      whitelistChan         chan int64
      // 用户删除通知管道
      dropUserChan          chan struct{}
      // 用户名
      user                  string
      // 密码
      password              string
      // 连接池
      pool                  *connectpool.ConnectPool
      // 日志记录对象
      logger                *logrus.Entry
      // 保护只释放一次的锁
      once                  sync.Once
      // 主动取消的上下文
      ctx                   context.Context
      // 主动取消函数
      cancel                context.CancelFunc
      // ip白名单的读写锁
      ipNetsLock            sync.RWMutex
      // ip 白名单
      ipNets                []*net.IPNet
  }
  // 创建连接池
  func NewConnectorPool(user, password string) (*ConnectorPool, error) 
  // 从连接池获取连接
  func (cp *ConnectorPool) Get() (unsafe.Pointer, error)
  // 连接放回池
  func (cp *ConnectorPool) Put(c unsafe.Pointer) error
  // 验证密码
  func (cp *ConnectorPool) verifyPassword(password string) bool
  // 验证 ip
  func (cp *ConnectorPool) verifyIP(ip net.IP) bool
  // 释放连接池
  func (cp *ConnectorPool) Release()
  
  // 应用拿到的连接
  type Conn struct {
      TaosConnection unsafe.Pointer
      pool           *ConnectorPool
  }
  
  // 获取连接（应用直接调用）
  func GetConnection(user, password string, clientIp net.IP) (*Conn, error)
  // 验证 ip（应用直接调用）
  func VerifyClientIP(user, password string, clientIP net.IP) (authed bool, valid bool, connectionPoolExits bool)
  ```

1. 使用几种类型的图表来解释设计
   - 获取连接流程图
    ![](./images/wb_XCRtw2P3eh0QbOb6D90cTNNTnOh.png)

   - 过程1 从连接池获取连接
    ![](./images/wb_OtiTwkP3ahx8fJbBICBcPiiyn8e.png)

   - 过程2 创建连接池
    ![](./images/wb_Su2iwb57ehUyTebYOWYcCWyjnxg.png)

   - 过程3 释放连接池
    ![](./images/wb_JShOwWQW0hjltXbNWM0cT59pn6b.png)

   - 连接放回连接池
    ![](./images/wb_DyK1wYKO3hFUgmbPyfwcMGZQn4b.png)

### 7.4 同步接口封装

1. 组件设计：
   - 使用 cgo 封装 TDengine C 接口，使用 go 连接器的封装`github.com/taosdata/driver-go/v3/wrapper`。
   - 使用 channel 控制 C 调用并发度，代码位置 `thread/locker.go`。

### 7.5 异步接口封装

1. 组件设计：
   - 使用 cgo 封装 TDengine C 接口，代码位置 使用 go 连接器的封装`github.com/taosdata/driver-go/v3/wrapper`。
   - 使用 channel 控制 C 调用并发度，代码位置 `thread/locker.go`。
   - 二次封装带并发控制的封装接口，代码位置 `db/async`。
   - 初始化一万个异步接口使用的上下文 `db/init.go`。
2. 列出系统中的关键数据结构
   - 异步接口使用的上下文
  ```go {wrap}
  // 异步请求上下文
  type Handler struct {
      Handler cgo.Handle
      Caller  *Caller
  }
  
  // 上下文池
  type HandlerPool struct {
      // 锁
      mu       sync.RWMutex
      // 上下文总数量
      count    int
      // 空闲上下文
      handlers chan *Handler
      // 等待上下文的请求链表
      reqList  *list.List
  }
  
  // 等待上下文的请求
  type poolReq struct {
      idleHandler *Handler
  }
  
  
  ```

   - 查询和拉取结果
  ```go {wrap}
  // 回调结果
  type Result struct {
      Res unsafe.Pointer
      N   int
  }
  
  // 上下文内容
  type Caller struct {
      // 查询结果 channel
      QueryResult chan *Result
      // 获取数据 channel
      FetchResult chan *Result
  }
  
  // 查询回调函数内调用
  func (c *Caller) QueryCall(res unsafe.Pointer, code int) {
      c.QueryResult <- &Result{
         Res: res,
         N:   code,
      }
  }
  
  // 拉取结果回调函数内调用
  func (c *Caller) FetchCall(res unsafe.Pointer, numOfRows int) {
      c.FetchResult <- &Result{
         Res: res,
         N:   numOfRows,
      }
  }
  ```

   - 异步接口 `db/async`
  ```go {wrap}
  // 异步接口
  type Async struct {
      // 上下文池
      HandlerPool *HandlerPool
  }
  
  // sql 执行结果
  type ExecResult struct {
      AffectedRows int
      FieldCount   int
      Header       *wrapper.RowsHeader
      Data         [][]driver.Value
  }
  
  // 完整的执行和获取数据
  func (a *Async) TaosExec(taosConnect unsafe.Pointer, sql string, timeFormat wrapper.FormatTimeFunc, reqID int64) (*ExecResult, error)
  
  // 异步执行 SQL
  func (a *Async) TaosQuery(taosConnect unsafe.Pointer, sql string, handler *Handler, reqID int64) (*Result, error)
  
  // 异步获取单行数据
  func (a *Async) TaosFetchRowsA(res unsafe.Pointer, handler *Handler) (*Result, error)
  
  // 异步获取原始块数据
  func (a *Async) TaosFetchRawBlockA(res unsafe.Pointer, handler *Handler) (*Result, error)
  
  // 异步执行 SQL 不获取数据（用在非查询 SQL）
  func (a *Async) TaosExecWithoutResult(taosConnect unsafe.Pointer, sql string, reqID int64) error
  ```

1. 使用几种类型的图表来解释设计
   - 异步查询时序图
  ![](./images/wb_SyHJwN1FNhMpgwb83v0cLnPBnhh.png)

### 7.6 TMQ 接口封装

1. 组件设计：
   - 每个连接使用 C 原生创建一个线程，所有操作都在这个线程上执行，并提供异步接口，但要求 Go 调用使用mutex 保证只能串行执行。连接关闭后销毁 C 创建的线程。代码位置 `db/asynctmq`。
   - `controller/ws/tmq` 封装了带 mutex 和日志的 TMQ 调用。
2. 列出系统中的关键数据结构
   - C 异步任务
    ```c {wrap}
    // 定义 TMQ 事件类型的枚举，用于描述不同的操作类型。
    typedef enum {
        TAOSA_TMQ_POLL = 1,                 // 拉取消息
        TAOSA_TMQ_FREE = 2,                 // 释放资源
        TAOSA_TMQ_COMMIT = 3,               // 提交偏移量
        TAOSA_TMQ_FETCH_RAW_BLOCK = 4,      // 获取原始数据块
        TAOSA_TMQ_NEW_CONSUMER = 5,         // 创建新的消费者
        TAOSA_TMQ_SUBSCRIBE = 6,            // 订阅主题
        TAOSA_TMQ_UNSUBSCRIBE = 7,          // 取消订阅
        TAOSA_TMQ_CONSUMER_CLOSE = 8,       // 关闭消费者
        TAOSA_TMQ_GET_RAW = 9,              // 获取原始数据
        TAOSA_TMQ_GET_JSON_META = 10,       // 获取 JSON 元数据
        TAOSA_TMQ_GET_TOPIC_ASSIGNMENT = 11,// 获取主题分配信息
        TAOSA_TMQ_OFFSET_SEEK = 12,         // 偏移量定位
        TAOSA_TMQ_COMMIT_OFFSET = 13,       // 提交偏移量
        TAOSA_TMQ_COMMITTED = 14,           // 获取已提交的偏移量
        TAOSA_TMQ_POSITION = 15             // 获取当前偏移量
    } TMQ_EVENT;
    
    // TMQ 线程结构体，用于管理线程的任务和状态。
    typedef struct tmq_thread {
        TMQ_EVENT event;                   // 当前事件类型
        int shutdown;                      // 标识是否关闭线程
        void *task;                        // 当前任务的指针
        pthread_mutex_t lock;              // 线程锁
        pthread_cond_t notify;             // 条件变量，用于线程间通信
        pthread_t thread;                  // 线程标识符
    } tmq_thread;
    
    // 定义各种 TMQ 回调函数类型。
    typedef void (*adapter_tmq_poll_a_cb)(uintptr_t param, void *res); // 拉取消息回调
    typedef void (*adapter_tmq_free_result_cb)(uintptr_t param);       // 释放结果回调
    typedef void (*adapter_tmq_commit_cb)(uintptr_t param, int32_t code); // 提交偏移量回调
    typedef void (*adapter_tmq_fetch_raw_block_cb)(uintptr_t param, int32_t code, int32_t block_size, void *pData); // 获取原始数据块回调
    typedef void (*adapter_tmq_new_consumer_cb)(uintptr_t param, tmq_t *tmq, char *errstr); // 创建消费者回调
    typedef void (*adapter_tmq_subscribe_cb)(uintptr_t param, int32_t errcode); // 订阅回调
    typedef void (*adapter_tmq_unsubscribe_cb)(uintptr_t param, int32_t errcode); // 取消订阅回调
    typedef void (*adapter_tmq_consumer_close_cb)(uintptr_t param, int32_t errcode); // 关闭消费者回调
    typedef void (*adapter_tmq_get_raw_cb)(uintptr_t param, int32_t errcode); // 获取原始数据回调
    typedef void (*adapter_tmq_get_json_meta_cb)(uintptr_t param, char *meta); // 获取 JSON 元数据回调
    typedef void (*adapter_tmq_get_topic_assignment_cb)(uintptr_t param, char *topic_name, int32_t errcode, tmq_topic_assignment *assignment, int32_t numOfAssignment); // 获取主题分配信息回调
    typedef void (*adapter_tmq_offset_seek_cb)(uintptr_t param, char *topic_name, int32_t code); // 偏移量定位回调
    typedef void (*adapter_tmq_commit_offset_cb)(uintptr_t param, char *topic_name, int32_t code); // 提交偏移量回调
    typedef void (*adapter_tmq_committed_cb)(uintptr_t param, char *topic_name, int64_t errcode); // 获取已提交偏移量回调
    typedef void (*adapter_tmq_position_cb)(uintptr_t param, char *topic_name, int64_t errcode); // 获取当前位置偏移量回调
    
    // 定义各种任务结构体，每个结构体对应一个 TMQ 操作。
    typedef struct poll_task {
        tmq_t *tmq;                        // TMQ 指针
        int64_t timeout;                   // 拉取超时时间
        uintptr_t param;                   // 用户参数
        adapter_tmq_poll_a_cb cb;          // 拉取消息的回调函数
    } poll_task;
    
    typedef struct free_task {
        TAOS_RES *res;                     // 要释放的资源指针
        adapter_tmq_free_result_cb cb;     // 释放资源回调函数
        uintptr_t param;                   // 用户参数
    } free_task;
    
    typedef struct commit_task {
        tmq_t *tmq;                        // TMQ 指针
        TAOS_RES *msg;                     // 消息指针
        adapter_tmq_commit_cb cb;          // 提交回调函数
        uintptr_t param;                   // 用户参数
    } commit_task;
    
    typedef struct fetch_raw_block_task {
        TAOS_RES *res;                     // 数据资源
        adapter_tmq_fetch_raw_block_cb cb; // 获取原始数据块回调函数
        uintptr_t param;                   // 用户参数
    } fetch_raw_block_task;
    
    typedef struct new_consumer_task {
        tmq_conf_t *conf;                  // 消费者配置
        char *errstr;                      // 错误信息字符串
        int32_t errstrLen;                 // 错误信息长度
        adapter_tmq_new_consumer_cb cb;    // 创建消费者回调函数
        uintptr_t param;                   // 用户参数
    } new_consumer_task;
    
    typedef struct subscribe_task {
        tmq_t *tmq;                        // TMQ 指针
        tmq_list_t *topic_list;            // 订阅的主题列表
        adapter_tmq_subscribe_cb cb;       // 订阅回调函数
        uintptr_t param;                   // 用户参数
    } subscribe_task;
    
    typedef struct unsubscribe_task {
        tmq_t *tmq;                        // TMQ 指针
        adapter_tmq_unsubscribe_cb cb;     // 取消订阅回调函数
        uintptr_t param;                   // 用户参数
    } unsubscribe_task;
    
    typedef struct consumer_close_task {
        tmq_t *tmq;                        // TMQ 指针
        adapter_tmq_consumer_close_cb cb;  // 关闭消费者回调函数
        uintptr_t param;                   // 用户参数
    } consumer_close_task;
    
    typedef struct tmq_get_raw_task {
        TAOS_RES *res;                     // 结果资源
        tmq_raw_data *raw;                 // 原始数据
        adapter_tmq_get_raw_cb cb;         // 获取原始数据回调函数
        uintptr_t param;                   // 用户参数
    } tmq_get_raw_task;
    
    typedef struct tmq_get_json_meta_task {
        TAOS_RES *res;                     // 结果资源
        adapter_tmq_get_json_meta_cb cb;   // 获取 JSON 元数据回调函数
        uintptr_t param;                   // 用户参数
    } tmq_get_json_meta_task;
    
    typedef struct tmq_get_topic_assignment_task {
        tmq_t *tmq;                        // TMQ 指针
        char *topic_name;                  // 主题名称
        adapter_tmq_get_topic_assignment_cb cb; // 获取主题分配信息回调函数
        uintptr_t param;                   // 用户参数
    } tmq_get_topic_assignment_task;
    
    typedef struct tmq_offset_seek_task {
        tmq_t *tmq;                        // TMQ 指针
        char *topic_name;                  // 主题名称
        int32_t vgId;                      // 分区 ID
        int64_t offset;                    // 偏移量
        adapter_tmq_offset_seek_cb cb;     // 偏移量定位回调函数
        uintptr_t param;                   // 用户参数
    } tmq_offset_seek_task;
    
    typedef struct tmq_commit_offset_task {
        tmq_t *tmq;                        // TMQ 指针
        char *topic_name;                  // 主题名称
        int32_t vgId;                      // 分区 ID
        int64_t offset;                    // 偏移量
        adapter_tmq_commit_offset_cb cb;   // 提交偏移量回调函数
        uintptr_t param;                   // 用户参数
    } tmq_commit_offset_task;
    
    typedef struct tmq_committed_task {
        tmq_t *tmq;                        // TMQ 指针
        char *topic_name;                  // 主题名称
        int32_t vgId;                      // 分区 ID
        adapter_tmq_committed_cb cb;       // 获取已提交偏移量回调函数
        uintptr_t param;                   // 用户参数
    } tmq_committed_task;
    
    typedef struct tmq_position_task {
        tmq_t *tmq;                        // TMQ 指针
        char *topic_name;                  // 主题名称
        int32_t vgId;                      // 分区 ID
        adapter_tmq_position_cb cb;        // 获取当前位置偏移量回调函数
        uintptr_t param;                   // 用户参数
    } tmq_position_task;
    
    ```

   - 异步上下文池
    ```c {wrap}
    type TMQHandlerPool struct {
        // 锁
        mu       sync.RWMutex
        // 上下文总数量
        count    int
        // 空闲上下文
        handlers chan *TMQHandler
        // 上下文等待链表
        reqList  *list.List
    }
    
    // 上下文等待请求
    type poolReq struct {
        idleHandler *TMQHandler
    }
    
    // 上下文
    type TMQHandler struct {
        Handler cgo.Handle
        Caller  *TMQCaller
    }
    ```

   - 结果和上下文
    ```c {wrap}
    // 拉取数据的元数据块的结果
    type FetchRawBlockResult struct {
        // 错误码
        Code      int
        // 行数
        BlockSize int
        // 数据块
        Block     unsafe.Pointer
    }
    
    type NewConsumerResult struct {
        // 消费者
        Consumer unsafe.Pointer
        // 错误信息
        ErrStr   string
    }
    
    type GetTopicAssignmentResult struct {
        // 错误码
        Code       int32
        // 主题分配信息
        Assignment []*Assignment
    }
    
    type ListTopicsResult struct {
        // 错误码
        Code   int32
        // 主题
        Topics []string
    }
    
    // 上下文内容
    type TMQCaller struct {
        PollResult               chan unsafe.Pointer
        FreeResult               chan struct{}
        CommitResult             chan int32
        SubscribeResult          chan int32
        UnsubscribeResult        chan int32
        ConsumerCloseResult      chan int32
        GetRawResult             chan int32
        OffsetSeekResult         chan int32
        FetchRawBlockResult      chan *FetchRawBlockResult
        NewConsumerResult        chan *NewConsumerResult
        GetJsonMetaResult        chan unsafe.Pointer
        GetTopicAssignmentResult chan *GetTopicAssignmentResult
        CommittedResult          chan int64
        PositionResult           chan int64
        ListTopicsResult         chan *ListTopicsResult
    }
    ```

### 7.7 RESTful 接口

1. 组件设计：
   - HTTP 框架使用 gin，全局使用跨域中间件（github.com/gin-contrib/cors）、请求日志记录中间件（log/web.go GinLog）、请求崩溃恢复日志中间件（log/web.go GinRecoverLog），启用 gzip 压缩。
   - `setStartTime`中间件在上下文中添加请求开始时间。
   - `CheckAuth` 中间件负责解析 HTTP 验证信息 Basic 验证和 Taosd 自定义验证，将解析后的用户名和密码放到上下文。
   - rest 路由组添加中间件检查是否停止查询和写入。
   - 存在以下路由：
      - `POST rest/sql` 执行 SQL，使用中间件 `setStartTime`和 `CheckAuth`。
      - `POST rest/sql/:db` 带有db信息的执行 SQL，使用中间件 `setStartTime`和 `CheckAuth`。
      - `POST rest/sql/:db/vgid` 获取表的 vgroupid，使用中间件  `CheckAuth`。
      - `GET rest/login/:user/:password` 获取自定义验证。
      - `POST rest/upload` 上传 csv 进行写入，使用中间件 `CheckAuth`。
   - 代码位置 `controller/rest`。
2. 列出系统中的关键数据结构
   - WebController 接口，实现 WebController 接口的结构体可在 init 函数调用 AddController 将路由添加到对外接口。
    ```go {wrap}
    type WebController interface {
        Init(r gin.IRouter)
    }
    
    var controllers []WebController
    
    func AddController(controller WebController) {
        controllers = append(controllers, controller)
    }
    
    func GetControllers() []WebController {
        return controllers
    }
    ```

   - Restful 结构体实现 WebController 接口
    ```go {wrap}
    type Restful struct {
        uploadReplacer *strings.Replacer
    }
    
    // 实现 WebController 接口
    func (ctl *Restful) Init(r gin.IRouter) {
        // csv 转 sql 内容转义
        ctl.uploadReplacer = strings.NewReplacer(
           "\\", "\\\\",
           "'", "\\'",
           "(", "\\(",
           ")", "\\)",
        )
        // 路由组 rest
        api := r.Group("rest")
        // 检查是否停止查询和写入
        api.Use(func(c *gin.Context) {
           if monitor.AllPaused() {
              c.AbortWithStatusJSON(http.StatusServiceUnavailable, "memory exceeds threshold")
              return
           }
        })
        // 执行 sql
        api.POST("sql", setStartTime, CheckAuth, ctl.sql)
        // 执行带 db 信息的 sql
        api.POST("sql/:db", setStartTime, CheckAuth, ctl.sql)
        // 获取表的 vgroupid
        api.POST("sql/:db/vgid", CheckAuth, ctl.tableVgID)
        // 获取自定义验证
        api.GET("login/:user/:password", ctl.des)
        // 上传 csv
        api.POST("upload", CheckAuth, ctl.upload)
    }
    ```

1. 使用几种类型的图表来解释设计
   - `setStartTime` 中间件流程图
    ![](./images/wb_BX03wTwd0hO903b6MJhcSfCBnHe.png)

   - `CheckAuth` 中间件流程图
    ![](./images/wb_JE8XwDvwbhdLZCb3jVacl4GInHd.png)

   - 解析自定义验证流程
    ![](./images/wb_BE5vwahdshS8GKb2SEvcXaTanyg.png)

   - `/rest/sql` 和 `/rest/sql/:db` 流程图
    ![](./images/wb_Gf4SwxBcMhHUN2bU4frcQzdin5d.png)

    流程 1： 获取连接执行 SQL
    ![](./images/wb_THFXwgKwlhAxI6bXjcDcIjXJnkc.png)

    流程 2： 执行 SQL 返回结果
    ![](./images/wb_TBAjwlC1khqiuzbfhjqc0vTLnAb.png)

    流程 3： 非查询语句返回结果
    ![](./images/wb_KJ53wDDPhh3b7abnhRhc09lgnTc.png)

    流程 4： 查询语句返回结果
    ![](./images/wb_HdXXwlVt8hYDupbtJYSchqg0nNo.png)

   - 获取 vgroupid 流程图
    ![](./images/wb_HyxswXRCHhVDLMbvWXqcwExInYc.png)

   - 获取自定义验证流程图
    ![](./images/wb_LcDSwiSnQhnIwJbA0Ktci65mnEd.png)

   - 上传 csv 写入流程图
    ![](./images/wb_YgrQwDCgfhKfcLbwzh4cRj3RnEe.png)

### 7.8 WebSocket 接口

1. 组件设计：
以 `github.com/gorilla/websocket`作为 WebSocket 基础库进行二次封装 `github.com/huskar-t/melody`，提供了 HTTP 协议升级，获取和发送等功能的封装。
1. 列出系统中的关键数据结构
   - melody 中 WebSocket 服务端
    ```go {wrap}
    // WebSocket 服务端配置
    type Config struct {
        // 发送超时时间
        WriteWait         time.Duration 
        // 等待 pong 时间(真正等待时间=PongWait+PingPeriod）
        PongWait          time.Duration 
        // 发送 ping 的间隔
        PingPeriod        time.Duration 
        // 最大消息大小
        MaxMessageSize    int64 
        // 缓存未发送消息数
        MessageBufferSize int 
    }
    
    // 默认配置
    func newConfig() *Config {
        return &Config{
           WriteWait:         90 * time.Second,
           PongWait:          60 * time.Second,
           PingPeriod:        (60 * time.Second * 9) / 10,
           MaxMessageSize:    0,
           MessageBufferSize: 1,
        }
    }
    
    // WebSocket 服务端封装
    type Melody struct {
        // 配置项
        Config               *Config
        // 协议升级配置
        Upgrader             *websocket.Upgrader
        // 文本消息处理方法
        messageHandler       handleMessageFunc
        // 二进制消息处理方法
        messageHandlerBinary handleMessageFunc
        // 文本消息发送后调用（未用到）
        messageSentHandler       handleMessageFunc
        // 二进制消息发送后调用（未用到）
        messageSentHandlerBinary handleMessageFunc
        // 错误处理方法
        errorHandler         handleErrorFunc
        // 关闭后调用方法
        closeHandler         handleCloseFunc
        // 新连接创建后调用方法
        connectHandler       handleSessionFunc
        // 连接断开后调用方法
        disconnectHandler    handleSessionFunc
        // 收到 pong 后调用方法
        pongHandler          handleSessionFunc
        // 广播用（未用到）
        hub                      *hub
    }
    ```

   - JSON 请求总格式，请求以 action 区分请求，以下各 JSON 请求均为 args 参数
    ```go {wrap}
    type Request struct {
        Action string          `json:"action"`
        Args   json.RawMessage `json:"args"`
    }
    ```

   - 基础 JSON 响应格式
    ```go {wrap}
    type BaseResponse struct {
        // 错误码
        Code    int    `json:"code"`
        // 错误信息
        Message string `json:"message"`
        // 操作
        Action  string `json:"action"`
        // 请求 id
        ReqID   uint64 `json:"req_id"`
        // 执行时间
        Timing  int64  `json:"timing"`
        // 是否为二进制协议
        binary  bool
        // 是否无响应
        null    bool
    }
    ```

#### 7.8.1 查询写入接口

1. 组件设计：
   - 路由地址 `/ws`，包含数据写入查询等接口，代码路径 `controller/ws/ws`。
   - 所有消息异步处理，使用 sync.WaitGroup 来等待任务完成，当 ws 关闭或出错时等待异步处理完成执行清理和连接关闭。
2. 列出系统中的关键数据结构
   - 控制器
  ```go {wrap}
  type webSocketCtl struct {
      // WebSocket 服务端实例
      m *melody.Melody
  }
  
  // 实现 WebController 接口
  func (ws *webSocketCtl) Init(ctl gin.IRouter)
  ```

   - 消息处理器
  ```go {wrap}
  type messageHandler struct {
      // TDengine 连接
      conn         unsafe.Pointer
      // 关闭标志
      closed       uint32
      // 保证只关闭一次的锁
      once         sync.Once
      // 异步任务等待完成
      wait         sync.WaitGroup
      // 删除用户的通知管道
      dropUserChan chan struct{}
      // 读写锁（保护连接和关闭）
      sync.RWMutex
      // 查询结果管理
      queryResults *QueryResultHolder
      // stmt 实例管理
      stmts        *StmtHolder        // stmt bind message
      // 退出信号
      exit                  chan struct{}
      // 白名单变动通知管道
      whitelistChangeChan   chan int64
      // 当前客户端连接 session
      session               *melody.Session
      // 客户端 ip
      ip                    net.IP
      // 客户端 ip 字符串
      ipStr                 string
      // 客户端端口
      port                  string
      // 客户端应用名称
      appName               string
      // 白名单变动通知句柄
      whitelistChangeHandle cgo.Handle
      // 删除用户通知句柄
      dropUserHandle        cgo.Handle
  }
  ```

##### 7.8.1.1 获取客户端版本

1. 组件设计：
   - 代理 C 接口 `taos_get_client_info`。
2. 列出系统中的关键数据结构
   - 请求和响应协议
  ```json {wrap}
  // 请求
  type versionRequest struct {
      ReqID uint64 `json:"req_id"`
  }
  // 响应
  type versionResp struct {
      Code    int    `json:"code"`
      Message string `json:"message"`
      Action  string `json:"action"`
      ReqID   uint64 `json:"req_id"`
      Timing  int64  `json:"timing"`
      Version string `json:"version"`
  }
  ```

##### 7.8.1.2 连接

1. 组件设计：
   - 代理 C 接口 `taos_connect`，不允许重复连接
   - 连接成功之后检查白名单，注册白名单变更事件，注册用户删除事件，设置连接模式，设置连接属性
   - 创建协程等待事件通知
2. 列出系统中的关键数据结构
   - 请求和响应协议
  ```go {wrap}
  // 请求
  type connRequest struct {
      ReqID     uint64 `json:"req_id"`
      User      string `json:"user"`
      Password  string `json:"password"`
      DB        string `json:"db"`
      Mode      *int   `json:"mode"`
      TZ        string `json:"tz"`
      App       string `json:"app"`
      IP        string `json:"ip"`
      Connector string `json:"connector"`
  }
  
  // 响应
  type BaseResponse struct {
      Code    int    `json:"code"`
      Message string `json:"message"`
      Action  string `json:"action"`
      ReqID   uint64 `json:"req_id"`
      Timing  int64  `json:"timing"`
  }
  ```

1. 使用几种类型的图表来解释设计
   - 创建连接时序图
  ![](./images/wb_P9iHwlgoFhAzf2bWtKhckWVhnke.png)

##### 7.8.1.3 执行 SQL 

JSON 与二进制实现相同，二进制能节省反序列化时间
1. 组件设计：
   - 代理 C 接口 `taos_query_a_with_reqid ` `taos_is_update_query` ,非查询请求调用 `taos_affected_rows` 查询请求调用 `taos_free_result` `taos_num_fields` `taos_fetch_field``s_e` `taos_result_precision`。
   - 执行 sql 之后判断 sql 类型，如果是写入获取影响函数并返回，如果是查询则获取元数据，将查询结果放到管理器后返回。
2. 列出系统中的关键数据结构
   - 请求和响应
  ```go {wrap}
  type queryRequest struct {
      ReqID uint64 `json:"req_id"`
      Sql   string `json:"sql"`
  }
  
  type queryResponse struct {
      Code             int                `json:"code"`
      Message          string             `json:"message"`
      Action           string             `json:"action"`
      ReqID            uint64             `json:"req_id"`
      Timing           int64              `json:"timing"`
      ID               uint64             `json:"id"`
      IsUpdate         bool               `json:"is_update"`
      AffectedRows     int                `json:"affected_rows"`
      FieldsCount      int                `json:"fields_count"`
      FieldsNames      []string           `json:"fields_names"`
      FieldsTypes      jsontype.JsonUint8 `json:"fields_types"`
      FieldsLengths    []int64            `json:"fields_lengths"`
      Precision        int                `json:"precision"`
      FieldsPrecisions []int64            `json:"fields_precisions"`
      FieldsScales     []int64            `json:"fields_scales"`
  }
  ```

   - 查询结果管理器
    ```go {wrap}
    type QueryResultHolder struct {
        // 查询结果 id 原子自增
        index   uint64
        // 查询结果以链表形式存放
        results *list.List
        // 链表的锁
        sync.RWMutex
    }
    ```

1. 使用几种类型的图表来解释设计
   - 执行查询时序图
  ![](./images/wb_JXlkw6OgBh4EsXbGdzEcutMSn9b.png)

##### 7.8.1.4 获取查询结果

1. 组件设计：
   - 代理 C 接口 `taos_fetch_raw_block_a` `taos_free_result`。
   - 获取结果用来判断是否有后续数据以及获取数据解析所必须的信息。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type fetchRequest struct {
      ReqID uint64 `json:"req_id"`
      // 查询结果 id
      ID    uint64 `json:"id"`
  }
  
  type fetchResponse struct {
      BaseResponse
      // 查询结果 id
      ID        uint64 `json:"id"`
      // 是否已完成
      Completed bool   `json:"completed"`
      // 已废弃字段
      Lengths   []int  `json:"lengths"`
      // 结果块行数
      Rows      int    `json:"rows"`
  }
  ```

1. 使用几种类型的图表来解释设计
   - 获取查询结果时序图
  ![](./images/wb_FAXpwp3aIhCfnCbRgi1crbyjnod.png)

##### 7.8.1.5 获取查询结果数据块

1. 组件设计：
   - 拿获取查询结果暂存的数据块。
2. 列出系统中的关键数据结构
   - 请求为 JSON 格式
    ```go {wrap}
    type fetchBlockRequest struct {
        ReqID uint64 `json:"req_id"`
        // 查询结果 id
        ID    uint64 `json:"id"`
    }
    ```

   - 响应为二进制

    | 偏移 | 类型 | 说明 |
| --- | --- | --- |
| 0 | uint64 | 执行时间（纳秒） |
| 8 | uint64 | 查询结果 id |
| 16 | []byte | raw block 结果块 |

  raw block 格式为 C 接口 `taos_get_raw_block` 返回，格式如下
  ```go
  // +------------------+--------------+--------------+------------------+-----------------+-------------------+--------------------------------------------+------------------------------------+-------------+-----------+-------------+-----------+
  // |  version         | total length | total rows    |  total columns  |   flag  seg     |  group id         | col1_schema(type+bytes) | col2_schema(type+bytes) | col3_schema(type+bytes)... | column#1 length, column#2 length...| col1 bitmap or col1 offset | col1 data | col2 bitmap or col2 offset  | col2 data | ....
  // |  sizeof(int32_t) |sizeof(int32) | sizeof(int32) |  sizeof(int32)  |  sizeof(int32)  |  sizeof(uint64_t) |           (sizeof(int8_t)+sizeof(int32_t))*numOfCols                           | sizeof(int32_t) * numOfCols        | 
  // +------------------+--------------+--------------+------------------+-----------------+-------------------+------+------------------------------------+-------------+-----------+-------------+-----------+
  ```

  具体描述如下：
  - 第一个字段：版本号，固定大小，可忽略，占用4个字节。
  - 第二个字段：raw block 数据的总长度，占用4个字节。
  - 第三个字段：总行数，占用4个字节。
  - 第四个字段：总列数，占用4个字节。
  - 第五个字段：flag，固定大小，可忽略，占用4个字节。
  - 第六个字段：group id，block分组的id，可忽略，占用8个字节 。
  - 第七个字段：所有列的 schema，每个列包含类型（1个字节）+所需大小（4个字节）。
  - 第八个字段：每列数据长度。
  - 第九个字段：
    - 每列数据内容，具体分变长的string类型和固定长度的类型。
    - 变长的类型，通过前面每行的offset来标记位置，offset=-1，表示该行为NULL，变长数据前两字节为长度，后面为真实数据。
    - 固定长度的类型，通过bitmap来标记，bit位为1表示该行为NULL，根据固定长度获取真实数据（比如int32类型占4个字节固定长度）。

##### 7.8.1.6 二进制协议获取查询结果 raw block

1. 组件设计：
   - 此接口直接返回是否有数据块和数据块内容，不需要每次都先获取结果检查是否有数据块然后再获取结果数据块，相当于获取查询结果和获取查询结果数据块的功能总和

##### 7.8.1.7 释放查询结果

1. 组件设计：
   - 释放查询结果，代理 C 接口 `taos_free_result`，所有未完成的查询都应该调用此接口进行释放。
2. 列出系统中的关键数据结构
   - 请求为 JSON 格式，无响应
  ```go {wrap}
  type freeResultRequest struct {
      ReqID uint64 `json:"req_id"`
      // 查询结果 id
      ID    uint64 `json:"id"`
  }
  ```

##### 7.8.1.8 schemaless 协议写入

1. 组件设计：
   - schemaless 数据写入，代理 C 接口 `taos_schemaless_insert_raw_ttl_with_reqid_tbname_key`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  // 请求
  type schemalessWriteRequest struct {
      ReqID        uint64 `json:"req_id"`
      Protocol     int    `json:"protocol"`
      // 时间精度见下表
      Precision    string `json:"precision"`
      // 数据表超时时间
      TTL          int    `json:"ttl"`
      // 协议数据
      Data         string `json:"data"`
      // 表名键值
      TableNameKey string `json:"table_name_key"`
  }
  
  type schemalessWriteResponse struct {
      Code         int    `json:"code"`
      Message      string `json:"message"`
      Action       string `json:"action"`
      ReqID        uint64 `json:"req_id"`
      Timing       int64  `json:"timing"`
      // 数据库影响行数
      AffectedRows int    `json:"affected_rows"`
      // 数据总行数
      TotalRows    int32  `json:"total_rows"`
  }
  ```

   - 支持的协议

  | 协议 | 值 |
| --- | --- |
| influxdb | 1 |
| openTSDB 行数据 | 2 |
| openTSDB JSON | 3 |

   - 时间精度列表

  | 精度 | 值 | C 枚举 |
| --- | --- | --- |
| 纳秒 | ns | 6 |
| 微秒 | u 或 μ | 5 |
| 毫秒 | ms | 4 |
| 秒 | s | 3 |
| 分钟 | m | 2 |
| 小时 | h | 1 |
| 不设置 | 空字符串 | 0 |

1. 使用几种类型的图表来解释设计
   - Schemaless 写入时序图
  ![](./images/wb_X858wGwlahqSetbXhSRc0DiGnRe.png)

##### 7.8.1.9 stmt 初始化

1. 组件设计：
   - 初始化 stmt，代理 C 接口 `taos_stmt_init_with_reqid`。
   - 将 stmt 放到 stmt 管理器。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmtInitRequest struct {
      ReqID uint64 `json:"req_id"`
  }
  
  type stmtInitResponse struct {
      Code     int    `json:"code"`
      Message  string `json:"message"`
      Action   string `json:"action"`
      ReqID    uint64 `json:"req_id"`
      Timing   int64  `json:"timing"`
      // stmt id
      StmtID  uint64 `json:"stmt_id"`
  }
  ```

   - Stmt 管理器
  ```go {wrap}
  type StmtHolder struct {
      // stmt id 生成，原子自增
      index   uint64
      // stmt 链表
      results *list.List
      // stmt 链表锁
      sync.RWMutex
  }
  ```

##### 7.8.1.10 stmt 准备语句

1. 组件设计：
   - stmt 准备语句，代理 C 接口 `taos_stmt_prepare` `taos_stmt_is_insert`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type StmtPrepareRequest struct {
      ReqID  uint64 `json:"req_id"`
      StmtID uint64 `json:"stmt_id"`
      // 准备语句
      SQL    string `json:"sql"`
  }
  
  type StmtPrepareResponse struct {
      Code     int    `json:"code"`
      Message  string `json:"message"`
      Action   string `json:"action"`
      ReqID    uint64 `json:"req_id"`
      Timing   int64  `json:"timing"`
      // stmt id 
      StmtID   uint64 `json:"stmt_id"`
      // 准备语句是否是插入
      IsInsert bool   `json:"is_insert"`
  }
  ```

##### 7.8.1.11 stmt 设置表名

1. 组件设计：
   - stmt 设置表名，代理 C 接口 `taos_stmt_set_tbname`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmtSetTableNameRequest struct {
      ReqID  uint64 `json:"req_id"`
      StmtID uint64 `json:"stmt_id"`
      // 表名
      Name   string `json:"name"`
  }
  
  type stmtSetTableNameResponse struct {
      Code     int    `json:"code"`
      Message  string `json:"message"`
      Action   string `json:"action"`
      ReqID    uint64 `json:"req_id"`
      Timing   int64  `json:"timing"`
      // stmt id
      StmtID  uint64 `json:"stmt_id"`
  }
  ```

##### 7.8.1.12 stmt 设置标签

1. 组件设计：
   - stmt 设置标签，代理 C 接口 `taos_stmt_set_tags`。
   - 建议应用使用二进制协议 stmt 设置标签，解析协议比 JSON 轻量。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmtSetTagsRequest struct {
      ReqID  uint64          `json:"req_id"`
      StmtID uint64          `json:"stmt_id"`
      // []interface{} 以行组织的 tag  
      Tags   json.RawMessage `json:"tags"`
  }
  
  type stmtSetTagsResponse struct {
      Code     int    `json:"code"`
      Message  string `json:"message"`
      Action   string `json:"action"`
      ReqID    uint64 `json:"req_id"`
      Timing   int64  `json:"timing"`
      StmtID   uint64 `json:"stmt_id"`
  }
  ```

1. 使用几种类型的图表来解释设计
   - stmt 设置标签时序图
  ![](./images/wb_Jh6bwrsQ3h4EwrbxRaHcAW6MnyE.png)

##### 7.8.1.13 二进制协议 stmt 设置标签

1. 组件设计：
   - stmt 设置标签，代理 C 接口 `taos_stmt_set_tags`。
   - 使用 raw block 结构组织标签数据。
2. 列出系统中的关键数据结构
   - 请求二进制

    | 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 请求 id | uint64 |
| 8 | stmt_id | uint64 |
| 16 | action 固定值 1 | uint64 |
| 24 | rawblock | []byte |

   - 响应 JSON ：
    ```go {wrap}
    type stmtSetTagsResponse struct {
        BaseResponse
        StmtID uint64 `json:"stmt_id"`
    }
    ```

1. 使用几种类型的图表来解释设计
  ![](./images/wb_Z29lwZXUnh5R4CbgJWmcuCf3nJg.png)

##### 7.8.1.14 stmt 绑定

1. 组件设计：
   - stmt 设置绑定数据，代理 C 接口 `taos_stmt_bind_param_batch`。
   - 绑定数据以列形式组织的二维数组例如：
    ```go {wrap}
    // 写入四行数据
    // 1，"a"
    // 2, "b"
    // 3, "c"
    // 4, "d"
    [[1,2,3,4],["a","b","c","d"]]
    ```

   - 建议使用二进制协议 stmt 绑定。
1. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmtBindRequest struct {
      ReqID   uint64          `json:"req_id"`
      StmtID  uint64          `json:"stmt_id"`
      // [][]interface 以列组织的绑定数据
      Columns json.RawMessage `json:"columns"`
  }
  
  type stmtBindResponse struct {
      BaseResponse
      StmtID uint64 `json:"stmt_id"`
  }
  ```

1. 使用几种类型的图表来解释设计
   - stmt 绑定数据时序图
  ![](./images/wb_SHmBwsF6thvhP8bcHJfcz9tWn8e.png)

##### 7.8.1.15 二进制协议 stmt 绑定

1. 组件设计：
   - stmt 设置绑定数据，代理 C 接口 `taos_stmt_bind_param_batch`。
   - 绑定数据以 raw block 格式组织。
2. 列出系统中的关键数据结构
   - 请求二进制

  | 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 请求 id | uint64 |
| 8 | stmt_id | uint64 |
| 16 | action 固定值 2 | uint64 |
| 24 | rawblock | []byte |

   - 响应 JSON
  ```go {wrap}
  type stmtBindResponse struct {
      Code     int    `json:"code"`
      Message  string `json:"message"`
      Action   string `json:"action"`
      ReqID    uint64 `json:"req_id"`
      Timing   int64  `json:"timing"`
      StmtID uint64 `json:"stmt_id"`
  }
  ```

1. 使用几种类型的图表来解释设计
   - stmt 绑定数据时序图
  ![](./images/wb_Neh6wrJ3Sh5BElbpKjcc0zuznRe.png)

##### 7.8.1.16 stmt 添加批量

1. 组件设计：
   - stmt 添加批量，代理 C 接口 `taos_stmt_add_batch`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmtAddBatchRequest struct {
      ReqID  uint64 `json:"req_id"`
      StmtID uint64 `json:"stmt_id"`
  }
  
  type stmtAddBatchResponse struct {
      Code     int    `json:"code"`
      Message  string `json:"message"`
      Action   string `json:"action"`
      ReqID    uint64 `json:"req_id"`
      Timing   int64  `json:"timing"`
      StmtID uint64 `json:"stmt_id"`
  }
  ```

##### 7.8.1.17 stmt 执行

1. 组件设计：
   - stmt 执行，代理 C 接口 `taos_stmt_execute` `taos_stmt_affected_rows_once`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmtExecRequest struct {
      ReqID  uint64 `json:"req_id"`
      StmtID uint64 `json:"stmt_id"`
  }
  
  type stmtExecResponse struct {
      Code     int    `json:"code"`
      Message  string `json:"message"`
      Action   string `json:"action"`
      ReqID    uint64 `json:"req_id"`
      Timing   int64  `json:"timing"`
      StmtID   uint64 `json:"stmt_id"`
      // 受影响行数
      Affected int    `json:"affected"`
  }
  ```

1. 使用几种类型的图表来解释设计
   - stmt 执行时序图
  ![](./images/wb_J17wwp49mhSEnlbBuasccJEKnAe.png)

##### 7.8.1.18 stmt 获取需要绑定的标签信息

1. 组件设计：
   - stmt 获取需要绑定 tag 信息，代理 C 接口 `taos_stmt_get_tag_fields`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmtGetTagFieldsRequest struct {
      ReqID  uint64 `json:"req_id"`
      StmtID uint64 `json:"stmt_id"`
  }
  
  type stmtGetTagFieldsResponse struct {
      Code    int                     `json:"code"`
      Message string                  `json:"message"`
      Action  string                  `json:"action"`
      ReqID   uint64                  `json:"req_id"`
      Timing  int64                   `json:"timing"`
      StmtID  uint64                  `json:"stmt_id"`
      Fields  []*stmtCommon.StmtField `json:"fields,omitempty"`
  }
  
  // stmtCommon.StmtField
  type StmtField struct {
      Name      string `json:"name"`
      FieldType int8   `json:"field_type"`
      Precision uint8  `json:"precision"`
      Scale     uint8  `json:"scale"`
      Bytes     int32  `json:"bytes"`
  }
  ```

##### 7.8.1.19 stmt 获取需要绑定的列信息

1. 组件设计：
   - stmt 获取需要绑定列信息，代理 C 接口 `taos_stmt_get_col_fields`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmtGetColFieldsRequest struct {
      ReqID  uint64 `json:"req_id"`
      StmtID uint64 `json:"stmt_id"`
  }
  
  type stmtGetColFieldsResponse struct {
      Code    int                     `json:"code"`
      Message string                  `json:"message"`
      Action  string                  `json:"action"`
      ReqID   uint64                  `json:"req_id"`
      Timing  int64                   `json:"timing"`
      StmtID  uint64                  `json:"stmt_id"`
      Fields  []*stmtCommon.StmtField `json:"fields"`
  }
  
  // stmtCommon.StmtField
  type StmtField struct {
      Name      string `json:"name"`
      FieldType int8   `json:"field_type"`
      Precision uint8  `json:"precision"`
      Scale     uint8  `json:"scale"`
      Bytes     int32  `json:"bytes"`
  }
  ```

##### 7.8.1.20 获取 stmt 查询结果

1. 组件设计：
   - stmt 获取需要绑定列信息，代理 C 接口 `taos_stmt_use_result`。
   - 返回 result_id 字段后应用可以将此字段当做 SQL 查询结果 id 进行数据获取和解析。
   - 查询结果如果没有获取完数据需要调用释放查询结果接口。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmtUseResultRequest struct {
      ReqID  uint64 `json:"req_id"`
      StmtID uint64 `json:"stmt_id"`
  }
  
  type stmtUseResultResponse struct {
      Code    int    `json:"code"`
      Message string `json:"message"`
      Action  string `json:"action"`
      ReqID   uint64 `json:"req_id"`
      Timing  int64  `json:"timing"`
      StmtID  uint64 `json:"stmt_id"`
      // 结果 id 等同于 sql 查询结果 id
      ResultID uint64 `json:"result_id"`
      // 字段数
      FieldsCount int `json:"fields_count"`
      // 字段名
      FieldsNames []string `json:"fields_names"`
      // 字段名
      FieldsTypes jsontype.JsonUint8 `json:"fields_types"`
      // 字段长度
      FieldsLengths []int64 `json:"fields_lengths"`
      // 结果时间精度
      Precision        int     `json:"precision"`
      // 字段精度(decimal)
      FieldsPrecisions []int64 `json:"fields_precisions"`
      // 字段小数位数(decimal)
      FieldsScales     []int64 `json:"fields_scales"`
  }
  ```

1. 使用几种类型的图表来解释设计
   - 获取 stmt 查询结果时序图
  ![](./images/wb_WYVswOV3yhrWA2b4MhacakX3njd.png)

##### 7.8.1.21 stmt 需要绑定的参数个数

1. 组件设计：
   - stmt 获取需要绑定参数个数，代理 C 接口 `taos_stmt_num_params`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmtNumParamsRequest struct {
      ReqID  uint64 `json:"req_id"`
      StmtID uint64 `json:"stmt_id"`
  }
  
  type stmtNumParamsResponse struct {
      Code             int                `json:"code"`
      Message          string             `json:"message"`
      Action           string             `json:"action"`
      ReqID            uint64             `json:"req_id"`
      Timing           int64              `json:"timing"`
      // 绑定参数个数
      NumParams int    `json:"num_params"`
  }
  ```

##### 7.8.1.22 stmt 获取指定绑定列的信息

1. 组件设计：
   - stmt 获取指定绑定列的信息，代理 C 接口 `taos_stmt_get_param`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmtGetParamRequest struct {
      ReqID  uint64 `json:"req_id"`
      StmtID uint64 `json:"stmt_id"`
      // 绑定参数序号
      Index  int    `json:"index"`
  }
  
  type stmtGetParamResponse struct {
      Code             int                `json:"code"`
      Message          string             `json:"message"`
      Action           string             `json:"action"`
      ReqID            uint64             `json:"req_id"`
      Timing           int64              `json:"timing"`
      StmtID   uint64 `json:"stmt_id"`
      // 绑定参数序号
      Index    int    `json:"index"`
      // 数据类型
      DataType int    `json:"data_type"`
      // 数据长度
      Length   int    `json:"length"`
  }
  ```

##### 7.8.1.23 关闭 stmt

1. 组件设计：
   - stmt 关闭，代理 C 接口 `taos_stmt_close`。
2. 列出系统中的关键数据结构
   - 请求
  ```go {wrap}
  type stmtCloseRequest struct {
      ReqID  uint64 `json:"req_id"`
      StmtID uint64 `json:"stmt_id"`
  }
  ```

   - 响应
  正常请求无响应，当发过来的 stmt_id 不存在时返回错误。
  ```go {wrap}
  type WSStmtErrorResp struct {
      Code     int    `json:"code"`
      Message  string `json:"message"`
      Action   string `json:"action"`
      ReqID    uint64 `json:"req_id"`
      Timing   int64  `json:"timing"`
      StmtID   uint64 `json:"stmt_id"`
  }
  ```

1. 使用几种类型的图表来解释设计
   - 获取 stmt 查询结果时序图
  ![](./images/wb_Sh2UwKFqRhna2YbPAsNcbr83nHf.png)

##### 7.8.1.24 获取查询结果的列数目

1. 组件设计：
   - 获取结果列数，代理 C 接口 `taos_num_fields`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  // 请求
  type numFieldsRequest struct {
      ReqID    uint64 `json:"req_id"`
      ResultID uint64 `json:"result_id"`
  }
  
  // 响应
  type numFieldsResponse struct {
      Code     int    `json:"code"`
      Message  string `json:"message"`
      Action   string `json:"action"`
      ReqID    uint64 `json:"req_id"`
      Timing   int64  `json:"timing"`
      // 列数
      NumFields int    `json:"num_fields"`
  }
  ```

##### 7.8.1.25 获取当前的 db

1. 组件设计：
   - 获取结果列数，代理 C 接口 `taos_get_current_db`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  // 请求
  type requestID struct {
      ReqID uint64 `json:"req_id"`
  }
  
  // 响应
  type getCurrentDBResponse struct {
      Code     int    `json:"code"`
      Message  string `json:"message"`
      Action   string `json:"action"`
      ReqID    uint64 `json:"req_id"`
      Timing   int64  `json:"timing"`
      // 当前 db
      DB      string `json:"db"`
  }
  ```

##### 7.8.1.26 获取服务端信息

1. 组件设计：
   - 获取服务端信息，代理 C 接口 `taos_get_server_info`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  // 请求
  type getServerInfoRequest struct {
      ReqID uint64 `json:"req_id"`
  }
  
  // 响应
  type getServerInfoResponse struct {
      BaseResponse
      // 服务端信息
      Info    string `json:"info"`
  }
  ```

##### 7.8.1.27 tmq 消息写入

1. 组件设计：
   - 写入 tmq 订阅到的原始数据，代理 C 接口 `tmq_write_raw`。
2. 列出系统中的关键数据结构
   - 请求为二进制协议

  | 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 请求 id | uint64 |
| 8 | 固定值 0 | uint64 |
| 16 | action 固定值 3 | uint64 |
| 24 | 原始数据长度 | uint32 |
| 28 | 消息类型 | uint16 |
| 30 | tmq 原始数据 | []byte |

   - 响应为 JSON
  ```go {wrap}
  type commonResp struct {
      Code    int    `json:"code"`
      Message string `json:"message"`
      Action  string `json:"action"`
      ReqID   uint64 `json:"req_id"`
      Timing  int64  `json:"timing"`
  }
  ```

##### 7.8.1.28 查询结果 raw block 写入

1. 组件设计：
   - 写入查询到的 raw block 结果到指定表，代理 C 接口 `taos_write_raw_block_with_reqid`。
2. 列出系统中的关键数据结构
   - 请求为二进制协议

  | 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 请求 id | uint64 |
| 8 | 固定值 0 | uint64 |
| 16 | action 固定值 4 | uint64 |
| 24 | Block 包含的行数 | int32 |
| 28 | table_length 要写入的表名长度 | uint16 |
| 30 | 根据 table_length 获取表名 | []byte |
| 30 + table_length | rawblock | []byte |

   - 响应为 JSON
  ```go {wrap}
  type commonResp struct {
      Code    int    `json:"code"`
      Message string `json:"message"`
      Action  string `json:"action"`
      ReqID   uint64 `json:"req_id"`
      Timing  int64  `json:"timing"`
  }
  ```

##### 7.8.1.29 查询结果 raw block 带列信息写入

1. 组件设计：
   - 写入查询到的 raw block 结果到指定表，代理 C 接口 `taos_write_raw_block_with_reqid`。
2. 列出系统中的关键数据结构
   - 请求为二进制协议

  | 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 请求 id | uint64 |
| 8 | 固定值 0 | uint64 |
| 16 | action 固定值 5 | uint64 |
| 24 | Block 包含的行数 | int32 |
| 28 | table_length 要写入的表名长度 | uint16 |
| 30 | 根据 table_length 获取表名 | []byte |
| 30 + table_length | rawblock | []byte |
| 30 + table_length + rawblock_length | field 信息格式如下 typedef struct taosField { char name[65]; int8_t type; int32_t bytes; } TAOS_FIELD; 内存分布为 name 65 byte type 1 byte padding 2 byte（对齐） bytes 4 byte （rawblock_length 从 raw block 获取） | []byte |

   - 响应为 JSON
  ```go {wrap}
  type commonResp struct {
      Code    int    `json:"code"`
      Message string `json:"message"`
      Action  string `json:"action"`
      ReqID   uint64 `json:"req_id"`
      Timing  int64  `json:"timing"`
  }
  ```

##### 7.8.1.30 stmt2 初始化

1. 组件设计：
   - 初始化 stmt，代理 C 接口 `taos_stmt2_init`。
   - 将 stmt2 放到 stmt 管理器。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmt2InitRequest struct {
      ReqID               uint64 `json:"req_id"`
      SingleStbInsert     bool   `json:"single_stb_insert"`
      SingleTableBindOnce bool   `json:"single_table_bind_once"`
  }
  
  type stmt2InitResponse struct {
      Code    int    `json:"code"`
      Message string `json:"message"`
      Action  string `json:"action"`
      ReqID   uint64 `json:"req_id"`
      Timing  int64  `json:"timing"`
      StmtID  uint64 `json:"stmt_id"`
  }
  ```

   - Stmt 管理器
  ```go {wrap}
  type StmtHolder struct {
      // stmt id 生成，原子自增
      index   uint64
      // stmt 链表
      results *list.List
      // stmt 链表锁
      sync.RWMutex
  }
  ```

##### 7.8.1.31 stmt2 准备语句

1. 组件设计：
   - stmt 准备语句，代理 C 接口 `taos_stmt2_prepare` `taos_stmt2_is_insert` `taos_stmt2_get_fields` `taos_stmt2_free_fields`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmt2PrepareRequest struct {
      ReqID     uint64 `json:"req_id"`
      StmtID    uint64 `json:"stmt_id"`
      SQL       string `json:"sql"`
      GetFields bool   `json:"get_fields"`
  }
  
  type stmt2PrepareResponse struct {
      Code        int                   `json:"code"`
      Message     string                `json:"message"`
      Action      string                `json:"action"`
      ReqID       uint64                `json:"req_id"`
      Timing      int64                 `json:"timing"`
      StmtID      uint64                `json:"stmt_id"`
      IsInsert    bool                  `json:"is_insert"`
      Fields      []*stmt.Stmt2AllField `json:"fields"`
      FieldsCount int                   `json:"fields_count"`
  }
  ```

##### 7.8.1.32 二进制协议 stmt2 绑定

1. 组件设计：
   - stmt 设置绑定数据，代理 C 接口 `taos_stmt2_bind_param`。
   - 绑定数据以 Length-Value 方式展平的 C 绑定结构。
2. 列出系统中的关键数据结构
绑定结构
```cpp {wrap}
typedef struct TAOS_STMT2_BIND {
  int      buffer_type;
  void    *buffer;
  int32_t *length;
  char    *is_null;
  int      num;
} TAOS_STMT2_BIND;

typedef struct TAOS_STMT2_BINDV {
  int               count;
  char            **tbnames;
  TAOS_STMT2_BIND **tags;
  TAOS_STMT2_BIND **bind_cols;
} TAOS_STMT2_BINDV;
```

1. 使用几种类型的图表来解释设计
   - stmt 绑定数据时序图
  ![](./images/wb_Ajt4wUq1WhHfiJbWdtTcbGCGnpc.png)

##### 7.8.1.33 stmt2 执行

1. 组件设计：
   - stmt 执行，代理 C 接口 `taos_stmt2_exec`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmt2ExecRequest struct {
      ReqID  uint64 `json:"req_id"`
      StmtID uint64 `json:"stmt_id"`
  }
  
  type stmt2ExecResponse struct {
      Code     int    `json:"code"`
      Message  string `json:"message"`
      Action   string `json:"action"`
      ReqID    uint64 `json:"req_id"`
      Timing   int64  `json:"timing"`
      StmtID   uint64 `json:"stmt_id"`
      Affected int    `json:"affected"`
  }
  ```

1. 使用几种类型的图表来解释设计
   - stmt 执行时序图
  ![](./images/wb_YWyOwu2goh4xcUbJfp7czBJOnRF.png)

##### 7.8.1.34 获取 stmt2 查询结果

1. 组件设计：
   - stmt 获取需要绑定列信息，代理 C 接口 `taos_stmt2_result`。
   - 返回 result_id 字段后应用可以将此字段当做 SQL 查询结果 id 进行数据获取和解析。
   - 查询结果如果没有获取完数据需要调用释放查询结果接口。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmt2UseResultRequest struct {
      ReqID  uint64 `json:"req_id"`
      StmtID uint64 `json:"stmt_id"`
  }
  
  type stmt2UseResultResponse struct {
      Code             int                `json:"code"`
      Message          string             `json:"message"`
      Action           string             `json:"action"`
      ReqID            uint64             `json:"req_id"`
      Timing           int64              `json:"timing"`
      StmtID           uint64             `json:"stmt_id"`
      ID               uint64             `json:"id"`
      FieldsCount      int                `json:"fields_count"`
      FieldsNames      []string           `json:"fields_names"`
      FieldsTypes      jsontype.JsonUint8 `json:"fields_types"`
      FieldsLengths    []int64            `json:"fields_lengths"`
      Precision        int                `json:"precision"`
      FieldsPrecisions []int64            `json:"fields_precisions"`
      FieldsScales     []int64            `json:"fields_scales"`
  }
  ```

1. 使用几种类型的图表来解释设计
   - 获取 stmt 查询结果时序图
  ![](./images/wb_Szd0wTUOyhG9X1bnXx2cWxZBnCd.png)

##### 7.8.1.35 关闭 stmt2

1. 组件设计：
   - stmt 关闭，代理 C 接口 `taos_stmt2_close`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type stmt2CloseRequest struct {
      ReqID  uint64 `json:"req_id"`
      StmtID uint64 `json:"stmt_id"`
  }
  
  type stmt2CloseResponse struct {
      Code    int    `json:"code"`
      Message string `json:"message"`
      Action  string `json:"action"`
      ReqID   uint64 `json:"req_id"`
      Timing  int64  `json:"timing"`
      StmtID  uint64 `json:"stmt_id"`
  }
  ```

1. 使用几种类型的图表来解释设计
   - 获取 stmt 查询结果时序图
  ![](./images/wb_EjREwEzbBhhTERb3bMrcc5dGnOU.png)

##### 7.8.1.36 检查服务状态

1. 组件设计：
   - 检查服务状态，代理 C 接口 `taos_check_server_status`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type checkServerStatusRequest struct {
      ReqID uint64  `json:"req_id"`
      FQDN  *string `json:"fqdn"`
      Port  int32   `json:"port"`
  }
  
  type checkServerStatusResponse struct {
      Code    int    `json:"code"`
      Message string `json:"message"`
      Action  string `json:"action"`
      ReqID   uint64 `json:"req_id"`
      Timing  int64  `json:"timing"`
      Status  int32  `json:"status"`
      Details string `json:"details"`
  }
  ```

##### 7.8.1.37 设置连接属性

1. 组件设计：
   - 检查服务状态，代理 C 接口 `taos_options_connection`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type optionsConnectionRequest struct {
      ReqID   uint64    `json:"req_id"`
      Options []*option `json:"options"`
  }
  type option struct {
      Option int     `json:"option"`
      Value  *string `json:"value"`
  }
  
  type commonResp struct {
      Code    int    `json:"code"`
      Message string `json:"message"`
      Action  string `json:"action"`
      ReqID   uint64 `json:"req_id"`
      Timing  int64  `json:"timing"`
  }
  ```

##### 7.8.1.38 验证 SQL 合法

1. 组件设计：
   - 验证 SQL 合法，代理 C 接口 `taos_validate_sql`。
2. 列出系统中的关键数据结构
   - 请求与响应
  ```go {wrap}
  type validateSqlResponse struct {
      Code       int    `json:"code"`
      Message    string `json:"message"`
      Action     string `json:"action"`
      ReqID      uint64 `json:"req_id"`
      Timing     int64  `json:"timing"`
      ResultCode int64  `json:"result_code"`
  }
  ```

#### 7.8.2 TMQ 接口

1. 组件设计：
   - 路由地址 `/rest/tmq`，代码路径`controller/ws/tmq`。
   - 所有消息异步处理，使用 sync.WaitGroup 来等待任务完成，当 ws 关闭或出错时等待异步处理完成执行清理和连接关闭。
   - 当新连接创建后会创建一个 C 线程用来执行长耗时 C 方法，此线程以下称为 tmq 任务线程，并给 Go 提供异步方法。
2. 列出系统中的关键数据结构
   - 控制器
  ```go {wrap}
  type TMQController struct {
      tmqM *melody.Melody
  }
  
  // 实现 WebController 接口
  func (s *TMQController) Init(ctl gin.IRouter)
  ```

   - 消息处理器
  ```go {wrap}
  type TMQ struct {
      // 消费者
      consumer              unsafe.Pointer
      // 保存当前消费出来的 tmq 消息，结构见下
      tmpMessage            *Message
      // 控制 tmq 任务线程串行提交的锁
      asyncLocker           sync.Mutex
      // tmq 任务线程
      thread                unsafe.Pointer
      // tmq 任务线程回调上下文
      handler               *tmqhandle.TMQHandler
      // 标志是否自动提交
      isAutoCommit          bool
      // 标志是否已经消掉订阅
      unsubscribed          bool
      // 标志是否已经关闭
      closed                bool
      // 自动提交间隔
      autocommitInterval    time.Duration
      // 下次自动提交时间
      nextTime              time.Time
      // 退出信号
      exit                  chan struct{}
      // 用户删除事件 channel
      dropUserChan          chan struct{}
      // 白名单变更事件 channel
      whitelistChangeChan   chan int64
      // 客户端连接 session
      session               *melody.Session
      // 客户端 ip
      ip                    net.IP
      // 客户端 ip 字符串
      ipStr                 string
      // 等待任务完成
      wg                    sync.WaitGroup
      // tmq 的内部连接
      conn                  unsafe.Pointer
      // 保护消费者的锁
      sync.Mutex
  }
  
  type Message struct {
      // 消息 id
      Index    uint64
      // 主题
      Topic    string
      // vgroup id
      VGroupID int32
      // 偏移量
      Offset   int64
      // 消息类型
      Type     int32
      // C 指针
      CPointer unsafe.Pointer
      // 缓存 buffer
      buffer   []byte
  }
  ```

##### 7.8.2.1 获取客户端版本

1. 组件设计：
   - 代理 C 接口 `taos_get_client_info`。
2. 列出系统中的关键数据结构
   - 请求和响应协议
  ```json {wrap}
  // 请求
  type versionRequest struct {
      ReqID uint64 `json:"req_id"`
  }
  // 响应
  type WSVersionResp struct {
      Code    int    `json:"code"`
      Message string `json:"message"`
      Action  string `json:"action"`
      Version string `json:"version"`
  }
  ```

##### 7.8.2.2 订阅

1. 组件设计：
   - 代理 C 接口 `tmq_conf_new`、`tmq_conf_destroy`、`tmq_conf_set`、`tmq_consumer_new`、`tmq_list_new`、`tmq_list_destroy`、`tmq_list_append`和`tmq_subscribe`。
   - 订阅包含两种情况
      - 未创建过订阅者：通过请求参数设置订阅参数，创建 consumer 之后订阅主题最后检查白名单并订阅白名单变更事件和用户删除事件。
      - 创建过订阅者并已经取消订阅需要重新订阅：忽略订阅参数，只订阅主题。
   - 自动提交永远设置为 false ，由 taosAdapter 来控制自动提交以避免由于网络延迟导致的意外自动提交,自动提交发生在拉取下一条消息时。
2. 列出系统中的关键数据结构
   - 请求和响应协议
  ```go {wrap}
  type TMQSubscribeReq struct {
      ReqID                uint64   `json:"req_id"`
      // TDengine 用户名
      User                 string   `json:"user"`
      // TDengine 密码
      Password             string   `json:"password"`
      // TDengine 数据库
      DB                   string   `json:"db"`
      // 订阅组 id
      GroupID              string   `json:"group_id"`
      // 客户端 id
      ClientID             string   `json:"client_id"`
      // 消费组订阅的初始位置
      OffsetReset          string   `json:"offset_reset"`
      // 订阅主题
      Topics               []string `json:"topics"`
      // 是否启用消费位点自动提交
      AutoCommit           string   `json:"auto_commit"`
      // 消费记录自动提交消费位点时间间隔，单位为毫秒
      AutoCommitIntervalMS string   `json:"auto_commit_interval_ms"`
      // 是否从 tsdb 订阅数据
      SnapshotEnable       string   `json:"snapshot_enable"`
      // 是否允许从消息中解析表名
      WithTableName        string   `json:"with_table_name"`
  }
  
  type TMQSubscribeResp struct {
      Code    int    `json:"code"`
      Message string `json:"message"`
      Action  string `json:"action"`
      ReqID   uint64 `json:"req_id"`
      Timing  int64  `json:"timing"`
  }
  ```

  请求参数与 TMQ 参数对应表

  | 请求参数 | TMQ 参数 |
| --- | --- |
| user | td.connect.user |
| password | td.connect.pass |
| db | td.connect.db |
| group_id | group.id |
| client_id | client.id |
| offset_reset | auto.offset.reset |
| auto_commit | enable.auto.commit |
| auto_commit_interval_ms | auto.commit.interval.ms |
| snapshot_enable | experimental.snapshot.enable |
| with_table_name | msg.with.table.name |

1. 使用几种类型的图表来解释设计
   - tmq 订阅时序图
  ![](./images/wb_UpKIwOnYJhixmMb3OK2cd3hbnGb.png)

##### 7.8.2.3 拉取消息

1. 组件设计：
   - 代理 C 接口 `tmq_consumer_poll`。
   - 拉取之前检查自动提交，如果配置自动提交并且达到自动提交时间则进行提交。
   - 拉取消息之后检查指针是否为空。
      - 如果为空则返回无消息。
      - 如果不为空则释放上一个消息，之后获取新消息的 topic db vgroupid offset 类型。
2. 列出系统中的关键数据结构
   - 请求和响应协议
  ```go {wrap}
  type TMQPollReq struct {
      ReqID        uint64 `json:"req_id"`
      // 等待时间（毫秒）
      BlockingTime int64  `json:"blocking_time"`
  }
  
  type TMQPollResp struct {
      Code        int    `json:"code"`
      Message     string `json:"message"`
      Action      string `json:"action"`
      ReqID       uint64 `json:"req_id"`
      Timing      int64  `json:"timing"`
      // 是否有消息
      HaveMessage bool   `json:"have_message"`
      // 消息所属 topic
      Topic       string `json:"topic"`
      // 消息所属 db
      Database    string `json:"database"`
      // 消息所属 vgroup
      VgroupID    int32  `json:"vgroup_id"`
      // 消息类型
      MessageType int32  `json:"message_type"`
      // 消息 id
      MessageID   uint64 `json:"message_id"`
      // 消息偏移量
      Offset      int64  `json:"offset"`
  }
  ```

1. 使用几种类型的图表来解释设计
   - tmq 拉取数据时序图
  ![](./images/wb_A9xfwKmklhO7akbHAylcPdYmnjh.png)

##### 7.8.2.4 获取数据结果

1. 组件设计：
   - 代理 C 接口 `taos_fetch_raw_block` 和 `tmq_get_table_name`。
   - 与获取查询结果类似，用来判断是否有后续数据以及获取数据解析所必须的信息。
2. 列出系统中的关键数据结构
   - 请求和响应协议
  ```go {wrap}
  type TMQFetchReq struct {
      ReqID     uint64 `json:"req_id"`
      MessageID uint64 `json:"message_id"`
  }
  type TMQFetchResp struct {
      Code          int                `json:"code"`
      Message       string             `json:"message"`
      Action        string             `json:"action"`
      ReqID         uint64             `json:"req_id"`
      Timing        int64              `json:"timing"`
      // 消息 id
      MessageID     uint64             `json:"message_id"`
      // 是否完成
      Completed     bool               `json:"completed"`
      // 数据对应的表名
      TableName     string             `json:"table_name"`
      // 数据块行数
      Rows          int                `json:"rows"`
      // 字段数
      FieldsCount   int                `json:"fields_count"`
      // 字段名称
      FieldsNames   []string           `json:"fields_names"`
      // 字段类型
      FieldsTypes   jsontype.JsonUint8 `json:"fields_types"`
      // 字段长度
      FieldsLengths []int64            `json:"fields_lengths"`
      // 结果时间精度
      Precision     int                `json:"precision"`
  }
  ```

1. 使用几种类型的图表来解释设计
   - tmq 获取数据结果数据时序图
  ![](./images/wb_E8qIwdGO5hRbkBb7ActckN9cnpf.png)

##### 7.8.2.5 获取结果数据块

1. 组件设计：
   - 返回获取结果暂存的数据块
2. 列出系统中的关键数据结构
   - 请求
  ```go {wrap}
  type TMQFetchBlockReq struct {
      ReqID     uint64 `json:"req_id"`
      MessageID uint64 `json:"message_id"`
  }
  ```

   - 响应为二进制

  | 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| message_id | uint64 | 消息 id |

##### 7.8.2.6 获取消息原始数据

1. 组件设计：代理 C 接口 tmq_get_raw
2. 列出系统中的关键数据结构
   - 请求
  ```go {wrap}
  type TMQFetchRawMetaReq struct {
      ReqID     uint64 `json:"req_id"`
      MessageID uint64 `json:"message_id"`
  }
  ```

   - 成功返回二进制

  | 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 执行时间（纳秒） | uint64 |
| 8 | 请求 id | uint64 |
| 16 | 消息 id | uint64 |
| 24 | action 固定值 3 | uint64 |
| 32 | Block 长度 | uint32 |
| 36 | 消息类型（内部类型） | uint16 |
| 38 | tmq raw block | []byte |

   - 失败返回 JSON
  ```go {wrap}
  type WSTMQErrorResp struct {
      Code      int     `json:"code"`
      Message   string  `json:"message"`
      Action    string  `json:"action"`
      ReqID     uint64  `json:"req_id"`
      Timing    int64   `json:"timing"`
      MessageID *uint64 `json:"message_id,omitempty"`
  }
  ```

##### 7.8.2.7 获取消息原始数据新格式

1. 组件设计：由于获取消息原始数据返回格式不统一新增此接口，功能相同，返回结果统一为二进制
2. 列出系统中的关键数据结构：
响应格式：

| 序号 | 名称 | 类型 | 字节数 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | Time | uint64 | 8 | 新格式固定为0xffffffff,用来做标志位和兼容 |
| 2 | Action | uint64 | 8 | Fetch Raw 响应值为 8 |
| 3 | Version | uint16 | 2 | 1 |
| 4 | Time | uint64 | 8 | 执行时间，单位 ns |
| 5 | ReqID | uint64 | 8 | 请求 id |
| 6 | Code | uint32 | 4 | 错误码 |
| 7 | MessageLen | uint32 | 4 | 当 Code = 0 时 MessageLen = 0 |
| 8 | Message | string | MessageLen | 错误内容 |
| 9 | MessageID | uint64 | 8 | 消息 id |
| 10 | MetaType | uint16 | 2 | 元数据类型 |
| 11 | RawBlockLength | uint32 | 4 | raw block 长度 |
| 12 | TMQRawBlock | byte[] | RawBlockLength | raw block 内容 |

##### 7.8.2.8 获取json元数据

1. 组件设计：代理 C 接口 tmq_get_json_meta
2. 列出系统中的关键数据结构
   - 请求和响应
  ```go {wrap}
  type TMQFetchJsonMetaReq struct {
      ReqID     uint64 `json:"req_id"`
      MessageID uint64 `json:"message_id"`
  }
  type TMQFetchJsonMetaResp struct {
      Code      int             `json:"code"`
      Message   string          `json:"message"`
      Action    string          `json:"action"`
      ReqID     uint64          `json:"req_id"`
      Timing    int64           `json:"timing"`
      MessageID uint64          `json:"message_id"`
      Data      json.RawMessage `json:"data"`
  }
  ```

##### 7.8.2.9 提交消息

1. 组件设计：代理 C 接口 tmq_commit_sync
2. 列出系统中的关键数据结构
   - 请求和响应
  ```go {wrap}
  type TMQCommitReq struct {
      ReqID     uint64 `json:"req_id"`
      MessageID uint64 `json:"message_id"` // unused
  }
  
  type TMQCommitResp struct {
      Code      int    `json:"code"`
      Message   string `json:"message"`
      Action    string `json:"action"`
      ReqID     uint64 `json:"req_id"`
      Timing    int64  `json:"timing"`
      MessageID uint64 `json:"message_id"`
  }
  ```

##### 7.8.2.10 取消订阅

1. 组件设计：代理 C 接口 tmq_unsubscribe
2. 列出系统中的关键数据结构
   - 请求和响应
  ```go {wrap}
  type TMQUnsubscribeReq struct {
      ReqID uint64 `json:"req_id"`
  }
  
  type TMQUnsubscribeResp struct {
      Code    int    `json:"code"`
      Message string `json:"message"`
      Action  string `json:"action"`
      ReqID   uint64 `json:"req_id"`
      Timing  int64  `json:"timing"`
  }
  ```

##### 7.8.2.11 获取分配信息

1. 组件设计：代理 C 接口 tmq_get_topic_assignment
2. 列出系统中的关键数据结构
   - 请求和响应
  ```go {wrap}
  type TMQGetTopicAssignmentReq struct {
      ReqID uint64 `json:"req_id"`
      Topic string `json:"topic"`
  }
  
  type TMQGetTopicAssignmentResp struct {
      Code       int                     `json:"code"`
      Message    string                  `json:"message"`
      Action     string                  `json:"action"`
      ReqID      uint64                  `json:"req_id"`
      Timing     int64                   `json:"timing"`
      Assignment []*tmqhandle.Assignment `json:"assignment"`
  }
  ```

##### 7.8.2.12 设置偏移量

1. 组件设计：代理 C 接口 tmq_offset_seek
2. 列出系统中的关键数据结构
   - 请求和响应
  ```go {wrap}
  type TMQOffsetSeekReq struct {
      ReqID    uint64 `json:"req_id"`
      Topic    string `json:"topic"`
      VgroupID int32  `json:"vgroup_id"`
      Offset   int64  `json:"offset"`
  }
  
  type TMQOffsetSeekResp struct {
      Code    int    `json:"code"`
      Message string `json:"message"`
      Action  string `json:"action"`
      ReqID   uint64 `json:"req_id"`
      Timing  int64  `json:"timing"`
  }
  ```

##### 7.8.2.13 提交偏移量

1. 组件设计：代理 C 接口 tmq_commit_offset_sync
2. 列出系统中的关键数据结构
   - 请求和响应
  ```go {wrap}
  type TMQCommitOffsetReq struct {
      ReqID    uint64 `json:"req_id"`
      Topic    string `json:"topic"`
      VgroupID int32  `json:"vgroup_id"`
      Offset   int64  `json:"offset"`
  }
  
  type TMQCommitOffsetResp struct {
      Code     int    `json:"code"`
      Message  string `json:"message"`
      Action   string `json:"action"`
      ReqID    uint64 `json:"req_id"`
      Timing   int64  `json:"timing"`
      Topic    string `json:"topic"`
      VgroupID int32  `json:"vgroup_id"`
      Offset   int64  `json:"offset"`
  }
  ```

##### 7.8.2.14 获取已提交偏移量

1. 组件设计：代理 C 接口 tmq_committed
2. 列出系统中的关键数据结构
   - 请求和响应
  ```go {wrap}
  type TMQCommittedReq struct {
      ReqID          uint64          `json:"req_id"`
      TopicVgroupIDs []TopicVgroupID `json:"topic_vgroup_ids"`
  }
  
  type TMQCommittedResp struct {
      Code      int     `json:"code"`
      Message   string  `json:"message"`
      Action    string  `json:"action"`
      ReqID     uint64  `json:"req_id"`
      Timing    int64   `json:"timing"`
      Committed []int64 `json:"committed"`
  }
  ```

##### 7.8.2.15 获取当前位置

1. 组件设计：代理 C 接口 tmq_position
2. 列出系统中的关键数据结构
   - 请求和响应
  ```go {wrap}
  type TMQPositionReq struct {
      ReqID          uint64          `json:"req_id"`
      TopicVgroupIDs []TopicVgroupID `json:"topic_vgroup_ids"`
  }
  
  type TMQPositionResp struct {
      Code     int     `json:"code"`
      Message  string  `json:"message"`
      Action   string  `json:"action"`
      ReqID    uint64  `json:"req_id"`
      Timing   int64   `json:"timing"`
      Position []int64 `json:"position"`
  }
  ```

##### 7.8.2.16 获取订阅的主题

1. 组件设计：代理 C 接口 tmq_subscription
2. 列出系统中的关键数据结构
   - 请求和响应
  ```go {wrap}
  type TMQListTopicsReq struct {
      ReqID uint64 `json:"req_id"`
  }
  
  type TMQListTopicsResp struct {
      Code    int      `json:"code"`
      Message string   `json:"message"`
      Action  string   `json:"action"`
      ReqID   uint64   `json:"req_id"`
      Timing  int64    `json:"timing"`
      Topics  []string `json:"topics"`
  }
  ```

### 7.9 数据收集软件接入

1. 组件设计：
taosAdapter 其他数据接入采用插件化设计，只要满足 `Plugin` 接口即可在 `init`方法中调用 `Register`注册自身，在 `system/plugin.go` 中使用下划线引入对应 plugin 包即可。
1. 列出系统中的关键数据结构
```go {wrap}
type Plugin interface {
        // 初始化插件，传入 http 路由组，插件可复用 http 端口，添加自己的路由
        Init(r gin.IRouter) error
        // 启动插件
        Start() error
        // 停止插件
        Stop() error
        // 返回插件名称
        String() string
        // 返回插件版本
        Version() string
}

// 插件组
var plugins = map[string]Plugin{}

// 注册插件
func Register(plugin Plugin) {
    name := fmt.Sprintf("%s/%s", plugin.String(), plugin.Version())
    if _, ok := plugins[name]; ok {
       logger.Panicf("duplicate registration of plugin %s", name)
    }
    plugins[name] = plugin
}

// 初始化插件，将插件名称和版本作为路由前缀生成路由组，再调用插件的 Init 方法
func Init(r gin.IRouter) {
    for name, plugin := range plugins {
       logger.Infof("init plugin %s", name)
       router := r.Group(name)
       err := plugin.Init(router)
       if err != nil {
          logger.WithError(err).Panicf("init plugin %s", name)
       }
    }
    logger.Infoln("all plugin init finish")
}

// 启动全部插件，遍历插件组调用 Start 方法
func Start() {
    for name, plugin := range plugins {
       err := plugin.Start()
       if err != nil {
          logger.WithError(err).Panicf("start plugin %s", name)
       }
    }
    logger.Infoln("all plugin start finish")
}

// 关闭全部插件，遍历插件组调用 Stop 方法
func Stop() {
    for name, plugin := range plugins {
       err := plugin.Stop()
       if err != nil {
          logger.WithError(err).Warnf("stop plugin %s", name)
       }
    }
}

// 关闭插件并提供超时上下文，协程内关闭全部插件，等待关闭完成或等待超时
func StopWithCtx(ctx context.Context) {
    done := make(chan struct{})
    go func() {
       defer close(done)
       Stop()
    }()

    select {
    case <-ctx.Done():
    case <-done:
    }
}
```

1. Schemaless 写入流程图
   - schemaless 指定 db
    ![](./images/wb_WWD2wH1TohNCCsbdLSac3WNJnwG.png)

   - schemaless 协议写入
    ![](./images/wb_D8PtwOGT1hnGjQbLShXcS1y6npb.png)

#### 7.9.1 InfluxDB

1. 组件设计：
   - 对外提供 HTTP 接口以提供 [`influxdb write v1`](https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/)协议写入。
   - 名称 influxdb，版本 v1，路由 write，因此访问地址为 `/influxdb/v1/write`。
   - TDengine 连接使用连接池，写入调用 C 接口 `taos_schemaless_insert_raw_ttl_with_reqid`。
   - 支持以下请求参数
      - precision：时间精度与 influxdb 相同默认纳秒
         - 纳秒：`ns`
         - 微秒：`u` 或 `µ` 
         - 毫秒：`ms`
         - 秒：`s`
         - 分钟：`m`
         - 小时：`h`
      - db：写入数据库名，**必须参数****。**
      - u：用户名。
      - p：密码。
      - ttl：表保存时间，单位：天。
      - req_id：请求 id。
2. 列出系统中的关键数据结构
   - 配置
  ```go {wrap}
  type Config struct {
      // 是否启用 influxdb 插件
      Enable bool
  }
  ```

   - 插件
  ```go {wrap}
  type Influxdb struct {
      // 配置
      conf Config
  }
  
  // 实现 Plugin 接口
  func (p *Influxdb) String() string {
      return "influxdb"
  }
  func (p *Influxdb) Version() string {
      return "v1"
  }
  func (p *Influxdb) Init(r gin.IRouter) error
  func (p *Influxdb) Start() error
  func (p *Influxdb) Stop() error 
  ```

1. 使用几种类型的图表来解释设计
   - 请求写入流程图
    ![](./images/wb_FdXewBwmIh1y2NbRMp1ch0Agnwb.png)

#### 7.9.2 collectd

1. 组件设计：
   - taosAdapter 支持 collectd 自身协议写入，以及 collected 的 OpenTSDB 插件写入，collected 的 OpenTSDB 插件写入在 opentsdb_telnet 章节描述，此章只描述 collected 自身协议。
   - taosAdapter 对外提供 udp 端口接入 collect 数据，使用 `telegraf` 的 `collectd`解码模块解析 `collectd`协议。 `github.com/influxdata/telegraf/plugins/parsers/collectd`。
   - 解码后的 collectd 协议经过 telegraf 的 influxdb 编码模块编码成 influxdb 协议，通过调用 C 接口`taos_schemaless_insert_raw_ttl_with_reqid`写入 TDengine。`github.com/influxdata/telegraf/plugins/serializers/influx`。
2. 列出系统中的关键数据结构
   - 配置
  ```go {wrap}
  type Config struct {
      // 是否启用
      Enable   bool
      // udp 端口
      Port     int
      // 写入的数据库
      DB       string
      // TDengine 用户名
      User     string
      // TDengine 密码
      Password string
      // 写入的工作协程
      Worker   int
      // 数据表存活时间
      TTL      int
  }
  ```

   - 插件
  ```go {wrap}
  type Plugin struct {
      // 配置
      conf       Config
      // udp 连接
      conn       *net.UDPConn
      // collectd 解析器
      parser     *collectd.CollectdParser
      // 指标管道
      metricChan chan *MetricWithClientIP
      // 关闭通知
      closeChan  chan struct{}
  }
  ```

1. 使用几种类型的图表来解释设计
   - 启动流程
  ![](./images/wb_SpsSwwLWhhjXo3bqkMvctR6nn0c.png)

   - 数据处理时序图
  ![](./images/wb_Gdm4whLfKhvPSlbJbQycZMFTnRd.png)

   - 写入流程
  ![](./images/wb_HS0fwxT7oh6EGcbDAzxcAHsqnRg.png)

#### 7.9.3 OpenTSDB HTTP

1. 组件设计：
   - 对外提供 HTTP 协议提供 OpenTSDB JSON 协议写入和 telnet 协议写入。
   - 名称 opentsdb，版本 v1，JSON 协议路由 `put/json/:db` ，telnet 协议路由 `put/telnet/:db`。
2. 列出系统中的关键数据结构
   - 配置
  ```go {wrap}
  type Config struct {
      // 启用
      Enable bool
  }
  ```

   - 插件
  ```go {wrap}
  type Plugin struct {
      // 配置
      conf       Config
  }
  
  func (p *Plugin) String() string {
      return "opentsdb"
  }
  
  func (p *Plugin) Version() string {
      return "v1"
  }
  func (p *Plugin) Init(r gin.IRouter) error 
  func (p *Plugin) Start() error 
  func (p *Plugin) Stop() error
  ```

1. 使用几种类型的图表来解释设计
   - JSON 与 telnet 处理流程图
![](./images/wb_WdomwNMEthHwowbam3acEBHsnyf.png)

#### 7.9.4 OpenTSDB_telnet

1. 组件设计：
   - 对外提供 tcp 端口支持 OpenTSDB telnet 协议写入。
   - 对每个 tcp 端口创建监听同时使用 channel 创建令牌桶来限制连接熟练。
   - 获取到新连接时检查 ip 是否在白名单，是否达到连接上限。
   - 创建协程读取数据。
   - 读取数据协程先创建 ticker 保证一段时间后数据被写入数据库不至于缓存太久，再创建数据管道用来异步处理数据，之后创建协程用来处理读取到的数据，最后循环读取数据直到 \n
      - 当读到数据为 version 时返回 "1"。
      - 读取出错时关闭当前连接并退出。
      - 读取到的数据发送到管道中。
   - 数据处理协程监听连接关闭信号，退出信号，ticker 信号和数据管道
      - 收到连接关闭信号时关闭ticker并将缓存数据写入数据库之后退出。
      - 收到退出信号时关闭 ticker 之后退出。
      - 收到 ticker 信号时将缓存数据写入 TDengine。
      - 收到数据管道数据时将数据添加到缓存中，如果缓存数量超过设定阈值将缓存数据写入 TDengine。
2. 列出系统中的关键数据结构
   - 配置
  ```go {wrap}
  type Config struct {
      // 启用
      Enable            bool
      // 端口列表与 DBList 对应
      PortList          []int
      // tcp 保持存活
      TCPKeepAlive      bool
      // 最多 tcp 连接（针对每一个端口限制，非全部端口）
      MaxTCPConnections int
      // 写入的数据库列表
      DBList            []string
      // TDengine 用户名
      User              string
      // TDengine 密码
      Password          string
      // 批量写入大小
      BatchSize         int
      // 强制写入时间间隔
      FlushInterval     time.Duration
      // 数据表存活时间
      TTL               int
  }
  ```

   - 插件
  ```go {wrap}
  type Plugin struct {
      // 配置
      conf         Config
      // 停止通知
      done         chan struct{}
      // tcp 任务结束等待
      wg           sync.WaitGroup
      // tcp 监听者列表
      TCPListeners []*TCPListener
  }
  
  func (p *Plugin) String() string {
      return "opentsdb_telnet"
  }
  
  func (p *Plugin) Version() string {
      return "v1"
  }
  func (p *Plugin) Init(_ gin.IRouter) error
  func (p *Plugin) Start() error 
  func (p *Plugin) Stop() error
  ```

1. 使用几种类型的图表来解释设计
   - 启动流程图 `func (p *Plugin) Start() error`
  ![](./images/wb_B08bwu8xthvRx0bYvKkcAZignhD.png)

   - 收到新连接处理流程 `func (l *TCPListener) start() error`
  ![](./images/wb_XJTQwdHYzhKmKnbtV3VchYP9nzg.png)

   - 读取数据流程`func (c *Connection) handle() `
  ![](./images/wb_ETlewm0SlhJWhZbFxcRcNP3UnIf.png)

   - 数据写入处理流程 （`func (c *Connection) handle() `内部协程）
  ![](./images/wb_Qlk9wevR6hcWdQbyH92cBQqMnbe.png)

#### 7.9.5 statsd

1. 组件设计：
   - 使用 `github.com``\influxdata\telegraf\plugins\inputs\statsd`包的解码器、统计计算和连接管理。
   - 采集流程
      - 创建指标接收管道 `metricChan`。
      - 创建协程接收指标
         - 接收指标后调用 `HandleMetrics` 将指标序列化成 influxDB 协议。
         - 调用 schemaless 写入到 TDengine。
      - 创建 Statsd 数据处理实例。
      - Statsd 开始采集 `Start`
         - 创建 tcp/upd 监听。
         - 创建 5 个协程解析数据 `parser`。
      - 创建 `GatherInterval`的 ticker，创建协程定时调用 `Statsd.Gather`,调用后数据指标会推送到 `metricChan`。
   - 数据处理流程
      - 通过 tcp/udp 接收到 statsd 数据，tcp 处理收到数据的方法 `func (s *Statsd) handler(conn *net.TCPConn, id string) `，udp 处理收到数据的方法 `func (s *Statsd) udpListen(conn *net.UDPConn) error`。
      - 数据发送到 Statsd.in，如果 in 管道已满则打印日志并丢弃。
      - 解析数据 `func (s *Statsd) parseStatsdLine(line string) error`。
      - 聚合数据 `func (s *Statsd) aggregate(m metric)`。
   - `Gather` 获取聚集结果
      - 遍历 distributions（分布）调用 acc.AddFields 将分布结果生成 telegraf.Metric 推送到 `metricChan`。
      - 遍历 timings（计时）计算平均值（mean）标准差（stddev）总数（sum）最大值（upper）最小值（lower）数量（count）后调用 acc.AddFields。
      - 遍历 gauges（仪表）调用 acc.AddGauge。
      - 遍历 counters（计数器）调用 acc.AddCounter。
      - 遍历 sets（集合）调用 acc.AddFields。

#### 7.9.6 prometheus

1. 组件设计：
   - 实现 Prometheus remote_read 和 remote_write HTTP 接口。
   - remote_read 路由 `remote_read/:db` ，remote_write 路由 `remote_write/:db`。
   - 读取数据 (`Read`)
    - 从 HTTP 请求中获取数据库名和用户认证信息。
    - 解码请求数据：
      - 使用 Snappy 解压缩数据。
      - 解析 Protocol Buffers 数据，生成 `prompb.ReadRequest`。
    - 获取 TDengine 的数据库连接。
    - 调用 `processRead` 函数处理查询逻辑，返回查询结果。
    - 将结果数据编码为 Protocol Buffers 格式，并使用 Snappy 压缩后返回。
   - 写入数据 (`Write`)
    - 获取数据库名和用户认证信息。
    - 解析请求中的 `ttl` 参数（表存活时间）。
    - 解码请求数据：
      - 使用 Snappy 解压缩数据。
      - 解析 Protocol Buffers 数据，生成 `prompbWrite.WriteRequest`。
    - 获取 TDengine 的数据库连接。
    - 调用 `processWrite` 函数将数据写入数据库。
   - `processWrite`  处理
    - 选择数据库：
      - 调用 `tool.SchemalessSelectDB` 选择指定的数据库。
      - 如果数据库切换失败，则返回错误。
    - 生成 SQL 语句：
      - 使用 `generateWriteSql` 方法将 `WriteRequest` 的时间序列数据转换为 TDengine 插入语句。
      - 支持基于标签自动生成唯一表名，并支持 `TTL`。
    - 执行插入：
      - 异步执行生成的 SQL。
      - 若遇到表不存在等错误，自动创建 `metrics` 表并重试插入。
   - `processRead` 处理
    - 选择数据库：
      - 使用 `wrapper.TaosSelectDB` 切换到目标数据库。
    - 生成查询 SQL：
      - 调用 `generateReadSql` 生成查询语句。
      - 支持标签匹配、多种匹配模式 `EQ`、`NEQ`、`RE` 。
    - 执行查询：
      - 异步执行 SQL。
      - 数据解析时，根据 TDengine 的时间精度处理时间戳。
    - 构造响应：
      - 将查询结果转换为 `prompb.TimeSeries` 格式。
      - 按表名分组结果，构造多组时间序列。
1. 列出系统中的关键数据结构
   - 配置
  ```go {wrap}
  type Config struct {
      Enable bool
  }
  ```

   - 插件
  ```go {wrap}
  type Plugin struct {
      conf Config
  }
  func (p *Plugin) String() string {
      return "prometheus"
  }
  
  func (p *Plugin) Version() string {
      return "v1"
  }
  func (p *Plugin) Start() error
  func (p *Plugin) Stop() error
  ```

#### 7.9.7 node_exporter

1. 组件设计：
   - 获取数据库连接
   - 遍历请求列表 (request)
      - 单个请求处理 (requestSingle)
         - 执行 HTTP 请求
         - 解析响应数据
         - 转换为 InfluxDB 格式
         - 写入 TDengine
         - 释放数据库连接
2. 列出系统中的关键数据结构
   - 配置
  ```go {wrap}
  type Config struct {
      // 启用
      Enable                bool
      // 写入的数据库
      DB                    string
      // TDengine 用户名
      User                  string
      // TDengine 密码
      Password              string
      // node_exporter 地址列表
      URLs                  []string
      // 响应超时事件
      ResponseTimeout       time.Duration
      // node_exporter Basic 用户名
      HttpUsername          string
      // node_exporter Basic 密码
      HttpPassword          string
      // node_exporter Bearer 验证 token
      HttpBearerTokenString string
      // node_exporter https 根证书
      CaCertFile            string
      // node_exporter https 应用证书
      CertFile              string
      // node_exporter https 应用证书 key
      KeyFile               string
      // 跳过 https 验证
      InsecureSkipVerify    bool
      // 采集间隔
      GatherDuration        time.Duration
      // 数据表超时时间
      TTL                   int
  }
  ```

   - 插件
  ```go {wrap}
  type NodeExporter struct {
      // 配置
      conf     Config
      // 预处理的 http 请求
      request  []*Req
      // 退出信号
      exitChan chan struct{}
  }
  ```

#### 7.9.8 JSON 数据写入

1. 组件设计：
   - 使用 JSONata 转换为展平的数据后按照库、超级表、子表、时间升序排序
   - 转换为 SQL 进行写入
   - sql注入考虑：
      - 关键字符已转义，即使构造出多条语句也只识别第一条语句。
      - 此接口只获取影响行数不获取查询结果，也不存在结果回显。
2. 列出系统中的关键数据结构
   - 配置结构
  ```go {wrap}
  type Config struct {
      Enable bool
      Rules  []*Rule
  }
  
  type Rule struct {
      Endpoint       string
      DB             string
      DBKey          string
      SuperTable     string
      SuperTableKey  string
      SubTable       string
      SubTableKey    string
      TimeKey        string
      TimeFormat     string
      Timezone       string
      TimeFieldName  string
      Transformation string
      Fields         []*Field
  }
  
  type Field struct {
      Key      string
      Optional bool
  }
  ```

   - 解析后的规则
  ```go {wrap}
  type ParsedRule struct {
      TransformationExpr   *jsonata.Expr
      DB                   string
      DBKey                string
      SuperTable           string
      SuperTableKey        string
      SubTable             string
      SubTableKey          string
      TimeKey              string
      TimeFormat           string
      Timezone             *time.Location
      TimeFieldName        string
      SqlAllColumns        string
      FieldKeys            []string
      FieldOptionals       []bool
      TransformationString string
  }
  ```

#### 7.9.9 OpenMetrics

1. 组件设计：
   - 根据用户配置的地址和验证信息定时获取数据
   - 根据返回的 header 判断协议类型进行解析并转换为无模式格式数据进行写入
2. 列出系统中的关键数据结构
   - 配置结构
  ```go {wrap}
  type Config struct {
      Enable                 bool
      User                   string
      Password               string
      DBs                    []string
      URLs                   []string
      ResponseTimeoutSeconds []int
      HttpUsernames          []string
      HttpPasswords          []string
      HttpBearerTokenStrings []string
      CaCertFiles            []string
      CertFiles              []string
      KeyFiles               []string
      GatherDurationSeconds  []int
      TTL                    []int
      IgnoreTimestamp        bool
      InsecureSkipVerify     bool
  }
  ```

## 8. 接口规范

1. API 文档
   - 日志 API
  ```go {wrap}
  // 获取日志实例
  func GetLogger(model string)
  // 当前日志级别是否显示 debug
  func IsDebug() bool
  // debug 日志获取当前时间，当 isDebug 为 true 时返回当前时间，false 时返回 0 时间
  func GetLogNow(isDebug bool) time.Time
  // debug 日志计算时间间隔，当 isDebug 为 true 时计算时间间隔，false 时返回 0
  func GetLogDuration(isDebug bool, s time.Time)
  ```

   - 连接池接口
  ```go {wrap}
  // 创建连接池
  func NewConnectorPool(user, password string) (*ConnectorPool, error) 
  // 从连接池获取连接
  func (cp *ConnectorPool) Get() (unsafe.Pointer, error)
  // 连接放回池
  func (cp *ConnectorPool) Put(c unsafe.Pointer) error
  // 验证密码
  func (cp *ConnectorPool) verifyPassword(password string) bool
  // 验证 ip
  func (cp *ConnectorPool) verifyIP(ip net.IP) bool
  // 释放连接池
  func (cp *ConnectorPool) Release()
  
  // 应用拿到的连接
  type Conn struct {
      TaosConnection unsafe.Pointer
      pool           *ConnectorPool
  }
  
  // 获取连接（应用直接调用）
  func GetConnection(user, password string, clientIp net.IP) (*Conn, error)
  // 验证 ip（应用直接调用）
  func VerifyClientIP(user, password string, clientIP net.IP) (authed bool, valid bool, connectionPoolExits bool)
  ```

   - 异步执行 SQL 封装
  ```go {wrap}
  // sql 执行结果
  type ExecResult struct {
      AffectedRows int
      FieldCount   int
      Header       *wrapper.RowsHeader
      Data         [][]driver.Value
  }
  
  // 完整的执行和获取数据
  func (a *Async) TaosExec(taosConnect unsafe.Pointer, sql string, timeFormat wrapper.FormatTimeFunc, reqID int64) (*ExecResult, error)
  
  // 异步执行 SQL
  func (a *Async) TaosQuery(taosConnect unsafe.Pointer, sql string, handler *Handler, reqID int64) (*Result, error)
  
  // 异步获取单行数据
  func (a *Async) TaosFetchRowsA(res unsafe.Pointer, handler *Handler) (*Result, error)
  
  // 异步获取原始块数据
  func (a *Async) TaosFetchRawBlockA(res unsafe.Pointer, handler *Handler) (*Result, error)
  
  // 异步执行 SQL 不获取数据（用在非查询 SQL）
  func (a *Async) TaosExecWithoutResult(taosConnect unsafe.Pointer, sql string, reqID int64) error
  ```

   - 查询结果数据块拼接 JSON
  ```go {wrap}
  // raw block 解析结果并编码成 JSON 写入 builder
  // builder：JSON 构建流
  // colType：列类型
  // pHeader：边长类型偏移量指针
  // pStart：数据位置指针
  // row：行数
  // precision： 时间精度
  // timeFormat：时间转换方法
  func JsonWriteRawBlock(builder *jsonbuilder.Stream, colType uint8, pHeader, pStart unsafe.Pointer, row int, precision int, timeFormat FormatTimeFunc)
  ```

   - schemaless 写入封装
  ```go {wrap}
  // 写入 influxdb 数据
  // conn：TDengine 连接
  // data：influxdb 数据
  // db：写入数据库
  // precision：时间精度
  // ttl：表存活时间
  // reqID：请求ID
  func InsertInfluxdb(conn unsafe.Pointer, data []byte, db, precision string, ttl int, reqID int64) error
  
  // 写入 OpenTSDB JSON 数据
  // conn：TDengine 连接
  // data：JSON 数据
  // db：写入数据库
  // precision：时间精度
  // ttl：表存活时间
  // reqID：请求ID
  func InsertOpentsdbJson(conn unsafe.Pointer, data []byte, db string, ttl int, reqID int64) error
  
  // 写入 OpenTSDB telnet 数据
  // conn：TDengine 连接
  // data：OpenTSDB telnet 数据
  // db：写入数据库
  // precision：时间精度
  // ttl：表存活时间
  // reqID：请求ID
  func InsertOpentsdbTelnet(conn unsafe.Pointer, data []string, db string, ttl int, reqID int64) error
  ```

## 9. 安全考虑

1. 在与客户端和数据库交互时，`taosAdapter` 必须确保只有授权用户或客户端才能访问服务。
2. 可采用加密通道（例如 HTTPS、WSS）进行通信，防止明文数据传输带来的安全风险。
3. 控制内存占用，保持进程安全。
4. 控制连接数占用，防止连接数过多引起服务端崩溃或无响应、响应超时。
1. 

## 10. 性能和可扩展性

- 写入性能：taosBenchmark rest模式写入，并发1000，典型电表表结构，100w子表，每子表写入100，数据随机，qps达到40w/s以上。
- 查询性能：使用jmeter向JDBC rest请求，select last_row(*) from test.${tbname}语句，tbname随机，并发1000，循环50次，qps达到1000/s以上。
- 启动时间：taosAdapter 应在 5 秒内完成初始化并开始接收请求。

## 11. 部署和配置

1. 部署流程：taosAdapter 随 TDengine 安装包一起安装部署，单独部署时需要安装好 TDengine 客户端。
2. 配置管理：taosAdapter 连接 TDengine 依赖 taosc，需要配置好 taosc 的 firstep 和 secondep 等参数。
3. 版本控制：保持对外接口兼容性，当有破坏性功能时新加接口，保证原有接口仍可工作。

## 12. 监控和维护

1. 监控：
   - 日志记录了 C 执行时间可以得到执行慢的调用。
   - SQL 执行以及结果上报给 taosKeeper，可以通过 taosKeeper 获得统一监控。
2. 日志记录和诊断：
   - 记录 C 执行时间以及获取连接池的时间，通过日志可以看出来哪些 C 执行时间长以及是否由于连接池导致阻塞。
   - 记录操作失败日志，可以得到请求 id 以及错误内容。
3. 维护：
   - 废弃接口不再加新功能，只做维护。
   - 新功能开发尽量保证兼容性。
   - 订阅相关功能在 tmq 包下添加。
   - 查询写入等相关的功能在 ws 包下添加。

## 13. 参考资料

1. [C/C++ 连接器-Function Spec](https://taosdata.feishu.cn/wiki/Hk2Swj9bdipmZCkK0NEcZCKankd)
2. [taosAdapter-Function Spec](https://taosdata.feishu.cn/wiki/Xf3zweDQRiFhwNkBSWScVj01nVc)
3. **InfluxDB V1 写接口：** https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/
4. **OpenTSDB：**
  - [http://opentsdb.net/docs/build/html/api_http/put.html](http://opentsdb.net/docs/build/html/api_http/put.html)
  - [http://opentsdb.net/docs/build/html/api_telnet/put.html](http://opentsdb.net/docs/build/html/api_telnet/put.html)
1. **Prometheus remote_read 和 remote_write：** https://prometheus.io/blog/2019/10/10/remote-read-meets-streaming/
2. **node_exporter：** https://github.com/prometheus/node_exporter
3. **collectd：** https://www.collectd.org/
4. **StatsD：** https://github.com/statsd/statsd
5. **icinga2 OpenTSDB writer：** https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer
6. **TCollector：** http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html
