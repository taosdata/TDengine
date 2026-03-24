# taosX 异常处理机制

## 1. 背景

目前支持的数据源对于数据源异常、数据库异常、Agent 异常的处理机制不完善，用户行为也不明确。
1. 对于任务执行过程中的遇到的错误用户无法感知，造成用户界面的响应等待或状态不符合预期。
2. 网络中断、数据源异常、写入数据库异常等无法在用户界面观察到。
3. 部分异常状态导致的错误会导致内存持续增长。
本文档希望对 taosX 所有的异常事件的响应规则进行描述，并约定用户界面的行为。taosX 的内部实现不在此表述，请另行参考设计文档 [taosX 异常处理机制优化](https://taosdata.feishu.cn/wiki/Hqz1wLmpniI7VzkxFevcNG0inYd) ，不一致的以此文档为准。

## 2. 行为说明

异常处理优化的范围包括：
1. 异常事件约定
2. REST API 
3. gRPC API （Agent）
4. UI

### 2.1 异常事件约定

taosX 服务中的异常事件包含以下几种类型：
- 服务异常：指 taosx 服务因系统资源（如磁盘空间不足）、数据库异常、运行时线程 panic 、部分服务停止（REST API 或 gRPC）等原因，导致 taosx 不具备正常运行条件，需要报警及人工介入。服务异常时，需要保证正在执行的任务可正常执行或安全停止、通知功能正常。保证异常状态可被用户界面感知，此时服务降级，可以不再响应任务的新增、删除等操作。
  - 磁盘空间不足：发送服务异常告警，有可能影响任务正常执行，需要人工介入清理或增加磁盘空间。
  - 数据库异常：发送数据库异常通知，正在运行的任务不受影响，但不响应任务操作（创建、修改、删除、停止，相关 API 调用时返回错误），需要人工介入，手动修复数据库或重启服务。
  - 运行时有线程 panic：发送服务异常告警，任务可正常执行。
  - REST API 服务停止：日志记录异常停止原因，关闭其他服务组件并退出，正在运行中的任务转为 **suspended** 。
  - gRPC 服务停止：日志记录异常停止原因，发送服务异常告警，尝试重新拉起 gRPC 服务（重试 5 次，成功启动次数重置），重试后无法启动 gRPC 服务，发送异常告警到前端，此时所有 Agent 状态为 pending ，不响应 Agent 相关 API，根据 taosX 服务启动策略，决定是否立即停止服务（默认停止服务，云服务下自动重启 POD 即重启 taosx 服务，企业版部署下由 systemd 机制重启）。
- 服务关闭：指 taosx 被停止服务或被其他操作停止。当服务收到停止信号（kill -2/-15, systemctl stop 或 ctrl+c），应优雅关闭，再次启动后相关任务恢复运行。
   - 发送服务关闭通知到前端，并向所有正在执行中的任务发送关闭信号，停止任务执行，缓存并将任务状态置为 **suspended**。
   - 下次服务重启后，将 suspended/scheduled/queued/running 状态的所有任务恢复运行。
- 服务崩溃：指服务无法再通过执行操作保证服务优雅关闭。在被 kill -9、OOM、服务器宕机、断电等情况下，应保证服务重启时恢复任务执行，即 taosx 应当具备在任意服务状态时中断恢复的能力。
   - taosX 服务重启后，如果配置了自动恢复任务，则将 suspended/scheduled/queued/running 状态的所有任务恢复运行。如果配置不自动恢复，则将 scheduled/queued/running 状态（非正常关闭导致的非预期状态）的任务转为 `suspended`。
   - Agent 服务崩溃，taosx 将 Agent 状态置为 pending，Agent下正在执行的任务置为 suspended。Agent重连后，所有 suspended/scheduled/queued/running  状态的任务继续下发并执行。Agent 启动的连接器进程在 Agent 断开后退出。
- 任务执行异常：指任务执行过程中可以被获取的错误（不包含不能被任务执行器获取的异常，如 OOM、Crash 等）
  - 可恢复的异常：包括 taosd/taosadapter 无法响应、Agent/Server 网络中断、数据源连接断开等。
      - 当遇到以上异常时，任务状态转为 `**pending**`** **等待服务恢复。
      - taosd/taosadapter/agent 恢复正常后，任务状态转为 `**running**`。
  - 连接器外部进程崩溃：收集崩溃错误信息，告警并将任务状态置为 pending 并尝试恢复进程，重试多次仍然失败，任务状态转为 failed 。
  - 运行过程中的警告：不影响任务继续执行，但需要发出警告由用户决定是否进行人工干预，包括：
    - 数据质量差
    - 部分数据无法写入（数据不合法、表不存在等导致产生非预期的结果）
    - 根据任务健康策略确定是否将警告升级为异常（导致任务状态变更）。
  - 不可恢复的异常：当超出允许的重试次数后，可恢复的异常可能升级为不可恢复的异常，此时任务执行失败(**failed**)。
- 任务执行失败：指任务无法继续执行（消费完所有数据或无法继续消费数据，或根据用户策略配置收到不可恢复的异常），任务执行失败（**failed**）。
本次优化主要关注 1. 优雅关闭 2. 任务异常时的用户可见性。

### 2.2 REST API

#### 2.2.1 任务状态

- API：`GET /tasks`，`GET /tasks/:id`
任务属性 `status` 扩展为以下几种：

```plaintext
created -> queued  -----+--------> running -> completed
                        |            |
             +----------+            |
             |                       +-----> failed  
             |                       |                                  
             |       Stop       -->  +-----> stopping -> stopped
             |                       |
             |       Shudown    -->  +-----> suspending -> suspended
             |                       |
             |  Agent disconnected ->|-----> waiting
             |                       |          |    <---- Agent resumed
             |                       |<---------+          
             |<----- Scheduler tick  |           
             |                       |
       interrupted <-----------------+
```

- `created`：任务创建后的初始状态。当服务重新启动时，该状态的任务不启动。
- `queued`：启动任务后，任务进入 `queued` 状态，表示任务已进入执行队列。
- `running`: 任务开始执行，即置为 running 。
- `stopping/stopped`: 任务被手动停止，即置为 `stopping` 状态，当任务停止完成后，置为 `stopped` 状态。
- `suspending/suspended`：taosX 服务关闭导致任务关闭，进入 `suspending` 状态，任务被彻底关闭后置为 `suspended`。
- `waiting/resumed`：agent 断开后，任务进入 `waiting` 状态，待 Agent 上线后，进入 `resumed` 状态，待 Agent 任务状态恢复重新进入 `running` 状态。
- `interrupted`：任务在多次重试或中间过程中出现异常导致一次任务中止，进入 `interrupted` 状态，此任务是临时状态，任务会自动进行下一次执行，下次执行开始后，任务重新进入 running 状态。

**UI**** 创建数据源任务行为描述**：
1. 当 agent 不在线时，任务拒绝创建。
2. 当 agent 在线时，任务正常创建，状态初始为 `created`，随后进入 `queued` 状态，任务开始调度，任务启动后，任务转为 `running`。
**UI**** Start/Stop 行为描述**：
1. 当任务处于初始状态时，允许 start，初始状态包括：`created`。
2. 当任务处于运行状态时，允许 stop，运行状态包括： `queued`, `running`, `interrupted`, `waiting`, `resumed`。
3. 当任务处于中间状态时，不允许 start/stop，中间状态包括： `stopping`, `suspending`。此时任务正在进行等待任务结束、缓存或资源释放操作。
4. 当任务处于结束状态时，允许 start，结束状态包括：`failed`, `stopped`, `suspended`， `completed`。
5. 当任务处于异常状态时，需要显示异常信息，异常状态包括： `failed`, `interrupted`, `suspending`,  `suspended`，`waiting`。

#### 2.2.2 ~~Agent 状态~~

- ~~API~~~~：~~`~~GET /agents~~`~~，~~`~~GET /agents/:id~~`
~~Agent~~~~ 状态属性 ~~`~~status~~`~~ 添加一个 ~~`~~closed~~`~~ 状态~~~~，表示 Agent 服务关闭。服务关闭指 Agent 主动发送退出信号（一般是通过 Ctrl+C 或 systemctl stop 执行关闭后，Agent 将退出信号发送到控制流，双方关闭所有链接并退出）。~~~~ Agent 非正常关闭，Agent 状态转为 ~~`~~pending~~`~~ 等待连接~~~~ 。~~

#### 2.2.3 创建任务

- API： `POST /tasks/` 
任务属性通过对 trigger 进行扩展，修改后的 trigger 参数支持两种
1. 向后兼容的定时器
  ```json
  { trigger: "schedule:@daily" }
  ```

1. 新版配置
  ```json
  { trigger: {
    "schedule": null,
    "resume": "always",
    "healthy": "timeout(1m)",
    "interval": "5s" 
  } }
  ```

  第一种形式等同于：
  ```json
  { trigger: {
    "schedule": null,
    "resume": "never",
    "healthy": "intolerant"
  } }
  ```

其中参数定义如下
- `schedule`  的定义同之前一致，是定时任务表达式：`@daily`, `@weekly` 等。默认为空（null），表示立即执行任务。允许的取值包括：
  - `@hourly`：每小时执行一次
  - `@daily`：每天执行一次
  - `@weekly`：每周执行一次
  - `@monthly`：每月执行一次
  - `@yearly`：每年执行一次
  - 表达式 `sec  min   hour   day-of-month   month   day-of-week   year` （如`0   30   9,12,15     1,15       May-Aug  Mon,Wed,Fri  2018` 表示 2018 年 5-8 月每月 1 日或 15 日且为周一三五的 9/12/15 点的 30 分 0 秒执行该任务）。
**注意：**以上所有时间表示，以 UTC-0 为基准，即：每天执行时间为 UTC 00:00 而不是本地时间。
- `resume` 表示异常时的处置方式。默认为 `always`，允许的取值如下：
  - `never`： 从不恢复
  - `once`：仅服务中断后启动时恢复一次，任务异常时不恢复。
  - `retries(<num>)`: 重试次数 `num`。如 `retries(5)` 表示重试 5 次，5 次后仍然失败，置为 **failed** 。
  - `always`：当遇到错误后，始终尝试继续启动任务。
- `healthy`：表示任务过程异常中捕获的错误的处置方式，默认为 `intolerant`，即将错误视为不可恢复，任务状态置为 `failed`。
  - `ignore`：表示忽略错误
  - `intolerant`：表示将错误视为不可恢复，遇到错误后直接上报错误并终止任务运行。
  - `timeout(unit)`: 表示将错误视为 `unhealthy`，并通知前端，在一段时间后不再发生错误，则视为 `healthy`。
  - `errorrate(1/5m)`：根据错误率上报异常状态，并通知前端。
- `interval`: 表示任务过程中遇到错误时恢复任务的间隔时间，默认为 5s 。POST 发送时此值应为可读时间间隔表示，如 "1s", "5s", "10s"

#### 2.2.4 订阅通知

- API： `ws /notifications`
- 消息格式：
  ```json
  {
      scope: enum { service|http|grpc|agent|task },
      messages: [{
         id: i64,
         at: datetime,
         level: enum {error|warn|info|debug|trace},
         activity: string,
         status: string,
         context: enum { null | string | json-object }
      }]
  }
  ```

  其中： 
  - `scope`：表示通知主体，`service` 表示服务最底层，`http` 表示 REST API，`grpc` 表示 gRPC API，`agent` 表示 Agent 服务，`task` 表示同步任务。
  - `messages` 可以是一个 object，也可以是一个消息数组，格式沿用 `tasks/:id/activities` 的返回结构。前端可使用此订阅替代之前的 `activities` API。 

### 2.3 gRPC API

#### 2.3.1 gRPC Server

-  API 接口无变化，Agent 与 Server 之间包含控制流与数据流两种连接。
- Server 端的异常处理行为：
  - Agent 主动退出，此时服务端将 Agent 置为 `closed`，关闭所有连接，Agent 下正在运行的任务停止消费，在队列中的数据写入缓存，任务状态置为 `paused`。
  - Agent 异常退出，有三种情况：
    - 控制流中断，此时没有任务正在执行，则将 Agent 状态置为 pending。
    - 控制流中断，此时仍有任务正在执行，Agent 状态置为 pending，任务状态取决于数据流是否正常，数据流异常时，发送 pending 通知，任务进入 pending 状态。
    - 控制流正常，数据流异常，此时发送任务 pending 通知，任务进入 pending 状态，等待 Agent 发起下一次连接。

#### 2.3.2 gRPC Agent

- Agent 控制流发起连接，连接初始化完毕后，Agent 由 `created` 进入 `idle` 的正常连接状态，等待任务下发。
- 在 idle 状态下，如果发生网络连接断开（TonicError），重试控制流（间隔 5s）直到成功连接。记录连接异常日志，并在下次连接后上报。
- 收到任务后，进入 busy 状态。
- 在 busy 状态下，控制流发生网络中断，继续重试连接直到重新连接成功。
- 在 busy 状态下，数据流发生网络中断，继续重试连接直到重新连接成功，同时发送任务执行状态告警到控制流。此时数据无法正确发送到服务端，数据收到后进行缓存，必要时（因某种原因导致缓存异常，无法正常工作）停止任务。连接正常后，优先消费缓存中的数据。（？这里仍存疑，是否有必要所有数据源都进行缓存？对数据源级别或任务级别提供缓存策略应该是更合理的做法。比如 kafka 订阅支持 Offset，这样不必在本地进行数据缓存，MQTT / OPC 则需要进行缓存。如果提供缓存策略控制，应该基于任务级别还是数据源级别呢？任务级别的可控性更高一些，但会更复杂。）
- 在 busy 状态下，控制流和数据流同时发生网络中断，此时在服务端有异常告警。如果数据源连接正常，在缓存可用时进行数据缓存，待连接正常后，发送缓存数据。缓存异常，Agent 下正在运行的任务都停止运行，Agent 服务退出。
- 用户应监控 Agent 服务状态，及时处理异常事件，尽量保证数据源消费数据的完整性。

### 2.4 Explorer 前端

#### 2.4.1 接收通知

-  Explorer 订阅 taosX WebSocket 服务通知 /notifications , 并对不同通知范围进行消息处理：
   - `service` 应进行总体服务事件记录，对于 warning 及 error，应有明确的前端提醒。
   - `agent` 应对每个 Agent 状态进行实时展示，显示事件列表。
   - `tasks` 应对每个任务状态进行实时展示，并显示事件列表，如有 warning/error 级别事件，前端应有明确的 UI 提示。
缓存策略

## 3. 异常测试用例

### 3.1 任务运行中 taosX 关闭

### 3.2 任务运行中 taosd 关闭

### 3.3 任务运行中 taosadapter 关闭
