# taosX 事件追踪

## 1. 背景

taosX 是一个可观测的系统。它的可观测性不仅应体现在完善的 metrics，比如任务进度统计、传输的数据量统计、传输速率统计，还应体现在对系统事件可追踪性。这里的事件追踪主要是指通过分析系统日志，确定业务是否正常进行，如果异常是在哪个环节发生的异常，原因是什么。
在 1.3.0 之前的版本中，我们已经引入了 [tracing ](https://docs.rs/tracing/latest/tracing/)库, 利用该库提供的 [span ](https://docs.rs/tracing/latest/tracing/span/index.html)功能， taosx 具有了记录结构化事件的能力。
在 1.3.0 版本中，我们为每个业务都生成唯一的 Trace ID, 仅用 Trace ID 就能把某个业务相关的所有事件串联起来。
我们对事件的定义，除了包括用户主动触发的事件，如创建/删除/暂停/恢复任务，还包括数据传输事件。数据采集器（或称为连接器）发送给 agent 或 taosX 的每一批数据都会被追踪。我们把为追踪数据而生成的 Trace ID 称为 Data Trace ID。

TD-26000

## 2. 行为说明

### 2.1 范围

1. Data Trace ID 目前只支持需要通过 IPC 模块传输数据的数据源，目前包括：
   - Influxdb
   - Mqtt
   - Opc ua/da
   - PI
   - OpentsDB
   - CSV
2. 有无 Agent，Sever 模式和 CLI 模式， Data Trace ID 行为一致
3. Data Trace ID 会与 taosd 的 Query ID 关联，通过 Data Trace ID 可以查询到本批数据在 taosd 的具体操作
4. 支持从上游应用如云服务传入业务的 Trace ID。
5. 如果上游应用在调接口时没有传入 Trace ID， 将为每个业务自动生成 Trace ID
6. 重构日志输出模块，采用自定义的格式打印日志

### 2.2 业务 Trace ID 设计

Trace ID 是长度小于 128 个字符的任意字符串。
Taosx HTTP API 的调用者可以主动传入 Trace ID。taosx 检查请求头是否包含 "**Trace-Id**" 字段，如果包含，将把它的值作为 Trace ID。
如果请求头不包含 Trace-Id，则生成随机的 u32 类型的整数，以 8 位十六进制字符形式在日志中表示, 例如：TID:0x111a9d7f。

### 2.3 Data Trace ID 设计

Data Trace ID 用于跟踪传输的每一批数据。从技术角度讲我们追踪的对象是 Arrow 格式的 [RecordBatch](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html)。RecordBatch 是连接器发送数据的基本单位，也是 Agent/TaosX 接收数据的基本单位。RecordBatch 的传输依赖提前建立好的 Stream。我们为每个 Stream 也生成了一个 Trace ID 称为 Stream ID。
Data Trace ID = Stream ID + Batch Number
其中 Stream ID 为随机生成的占 16 个 bit 的随机数。Batch Number 为占 32 个 bit 的无符号整数，从 1 开始自增。如果以十六进制形式打印则为12位的十六进制数，例如：0x8bc500000004。黄色部分为 Stream ID，紫色部分为 BatchNumber。
备注： 日志中的，例如 DTID:8bc5， 准确地说是 Stream ID。

### 2.4 Taosd Request ID 的设计

按照文档 [撰写日志的基本要求](https://taosdata.feishu.cn/wiki/OpLBwZhpRiKXxrkWllLcLXA9nBf) 的要求， Request ID 占 64 bits，以 16 位十六进制字符的形式在日志中表示。
一个 RecordBatch 可能包含很多数据，也可能不包含数据，只包含创建表的语句。总之一个 Recorad Batch 会对应很多对 taosd 的操作。可能是 create table, 可能是 stmt 写入，可能是 query， 可能是 write_raw_block。
对于 SQL 写入和 raw_block 写入，Request ID = Data Trace ID + 16 bit 自增整数**， **例如：0x8bc5000000047896。
对于 STMT 写入，由于只有在初始化 STMT对象的时候可以携带 req_id，且一个 Stream 共享一个 Stmt 对象，因此 Request ID = Stream ID。涉及的数据源有：PI，InfluxDB 和 OpentsDB

### 2.5 日志规范

#### 2.5.1 日志路径

Windows 系统默认日志路径是 "C:\TDengine\logs"；
Linux 系统默认日志路径是 "/var/log/taos/"。

#### 2.5.2 日志格式

1. 精确到微秒的时间戳，如： 10/06 16:13:52.091324
2. 日志级别，如： INFO
3. 模块名称:行号，如 [sink:56]
4. Trace ID 或 Data Trace ID, 如： TID:0x423424232,DTID:1234
5. Span。 如果日志带有 span 则显示 span 和 span 的属性
6. 日志内容
各部分用空格分割。
总体来说就是:
```plaintext
时间戳 日志级别 [模块名称:行号]  TID:xxxxx,DTID:xxxx [Span] 日志内容
```

例如：
```plaintext
10/24 18:36:36.053038 ERROR [actix-server worker 0] [HTTP-Server] TID:0xb543d772 - Error encountered while processing the incoming HTTP request: code=[0xFFFF] Incomplete message="agent name has existed"
```

#### 2.5.3 taosX 日志

1. 在有 Agent 参与的情况下，每一批数据，至少由 3 条日志追踪：
   - Agent IPC 模块收到数据的日志， INFO 级别
   - TaosX RPC 模块收到数据的日志， INFO 级别
   - Sink 模块把数据最终写入 taosd 的日志， DEBUG 级别
2. 在没有 Agent 参与的情况下, 每一批数据，至少由 2 条日志追踪：
   - TaosX IPC 模块或内置数据源模块收到数据的日志， INFO 级别
   - Sink 模块把数据最终写入 taosd 的日志， DEBUG 级别

## 3. 使用场景

### 3.1 用接口路径过滤出 Trace ID

例如用户刚刚进行了删除任务的操作，但是操作失败了，需要查询具体日志，此时可以先执行：
```bash
grep /delete taosx-server.log
```

### 3.2 根据任务 ID 过滤日志

用任务 ID 可以过滤和某个 task 相关的所有日志。可同时从 taosX 日志， agent 日志和连接器日志中提取日志。例如：
```bash
grep task.id=134 taosx.log agent.log influxdb.log
```

### 3.3 根据业务的 Trace ID 过滤日志

这是最精确的查询特定业务相关日志的方法
```bash
grep TID:0x12313232 taosx.log agent.log influxdb.log
```

### 3.4 根据 Stream ID 过滤 stream 的所有日志

```bash
grep DTID:8bc5 taosx.log agent.log
```

### 3.5 用 Data Trace ID 过滤一批数据对应 taosd 的所有操作

```shell
 grep -n '0xd5d600000000' taos*
```

## 4. 日志示例

### 4.1 需要 agent 的任务

以 opc-ua 任务为例
agent 日志示例
```prolog
11/01 19:04:57.163835  INFO [main] Start task 8
11/01 19:04:57.228175  INFO [taosx-agent] [run_task->opc_to_taos{task.id=8}] Using opc config file C:\Users\dingb\AppData\Local\Temp\.tmpEvZJXw
11/01 19:04:57.230604  INFO [taosx-agent] [run_task->opc_to_taos{task.id=8}] log path created: C:\TDengine\log\opc
11/01 19:04:57.230808  INFO [taosx-agent] [run_task->opc_to_taos{task.id=8}] log file dir: C:\TDengine\log\opc\opc.log
11/01 19:04:58.338031  INFO [taosx-agent] [run_task->opc_to_taos{task.id=8}->build_ipc{ipc.listen="127.0.0.1:6051" ipc.target=taos+http://192.168.0.31:6041/ci_opcua}->agent_ipc_listener] new tcp client!: 127.0.0.1:56076
11/01 19:04:58.340591  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] token: 10920b91d4500aad22d8ad3c512c6c3d
11/01 19:04:58.347688  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] reading batches client="127.0.0.1:56076" remote="http://localhost:6055"
11/01 19:04:58.659325  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] Handshake done
11/01 19:04:58.659775  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] Do putting
11/01 19:04:58.660217  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc500000001
11/01 19:04:58.667224  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc500000002
11/01 19:04:58.785550  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] Get putting stream response
11/01 19:04:59.337934  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc500000003
11/01 19:05:00.335260  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc500000004
11/01 19:05:01.323489  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc500000005
11/01 19:05:02.328529  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc500000006
11/01 19:05:03.323497  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc500000007
11/01 19:05:04.328334  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc500000008
11/01 19:05:05.332848  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc500000009
11/01 19:05:06.327752  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc50000000a
11/01 19:05:07.332891  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc50000000b
11/01 19:05:08.337810  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc50000000c
11/01 19:05:09.325774  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc50000000d
11/01 19:05:10.331358  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc50000000e
11/01 19:05:11.325085  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc50000000f
11/01 19:05:11.544934  INFO [main] At [2023-11-01 11:05:11.542 +00:00] action `stop` triggered
11/01 19:05:11.545321  INFO [main] Stop task 8
11/01 19:05:11.545547  INFO [taosx-agent] [8] Remove runner for task 8, wait for finished id=8
11/01 19:05:11.545750  INFO [taosx-agent] opc task cancelled
11/01 19:05:11.545890  INFO [taosx-agent] OPC to taos task done
11/01 19:05:11.548020  INFO [taosx-agent] DTID:8bc5 [ipc_tcp_forward{task_id=8}] send batch 0x8bc500000010
```

server 日志示例
```prolog
11/01 19:04:56.155912  INFO [actix-server worker 1] TID:0x111a9d7f [http] 127.0.0.1 "POST /tasks? HTTP/1.1" python-requests/2.31.0
11/01 19:04:56.157037  INFO [actix-server worker 1] TID:0x111a9d7f [http->task::create] create new task
11/01 19:04:56.332709  INFO [actix-server worker 1] TID:0x111a9d7f [http->task::create] list tasks
11/01 19:04:56.472833  INFO [actix-server worker 1] TID:0x111a9d7f [http->task::create->start_task{task.id=8 task.agent=1}] job created task.id=8 job.id=95be1d69-60ed-4654-bb9b-e3ab87e93931
11/01 19:04:56.473761  INFO [actix-server worker 1] TID:0x111a9d7f [http->task::create->start_task{task.id=8 task.agent=1}] Job creator created
11/01 19:04:56.473973  INFO [taosx] task: 8 "Enqueue task 8 by job id: 95be1d69-60ed-4654-bb9b-e3ab87e93931" "queued"
11/01 19:04:56.473992  INFO [taosx] act=TaskActivity(Activity { id: 8, at: 2023-11-01T11:04:56.473831800Z, level: Info, activity: "Enqueue task 8 by job id: 95be1d69-60ed-4654-bb9b-e3ab87e93931", status: "queued", context: None })
11/01 19:04:56.474190  INFO [actix-server worker 1] TID:0x111a9d7f [http] "POST /tasks?" status code: 201, body: Sized(1300)
11/01 19:04:57.050271  INFO [taosx] job notify: 95be1d69-60ed-4654-bb9b-e3ab87e93931 Scheduled
11/01 19:04:57.050868  INFO [taosx] task: 8 "Started task 8 by job id: 95be1d69-60ed-4654-bb9b-e3ab87e93931" "running"
11/01 19:04:57.050886  INFO [taosx] act=TaskActivity(Activity { id: 8, at: 2023-11-01T11:04:57.050720400Z, level: Info, activity: "Started task 8 by job id: 95be1d69-60ed-4654-bb9b-e3ab87e93931", status: "running", context: None })
11/01 19:04:57.050973  INFO [taosx] Scheduling task 8
11/01 19:04:57.051475  INFO [taosx] job notify: 95be1d69-60ed-4654-bb9b-e3ab87e93931 Started
11/01 19:04:57.051673  INFO [taosx] Starting task 8
11/01 19:04:57.155526  INFO [taosx] [run_task{task.id=8 task.jid=95be1d69-60ed-4654-bb9b-e3ab87e93931 task.rid=0 task.agent=1}] start worker
11/01 19:04:57.217791  INFO [taosx] agent activity activity=Activity { id: 1, at: 2023-11-01T11:04:57.164400300Z, level: Info, activity: "Start task 8", status: "transferring", context: Some(Context(Object {"agent": Number(1), "task": Number(8)})) }
11/01 19:04:57.218228  INFO [taosx] Agent activity: Activity { id: 1, at: 2023-11-01T11:04:57.164400300Z, level: Info, activity: "Start task 8", status: "transferring", context: Some(Context(Object {"agent": Number(1), "task": Number(8)})) }
11/01 19:04:57.218476  INFO [taosx] agent: 1 "Start task 8" "transferring"
11/01 19:04:57.218488  INFO [taosx] act=AgentActivity(Activity { id: 1, at: 2023-11-01T11:04:57.164400300Z, level: Info, activity: "Start task 8", status: "transferring", context: Some(Context(Object {"agent": Number(1), "task": Number(8)})) })
11/01 19:04:58.656590  INFO [taosx] handshake with client Some(127.0.0.1:56078)
11/01 19:04:58.668782  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] Put stream by task id 8
11/01 19:04:58.784360  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] receive batch 0x8bc500000002
11/01 19:04:59.104525  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] Start IPC stream writer
11/01 19:04:59.211512  WARN [taosx] DTID:8bc5 [put_stream{task.id=8}->process_record->consume_point_record{target_precision=Millisecond}] Insert point record error sql="insert into `aaa` (`received_time`,`original_time`,`value`,`quality`) VALUES (1698836698335,1698836698000,-19.86288,0)" error="[0x2603] Internal error: `Table does not exist`"
11/01 19:04:59.212148  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}->process_record->consume_point_record{target_precision=Millisecond}] create stable sql: create stable if not exists `stb_ua_double` (`received_time` TIMESTAMP,`original_time` TIMESTAMP,`quality` INT,`value` double) tags (`point_id` VARCHAR(256), `point_name` VARCHAR(256))
11/01 19:04:59.224284  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}->process_record->consume_point_record{target_precision=Millisecond}] create child sql: create table IF NOT EXISTS `aaa` USING `stb_ua_double` (`point_id`, `point_name`) TAGS ("ns=3;i=1001", "Constant")
11/01 19:04:59.339211  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] receive batch 0x8bc500000003
11/01 19:04:59.501223  INFO [actix-server worker 2] TID:0xe4b5a0f0 [http] 127.0.0.1 "GET /tasks/8? HTTP/1.1" python-requests/2.31.0
11/01 19:04:59.502828  INFO [actix-server worker 2] TID:0xe4b5a0f0 [http] "GET /tasks/8?" status code: 200, body: Sized(1119)
11/01 19:05:00.336420  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] receive batch 0x8bc500000004
11/01 19:05:01.324851  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] receive batch 0x8bc500000005
11/01 19:05:02.329690  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] receive batch 0x8bc500000006
11/01 19:05:03.324409  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] receive batch 0x8bc500000007
11/01 19:05:04.329541  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] receive batch 0x8bc500000008
11/01 19:05:05.334081  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] receive batch 0x8bc500000009
11/01 19:05:06.328880  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] receive batch 0x8bc50000000a
11/01 19:05:07.333957  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] receive batch 0x8bc50000000b
11/01 19:05:08.338925  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] receive batch 0x8bc50000000c
11/01 19:05:09.326870  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] receive batch 0x8bc50000000d
11/01 19:05:10.332358  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] receive batch 0x8bc50000000e
11/01 19:05:11.326024  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] receive batch 0x8bc50000000f
11/01 19:05:11.540791  INFO [actix-server worker 3] TID:0xda7aaf63 [http] 127.0.0.1 "POST /tasks/8/stop? HTTP/1.1" python-requests/2.31.0
11/01 19:05:11.541742  INFO [actix-server worker 3] TID:0xda7aaf63 [http->task::stop{task.id=8}] Stop task by id 8
11/01 19:05:11.542029  INFO [actix-server worker 3] TID:0xda7aaf63 [http->task::stop{task.id=8}] task `8` will be removed task.id=8 job.id=95be1d69-60ed-4654-bb9b-e3ab87e93931
11/01 19:05:11.542245  INFO [actix-server worker 3] TID:0xda7aaf63 [http->task::stop{task.id=8}] task `8` will be removed task.id=8 job.id=95be1d69-60ed-4654-bb9b-e3ab87e93931
11/01 19:05:11.542573  INFO [taosx] task: 8 "stop" "stopping"
11/01 19:05:11.542604  INFO [taosx] act=TaskActivity(Activity { id: 8, at: 2023-11-01T11:05:11.542508500Z, level: Info, activity: "stop", status: "stopping", context: None })
11/01 19:05:11.542851  INFO [taosx] job is deleted: Ok(95be1d69-60ed-4654-bb9b-e3ab87e93931)
11/01 19:05:11.542948  INFO [actix-server worker 3] TID:0xda7aaf63 [http->task::stop{task.id=8}] Cancel task 8
11/01 19:05:11.543038  INFO [taosx] task cancelled
11/01 19:05:11.543355  INFO [taosx] act=TaskActivity(Activity { id: 8, at: 2023-11-01T11:05:11.543314200Z, level: Info, activity: "Task has been stopped", status: "stopped", context: None })
11/01 19:05:11.543351  INFO [actix-server worker 3] TID:0xda7aaf63 [http->task::stop{task.id=8}->wait_task{task=8}] Waiting for task 8 to stop
11/01 19:05:11.543500  INFO [taosx] job is deleted: Ok(95be1d69-60ed-4654-bb9b-e3ab87e93931)
11/01 19:05:11.543533  INFO [taosx] task stopped
11/01 19:05:11.544118  INFO [taosx] job notify: 95be1d69-60ed-4654-bb9b-e3ab87e93931 Done
11/01 19:05:11.544314  INFO [taosx] Done task 8
11/01 19:05:11.547609  INFO [taosx] task: 8 "Task has been stopped" "stopped"
11/01 19:05:11.549324  INFO [taosx] DTID:8bc5 [put_stream{task.id=8}] IPC stream writer stopped
11/01 19:05:11.549693  WARN [taosx] DTID:8bc5 [put_stream{task.id=8}] IPC stream worker stopped, err:receiving on a closed channel
```

taosc 日志
```prolog
root@slave11 /var/log/taos $ grep 0x8bc50000000f taoslog0.0
11/01 19:05:11.443224 00106436 TSC 0x2d8c new Request from connObj:0x14b, current:1, app current:1, total:11659, reqId:0x8bc50000000fb904
11/01 19:05:11.443242 00106436 TSC 0x2d8c SQL: insert into `aaa` (`received_time`,`original_time`,`value`,`quality`) values (1698836711324,1698836711000,70.37771,0), reqId:0x8bc50000000fb904
11/01 19:05:11.443418 00106436 QRY PARSER: 0x8bc50000000fb904 1 rows of 1 tables have been inserted
11/01 19:05:11.443452 00106436 QRY QID:0x8bc50000000fb904 input exec nodeList is empty
11/01 19:05:11.443457 00106436 QRY QID:0x8bc50000000fb904 set job levelIdx to 0
11/01 19:05:11.443465 00106436 QRY QID:0x8bc50000000fb904,TID:0xd2d,EID:-1 task initialized, max times 6:6
11/01 19:05:11.443470 00106436 QRY QID:0x8bc50000000fb904 level 0 initialized, taskNum:1
11/01 19:05:11.443473 00106436 QRY QID:0x8bc50000000fb904,TID:0xd2d,EID:-1 level:0, parentNum:0, childNum:0
11/01 19:05:11.443483 00106436 QRY QID:0x8bc50000000fb904 job refId:0xcec created
11/01 19:05:11.443493 00106436 QRY QID:0x8bc50000000fb904 job start EXEC operation
11/01 19:05:11.443506 00106436 QRY QID:0x8bc50000000fb904 job status updated from NULL to INIT
11/01 19:05:11.443509 00106436 QRY QID:0x8bc50000000fb904 job status updated from INIT to EXECUTING
11/01 19:05:11.443512 00106436 QRY QID:0x8bc50000000fb904 sch job refId 0xcec started
11/01 19:05:11.443515 00106436 QRY QID:0x8bc50000000fb904 job no need flow ctrl, queryJob:0
11/01 19:05:11.443525 00106436 QRY QID:0x8bc50000000fb904,TID:0xd2d,EID:0 start to launch REMOTE task, execId 0, retry 1
11/01 19:05:11.443536 00106436 QRY QID:0x8bc50000000fb904,TID:0xd2d,EID:0 task added to execTask list, numOfTasks:1
```

taosd 日志
```prolog
root@slave11 /var/log/taos $ grep 0x8bc50000000f taosdlog.0
11/01 19:05:11.447728 00001030 RPC DND-S conn 0x7f624801a180 submit received from 192.168.0.31:38350, local info:192.168.0.31:6030, len:167, cost:102us, gtid:0x8bc50000000fb904:0x73f138a8f0868f68
11/01 19:05:11.447741 00001030 RPC DND-S handle 0x7f6248016bd0 conn:0x7f624801a180 translated to app, refId:1106503, gtid:0x8bc50000000fb904:0x73f138a8f0868f68
11/01 19:05:11.447752 00001030 DND msg:submit is received, handle:0x7f6248016bd0 len:88 code:0x0 app:0x7fd5dc022c80 refId:1106503, gtid:0x8bc50000000fb904:0x73f138a8f0868f68
11/01 19:05:11.447764 00001030 DND msg:0x7f6248028558, is created, type:submit handle:0x7f6248016bd0 len:88, gtid:0x8bc50000000fb904:0x73f138a8f0868f68
11/01 19:05:11.447770 00001030 DND msg:0x7f6248028558, will be processed by vnode, gtid:0x8bc50000000fb904:0x73f138a8f0868f68
11/01 19:05:11.447781 00001030 DND vgId:134, msg:0x7f6248028558 put into vnode-write queue, gtid:0x8bc50000000fb904:0x73f138a8f0868f68
11/01 19:05:11.447856 00397995 VND vgId:134, msg:0x7f6248028558 get from vnode-write queue, weak:0 block:0 msg:0:1, handle:0x7f6248016bd0, gtid:0x8bc50000000fb904:0x73f138a8f0868f68
11/01 19:05:11.448102 00397995 RPC conn 0x7f624801a180 start to send resp (1/2), gtid:0x8bc50000000fb904:0x73f138a8f0868f68
11/01 19:05:11.448134 00397995 VND vgId:134, msg:0x7f6248028558 is freed, code:0x1, gtid:0x8bc50000000fb904:0x73f138a8f0868f68
11/01 19:05:11.448174 00001030 RPC DND-S conn 0x7f624801a180 submit-rsp is sent to 192.168.0.31:38350, local info:192.168.0.31:6030, len:85, gtid:0x8bc50000000fb904:0x73f138a8f0868f68
11/01 19:05:11.448249 00001030 RPC conn 0x7f624801a180 write data out, gtid:0x8bc50000000fb904:0x73f138a8f0868f68
```

### 4.2 不需要 agent 的任务

以 mqtt 任务为例， server 日志如下
```prolog
11/01 19:36:23.819988  INFO [actix-server worker 0] TID:0xc0ba38aa [http] 127.0.0.1 "GET /tasks? HTTP/1.1" python-requests/2.31.0
11/01 19:36:23.820702  INFO [actix-server worker 0] TID:0xc0ba38aa [http] list tasks
11/01 19:36:23.823076  INFO [actix-server worker 0] TID:0xc0ba38aa [http] "GET /tasks?" status code: 200, body: Sized(2)
11/01 19:36:26.179034  INFO [actix-server worker 1] TID:0x8472b64d [http] 127.0.0.1 "POST /tasks? HTTP/1.1" python-requests/2.31.0
11/01 19:36:26.180184  INFO [actix-server worker 1] TID:0x8472b64d [http->task::create] create new task
11/01 19:36:26.533951  INFO [actix-server worker 1] TID:0x8472b64d [http->task::create] list tasks
11/01 19:36:26.634920  INFO [actix-server worker 1] TID:0x8472b64d [http->task::create->start_task{task.id=12}] job created task.id=12 job.id=722bf6d2-43fb-480b-8b19-6c4e28c6eca2
11/01 19:36:26.635875  INFO [actix-server worker 1] TID:0x8472b64d [http->task::create->start_task{task.id=12}] Job creator created
11/01 19:36:26.636054  INFO [taosx] task: 12 "Enqueue task 12 by job id: 722bf6d2-43fb-480b-8b19-6c4e28c6eca2" "queued"
11/01 19:36:26.636046  INFO [taosx] act=TaskActivity(Activity { id: 12, at: 2023-11-01T11:36:26.635943300Z, level: Info, activity: "Enqueue task 12 by job id: 722bf6d2-43fb-480b-8b19-6c4e28c6eca2", status: "queued", context: None })
11/01 19:36:26.636358  INFO [actix-server worker 1] TID:0x8472b64d [http] "POST /tasks?" status code: 201, body: Sized(2470)
11/01 19:36:27.231154  INFO [taosx] job notify: 722bf6d2-43fb-480b-8b19-6c4e28c6eca2 Scheduled
11/01 19:36:27.231526  INFO [taosx] task: 12 "Started task 12 by job id: 722bf6d2-43fb-480b-8b19-6c4e28c6eca2" "running"
11/01 19:36:27.231541  INFO [taosx] act=TaskActivity(Activity { id: 12, at: 2023-11-01T11:36:27.231382200Z, level: Info, activity: "Started task 12 by job id: 722bf6d2-43fb-480b-8b19-6c4e28c6eca2", status: "running", context: None })
11/01 19:36:27.231727  INFO [taosx] Scheduling task 12
11/01 19:36:27.232276  INFO [taosx] job notify: 722bf6d2-43fb-480b-8b19-6c4e28c6eca2 Started
11/01 19:36:27.232608  INFO [taosx] Starting task 12
11/01 19:36:27.366376  INFO [taosx] [run_task{task.id=12 task.jid=722bf6d2-43fb-480b-8b19-6c4e28c6eca2 task.rid=0}] start worker
11/01 19:36:27.430791  INFO [taosx] [run_task{task.id=12 task.jid=722bf6d2-43fb-480b-8b19-6c4e28c6eca2 task.rid=0}->run_task] Using mqtt config file C:\Users\dingb\AppData\Local\Temp\.tmp1I3HW0 
log_level = "info"
remote = "127.0.0.1:6051"

[mqtt]
address = "tcp://192.168.1.42:1883"
version = "3.1"
client_id = "client575"
keep_alive = 60
clean_session = true

[topics]
"testmqtt/1" = 2

11/01 19:36:27.431906  INFO [taosx] [task::spawned{task.id=12}->listen_tcp_socket] listen on socket address: 127.0.0.1:6051
11/01 19:36:27.432350  INFO [taosx] [task::spawned{task.id=12}->listen_tcp_socket->plain_ipc_listener] waiting for IPC connections
11/01 19:36:27.432380  INFO [taosx] [task::spawned{task.id=12}->listen_tcp_socket->plain_ipc_listener_abort_handle] stop listener
11/01 19:36:27.432526  INFO [taosx] [run_task{task.id=12 task.jid=722bf6d2-43fb-480b-8b19-6c4e28c6eca2 task.rid=0}->run_task] log path created: C:\TDengine\log\mqtt
11/01 19:36:27.432955  INFO [taosx] [run_task{task.id=12 task.jid=722bf6d2-43fb-480b-8b19-6c4e28c6eca2 task.rid=0}->run_task] log file dir: C:\TDengine\log\mqtt\mqtt.log
11/01 19:36:27.457542  INFO [taosx] [task::spawned{task.id=12}->listen_tcp_socket->plain_ipc_listener] new tcp client!: 127.0.0.1:58187
11/01 19:36:27.457954  INFO [taosx] [task::spawned{task.id=12}] Spawned IPC reader
11/01 19:36:27.458262  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read] Prepare IPC stream reader client="127.0.0.1:58187"
11/01 19:36:27.483124  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read] Prepare IPC ACK writer client="127.0.0.1:58187"
11/01 19:36:27.484272  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read] Processing IPC stream client="127.0.0.1:58187"
11/01 19:36:27.484855  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process] IPC stream processing... client="127.0.0.1:58187"
11/01 19:36:27.698041  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process] Processing stream stream_type=Flat
11/01 19:36:27.699460  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000001
11/01 19:36:27.969137  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000002
11/01 19:36:28.173470  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000003
11/01 19:36:28.602269  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000004
11/01 19:36:28.857418  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000005
11/01 19:36:29.107333  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000006
11/01 19:36:29.311295  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000007
11/01 19:36:29.505047  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000008
11/01 19:36:29.654622  INFO [actix-server worker 2] TID:0x37c8178b [http] 127.0.0.1 "GET /tasks/12? HTTP/1.1" python-requests/2.31.0
11/01 19:36:29.656655  INFO [actix-server worker 2] TID:0x37c8178b [http] "GET /tasks/12?" status code: 200, body: Sized(2470)
11/01 19:36:29.714222  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000009
11/01 19:36:29.912256  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000000a
11/01 19:36:30.109115  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000000b
11/01 19:36:30.256444  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000000c
11/01 19:36:30.467503  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000000d
11/01 19:36:30.662484  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000000e
11/01 19:36:30.892348  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000000f
11/01 19:36:31.083437  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000010
11/01 19:36:31.235049  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000011
11/01 19:36:31.464261  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000012
11/01 19:36:31.705992  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000013
11/01 19:36:31.935909  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000014
11/01 19:36:32.426719  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000015
11/01 19:36:32.675940  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000016
11/01 19:36:32.914904  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000017
11/01 19:36:33.168311  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000018
11/01 19:36:33.355441  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000019
11/01 19:36:33.569213  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000001a
11/01 19:36:33.799043  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000001b
11/01 19:36:34.255621  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000001c
11/01 19:36:34.514404  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000001d
11/01 19:36:34.736413  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000001e
11/01 19:36:34.931575  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000001f
11/01 19:36:35.136466  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000020
11/01 19:36:35.372478  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000021
11/01 19:36:35.622221  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000022
11/01 19:36:35.869166  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000023
11/01 19:36:36.022420  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000024
11/01 19:36:36.230797  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000025
11/01 19:36:36.432947  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000026
11/01 19:36:36.630824  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000027
11/01 19:36:36.852673  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000028
11/01 19:36:37.288368  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000029
11/01 19:36:37.491687  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000002a
11/01 19:36:37.700487  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000002b
11/01 19:36:38.168074  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000002c
11/01 19:36:38.410930  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000002d
11/01 19:36:38.870872  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000002e
11/01 19:36:39.139069  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000002f
11/01 19:36:39.355563  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000030
11/01 19:36:39.556869  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000031
11/01 19:36:39.778945  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000032
11/01 19:36:40.250515  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000033
11/01 19:36:40.714905  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000034
11/01 19:36:40.914079  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000035
11/01 19:36:41.062596  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000036
11/01 19:36:41.526280  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000037
11/01 19:36:41.706066  INFO [actix-server worker 3] TID:0x770da981 [http] 127.0.0.1 "POST /tasks/12/stop? HTTP/1.1" python-requests/2.31.0
11/01 19:36:41.707158  INFO [actix-server worker 3] TID:0x770da981 [http->task::stop{task.id=12}] Stop task by id 12
11/01 19:36:41.707550  INFO [actix-server worker 3] TID:0x770da981 [http->task::stop{task.id=12}] task `12` will be removed task.id=12 job.id=722bf6d2-43fb-480b-8b19-6c4e28c6eca2
11/01 19:36:41.707951  INFO [actix-server worker 3] TID:0x770da981 [http->task::stop{task.id=12}] task `12` will be removed task.id=12 job.id=722bf6d2-43fb-480b-8b19-6c4e28c6eca2
11/01 19:36:41.708327  INFO [taosx] task: 12 "stop" "stopping"
11/01 19:36:41.708328  INFO [taosx] act=TaskActivity(Activity { id: 12, at: 2023-11-01T11:36:41.708217800Z, level: Info, activity: "stop", status: "stopping", context: None })
11/01 19:36:41.708562  INFO [taosx] job is deleted: Ok(722bf6d2-43fb-480b-8b19-6c4e28c6eca2)
11/01 19:36:41.708600  INFO [actix-server worker 3] TID:0x770da981 [http->task::stop{task.id=12}] Cancel task 12
11/01 19:36:41.708779  INFO [taosx] task cancelled
11/01 19:36:41.708824  INFO [taosx] mqtt task cancelled
11/01 19:36:41.709341  INFO [taosx] act=TaskActivity(Activity { id: 12, at: 2023-11-01T11:36:41.709279700Z, level: Info, activity: "Task has been stopped", status: "stopped", context: None })
11/01 19:36:41.709344  INFO [actix-server worker 3] TID:0x770da981 [http->task::stop{task.id=12}->wait_task{task=12}] Waiting for task 12 to stop
11/01 19:36:41.709451  INFO [taosx] job is deleted: Ok(722bf6d2-43fb-480b-8b19-6c4e28c6eca2)
11/01 19:36:41.709478  INFO [taosx] task stopped
11/01 19:36:41.710208  INFO [taosx] job notify: 722bf6d2-43fb-480b-8b19-6c4e28c6eca2 Done
11/01 19:36:41.710431  INFO [taosx] Done task 12
11/01 19:36:41.713027  INFO [taosx] mqtt to taos task done
11/01 19:36:41.713355  INFO [taosx] [task::spawned{task.id=12}->listen_tcp_socket->plain_ipc_listener] IPC stream listener would wait for handlers to finish ipc.handlers=1
11/01 19:36:41.713601  INFO [taosx] task: 12 "Task has been stopped" "stopped"
11/01 19:36:41.767244  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000038
11/01 19:36:41.964178  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d00000039
11/01 19:36:42.113760  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000003a
11/01 19:36:42.308090  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000003b
11/01 19:36:42.519099  INFO [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Start writing batch 0x4f8d0000003c
11/01 19:36:42.713639  INFO [actix-server worker 3] TID:0x770da981 [http->task::stop{task.id=12}->wait_task{task=12}] task has been completely stopped task.id=12
11/01 19:36:42.714228  INFO [actix-server worker 3] TID:0x770da981 [http->task::stop{task.id=12}] task 12 successfully stopped
11/01 19:36:42.714965  INFO [actix-server worker 3] TID:0x770da981 [http] "POST /tasks/12/stop?" status code: 200, body: Sized(2)
11/01 19:36:42.753168 ERROR [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] next message error, Io error: An existing connection was forcibly closed by the remote host. (os error 10054)
11/01 19:36:42.753690  WARN [taosx] DTID:4f8d [task::spawned{task.id=12}->ipc_tcp_read->ipc_process->ipc_flat_stream_reader] Receive IPC item error: Io error: An existing connection was forcibly closed by the remote host. (os error 10054)
11/01 19:36:42.754296 ERROR [taosx] [task::spawned{task.id=12}] ipc read err: Io error: An existing connection was forcibly closed by the remote host. (os error 10054)
11/01 19:36:42.754671  INFO [taosx] [task::spawned{task.id=12}->listen_tcp_socket->plain_ipc_listener] IPC stream handlers finished after 1.040968s
```
