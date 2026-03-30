# 监控指标导出工具-Function Spec

## 1. 修订记录

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/01/03 | 1.0 | 郭振伟 | 编写文档。 |
| 2026/01/07 | 1.1 | 佘彦杰 | 根据需求修改 |

## 2. 背景

taosKeeper 是 TDengine 3.0 版本中新增的监控指标导出工具，旨在方便用户对 TDengine 的运行状态和性能指标进行实时监控。通过简单的配置，TDengine 能够将其运行状态、指标等信息上报给 taosKeeper。当接收到监控数据后，taosKeeper 会利用 taosAdapter 提供的 RESTful 接口，将这些数据存储到 TDengine 中。

## 3. 定义

1. **dnode**：数据节点。dnode 是 TDengine 服务器侧执行代码 taosd 在物理节点上的一个运行实例。
2. **vnode**：虚拟节点。vnode 是 TDengine 中数据存储、查询以及备份的基本单元。
3. **mnode**：管理节点。mnode 是 TDengine 集群中的核心逻辑单元，负责监控和维护所有 dnode 的运行状态，并在节点之间实现负载均衡。
4. **qnode**：计算节点。qnode 是 TDengine 集群中负责执行查询计算任务的虚拟逻辑单元，同时也处理基于系统表的 show 命令。
5. **snode**：流计算节点。snode 是 TDengine 集群中专门负责处理流计算任务的虚拟逻辑单元。
6. **vgroup**：虚拟节点组。vgroup 是由不同 dnode 上的 vnode 组成的一个逻辑单元。

## 4. 行为说明

### 4.1 监控

#### 4.1.1 状态监控

taosd、taosAdapter 和 taosX 会通过 HTTP 接口定期上报自身的运行状态与性能指标。

#### 4.1.2 系统指标采集

taosKeeper 会定时采集本进程的 CPU 使用率以及内存使用情况。

#### 4.1.3 慢查询记录

taosd 会通过 HTTP 接口上报慢查询记录。

### 4.2 存储

通过 taosAdapter 将 taos、taosAdapter 和 taosX 上报的监控数据存入 TDengine 数据库。

### 4.3 接口

#### 4.3.1 adapter_report 接口

##### 4.3.1.1 请求说明

1. 请求方法：POST
2. 请求 URL：`/adapter_report`
3. 请求头：
  - `Content-Type: application/json`
1. 请求参数：
  请求体中需提供一个 AdapterReport 结构体实例，格式为 JSON。
  AdapterReport 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| ts | int64 | 否 | 时间戳 |
| metrics | AdapterMetrics | 否 | taosAdapter 指标信息 |
| endpoint | string | 否 | taosAdapter 端点 |

  
  AdapterMetrics 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| rest_total | int | 否 | REST 请求数量 |
| rest_query | int | 否 | REST 查询请求数量 |
| rest_write | int | 否 | REST 写入请求数量 |
| rest_other | int | 否 | REST 其它请求数量 |
| rest_in_process | int | 否 | REST 执行中数量 |
| rest_success | int | 否 | REST 请求成功数量 |
| rest_fail | int | 否 | REST 请求失败数量 |
| rest_query_success | int | 否 | REST 查询成功数量 |
| rest_query_fail | int | 否 | REST 查询失败数量 |
| rest_write_success | int | 否 | REST 写入成功数量 |
| rest_write_fail | int | 否 | REST 写入失败数量 |
| rest_other_success | int | 否 | REST 其它请求成功数量 |
| rest_other_fail | int | 否 | REST 其它请求失败数量 |
| rest_query_in_process | int | 否 | REST 执行中查询数量 |
| rest_write_in_process | int | 否 | REST 执行中写入数量 |
| ws_total | int | 否 | WebSocket 请求数量 |
| ws_query | int | 否 | WebSocket 查询请求数量 |
| ws_write | int | 否 | WebSocket 写入请求数量 |
| ws_other | int | 否 | WebSocket 其它请求数量 |
| ws_in_process | int | 否 | WebSocket 执行中数量 |
| ws_success | int | 否 | WebSocket 请求成功数量 |
| ws_fail | int | 否 | WebSocket 请求失败数量 |
| ws_query_success | int | 否 | WebSocket 查询成功数量 |
| ws_query_fail | int | 否 | WebSocket 查询失败数量 |
| ws_write_success | int | 否 | WebSocket 写入成功数量 |
| ws_write_fail | int | 否 | WebSocket 写入失败数量 |
| ws_other_success | int | 否 | WebSocket 其它请求成功数量 |
| ws_other_fail | int | 否 | WebSocket 其它请求失败数量 |
| ws_query_in_process | int | 否 | WebSocket 执行中查询数量 |
| ws_write_in_process | int | 否 | WebSocket 执行中写入数量 |

1. 请求示例：
  ```json
  {
      "ts": 1735882890,
      "metrics": {
          "rest_fail": 0,
          "rest_in_process": 0,
          "rest_other": 10,
          "rest_other_fail": 0,
          "rest_other_success": 10,
          "rest_query": 9,
          "rest_query_fail": 0,
          "rest_query_in_process": 0,
          "rest_query_success": 9,
          "rest_success": 22,
          "rest_total": 22,
          "rest_write": 3,
          "rest_write_fail": 0,
          "rest_write_in_process": 0,
          "rest_write_success": 3,
          "ws_fail": 0,
          "ws_in_process": 0,
          "ws_other": 0,
          "ws_other_fail": 0,
          "ws_other_success": 0,
          "ws_query": 0,
          "ws_query_fail": 0,
          "ws_query_in_process": 0,
          "ws_query_success": 0,
          "ws_success": 0,
          "ws_total": 0,
          "ws_write": 0,
          "ws_write_fail": 0,
          "ws_write_in_process": 0,
          "ws_write_success": 0
      },
      "endpoint": "localhost:6041"
  }
  ```

##### 4.3.1.2 响应说明

1. 成功响应：

  | HTTP 状态码 | 描述 | 响应示例（JSON） |
| --- | --- | --- |
| 200 | 成功 | {} |

1. 错误响应： 

  | HTTP 状态码 | 描述 | 响应示例（JSON） |
| --- | --- | --- |
| 400 | 客户端错误 | {"error":"get adapter report data error. xxx"} |
| 500 | 服务器内部错误 | {"error":"no connection"} |

#### 4.3.2 taosd-cluster-basic 接口

##### 4.3.2.1 请求说明

1. 请求方法：POST
2. 请求 URL：`/taosd-cluster-basic`
3. 请求头：
  - `Content-Type: application/json`
1. 请求参数：
  请求体中需提供一个 ClusterBasic 结构体实例，格式为 JSON。
  ClusterBasic 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| ts | string | 否 | 时间戳 |
| cluster_id | string | 否 | 集群 ID |
| first_ep | string | 否 | 第一个端点 |
| first_ep_dnode_id | int32 | 否 | 第一个端点的 dnode ID |
| cluster_version | string | 否 | 集群版本 |

1. 请求示例：
  ```json
  {
      "ts": "1735898577527",
      "dnode_id": 1,
      "dnode_ep": "dev:6030",
      "cluster_id": "3941541534719793555",
      "protocol": 1,
      "first_ep": "dev:6030",
      "first_ep_dnode_id": 1,
      "cluster_version": "3.3.5.0.alp"
  }
  ```

##### 4.3.2.2 响应说明

1. 成功响应：

  | HTTP 状态码 | 描述 | 响应示例（JSON） |
| --- | --- | --- |
| 200 | 成功 | {} |

1. 错误响应： 

  | HTTP 状态码 | 描述 | 响应示例（JSON） |
| --- | --- | --- |
| 400 | 客户端错误 | {"error":"insert taosd_cluster_basic error. xxx"} |
| 500 | 服务器内部错误 | {"error":"no connection"} |

#### 4.3.3 audit 接口

##### 4.3.3.1 请求说明

1. 请求方法：POST
2. 请求 URL：`/audit`
3. 请求头：
  - `Content-Type: application/json`
1. 请求参数：
  请求体中需提供一个 AuditInfoOld 结构体实例，格式为 JSON。
  AuditInfoOld 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| timestamp | int64 | 否 | 时间戳 |
| cluster_id | string | 否 | 集群 ID |
| user | string | 否 | 用户名 |
| operation | string | 否 | 操作类型 |
| db | string | 否 | 数据库名称 |
| resource | string | 否 | 访问资源 |
| client_add | string | 否 | 客户端地址 |
| details | string | 否 | 操作详情 |

1. 请求示例：
  ```json
  {
      "timestamp": 1736127045013,
      "cluster_id": "3941541534719793555",
      "user": "root",
      "operation": "login",
      "client_add": "127.0.0.1:58040",
      "db": "",
      "resource": "",
      "details": "app:taosadapter"
  }
  ```

##### 4.3.3.2 响应说明

1. 成功响应：

  | HTTP 状态码 | 描述 | 响应示例（JSON） |
| --- | --- | --- |
| 200 | 成功 | {} |

1. 错误响应： 

  | HTTP 状态码 | 描述 | 响应示例（JSON） |
| --- | --- | --- |
| 400 | 客户端错误 | {"error":"get audit data error. xxx"} |
| 500 | 服务器内部错误 | {"error":"no connection"} |

#### 4.3.4 audit_v2 接口

##### 4.3.4.1 请求说明

1. 请求方法：POST
2. 请求 URL：`/audit_v2`
3. 请求头：
  - `Content-Type: application/json`
1. 请求参数：
  请求体中需提供一个 AuditInfo 或者 AuditInfoOld 结构体实例，格式为 JSON。
  AuditInfo 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| timestamp | string | 否 | 时间戳 |
| cluster_id | string | 否 | 集群 ID |
| user | string | 否 | 用户名 |
| operation | string | 否 | 操作类型 |
| db | string | 否 | 数据库名称 |
| resource | string | 否 | 访问资源 |
| client_add | string | 否 | 客户端地址 |
| details | string | 否 | 操作详情 |

  
  AuditInfoOld 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| timestamp | int64 | 否 | 时间戳 |
| cluster_id | string | 否 | 集群 ID |
| user | string | 否 | 用户名 |
| operation | string | 否 | 操作类型 |
| db | string | 否 | 数据库名称 |
| resource | string | 否 | 访问资源 |
| client_add | string | 否 | 客户端地址 |
| details | string | 否 | 操作详情 |

1. 请求示例：
   - AuditInfo 结构体实例的 JSON 格式：
    ```json
    {
        "timestamp": "1736127045013418695",
        "cluster_id": "3941541534719793555",
        "user": "root",
        "operation": "login",
        "client_add": "127.0.0.1:58040",
        "db": "",
        "resource": "",
        "details": "app:taosadapter"
    }
    ```

   - AuditInfoOld 结构体实例的 JSON 格式：
    ```json
    {
        "timestamp": 1736127045013,
        "cluster_id": "3941541534719793555",
        "user": "root",
        "operation": "login",
        "client_add": "127.0.0.1:58040",
        "db": "",
        "resource": "",
        "details": "app:taosadapter"
    }
    ```

##### 4.3.4.2 响应说明

1. 成功响应：

  | HTTP 状态码 | 描述 | 响应示例（JSON） |
| --- | --- | --- |
| 200 | 成功 | {} |

1. 错误响应： 

  | HTTP 状态码 | 描述 | 响应示例（JSON） |
| --- | --- | --- |
| 400 | 客户端错误 | {"error":"timestamp format error. xxx"} |
| 500 | 服务器内部错误 | {"error":"no connection"} |

#### 4.3.5 audit-batch 接口

##### 4.3.5.1 请求说明

1. 请求方法：POST
2. 请求 URL：`/audit-batch`
3. 请求头：
  - `Content-Type: application/json`
1. 请求参数：
  请求体中需提供一个 AuditArrayInfo 结构体实例，格式为 JSON。
  AuditArrayInfo 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| records | []AuditInfo | 否 | 审批记录数组 |

  
  AuditInfo 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| timestamp | string | 否 | 时间戳 |
| cluster_id | string | 否 | 集群 ID |
| user | string | 否 | 用户名 |
| operation | string | 否 | 操作类型 |
| db | string | 否 | 数据库名称 |
| resource | string | 否 | 访问资源 |
| client_add | string | 否 | 客户端地址 |
| details | string | 否 | 操作详情 |

1. 请求示例：
  ```json
  [
      {
          "timestamp": "1736127045013418695",
          "cluster_id": "3941541534719793555",
          "user": "root",
          "operation": "login",
          "client_add": "127.0.0.1:58040",
          "db": "",
          "resource": "",
          "details": "app:taosadapter"
      }
  ]
  ```

##### 4.3.5.2 响应说明

1. 成功响应：

  | HTTP 状态码 | 描述 | 响应示例（JSON） |
| --- | --- | --- |
| 200 | 成功 | {} |

1. 错误响应： 

  | HTTP 状态码 | 描述 | 响应示例（JSON） |
| --- | --- | --- |
| 400 | 客户端错误 | {"error":"process records error. xxx"} |
| 500 | 服务器内部错误 | {"error":"no connection"} |

#### 4.3.6 report 接口

##### 4.3.6.1 请求说明

1. 请求方法：POST
2. 请求 URL：`/report`
3. 请求头：
  - Content-Type: application/json
1. 请求参数：
  请求体中需提供一个 Report 结构体实例，格式为 JSON。
  Report 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| ts | string | 否 | 时间戳 |
| dnode_id | int | 否 | dnode ID |
| dnode_ep | string | 否 | dnode ep |
| cluster_id | string | 否 | 集群 ID |
| protocol | int | 否 | 协议类型 |
| cluster_info | *ClusterInfo | 否 | 集群信息，仅由主节点报告。 |
| stb_infos | []StbInfo | 否 | 超级表信息数组 |
| vgroup_infos | []VgroupInfo | 否 | vgroup 信息数组，仅由主节点报告。 |
| grant_info | *GrantInfo | 否 | 授权信息，仅由主节点报告。 |
| dnode_info | DnodeInfo | 否 | dnode 信息 |
| disk_infos | DiskInfo | 否 | 磁盘信息 |
| log_infos | LogInfo | 否 | 日志信息 |

  
  ClusterInfo 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| first_ep | string | 否 | 集群 first ep |
| first_ep_dnode_id | int | 否 | 集群 first ep 的 dnode ID |
| version | string | 否 | TDengine 版本 |
| master_uptime | float32 | 否 | 主节点运行时间 |
| monitor_interval | int | 否 | 监控间隔时间 |
| dbs_total | int | 否 | 数据库总数 |
| tbs_total | int64 | 否 | 表总数 |
| stbs_total | int | 否 | 超级表总数 |
| vgroups_total | int | 否 | 虚拟组总数 |
| vgroups_alive | int | 否 | 存活的虚拟组数量 |
| vnodes_total | int | 否 | 虚拟节点总数 |
| vnodes_alive | int | 否 | 存活的虚拟节点数量 |
| connections_total | int | 否 | 总连接数 |
| topics_total | int | 否 | 主题总数 |
| streams_total | int | 否 | 流总数 |
| dnodes | []Dnode | 否 | 数据节点数组 |
| mnodes | []Mnode | 否 | 管理节点数组 |

  
  Dnode 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| dnode_id | int | 否 | dnode ID |
| dnode_ep | string | 否 | dnode ep |
| status | string | 否 | dnode 状态 |

  
  Mnode 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| mnode_id | int | 否 | mnode ID |
| mnode_ep | string | 否 | mnode ep |
| role | string | 否 | mnode 角色 |

  
  StbInfo 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| stb_name | string | 否 | 超级表名称 |
| database_name | string | 否 | 数据库名称 |

  
  VgroupInfo 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| vgroup_id | int | 否 | vgroup ID |
| database_name | string | 否 | 数据库名称 |
| tables_num | int64 | 否 | vgroup 表数量 |
| status | string | 否 | vgroup 状态 |
| vnodes | []Vnode | 否 | vnode 数组 |

  
  Vnode 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| dnode_id | int | 否 | dnode ID |
| vnode_role | string | 否 | vnode 角色 |

  
  GrantInfo 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| expire_time | int64 | 否 | 集群授权过期剩余时间（单位 秒） |
| timeseries_used | int64 | 否 | 集群已拥有的 time series 的数量 |
| timeseries_total | int64 | 否 | 集群授权允许使用 time series 的总数量 |

  
  DnodeInfo 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| uptime | float32 | 否 | dnode 的启动时间（单位 秒） |
| cpu_engine | float32 | 否 | dnode 的进程所使用的 CPU 百分比（取值范围 0~100） |
| cpu_system | float32 | 否 | dnode 所在节点的系统使用的 CPU 百分比（取值范围 0~100） |
| cpu_cores | float32 | 否 | CPU 核心数 |
| mem_engine | int | 否 | dnode 的进程所使用的内存（单位 KB） |
| mem_system | int | 否 | dnode 所在节点的系统所使用的内存（单位 KB） |
| mem_total | int | 否 | dnode 所在节点的总内存（单位 KB） |
| disk_engine | int64 | 否 | dnode 的进程使用的磁盘容量（单位 Byte） |
| disk_used | int64 | 否 | dnode 所在节点的磁盘已使用的容量（单位 Byte） |
| disk_total | int64 | 否 | dnode 所在节点的磁盘总容量（单位 Byte） |
| net_in | float32 | 否 | dnode 所在节点的网络传入速率（单位 Byte/s） |
| net_out | float32 | 否 | dnode 所在节点的网络传出速率（单位 Byte/s） |
| io_read | float32 | 否 | dnode 所在节点的 io 读取速率（单位 Byte/s） |
| io_write | float32 | 否 | dnode 所在节点的 io 写入速率（单位 Byte/s） |
| io_read_disk | float32 | 否 | dnode 所在节点的磁盘 io 写入速率（单位 Byte/s） |
| io_write_disk | float32 | 否 | dnode 所在节点的磁盘 io 写入速率（单位 Byte/s） |
| req_select | int | 否 | 查询请求总数 |
| req_select_rate | float32 | 否 | 查询请求成功率 |
| req_insert | int | 否 | 插入请求总数 |
| req_insert_success | int | 否 | 插入请求成功数 |
| req_insert_rate | float32 | 否 | 插入请求成功率 |
| req_insert_batch | int | 否 | 批量插入请求总数 |
| req_insert_batch_success | int | 否 | 批量插入请求成功数 |
| req_insert_batch_rate | float32 | 否 | 批量插入请求成功率 |
| errors | int | 否 | 错误总数 |
| vnodes_num | int | 否 | dnode 所在节点的 vnode 数量 |
| masters | int | 否 | 主节点数量 |
| has_mnode | int8 | 否 | 是否有 mnode |
| has_qnode | int8 | 否 | 是否有 qnode |
| has_snode | int8 | 否 | 是否有 snode |
| has_bnode | int8 | 否 | 是否有 bnode |

  
  DiskInfo 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| datadir | []DataDir | 否 | 数据目录数组 |
| logdir | LogDir | 否 | 日志目录 |
| tempdir | TempDir | 否 | 临时目录 |

  
  DataDir 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| name | string | 否 | 数据目录名称 |
| level | int | 否 | 数据目录级别 |
| avail | Decimal | 否 | 数据目录可用空间 |
| used | Decimal | 否 | 数据目录已用空间 |
| total | Decimal | 否 | 数据目录总空间 |

  
  LogDir 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| name | string | 否 | 日志目录名称 |
| avail | Decimal | 否 | 日志目录可用空间 |
| used | Decimal | 否 | 日志目录已用空间 |
| total | Decimal | 否 | 日志目录总空间 |

  
  TempDir 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| name | string | 否 | 临时目录名称 |
| avail | Decimal | 否 | 临时目录可用空间 |
| used | Decimal | 否 | 临时目录已用空间 |
| total | Decimal | 否 | 临时目录总空间 |

  
  LogInfo 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| summary | []Summary | 否 | 日志级别汇总信息数组 |

  
  Summary 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| level | string | 否 | 日志级别 |
| total | int | 否 | 对应级别的日志总数 |

1. 请求示例：
  ```json
  {
      "ts": "1735898577527",
      "dnode_id": 1,
      "dnode_ep": "dev:6030",
      "cluster_id": "3941541534719793555",
      "protocol": 1,
      "cluster_info": {
          "first_ep": "dev:6030",
          "first_ep_dnode_id": 1,
          "version": "3.3.5.0",
          "master_uptime": 1,
          "monitor_interval": 30,
          "dbs_total": 1,
          "tbs_total": 16,
          "stbs_total": 14,
          "vgroups_total": 1,
          "vgroups_alive": 1,
          "vnodes_total": 1,
          "vnodes_alive": 1,
          "connections_total": 15,
          "topics_total": 0,
          "streams_total": 0,
          "dnodes": [
              {
                  "dnode_id": 1,
                  "dnode_ep": "dev:6030",
                  "status": "ready"
              }
          ],
          "mnodes": [
              {
                  "mnode_id": 1,
                  "mnode_ep": "dev:6030",
                  "role": "leader"
              }
          ]
      },
      "stb_infos": [
          {
              "stb_name": "meters",
              "database_name": "power"
          }
      ],
      "vgroup_infos": [
          {
              "vgroup_id": 6,
              "database_name": "power",
              "tables_num": 1,
              "status": "leader",
              "vnodes": [
                  {
                      "dnode_id": 1,
                      "vnode_role": "leader"
                  }
              ]
          }
      ],
      "grant_info": {
          "expire_time": 0,
          "timeseries_used": 129,
          "timeseries_total": 0
      },
      "dnode_info": {
          "uptime": 0,
          "cpu_engine": 25.5,
          "cpu_system": 35.2,
          "cpu_cores": 8.0,
          "mem_engine": 4096,
          "mem_system": 8192,
          "mem_total": 16384,
          "disk_engine": 1073741824,
          "disk_used": 5368709120,
          "disk_total": 107374182400,
          "net_in": 1024.5,
          "net_out": 512.3,
          "io_read": 250.5,
          "io_write": 150.2,
          "io_read_disk": 200.4,
          "io_write_disk": 100.3,
          "req_select": 1000,
          "req_select_rate": 99.5,
          "req_insert": 5000,
          "req_insert_success": 4990,
          "req_insert_rate": 99.8,
          "req_insert_batch": 1000,
          "req_insert_batch_success": 995,
          "req_insert_batch_rate": 99.5,
          "errors": 10,
          "vnodes_num": 3,
          "masters": 1,
          "has_mnode": 1,
          "has_qnode": 1,
          "has_snode": 1,
          "has_bnode": 0
      },
      "disk_infos": {
          "datadir": [
              {
                  "name": "/var/lib/taos",
                  "level": 1,
                  "avail": 5368709120,
                  "used": 1073741824,
                  "total": 6442450944
              }
          ],
          "logdir": {
              "name": "/var/log/taos",
              "avail": 5368709120,
              "used": 1073741824,
              "total": 6442450944
          },
          "tempdir": {
              "name": "/tmp/taos",
              "avail": 5368709120,
              "used": 1073741824,
              "total": 6442450944
          }
      },
      "log_infos": {
          "level": "warn",
          "total": 100
      }
  }
  ```

##### 4.3.6.2 响应说明

| HTTP 状态码 | 描述 |
| --- | --- |
| 200 | 成功 |

#### 4.3.7 general-metric 接口

##### 4.3.7.1 请求说明

1. 请求方法：POST
2. 请求 URL：`/general-metric`
3. 请求头：
  - `Content-Type: application/json`
1. 请求参数：
  请求体中需提供一个 StableArrayInfo 结构体实例数组，格式为 JSON。
  StableArrayInfo 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| ts | string | 否 | 时间戳 |
| protocol | int | 否 | 协议版本 |
| tables | []StableInfo | 否 | 超级表信息数组 |

  
  StableInfo 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| name | string | 否 | 超级表名称 |
| metric_groups | []MetricGroup | 否 | 指标组数组 |

  
  MetricGroup 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| tags | []Tag | 否 | 标签数组 |
| metrics | []Metric | 否 | 指标数组 |

  
  Tag 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| name | string | 否 | 标签名称 |
| value | string | 否 | 标签值 |

  
  Metric 结构体：

  | 参数名 | 类型 | 是否必填 | 描述 |
| --- | --- | --- | --- |
| name | string | 否 | 指标名称 |
| value | float64 | 否 | 指标值 |

1. 请求示例：
  ```json
  [
      {
          "ts": "1735894275213",
          "protocol": 2,
          "tables": [
              {
                  "name": "taosd_cluster_info",
                  "metric_groups": [
                      {
                          "tags": [
                              {
                                  "name": "cluster_id",
                                  "value": "3941541534719793555"
                              }
                          ],
                          "metrics": [
                              {
                                  "name": "cluster_uptime",
                                  "value": 0,
                                  "type": 1
                              },
                              {
                                  "name": "dbs_total",
                                  "value": 1,
                                  "type": 1
                              },
                              {
                                  "name": "tbs_total",
                                  "value": 16,
                                  "type": 1
                              },
                              {
                                  "name": "stbs_total",
                                  "value": 14,
                                  "type": 1
                              },
                              {
                                  "name": "vgroups_total",
                                  "value": 1,
                                  "type": 1
                              },
                              {
                                  "name": "vgroups_alive",
                                  "value": 1,
                                  "type": 1
                              },
                              {
                                  "name": "vnodes_total",
                                  "value": 1,
                                  "type": 1
                              },
                              {
                                  "name": "vnodes_alive",
                                  "value": 1,
                                  "type": 1
                              },
                              {
                                  "name": "mnodes_total",
                                  "value": 1,
                                  "type": 1
                              },
                              {
                                  "name": "mnodes_alive",
                                  "value": 0,
                                  "type": 1
                              },
                              {
                                  "name": "connections_total",
                                  "value": 15,
                                  "type": 1
                              },
                              {
                                  "name": "topics_total",
                                  "value": 0,
                                  "type": 1
                              },
                              {
                                  "name": "streams_total",
                                  "value": 0,
                                  "type": 1
                              },
                              {
                                  "name": "dnodes_total",
                                  "value": 1,
                                  "type": 1
                              },
                              {
                                  "name": "dnodes_alive",
                                  "value": 0,
                                  "type": 1
                              },
                              {
                                  "name": "grants_expire_time",
                                  "value": 0,
                                  "type": 1
                              },
                              {
                                  "name": "grants_timeseries_used",
                                  "value": 129,
                                  "type": 1
                              },
                              {
                                  "name": "grants_timeseries_total",
                                  "value": 0,
                                  "type": 1
                              }
                          ]
                      }
                  ]
              }
          ]
      }
  ]
  ```

##### 4.3.7.2 响应说明

1. 成功响应：

  | HTTP 状态码 | 描述 | 响应示例（JSON） |
| --- | --- | --- |
| 200 | 成功 | {} |

1. 错误响应： 

  | HTTP 状态码 | 描述 | 响应示例（JSON） |
| --- | --- | --- |
| 400 | 客户端错误 | {"error":"get general metric data error. xxx"} |
| 500 | 服务器内部错误 | {"error":"no connection"} |

#### 4.3.8 slow-sql-detail-batch 接口

##### 4.3.8.1 请求说明

1. 请求方法：POST
2. 请求 URL：`/slow-sql-detail-batch`
3. 请求头：
  - `Content-Type: application/json`
1. 请求参数：
  请求体中需提供一个 SlowSqlDetailInfo 结构体数组，格式为 JSON。
  SlowSqlDetailInfo 结构体：

  | 参数名 | 类型 | 描述 |
| --- | --- | --- |
| start_ts | TIMESTAMP | 语句开始执行的时间，单位ms，主键 |
| request_id | UINT64_T | 本次请求的request id，为hash生产的随机值 |
| query_time | INT32_T | 执行该语句花费的时间, 单位ms |
| code | INT32_T | 语句执行返回码，0表示成功 |
| error_info | VARCHAR(128) | 当语句执行失败时，记录错误信息 |
| type | INT8_T | 该 SQL 语句的类型（1-查询，2-写入，4-其他） |
| rows_num | INT64_T | 结果集中的记录数目 |
| sql | VARCHAR(16384) | 该 SQL 语句的字符串 |
| process_name | VARCHAR(32) | 进程名称 |
| process_id | VARCHAR(32) | 进程 ID |
| db | VARCHAR(1024) | 所属数据库 |
| user | VARCHAR(32) | 执行 SQL 语句的用户 |
| ip | VARCHAR(32) | 如有可能，记录执行 SQL 语句的 IP 地址。（通过 taosadapter 执行的 SQL 其 IP 相同，设计时看有无办法特殊处理） |
| cluster_id | VARCHAR(32) | 集群 id |

1. 请求示例：
  ```json
  [
    {
      "start_ts": "1703226836762",
      "request_id": "1",
      "query_time": 100,
      "code": 0,
      "error_info": "",
      "type": 1,
      "rows_num": 5,
      "sql": "select * from abc;",
      "process_name": "abc",
      "process_id": "123",
      "db": "dbname",
      "user": "root",
      "ip": "127.0.0.1",
      "cluster_id": "1234567"
    },
    {
      "start_ts": "1703226836763",
      "request_id": "2",
      "query_time": 100,
      "code": 0,
      "error_info": "",
      "type": 1,
      "rows_num": 5,
      "sql": "insert into abc ('a', 'b') values ('aaa', 'bbb');",
      "process_name": "abc",
      "process_id": "123",
      "db": "dbname",
      "user": "root",
      "ip": "127.0.0.1",
      "cluster_id": "1234567"
    }
  ]
  ```

##### 4.3.8.2 响应说明

1. 成功响应：

  | HTTP 状态码 | 描述 | 响应示例（JSON） |
| --- | --- | --- |
| 200 | 成功 | {} |

1. 错误响应： 

  | HTTP 状态码 | 描述 | 响应示例（JSON） |
| --- | --- | --- |
| 400 | 客户端错误 | {"error":"get taos slow sql detail data error. xxx"} |
| 500 | 服务器内部错误 | {"error":"no connection"} |

#### 4.3.9 check_health 接口

##### 4.3.9.1 请求说明

1. 请求方法：GET
2. 请求 URL：`/check_health`
3. 请求示例：
  ```bash
  curl http://127.0.0.1:6043/check_health
  ```

##### 4.3.9.2 响应说明

1. 成功响应：

  | HTTP 状态码 | 描述 | 响应示例（JSON） |
| --- | --- | --- |
| 200 | 成功 | {"version":"3.3.0.0"} |

#### 4.3.10 metrics 接口

##### 4.3.10.1 请求说明

1. 请求方法：GET
2. 请求 URL：`/metrics`
3. 请求示例：
  ```bash
  curl http://127.0.0.1:6043/metrics
  ```

##### 4.3.10.2 响应说明

1. 成功响应：

  | HTTP 状态码 | 描述 | 响应示例 |
| --- | --- | --- |
| 200 | 成功 | # HELP taos_keeper_monitor_cpu # TYPE taos_keeper_monitor_cpu gauge taos_keeper_monitor_cpu{identify="dev:6043"} 0.050009001046419144 # HELP taos_keeper_monitor_mem # TYPE taos_keeper_monitor_mem gauge taos_keeper_monitor_mem{identify="dev:6043"} 0.3763760030269623 # HELP taos_keeper_monitor_total_reports # TYPE taos_keeper_monitor_total_reports counter taos_keeper_monitor_total_reports{identify="dev:6043"} 0 |

#### 4.3.11 性能

无。

## 5. 兼容性

新增功能时，不会对原有接口进行修改，通过新增接口的方式，以保持兼容性。
在新增功能时，始终遵循向后兼容的原则：
- 接口稳定性：原有接口保持不变，确保现有用户的使用不会受到任何影响。
- 新增接口扩展功能：通过新增接口的方式提供新功能，将新特性与现有功能解耦，避免修改旧接口引发兼容性问题。
- 兼容性保障：确保系统在升级后，旧版本的客户端或应用依然能够正常上报数据。

## 6. 运维

若存在新增或修改配置的情况，升级时需要交付修改配置文件。

## 7. 使用场景

1. 实时状态与性能监控：监控 taosd、taosAdapter 和 taosX 的运行状态及性能指标。
2. 系统性能指标采集：收集服务器的 CPU、内存等性能指标。
3. 慢查询记录：记录执行时间超过阈值的 SQL 查询。

## 8. 约束和限制

1. 依赖版本：taosKeeper 需要 TDengine 3.0 及以上版本支持，确保使用符合要求的版本以避免兼容性问题。
2. 协程池限制：当 taosKeeper 处理大量并发请求时，协程池的大小和并发限制可能成为瓶颈。对于高并发场景，建议调整协程池的配置，适当增加协程数量以满足需求。

## 9. 常见错误和排查

1. 启动报错，显示 connection refused。
  - 原因：taosKeeper 依赖 RESTful 接口查询数据，此错误通常表明无法连接到 taosAdapter。
  - 排查步骤：
    - 确认 taosAdapter 是否正常运行。
    - 检查 taoskeeper.toml 配置文件中设置的 taosAdapter 地址是否正确。
1. 监控的 TDengine 显示的监测指标数目不一致。
  - 原因：如果 TDengine 中未创建某些指标，taosKeeper 将无法获取对应的监测结果。
  - 排查步骤：确认所需的指标已经在 TDengine 中正确配置并启用。
1. 无法接收到 TDengine 的监控日志。
  - 原因：taos.cfg 中未正确配置监控相关参数。
  - 排查步骤：在 taos.cfg 文件中增加以下参数并重启 taosd 服务：
    ```plaintext
    monitor          1         // 启用监控功能
    monitorInterval  30        // 发送间隔（单位：秒）
    monitorFqdn      localhost // 接收消息的 FQDN，默认为空
    monitorPort      6043      // 接收消息的端口号
    monitorMaxLogs   100       // 每个监控间隔缓存的最大日志数量
    ```

## 10. 可观测性

提供 taosKeeper 日志，以便进行问题定位与排查工作 。

## 11. 安装和卸载

该组件随 TDengine 产品安装包一同发布，随 TDengine 安装和卸载。

## 12. 文档

需要在官方文档中添加/修改章节【taosKeeper 参考手册】。

## 13. 参考文档

1. TDengine REST API：https://docs.taosdata.com/reference/connector/rest-api/

## 14. 附录

无
