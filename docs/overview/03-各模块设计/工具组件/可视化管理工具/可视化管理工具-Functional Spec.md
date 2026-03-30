# 可视化管理工具-Functional Spec

## 1. **修订记录**

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-28 | 2026-01-28 | 1.0 | 霍琳贺 | 重新整理 |

## 2. **背景**

taosExplorer 是 TDengine 时序数据库的 Web 管理工具，作为 taosX 产品套件的核心组件。随着 TDengine 在物联网、工业互联网、车联网等领域的广泛应用，用户需要一个统一的 Web 界面来完成以下任务：
1. **数据库管理**：创建和管理数据库、超级表、子表，执行 SQL 查询
2. **数据接入**：配置和管理多种工业协议（OPC UA/DA、MQTT、PI）和数据源的接入任务
3. **系统监控**：监控集群状态、节点健康、任务运行情况
4. **用户管理**：企业级用户管理、权限控制、SSO 集成
5. **开发支持**：提供多语言编程示例、API 文档、工具集成指南
本文档描述 TDengine Explorer 的功能设计、API 接口、用户界面行为、部署方式等，为开发和测试提供详细的规格说明。

## 3. **定义**

1. **TDengine Explorer**：Web 管理界面，提供数据库管理、数据接入、监控等功能
2. **taosX**：TDengine 数据接入和同步工具套件，Explorer 作为其 Web 前端
3. **DNode**：Data Node，TDengine 数据节点
4. **MNode**：Management Node，TDengine 管理节点
5. **QNode**：Query Node，TDengine 查询节点
6. **ANode**：Agent Node，taosX 数据接入代理节点
7. **SSO**：Single Sign-On，单点登录
8. **OAuth 2.0/OIDC**：开放授权协议及 OpenID Connect 身份认证层
9. **OPC UA/DA**：工业自动化通信协议（Unified Architecture / Data Access）
10. **MQTT**：Message Queuing Telemetry Transport，轻量级消息传输协议
11. **PI**：OSIsoft PI 工业数据平台
12. **RBAC**：Role-Based Access Control，基于角色的访问控制

## 4. **行为说明**

### 4.1 **用户认证**

#### 4.1.1 **本地登录**

**界面说明**：
- 登录页面包含：用户名输入框、密码输入框、“记住我”复选框、登录按钮
- 支持回车键提交登录表单
- 密码输入框支持显示/隐藏密码
**API 接口**：
```http
POST /api/-/login
Content-Type: application/json

{
  "username": "root",
  "password": "mixed_password"
}
```

**返回示例**：
```json
{
  "code": 0,
  "message": "success",
  "data": {
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "user": {
      "id": 1,
      "username": "root",
      "role": "admin",
      "email": "admin@example.com"
    },
    "expires_in": 7200
  }
}
```

**错误码**：

| 错误码 | 说明 |
| --- | --- |
| 401 | 用户名或密码错误 |
| 500 | 系统内部错误 |

#### 4.1.2 **OAuth 2.0/OIDC SSO 登录**

**界面说明**：
- 登录页面显示“使用 SSO 登录”按钮
- 点击后跳转到 IdP 登录页面
- 认证成功后自动跳转回 Explorer 首页
**配置参数** (配置文件 `explorer.toml`)：
```toml
[oauth]
enabled = true
provider = "oidc"  # oidc, plain, custom
[oauth.oidc]
client_id = ""
client_secret = ""
issuer_url = ""
redirect_uri = ""
scopes = ""
```

**参数说明**：

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| oauth.enabled | bool | false | 是否启用 OAuth |
| oauth.provider | string | - | IdP 类型 |
| oauth.oidc.client_id | string | - | OAuth 客户端 ID |
| oauth.oidc.client_secret | string | - | OAuth 客户端密钥（加密存储） |
| oauth.oidc.issuer_url | string | - | OIDC 发行者 URL |
| oauth.oidc.redirect_uri | string | - | 回调 URL |
| oauth.oidc.scopes | []string | ["openid"] | 请求的权限范围 |

### 4.2 **面板**

#### 4.2.1 **Grafana 监控引导**

**界面说明**：
- 分步骤引导用户实现 TDengine 所在服务器的 Grafana 监控
- 每个步骤包含：说明文字、命令示例、文档链接
- 支持复制命令功能
**引导步骤**：
**步骤1：下载和安装 Grafana**
```bash

## 5. Ubuntu/Debian

sudo apt-get install -y adduser libfontconfig1
wget https://dl.grafana.com/enterprise/release/grafana-enterprise_10.2.3_amd64.deb
sudo dpkg -i grafana-enterprise_10.2.3_amd64.deb

## 6. CentOS/RHEL

sudo yum install -y https://dl.grafana.com/enterprise/release/grafana-enterprise-10.2.3-1.x86_64.rpm

## 7. macOS

brew install grafana
```

**步骤2：下载 TDengine 数据源插件**
```bash

## 8. 使用 grafana-cli 安装

grafana-cli plugins install tdengine-datasource

## 9. 或手动下载

cd /var/lib/grafana/plugins/
wget https://github.com/taosdata/grafana-datasource/releases/latest/download/tdengine-datasource.zip
unzip tdengine-datasource.zip
```

**步骤3：重启 Grafana 服务**
```bash

## 10. systemd

sudo systemctl restart grafana-server

## 11. macOS

brew services restart grafana
```

**步骤4：配置 TDengine 数据源**
- 登录 Grafana（默认 http://localhost:3000，admin/admin）
- 导航至 Configuration > Data Sources
- 点击 "Add data source"，搜索并选择 "TDengine"
- 配置参数：
  - **URL**: http://localhost:6041
  - **User**: root
  - **Password**: 用户密码
- 点击 "Save & Test"
**步骤5：创建监控面板**
- 导入预设面板：提供 TDengine 系统监控、集群状态、数据写入监控等预设模板
- 或手动创建面板，监控项包括：
  - CPU、内存、磁盘 I/O
  - TDengine 服务状态
  - 连接数、查询 QPS
  - 查询响应时间
  - 数据写入速率
**相关文档链接**：
- [Grafana 官方文档](https://grafana.com/docs/)
- [TDengine Grafana 插件文档](https://github.com/taosdata/grafana-datasource)
- [TDengine 监控指标说明](https://docs.taosdata.com/)

### 11.1 **Dashboard**

#### 11.1.1 **系统概览**

**界面组件**：
- 集群状态卡片：显示在线/离线 DNode/MNode/QNode/ANode 数量
- CPU/内存/磁盘使用率曲线图（近 1 小时）
- 当前连接数、活跃查询数
- 数据写入速率（点/秒）和查询 QPS

### 11.2 **数据浏览器**

#### 11.2.1 **SQL 执行**

**界面功能**：
- 代码编辑器：支持 SQL 语法高亮、自动缩进
- 自动补全：数据库名、表名、字段名、SQL 关键字
- 历史记录：保存最近 50 条执行的 SQL
- 快捷键：
  - Ctrl+Enter / Cmd+Enter：执行当前 SQL
  - Ctrl+/ / Cmd+/：注释/取消注释
  - Ctrl+S / Cmd+S：保存到收藏
**API 接口**：
```http
POST /api/-/rest/sql
Authorization: Bearer <token>

SELECT * FROM power.meters WHERE ts >= NOW - 1h LIMIT 100
```

#### 11.2.2 **数据库管理**

##### 11.2.2.1 数据库对象浏览

以层级结构显示：数据库、超级表、子表、字段。

##### 11.2.2.2 数据库管理

1. 新增数据库
   - 数据库名称
   - 性能调优相关参数：BUFFER、PAGES、TSDB_PAGESIZE、VGROUPS、CACHEMODEL、STT_TRIGGER、PAGESIZE、CACHESIZE。
   - 数据持久化存储参数：DURATION、KEEP、MINROWS、MAXROWS、COMP、RETENTION。
   - WAL 配置参数：RETENTION_PERIOD、WAL_SEGMENT_SIZE、WAL_ROLL_PERIOD 、WAL_LEVEL 、WAL_RETENTION_SIZE 
   - 特殊参数：SINGLE_STABLE 、TABLE_PREFIX 、PRECISION 、TABLE_SUFFIX 、REPLICA 。
2. 查看数据库信息
3. 修改数据库信息
4. 管理数据库权限
   - 显示对当前数据库有权限操作的用户列表：用户名、权限(R/W/all)
   - 添加权限：选择用户，设置权限(R/W/all)
   - 收回权限
5. 删除数据库

##### 11.2.2.3 超级表管理

1. 创建超级表
   - 超级表名称
   - 设置主键列名称
   - 添加列，设置列的名称及类型；至少添加一列
   - 添加标签列，设置标签列名称及其类型；至少添加一个数据列
2. 查看超级表信息
3. 删除超级表
4. 修改超级表

##### 11.2.2.4 子表管理

1. 创建子表
   - 子表名称
   - 指定标签列值
2. 删除子表
3. 编辑子表
4. 删除子表：确认修改后执行删除。

#### 11.2.3 **结果导出**

**界面操作**：
- 查询结果表格右上角显示“导出”按钮
- 支持导出格式：CSV、JSON
- CSV 导出包含表头行
- 限制：单次最多导出 100,000 条记录

### 11.3 **数据接入**

#### 11.3.1 **任务管理**

如下图所示，进入数据写入功能页面后，首先展示数据写入列表，初始加载当前系统中所有的数据写入任务。
![](./images/img_UgTCbSZAFoUJvBxhlUhcFCSung2.png)

支持**添加数据源：**
![](./images/img_IoIKbI3nJoKMvIxFURtcig9cnWf.png)

数据源详细配置见 4.6 章节。
支持启动、复制、修改、停止数据接入任务，支持批量启动、停止数据接入任务。
支持数据接入任务导出、导入。

#### 11.3.2 **数据映射**

**界面操作**：
- 左侧显示源字段列表
- 右侧显示目标表字段
- 拖拽连线完成映射
- 支持字段类型转换配置

### 11.4 **数据源详细配置**

本节详细说明14种数据源的配置方法和参数。

#### 11.4.1 **TDengine 订阅数据源**

**功能说明**：
基于 TMQ 订阅机制，从 TDengine 3.x 迁移或同步数据到当前 TDengine 实例。
**配置参数**：
1. **连接配置**
   - **Topic DSN**: 订阅主题的连接字符串
      - 格式：`tmq+ws://username:password@host:port/topic_name?group.id=group1`
      - 示例：`tmq+ws://root:taosdata@192.168.1.100:6041/topic_meters?group.id=explorer_sync`
2. **订阅设置**
   - **订阅初始位置**: 
      - `earliest`: 从最早数据开始订阅
      - `latest`: 从当前时间开始订阅（默认）
   - **订阅组 ID**: 消费者组标识，用于在源端观察消费情况
   - **客户端 ID**: 客户端标识
   - **超时时间**: 无新数据时的超时时间（秒），超时后任务自动完成
   - **同步已落盘数据**: 是否从 TSDB 同步已落盘数据（仅 earliest 模式有效）
   - **同步删表操作**: 是否同步 DROP TABLE 操作
   - **同步删除数据操作**: 是否同步 DELETE 数据操作

#### 11.4.2 **TDengine 查询数据源**

**功能说明**：
基于 SQL 查询，从 TDengine 2.x 迁移或同步数据到当前 TDengine 实例。
**配置参数**：
1. **连接配置**
   - **连接协议**: `ws` 或 `wss`
   - **服务器地址**: REST API 服务地址
   - **端口**: 服务端口（默认 6041）
   - **数据库**: 源数据库名称
2. **认证信息**
   - **用户名**: TDengine 用户名
   - **密码**: TDengine 密码
3. **迁移模式**
   - **模式**：
      - `migrate`: 仅迁移历史数据
      - `realtime`: 仅同步实时数据
      - `both`: 先迁移历史，再同步实时
   - **表结构**: 
      - `schema_only`: 仅迁移表结构
      - `data_only`: 仅迁移数据
      - `both`: 同时迁移结构和数据
   - **稀疏模式**: 针对多表低频场景的性能优化开关
   - **元数据轮询间隔**: 检查元数据变更的时间间隔（秒）
4. **迁移数据表**
   - **超级表列表**: 要迁移的超级表（为空则迁移所有）
   - **普通表列表**: 要迁移的普通表
5. **时间范围**（历史数据迁移）
   - **开始时间**: RFC3339 格式，如 `2024-01-01T00:00:00Z`
   - **结束时间**: RFC3339 格式
   - **查询单元**: 时间切片大小（分钟），控制每次查询的数据量
6. **实时同步配置**
   - **回溯时长**: 任务启动时回溯的时间（分钟）
   - **查询间隔**: 轮询源数据库的时间间隔（秒）
   - **乱序容忍度**: 乱序数据容忍时长（秒）
7. **高级选项**
   - **最大读取并发数**: 查询数据的并发线程数
   - **最大写入并发数**: 写入目标数据库的并发线程数
   - **错误记录文件**: 写入失败数据的日志文件路径

#### 11.4.3 **PI 数据源**

**功能说明**：
从 OSIsoft PI 系统同步实时数据，支持 PI Data Archive 和 PI AF Server 两种模式。
**配置参数**：
1. **连接配置**
   - **连接模式**: 
      - `data_archive_only`: 仅使用 PI Data Archive
      - `af_server`: 使用 AF SDK（支持 Data Archive 和 AF Server）
   - **PI 服务名**: PI 服务器地址（主机名）
   - **AF Server 名称**: PI 系统 AF Server 名称（AF 模式）
   - **AF Database Name**: AF 数据库名称（AF 模式）
2. **数据集配置**
   - **单列模式点位列表**: 上传 CSV 文件配置数据点位（Data Archive 模式）
      - 可下载空模板或当前 PI Server 的所有点位
      - CSV 字段：point_id, enable, stable, tbname, value_col, value_transform, type, quality_col, ts_col, received_ts_col, ts_transform, received_ts_transform, tag 列
   - **多列模式 AF 模板**: 上传 CSV 配置 AF 模板（AF Server 模式）
   - **AF 模式模板列表**: 选择要同步的 AF 模板
3. **自动回填**
   - **重启补偿时间**: 任务异常重启时回填的时长（分钟）
4. **高级选项**
   - **批次大小**: 单次发送的最大消息数或行数
   - **批次延时**: 单次读取最大延时（秒）

#### 11.4.4 **PI Backfill 数据源**

**功能说明**：
从 PI 系统迁移历史数据。配置与 PI 数据源相同，增加历史填充配置。
**附加配置参数**：
**历史填充**
- **Backfill 开始时间**: 
  - 固定时间：RFC3339 格式
  - 动态时间：`dynamic:earliest` 或 `dynamic:latest` 从 TDengine 获取
- **Backfill 结束时间**: 同上

#### 11.4.5 **OPC-UA 数据源**

**功能说明**：
从 OPC UA 服务器同步实时数据，支持多种安全模式和认证方式。
**配置参数**：
1. **连接配置**
   - **服务地址**: OPC UA 服务器端点
      - 格式：`host:port/path`
      - 示例：`192.168.1.100:4840/OPCUA/Server`
   - **安全模式**: 
      - `None`: 无安全
      - `Sign`: 签名
      - `SignAndEncrypt`: 签名并加密
   - **安全策略**: 
      - `None`
      - `Basic128Rsa15`
      - `Basic256`
      - `Basic256Sha256`
      - `Aes128_Sha256_RsaOaep`
      - `Aes256_Sha256_RsaPss`
   - **安全通信证书**: 客户端证书文件（.der 格式）
   - **安全通信私钥**: 私钥文件（.pem 格式）
   - **连接超时**: 连接超时时间（秒）
2. **认证方式**
   - **匿名访问**: 无需认证
   - **用户名密码**: 提供用户名和密码
   - **证书认证**: 提供认证证书和私钥文件
3. **点位集配置**
   - **方式1 - 上传 CSV 文件**: 
      - 下载空模板或根据过滤条件下载点位
      - CSV 字段：point_id, enable, stable, tbname, value_col, value_transform, type, quality_col, ts_col, received_ts_col, ts_transform, received_ts_transform, tag 列
   - **方式2 - 动态选择**: 
      - 根节点 ID
      - 命名空间
      - 正则匹配表达式
      - 主键列：original_ts 或 received_ts
      - 表名称模板
4. **采集配置**
   - **采集模式**: 
      - `subscription`: 订阅模式
      - `polling`: 轮询模式
   - **采集间隔**: 轮询模式下的轮询间隔（毫秒）
   - **采集超时**: 单次采集超时时间（秒）
5. **高级选项**
   - **日志级别**: trace, debug, info, warn, error
   - **最大写入并发数**: 写入并发线程数
   - **批次大小**: 单次发送的最大消息数
   - **批次延时**: 单次读取最大延时（秒）
   - **保存原始数据**: 
      - 是否开启
      - 最大保留天数
      - 原始数据存储目录

#### 11.4.6 **OPC-DA 数据源**

**功能说明**：
从 OPC DA 服务器同步实时数据（需要 Windows 环境或 Agent）。
**配置参数**：
1. **连接配置**
   - **服务地址**: OPC DA 服务器地址，格式：`host/ProgID`，示例：`localhost/Matrikon.OPC.Simulation.1`
2. **点位集配置**
   - **上传 CSV 文件**: CSV 字段：tag_name, enable, stable, tbname, value_col, value_transform, type, quality_col, ts_col, received_ts_col, ts_transform, received_ts_transform, tag 列
   - **动态选择**: 
      - 根节点
      - TagName 正则匹配
      - 主键列选择
      - 表名称模板
3. **采集配置**
   - **采集间隔**: 轮询间隔（毫秒）
4. **高级选项：**同 OPC-UA 高级选项

#### 11.4.7 **KingHistorian 数据源**

**功能说明**：
从 KingHistorian 实时库同步历史或实时数据（需要 Windows 环境或 Agent）。
**配置参数**：
1. **连接配置**
   - **服务地址**
   - **服务端口**
   - 连接超时
2. **点位集配置**
   - **上传 CSV 文件**，同 OPC-UA
3. **采集配置**
   - **历史数据迁移：**
      - 开始时间
      - 结束时间
      - 查询窗口
      - 乱序
      - 查询间隔
   - **实时数据同步：**
      - 最小间隔时间
4. **高级选项：**同 OPC-UA 高级选项

#### 11.4.8 **InfluxDB 数据源**

**功能说明**：
从 InfluxDB 数据库同步数据，支持 1.x 和 2.x 版本，支持历史迁移和实时同步。
**配置参数**：
1. **连接配置**
   - **连接协议**: `http` 或 `https`
   - **服务器地址**: InfluxDB 服务器地址
   - **端口**: 服务端口（1.x 默认 8086，2.x 默认 8086）
2. **认证配置**
   - **InfluxDB 1.x** (支持 1.7、1.8): 仅支持用户名、密码
   - **InfluxDB 2.x** (支持 2.1-2.7): 支持组织 ID + 令牌 Token
3. **任务详情**
   - **Bucket/Database**: InfluxDB 数据存储空间名称
   - **Measurements**: 要同步的测量值列表（为空则同步全部）
   - **起始时间**: RFC3339 格式
   - **结束时间**: RFC3339 格式（未来时间则持续同步）
   - **分片读取**: 每次读取的最大时间范围（小时）
   - **延迟**: 消除乱序数据的延迟时长（秒）
4. **高级选项**
   - **日志级别**
   - **最大读取并发数**
   - **最大写入并发数**
   - **批次大小**

#### 11.4.9 **OpenTSDB 数据源**

**功能说明**：
从 OpenTSDB 数据库同步数据，支持历史迁移和实时同步。
**配置参数**：
1. **连接配置**
   - **连接协议**: `http` 或 `https`
   - **服务器地址**: OpenTSDB 服务器地址
   - **服务器端口**: 默认 4242
2. **任务详情**
   - **Metrics**: 要同步的指标列表（为空则同步全部）
   - **起始时间**: RFC3339 格式
   - **结束时间**: RFC3339 格式
   - **分片读取**: 每次读取的最大时间范围（小时）
   - **延迟**: 消除乱序数据的延迟时长（秒）
3. **高级选项：**同 InfluxDB

#### 11.4.10 **MQTT 数据源**

**功能说明**：
从 MQTT Broker 订阅消息，支持消息解析、转换和映射到 TDengine。
**配置参数**：
1. **连接配置**
   - **MQTT 地址**: Broker 地址，格式：`tcp://host:port` 或 `ssl://host:port`，示例：`tcp://mqtt.example.com:1883`
2. **认证配置**
   - **匿名访问**: 无需认证
   - **用户名密码**
3. **SSL 证书**（ssl:// 连接）
   - **客户端证书**: 客户端证书文件
   - **客户端私钥**: 客户端私钥文件
   - **CA 证书**: CA 证书文件（可选）
4. **采集配置**
   - **MQTT 协议版本**: 3.1, 3.1.1, 5.0
   - **Client ID**: MQTT 客户端标识
   - **Keep Alive**: 心跳间隔（秒），默认 60
   - **Clean Session**: 是否清除会话，默认 true
   - **订阅主题**: Topic 列表，支持通配符，示例：`sensors/temperature/#`, `sensors/+/humidity`
   - **QoS 配置**: 0, 1, 2
   - **主题解析**：支持解析 Topic 到字段，示例：`+/+/metric_name`
5. **数据 Transform**
   - **解析规则**: 
      - `json`: JSON 格式解析，支持 JSON Path
      - `regex`: 正则表达式解析，支持正则表达式捕获组名称，示例：`*(?<y>[0-9])-(?<m>[0-9])-(?<d>[0-9])*`
      - `udt`: rhai 脚本用户自定义 Transform（User Defined Transform）
   - **字段拆分**: 
      - `json`：嵌套 JSON 层级拆分
      - `regex`：同解析规则
      - `split`：按分隔符拆分
      - `convert`：常量哈希表转换
      - `join`：数组字段拼接
   - **数据过滤**: 过滤条件表达式，支持任意 Rhai 条件表达式
   - **字段映射**: 源字段到目标字段的映射规则
6. **高级选项**
   - **日志级别**
   - **保留原始数据**: 
      - 是否开启
      - 最大保留天数
      - 原始数据存储目录

#### 11.4.11 **SparkPlugB 数据源**

**功能说明**：
从 MQTT Broker 订阅 SparkPlugB 消息，支持消息解析、转换和映射到 TDengine。
**配置参数**：
1. **连接配置：**同 MQTT
2. **订阅配置**
   - Group ID
   - 节点/设备列表
   - 消息类型
   - 下发 REBIRTH 命令
3. **数据 Transform**：同 MQTT

#### 11.4.12 **Kafka 数据源**

**功能说明**：
从 Kafka 消费消息，支持 SASL 认证和消息转换。
**配置参数**：
1. **连接配置**
   - **Kafka 地址**: Broker 地址列表，格式：`host1:port1,host2:port2`，示例：`kafka1:9092,kafka2:9092,kafka3:9092`
2. **SASL 认证**
   - **认证机制**: `PLAIN` 、`SCRAM-SHA-256`、`SCRAM-SHA-512`
   - **用户名 + 密码**
3. **采集配置**
   - **任务完成条件**: 超时时间（毫秒）或 `never`
   - **主题**: Topic 列表，逗号分隔，示例：`topic1,topic2,topic3`
   - **Offset**: 
      - `earliest`: 从最早消息开始
      - `latest`: 从最新消息开始
   - **获取数据的最大时长**: 等待数据的最长时间（毫秒）
4. **消息 Transform：**同 MQTT Transform 配置
5. **高级选项**
   - **最大读取并发数**: 启动的 Consumer 数量
   - **批次大小**: 单次发送的最大消息数

#### 11.4.13 **Pulsar(-Tuya) 数据源**

**功能说明**：
从 Apache Pulsar 消费消息写入 TDengine。
**配置参数**：
1. **连接配置**
   - **Broker 地址**: Broker 地址列表，格式：`host:port`
2. **认证配置**
   - **认证机制**:
      - BasicAuth
      - JWT
      - mTLS
      - CustomAuthentication
3. **采集配置**
   - 超时时间
   - 主题
   - 消费者名称
   - 订阅名称
   - Initial Position(Offset)：
      - `earliest`: 从最早消息开始
      - `latest`: 从最新消息开始
   - 字符编码
4. **消息 Transform：**同 MQTT Transform 配置
5. **高级选项**
   - **最大读取并发数**: 启动的 Consumer 数量
   - **批次大小**: 单次发送的最大消息数

#### 11.4.14 **CSV 数据源**

**功能说明**：
导入 CSV 文件数据到 TDengine。
**配置参数**：
1. **CSV 选项**
   - **包含表头**: 第一行是否为列名
   - **忽略前 N 行**: 跳过文件开头的行数
   - **字段分隔符**: 默认 `,`
   - **字段引用符**: 默认 `"`，用于包围含分隔符的字段
   - **注释前缀符**: 以此字符开头的行将被忽略，默认 `#`
2. **CSV 文件来源**
   - **上传文件**: 直接上传单个 CSV 文件
   - **服务器目录**: 指定服务器上的目录路径，加载该目录下所有 CSV 文件
3. **数据 Transform：同 MQTT**
4. **高级选项**
   - **最大读取并发数**
   - **批次大小**

#### 11.4.15 **AVEVA Historian 数据源**

**功能说明**：
从 AVEVA Historian 同步或迁移历史/实时数据。
**配置参数**：
1. **连接配置**
   - **Server 地址**: AVEVA Historian 服务器地址
   - **Server 端口**: 服务端口
2. **认证配置**
   - 用户名
   - 密码
3. **采集配置**
   - **采集模式**: 
      - `synchronize`: 实时同步
      - `migrate`: 历史数据迁移
   - **表**: 
      - `Runtime.dbo.History`: 历史数据表
      - `Runtime.dbo.Live`: 实时数据表
   - **标签**: TagName 列表或 `*`（全部，排除 Sys 开头）
   - **标签组大小**: 批量查询时每组的 Tag 数量，默认 10
   - **任务开始时间**: RFC3339 格式
   - **任务结束时间**: RFC3339 格式
   - **查询时间窗口**: 历史迁移时每次查询的时间跨度（小时）
   - **实时同步间隔**: 实时同步的查询间隔（秒）
   - **乱序时间上限**: 乱序数据容忍时长（秒）
4. **数据 Transform：同 Kafka**
5. **高级选项：同 Kafka**

#### 11.4.16 **MySQL 数据源**

**功能说明**：
从 MySQL 数据库迁移数据到 TDengine。
**配置参数**：
1. **连接配置**
   - **服务地址**: MySQL 服务器地址
   - **服务端口**: 默认 3306
   - **数据库**: 数据库名称
   - **字符集**: 默认 utf8mb4
   - **SSL 模式**: 
      - `disabled`: 不使用 SSL
      - `preferred`: 优先使用 SSL
      - `required`: 必须使用 SSL
2. **认证配置**：支持用户名密码
3. **SQL 查询**
   - **SQL 模板**: 查询语句模板，使用占位符，示例：`SELECT ts, temperature, humidity FROM sensors WHERE ts >= ? AND ts < ?`
   - **起始时间**: RFC3339 格式
   - **结束时间**: RFC3339 格式
   - **查询间隔**: 时间切片大小（分钟），避免单次查询数据过多
   - **延迟时长**: 同步未来数据时的等待时间（秒）
4. **Transform：同 MQTT**

#### 11.4.17 **PostgreSQL 数据源**

**功能说明**：
从 PostgreSQL 数据库迁移数据到 TDengine。
**配置参数**：
与 MySQL 数据源配置完全相同，仅连接协议和默认端口不同（PostgreSQL 默认 5432）。

#### 11.4.18 Oracle 数据源

**功能说明**：
从 Oracle 数据库迁移数据到 TDengine。
**配置参数**：
1. 与 MySQL 数据源配置相同，仅连接协议和默认端口不同（Oracle 默认 1521）。
2. 支持连接配置参数：最小连接数、最大连接数、连接超时

#### 11.4.19 Microsoft SQL Server 数据源

**功能说明**：
从 MSSQL 数据库迁移数据到 TDengine。
**配置参数**：
1. **连接配置：**与 MySQL 数据源配置相同，仅连接协议和默认端口不同（Oracle 默认 1521）。
2. **连接选项**：
   - 实例名称
   - 应用名称
   - 启用加密
   - 信任证书(CA)

#### 11.4.20 MongoDB 数据源

**功能说明**：
从 MongoDB 数据库迁移数据到 TDengine。
**配置参数**：
1. **连接配置：**
   - **服务地址**: MySQL 服务器地址
   - **服务端口**: 默认 3306
2. **认证配置：**
   - 用户名和密码
   - 认证数据库
3. **连接选项**：
   - 应用名称
   - 启用 SSL 加密
4. 采集配置：
   - 数据库
   - 集合
   - 子表字段
   - 查询模板
   - 查询排序
   - 起始时间
   - 结束时间
   - 查询间隔
   - 延迟时长
5. **数据转换：同 MySQL**

### 11.5 **编程接口**

#### 11.5.1 **编程语言示例**

**界面说明**：
- 提供 8 种主流编程语言的连接示例：Java、Python、Go、Node.js、C#、Rust、R、REST API
- 每种语言提供以下场景示例：
  - 连接数据库
  - 执行查询
  - 插入数据
  - 批量插入
  - 数据订阅（适用的语言）
- 代码块支持复制功能

### 11.6 **代理管理**

#### 11.6.1 **代理列表**

**界面说明**：
- 显示当前系统中创建的所有代理
- 列表信息包括：代理名称、状态（在线/离线）、连接状态、创建时间、最后活动时间、关联任务数
- 支持刷新、查看详情、删除操作
- 状态指示：绿色圆点（在线）、灰色圆点（离线）
**API 接口**：
```http

## 12. 获取代理列表

GET /api/x/agents
Authorization: Bearer <token>
```

**查看代理详情**：
```http
GET /api/x/agents/{agent_id}
Authorization: Bearer <token>
```

**删除代理**：
```http
DELETE /api/x/agents/{agent_id}
Authorization: Bearer <token>
```

#### 12.0.1 **创建代理**

**界面说明**：
分步骤引导用户创建和配置代理：
**步骤1：生成 Token**
界面显示：
- 代理名称输入框（必填）
- 代理描述输入框（可选）
- "生成 Token" 按钮
**API 接口**：
```http
POST /api/x/agents
Authorization: Bearer <token>
Content-Type: application/json
```

界面操作：
- Token 生成后显示在文本框中
- 提供"复制 Token"按钮
- 提示用户妥善保管 Token（仅显示一次）
**步骤2：下载代理程序**
界面显示：
- 不同平台的下载链接：
  - Windows x64
  - Linux x64
  - Linux ARM64
**步骤3：配置代理**
界面显示配置文件模板和说明：


#### 6.1 代理列表

**界面说明**：
- 显示当前系统中创建的所有代理
- 列表信息包括：代理名称、状态（在线/离线）、连接状态、创建时间、最后活动时间、关联任务数
- 支持刷新、查看详情、删除操作
- 状态指示：绿色圆点（在线）、灰色圆点（离线）
  
**API 接口**：
```http

## 13. 获取代理列表

GET /api/v1/agents
Authorization: Bearer <token>

## 14. 响应示例

{
  "code": 0,
  "data": [
    {
      "id": "agent-001",
      "name": "Factory-Agent-01",
      "status": "online",
      "connection_status": "connected",
      "created_at": "2026-01-20T10:30:00Z",
      "last_active_at": "2026-01-28T14:30:00Z",
      "last_heartbeat_at": "2026-01-28T14:45:00Z",
      "tasks_count": 5,
      "version": "1.0.5",
      "platform": "Windows Server 2019",
      "ip_address": "192.168.1.50"
    },
    {
      "id": "agent-002",
      "name": "Warehouse-Agent-01",
      "status": "offline",
      "connection_status": "disconnected",
      "created_at": "2026-01-15T08:20:00Z",
      "last_active_at": "2026-01-27T16:30:00Z",
      "tasks_count": 2,
      "version": "1.0.4",
      "platform": "Linux Ubuntu 20.04"
    }
  ]
}
```


**查看代理详情**：
```http
GET /api/v1/agents/{agent_id}
Authorization: Bearer <token>

## 15. 响应示例

{
  "code": 0,
  "data": {
    "id": "agent-001",
    "name": "Factory-Agent-01",
    "description": "Factory A OPC-DA Agent",
    "status": "online",
    "connection_status": "connected",
    "created_at": "2026-01-20T10:30:00Z",
    "last_active_at": "2026-01-28T14:30:00Z",
    "version": "1.0.5",
    "platform": "Windows Server 2019",
    "ip_address": "192.168.1.50",
    "cpu_usage": 15.2,
    "memory_usage": 45.6,
    "tasks": [
      {
        "task_id": "task-101",
        "task_name": "opcda_task_001",
        "task_type": "opcda",
        "status": "running"
      }
    ]
  }
}
```


**删除代理**：
```http
DELETE /api/v1/agents/{agent_id}
Authorization: Bearer <token>

## 16. 响应

{
  "code": 0,
  "message": "Agent deleted successfully"
}
```


#### 6.2 创建代理

**界面说明**：
分步骤引导用户创建和配置代理：

**步骤1：生成 Token**

界面显示：
- 代理名称输入框（必填）
- 代理描述输入框（可选）
- "生成 Token" 按钮
  
**API 接口**：
```http
POST /api/v1/agents/token
Authorization: Bearer <token>
Content-Type: application/json

{
  "name": "Factory-Agent-01",
  "description": "Factory A OPC-DA Agent"
}

## 17. 响应

{
  "code": 0,
  "data": {
    "agent_id": "agent-001",
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhZ2VudF9pZCI6ImFnZW50LTAwMSIsImV4cCI6MTczNzk3MjYwMH0.a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
    "expires_at": "2027-01-28T14:30:00Z"
  }
}
```

界面操作：
- Token 生成后显示在文本框中
- 提供"复制 Token"按钮
- 提示用户妥善保管 Token（仅显示一次）
**步骤2：下载代理程序**
界面显示：
- 不同平台的下载链接：
  - Windows x64
  - Windows ARM64
  - Linux x64
  - Linux ARM64
  - macOS x64 (Intel)
  - macOS ARM64 (Apple Silicon)
下载链接示例：
```plaintext
GET /api/v1/agents/download/{platform}
Authorization: Bearer <token>

## 18. platform: windows-x64, windows-arm64, linux-x64, linux-arm64, darwin-x64, darwin-arm64

```

**步骤3：配置代理**
界面显示配置文件模板和说明：
```toml

## 19. agent.toml - TaosX Agent 配置文件

endpoint = "http://localhost:6055"
token = ""
```

**步骤4：启动代理**
界面显示不同平台的启动命令：
**Linux/macOS:**
```bash

## 20. 启动代理

taosx-agent -c agent.toml

## 21. 后台运行

nohup ./taosx-agent -c agent.toml > /dev/null 2>&1 &

## 22. Systemd 管理

sudo systemctl daemon-reload
sudo systemctl enable taosx-agent
sudo systemctl start taosx-agent
sudo systemctl status taosx-agen
```

**systemd 服务文件示例** (`taosx-agent.service`):
```toml
[Unit]
Description=taosX Agent.
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=-/etc/default/taosx-agent
ExecStart=/usr/bin/taosx-agent
Restart=always
LimitNOFILE=1048576
LimitNPROC=infinity
LimitCORE=infinity
TimeoutStartSec=0
StartLimitBurst=3
StartLimitInterval=60s

[Install]
WantedBy=multi-user.target
```


**Windows:**
```powershell

## 23. 编辑配置文件

notepad C:\TDengine\agent.toml

## 24. 启动代理

cd C:\TDengine
.\taosx-agent.exe -c agent.toml

## 25. 服务启动

sc start taosx-agent
```

**步骤5：验证连接**
界面显示：
- 实时连接状态检测
- 自动刷新代理列表
- 连接成功后显示绿色提示："代理已成功连接！"
- 连接失败时显示红色提示和故障排查建议
**故障排查建议**：
1. 检查网络连接：确保代理服务器能访问 Explorer 服务器
2. 检查 Token 是否正确复制
3. 检查服务器 URL 是否正确
4. 检查防火墙规则
5. 查看代理日志：`tail -f /var/log/taosx_agent*.log`

### 25.1 数据采集代理

#### 25.1.1 协议文档

**界面说明**：
提供常见数据采集工具通过标准协议写入 TDengine 的配置指南。

#### 25.1.2 Prometheus Remote Write

**协议说明**：
Prometheus 可以通过 Remote Write API 将监控数据写入 TDengine。
**配置步骤**：
**1. 在 TDengine 中创建数据库**
```sql
CREATE DATABASE IF NOT EXISTS prometheus_data KEEP 365;
```

**2. 配置 Prometheus**

编辑 `prometheus.yml`:
```yaml

## 26. prometheus.yml

global:
  scrape_interval: 15s
  evaluation_interval: 15s

## 27. Remote Write 配置

remote_write:
  - url: "http://localhost:6041/prometheus/v1/remote_write/prometheus_data"
    basic_auth:
      username: "root"
      password: "taosdata"
    queue_config:
      capacity: 10000
      max_shards: 20
      max_samples_per_send: 5000
    write_relabel_configs:
      - source_labels: [__name__]
        regex: 'up|go_.*'
        action: keep
```

**3. 重启 Prometheus**
```bash
systemctl restart prometheus
```

**4. 验证数据写入**
```sql
USE prometheus_data;
SHOW STABLES;
SELECT * FROM metrics LIMIT 10;
```

**API 端点说明**：
```plaintext
POST /prometheus/v1/remote_write/{database}
Content-Type: application/x-protobuf
Content-Encoding: snappy
Authorization: Basic <base64(username:password)>

Body: Prometheus protobuf 格式数据（snappy 压缩）
```

#### 27.0.1 Telegraf

**协议说明**：
Telegraf 可以通过 InfluxDB 行协议将数据写入 TDengine。
**配置步骤**：
**1. 在 TDengine 中创建数据库**
```sql
CREATE DATABASE IF NOT EXISTS telegraf KEEP 365;
```

**2. 配置 Telegraf**
编辑 `telegraf.conf`:
```toml

## 28. telegraf.conf

## 29. 输入插件示例（采集系统指标）

[[inputs.cpu]]
  percpu = true
  totalcpu = true
  collect_cpu_time = false

[[inputs.disk]]
  ignore_fs = ["tmpfs", "devtmpfs", "devfs"]

[[inputs.mem]]

[[inputs.net]]

## 30. 输出到 TDengine

[[outputs.http]]
  url = "http://localhost:6041/influxdb/v1/write?db=telegraf"
  method = "POST"
  username = "root"
  password = "taosdata"
  data_format = "influx"
  timeout = "5s"
  
  # 性能优化
  [outputs.http.headers]
    Content-Type = "text/plain; charset=utf-8"
  
  # 批量发送配置
  flush_interval = "10s"
  metric_batch_size = 1000
```

**3. 启动 Telegraf**
```bash
telegraf --config telegraf.conf
```

**4. 验证数据写入**
```sql
USE telegraf;
SHOW STABLES;
SELECT * FROM cpu LIMIT 10;
```

#### 30.0.1 InfluxDB 行协议

**协议说明**：
TDengine 兼容 InfluxDB 行协议，可以直接接收 InfluxDB 格式的数据。

**行协议格式**：
```plaintext
measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
```


**示例**：
```plaintext
cpu,host=server01,region=us-west usage_user=23.5,usage_system=12.3 1609459200000000000
mem,host=server01 used_percent=68.5,available=8589934592 1609459200000000000
```


**写入数据**：

**HTTP API (InfluxDB 1.x 兼容)**：
```bash

## 31. 单条数据

curl -u root:taosdata -XPOST "http://localhost:6041/influxdb/v1/write?db=mydb" \
  --data-binary 'measurement,tag1=value1 field1=123.4,field2=567 1609459200000000000'

## 32. 批量数据（换行分隔）

curl -u root:taosdata -XPOST "http://localhost:6041/influxdb/v1/write?db=mydb" \
  --data-binary 'cpu,host=server01 usage=23.5 1609459200000000000
mem,host=server01 used=68.5 1609459200000000000
disk,host=server01 free=45.2 1609459200000000000'

## 33. 从文件读取

curl -u root:taosdata -XPOST "http://localhost:6041/influxdb/v1/write?db=mydb" \
  --data-binary @data.txt
```


**HTTP API (InfluxDB 2.x 兼容)**：
```bash
curl -XPOST "http://localhost:6041/influxdb/v2/write?bucket=mydb&org=myorg" \
  -H "Authorization: Token root:taosdata" \
  --data-binary 'measurement,tag1=value1 field1=123.4 1609459200000000000'
```


**参数说明**：
- `db` / `bucket`: 目标数据库名称
- `precision`: 时间戳精度（ns, us, ms, s），默认 ns
- `org`: 组织名称（InfluxDB 2.x API，可选）
  
#### 33.0.1 OpenTSDB JSON 协议

**协议说明**：
TDengine 支持 OpenTSDB JSON 格式的数据写入。

**JSON 格式**：
```json
{
  "metric": "sys.cpu.nice",
  "timestamp": 1346846400,
  "value": 18,
  "tags": {
    "host": "web01",
    "dc": "lga"
  }
}
```


**写入数据**：

**单条数据**：
```bash
curl -u root:taosdata -X POST "http://localhost:6041/opentsdb/v1/put/json/mydb" \
  -H "Content-Type: application/json" \
  -d '{
    "metric": "sys.cpu.nice",
    "timestamp": 1346846400,
    "value": 18,
    "tags": {
      "host": "web01",
      "dc": "lga"
    }
  }'
```


**批量数据**：
```bash
curl -u root:taosdata -X POST "http://localhost:6041/opentsdb/v1/put/json/mydb" \
  -H "Content-Type: application/json" \
  -d '[
    {
      "metric": "sys.cpu.nice",
      "timestamp": 1346846400,
      "value": 18,
      "tags": {"host": "web01", "dc": "lga"}
    },
    {
      "metric": "sys.cpu.user",
      "timestamp": 1346846400,
      "value": 42,
      "tags": {"host": "web01", "dc": "lga"}
    }
  ]'
```


**参数说明**：
- `metric`: 指标名称（对应 TDengine 超级表名）
- `timestamp`: Unix 时间戳（秒）
- `value`: 指标值
- `tags`: 标签（对应 TDengine 标签列）
  
#### 33.0.2 OpenTSDB Telnet 协议

**协议说明**：
TDengine 支持 OpenTSDB Telnet 文本协议，可以通过 TCP 连接写入数据。

**Telnet 协议格式**：
```plaintext
put <metric> <timestamp> <value> <tagk1>=<tagv1> [<tagk2>=<tagv2> ...]
```


**写入数据**：

**使用 telnet 客户端**：
```bash

## 34. 连接到 TDengine OpenTSDB Telnet 端口

telnet localhost 6046

## 35. 输入数据（每行一条）

put sys.cpu.nice 1346846400 18 host=web01 dc=lga
put sys.cpu.user 1346846400 42 host=web01 dc=lga
put sys.mem.free 1346846400 8589934592 host=web01 dc=lga

## 36. 退出

quit
```


**使用 netcat (nc)**：
```bash

## 37. 单条数据

echo "put sys.cpu.nice 1346846400 18 host=web01 dc=lga" | nc localhost 6046

## 38. 批量数据

cat <<EOF | nc localhost 6046
put sys.cpu.nice 1346846400 18 host=web01 dc=lga
put sys.cpu.user 1346846400 42 host=web01 dc=lga
put sys.mem.free 1346846400 8589934592 host=web01 dc=lga
EOF
```


**配置说明**：

在 `taos.cfg` 中配置 OpenTSDB 端口：
```toml

## 39. OpenTSDB Telnet 端口

opentsdb_telnet_port = 6046
```


**验证数据写入**：
```sql
USE mydb;
SHOW STABLES;
SELECT * FROM sys_cpu_nice LIMIT 10;
```

### 39.1 **流计算**

#### 39.1.1 **流任务列表**

**界面说明**：
- 显示当前数据库中所有流计算任务
- 列表信息：任务名称、状态（运行中/已停止）、源数据库、目标数据库、目标表、触发器类型、创建时间
- 支持查看创建 SQL
- 操作：启动、停止、删除、查看详情
**API 接口**：
```http
GET /api/v1/streams
Authorization: Bearer <token>

## 40. 响应

{
  "code": 0,
  "data": [
    {
      "name": "stream_avg_1m",
      "status": "running",
      "source_db": "power",
      "source_table": "meters",
      "target_db": "power",
      "target_table": "avg_1m",
      "trigger": "window_close",
      "created_at": "2026-01-20T10:00:00Z",
      "sql": "CREATE STREAM stream_avg_1m INTO avg_1m AS SELECT _wstart, AVG(current) FROM meters INTERVAL(1m)"
    }
  ]
}
```

#### 40.0.1 **流任务创建**

**界面说明**：
**SQL 创建**
- 提供 SQL 编辑器，支持语法高亮
- 模板示例：
  ```sql
  CREATE STREAM stream_name INTO target_table AS 
  SELECT _wstart, AVG(value) 
  FROM source_table 
  INTERVAL(1m);
  ```

### 40.1 **数据订阅**

#### 40.1.1 **Topic 列表**

**界面说明**：
- 显示当前数据库的所有订阅主题
- 列表信息：主题名称、数据库名称、创建 SQL、创建时间、DSN
- 支持复制 DSN
- 操作：查看详情、删除、共享

#### 40.1.2 **Topic 创建**

```sql
-- 订阅整个数据库
CREATE TOPIC topic_db AS DATABASE power;

-- 订阅超级表
CREATE TOPIC topic_stable AS STABLE power.meters;

-- 订阅查询结果
CREATE TOPIC topic_query AS SELECT * FROM power.meters WHERE groupid = 2;
```

#### 40.1.3 **Topic 共享**

**界面说明**：
- 选择要共享的用户（多选）
- 设置权限：只读、读写
- 支持取消共享

#### 40.1.4 **消费者管理**

**界面说明**：
- 显示所有主题的消费者
- 列表信息：订阅主题、消费者 ID、消费组、客户端 ID、状态、订阅时间、消费进度
- 状态：已连接、已断开、空闲

#### 40.1.5 **示例代码**

**界面说明**：
提供 Java、Go、Python、Rust 四种语言的完整订阅消费示例代码。

**Java 示例**：
```java
import com.taosdata.jdbc.tmq.*;
import java.time.Duration;
import java.util.*;

public class TmqConsumerExample {
    public static void main(String[] args) throws Exception {
        // 配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:6041");
        props.setProperty("group.id", "group1");
        props.setProperty("client.id", "client1");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("value.deserializer", "com.taosdata.jdbc.tmq.TMQDeserializer");
        
        // 创建消费者
        try (TmqConsumer<Map<String, Object>> consumer = 
             new TmqConsumer<>(props)) {
            
            // 订阅主题
            consumer.subscribe(Arrays.asList("topic_meters"));
            
            // 消费消息
            while (true) {
                ConsumerRecords<Map<String, Object>> records = 
                    consumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<Map<String, Object>> record : records) {
                    System.out.println("Received: " + record.value());
                }
            }
        }
    }
}
```


**Python 示例**：
```python
from taos.tmq import Consumer

## 41. 配置

conf = {
    "group.id": "group1",
    "client.id": "client1",
    "td.connect.user": "root",
    "td.connect.pass": "taosdata",
    "enable.auto.commit": "true",
}

## 42. 创建消费者

consumer = Consumer(conf)

## 43. 订阅主题

consumer.subscribe(["topic_meters"])

## 44. 消费消息

try:
    while True:
        records = consumer.poll(timeout=1.0)
        if records:
            for record in records:
                print(f"Received: {record.value()}")
finally:
    consumer.close()
```

### 44.1 ** 工具集成**

#### 44.1.1 **TDengine CLI**

**界面说明**：
- 提供 TDengine CLI 使用指南
- 包含常用命令示例
- 提供命令参考表
  
**示例命令**：
```bash

## 45. 连接数据库

taos -h localhost -P 6030 -u root -p taosdata

## 46. 执行 SQL

taos> USE power;
taos> SELECT COUNT(*) FROM meters;

## 47. 执行 SQL 文件

taos -s "source /path/to/script.sql"
```

#### 47.0.1 taosBenchmark

**界面说明**：
- 提供 taosBenchmark 性能测试工具指南
- 包含配置文件示例
- 解释测试结果指标
  
**示例命令**：
```bash

## 48. 默认测试（100 万条记录）

taosBenchmark

## 49. 自定义测试

taosBenchmark -f /path/to/config.json

## 50. 查询性能测试

taosBenchmark -f query.json
```

#### 50.0.1 taosDump

**界面说明**：
提供 taosDump 数据备份恢复工具的使用向导。

**功能介绍**：
taosDump 是 TDengine 的数据备份恢复工具，支持：
- 数据库结构和数据的导出
- 数据的导入恢复
- 支持多线程并发
- 支持数据压缩
  
**安装指南**：
```bash

## 51. Ubuntu/Debian

sudo apt-get install taosdump

## 52. CentOS/RHEL

sudo yum install taosdump

## 53. 从源码编译

git clone https://github.com/taosdata/taos-tools.git
cd taos-tools
mkdir build && cd build
cmake .. -DTOOLS_BUILD_TYPE=Release
make && sudo make install
```


**常见使用场景**：

**1. 导出整个数据库**
```bash
taosdump -h localhost -P 6030 -u root -p taosdata \
  -D power -o /backup/power
```


**2. 导出指定表**
```bash
taosdump -h localhost -D power -t meters,sensors \
  -o /backup/power_tables
```


**3. 导出时间范围内的数据**
```bash
taosdump -D power -S 1609459200000 -E 1640995199000 \
  -o /backup/power_2021
```


**4. 导入数据**
```bash
taosdump -i /backup/power -h localhost
```


**5. 导入到不同数据库**
```bash
taosdump -i /backup/power -h localhost -g power_new
```


**命令参数说明**：

| 参数 | 说明 | 示例 |
| --- | --- | --- |
| `-h` | TDengine 服务器地址 | `-h 192.168.1.100` |
| `-P` | 服务器端口 | `-P 6030` |
| `-u` | 用户名 | `-u root` |
| `-p` | 密码 | `-p taosdata` |
| `-D` | 数据库名称 | `-D power` |
| `-t` | 表名列表（逗号分隔） | `-t meters,sensors` |
| `-o` | 导出目录 | `-o /backup` |
| `-i` | 导入目录 | `-i /backup` |
| `-g` | 导入时重命名数据库 | `-g new_db` |
| `-S` | 开始时间（毫秒时间戳） | `-S 1609459200000` |
| `-E` | 结束时间（毫秒时间戳） | `-E 1640995199000` |
| `-T` | 线程数 | `-T 8` |
| `-N` | 批量插入行数 | `-N 10000` |
| `-c` | 压缩 | `-c` |

#### 53.0.1 永洪 BI 集成

**界面说明**：
引导用户将 TDengine 作为数据源接入永洪 BI。
**集成步骤**：
**步骤1：安装 JDBC 驱动**
1. 下载 TDengine JDBC 驱动
  - 从官网下载：https://docs.taosdata.com/connector/java/
  - 或使用 Maven 依赖：
    ```xml
    <dependency>
        <groupId>com.taosdata.jdbc</groupId>
        <artifactId>taos-jdbcdriver</artifactId>
        <version>3.2.5</version>
    </dependency>
    ```

1. 将 JDBC 驱动复制到永洪 BI 驱动目录
  ```bash
  cp taos-jdbcdriver-3.2.5-dist.jar /path/to/yonghong/drivers/
  ```

**步骤2：配置 JDBC 连接**
在永洪 BI 中配置数据源：
1. 打开永洪 BI 管理后台
2. 进入 "数据源管理"
3. 点击 "新建数据源"
4. 选择 "JDBC 数据源"
5. 填写连接信息：
  - **数据源名称**: TDengine
  - **驱动类**: `com.taosdata.jdbc.TSDBDriver`
  - **JDBC URL**: `jdbc:TAOS://localhost:6030/power?user=root&password=taosdata`
  - **用户名**: root
  - **密码**: taosdata
JDBC URL 格式说明：
```plaintext
jdbc:TAOS://[host]:[port]/[database]?[parameters]
```

常用参数：
- `user`: 用户名
- `password`: 密码
- `charset`: 字符集，默认 UTF-8
- `timezone`: 时区，默认 UTC
- `batchfetch`: 批量获取，默认 false
- `timestampFormat`: 时间戳格式
**步骤3：创建数据集**
1. 在永洪 BI 中创建新数据集
2. 选择刚配置的 TDengine 数据源
3. 选择数据表或编写 SQL 查询
  ```sql
  SELECT ts, AVG(current) AS avg_current, AVG(voltage) AS avg_voltage
  FROM meters
  WHERE ts >= NOW - 7d
  INTERVAL(1h)
  ```

1. 预览数据并保存数据集
**步骤4：制作报告**
1. 创建新报告
2. 从数据集中拖拽字段到报告设计区
3. 配置图表类型：
  - 折线图：展示时间序列数据
  - 柱状图：对比不同设备数据
  - 表格：详细数据展示
  - 仪表盘：实时指标监控
1. 设置筛选器和参数
2. 保存并发布报告

#### 53.0.2 **Grafana 集成**

1. 安装 TDengine 数据源插件
2. 配置数据源连接
3. 导入示例 Dashboard
4. 创建自定义面板

#### 53.0.3 **Power BI 集成**

1. 使用 ODBC 连接器
2. 配置 DSN
3. 连接 TDengine 数据库
4. 创建报表

## 54. 性能

### 54.1 响应时间

- **页面加载**：
  - 首次加载＜ 3秒（含静态资源、用户信息、配置）
  - 页面切换＜ 1秒（SPA 路由切换）
- **API 响应**：
  - 95% 请求 < 500ms
  - 99% 请求 < 1s
  - 超时设置：30s
- **SQL 执行**：
  - 小型查询（< 1万条）＜ 1秒
  - 中型查询（1-10万条）＜ 3秒
  - 大型查询（> 10万条）：分页加载或流式输出

### 54.2 并发性能

- **用户并发**：支持 100+ 用户同时在线
- **API 并发**：
  - 读操作：1000 QPS
  - 写操作：100 TPS
- **数据接入**：单任务 10,000+ 点/秒，50+ 任务并发

### 54.3 资源使用

- **内存**：
  - 基础运行：< 512MB
  - 正常负载：< 2GB
  - 峰值负载：< 4GB
- **CPU**：
  - 空闲时：< 5%
  - 正常负载：< 50%
  - 峰值负载：< 80%

### 54.4 性能优化策略

1. **前端优化**：
  - 代码分割和懒加载
  - 静态资源 CDN 加速
  - 缓存策略（用户信息、配置信息）
1. **后端优化**：
  - API 响应缓存（Redis）
  - 数据库连接池
  - 批量操作优化
1. **数据库优化**：
  - SQL 查询优化
  - 索引优化
  - 分页查询

## 55. **安全**

### 55.1 **认证与授权**

#### 55.1.1 **Token 机制**

- 使用 Token 进行身份验证
- Token 有效期：默认 2 小时，可配置
- 支持 Token 刷新机制（Refresh Token）
- Token 存储：HttpOnly Cookie + LocalStorage

#### 55.1.2 **密码安全**

- 密码加密：Bcrypt 算法（cost factor = 10）
- 密码复杂度要求：
  - 最小长度 8 位
  - 必须包含大写字母、小写字母、数字、特殊字符
- 密码错误锁定：5 次失败后锁定 15 分钟

#### 55.1.3 **权限控制 (RBAC)**

- 角色定义：
  - **admin**：管理员，拥有所有权限
  - **user**：普通用户，只能访问自己的资源
  - **readonly**：只读用户，只能查询数据
  - **operator**：运维人员，可管理任务和监控
- 每个 API 请求进行权限检查

#### 55.1.4 **OAuth 2.0/OIDC 安全**

- 使用 PKCE (Proof Key for Code Exchange) 增强安全性
- client_secret 加密存储 (AES-256-GCM)
- 验证 state 参数防止 CSRF 攻击
- 验证 ID Token 签名和有效期

### 55.2 **数据安全**

#### 55.2.1 **传输加密**

- 生产环境建议使用 HTTPS (TLS 1.2+)
- WebSocket 连接使用 WSS 协议
- 支持 HTTP Strict Transport Security (HSTS)

#### 55.2.2 **存储加密**

- 敏感配置加密存储：
  - 数据库密码
  - OAuth client_secret
  - API 密钥
- 加密算法：AES-256-GCM
- 密钥管理：环境变量或密钥管理服务

#### 55.2.3 **输入验证**

- SQL 注入防护：使用参数化查询
- XSS 防护：
  - Content Security Policy (CSP)
  - 输入过滤和转义
  - 输出编码
- CSRF 防护：CSRF Token 验证
- 文件上传限制：
  - 文件类型白名单
  - 文件大小限制（100MB）

### 55.3 **审计日志**

#### 55.3.1 **记录范围**

- 用户登录/登出
- 用户创建/删除/修改
- 权限变更
- 数据库/表删除
- 任务创建/删除/启停
- 配置修改

#### 55.3.2 **日志保留**

- 默认保留 90 天
- 支持导出到外部系统（Syslog、Elasticsearch）
- 普通用户无权删除或修改审计日志

## 56. **兼容性**

### 56.1 **浏览器兼容**

- Chrome/Edge 90+
- Firefox 88+
- Safari 14+
- 不支持 IE 11 及以下版本
- 最低屏幕分辨率：1280x720

### 56.2 **TDengine 版本兼容**

- 支持 TDengine 3.0.0+
- 不兼容 TDengine 2.x 版本
- 支持社区版、企业版、云服务版

### 56.3 **操作系统兼容**

- Linux：Ubuntu 18.04+, CentOS 7+, RHEL 7+
- Windows：Windows Server 2016+, Windows 10+
- macOS：macOS 10.15+

### 56.4 **API 兼容性**

- REST API 版本：v1 (当前版本)
- 保证同一主版本内 API 向后兼容
- 不兼容变更会通过主版本号更新（v1 -> v2）

### 56.5 **数据迁移**

- 支持从 TDengine Explorer 1.x 迁移到 2.x
- 提供数据迁移工具
- 配置文件自动转换

## 57. **运维**

### 57.1 **部署方式**

#### 57.1.1 **单机部署**

随 TSDB 部署。
```bash {wrap}
systemctl start taos-explorer
```

社区版支持单独部署，从 https://tdengine.com/downloads/?product=TDengine+TSDB+Explorer-OSS 下载后安装即可。

#### 57.1.2 **Docker / Kubernetes 部署**

支持 随 TSDB 进行 Docker/Kubernetes 部署。

### 57.2 **监控告警**

#### 57.2.1 **Prometheus 指标**

```plaintext

## 58. 访问 Prometheus metrics 端点

http://localhost:6060/metrics
```

### 58.1 **日志管理**

#### 58.1.1 **日志级别**

- **TRACE**：最详尽的交互信息，包含底层协议消息
- **DEBUG**：调试信息，包含详细的请求/响应
- **INFO**：正常业务日志，如用户登录、任务启动
- **WARN**：警告信息，如数据库连接慢
- **ERROR**：错误信息，如请求失败、异常

#### 58.1.2 **日志格式**

采用标准日志格式。

#### 58.1.3 **日志轮转**

- 按大小轮转：单文件 100MB
- 保留数量：10 个备份文件
- 压缩备份：自动 gzip 压缩

### 58.2 **升级维护**

#### 58.2.1 平滑升级

```bash

## 1. 备份数据和配置

cp -r /var/lib/taos/explorer /backup/explorer-$(date +%Y%m%d)
cp /etc/taos/explorer.toml /backup/

## 2. 下载新版本

..

## 3. 停止旧版本后升级

./install.sh

## 4. 启动新版本

systemctl start explorer

## 5. 验证升级

xdg-open http://localhost:6060
```

#### 58.2.2 回滚策略

```bash

## 1. 停止当前版本

systemctl stop explorer

## 2. 恢复备份

cp -r /backup/explorer-20260128 /var/lib/taos/explorer
cp /backup/explorer.toml /etc/taos/

## 3. 启动旧版本

systemctl start explorer
```

## 59. 使用场景

### 59.1 Use Case 1: 工业数据监控

**场景描述**：
- 制造企业需要实时监控生产线设备数据
- 数据来源：OPC UA 协议的 PLC 设备
- 需求：配置数据接入、查询历史数据、创建 Grafana 看板
**操作流程**：
1. 登录 Explorer，创建数据库 `factory`
2. 创建超级表 `sensors` 存储设备数据
3. 创建 OPC UA 接入任务，配置设备地址和节点
4. 启动任务，查看监控面板确认数据写入
5. 使用 SQL 浏览器查询分析数据
6. 配置 Grafana 数据源，创建实时监控看板

### 59.2 Use Case 2: IoT 设备管理

**场景描述**：
- 智能家居公司管理千万级 IoT 设备
- 数据来源：MQTT 协议
- 需求：数据接入、设备状态查询、异常告警
  
**操作流程**：
1. 创建数据库 `iot`，设置保留期 365 天
2. 创建超级表 `devices` 存储设备遥测数据
3. 配置 MQTT 订阅任务，订阅 topic `devices/+/telemetry`
4. 配置数据映射规则，将 JSON 数据映射到表结构
5. 创建流计算任务，实时检测设备异常
6. 使用数据订阅功能，将异常数据推送到告警系统

### 59.3 Use Case 3: 开发者快速上手

**场景描述**：
- 开发者首次使用 TDengine，需要快速学习和上手
- 需求：了解 TDengine 特性、查看示例代码、测试 API
**操作流程**：
1. 访问 Explorer，注册账号并登录
2. 浏览 Dashboard，了解集群状态
3. 使用 SQL 浏览器创建测试数据库和表
4. 查看“编程接口”页面，选择熟悉的编程语言
5. 复制示例代码，在本地运行测试
6. 查看 API 文档，在线测试 REST API
7. 下载 taosBenchmark，进行性能测试

### 59.4 Use Case 4: 企业 SSO 集成

**场景描述**：
- 企业已有 Keycloak 身份认证系统
- 需求：将 Explorer 集成到现有 SSO 系统
**操作流程**：
1. 在 Keycloak 创建客户端 `tdengine-explorer`
2. 配置 Redirect URI 为 `https://explorer.company.com/api/-/auth/callback`
3. 获取 client_id 和 client_secret
4. 修改 Explorer 配置文件，启用 OAuth
5. 重启 Explorer 服务
6. 访问登录页，点击“使用 SSO 登录”
7. 跳转到 Keycloak，使用企业账号登录
8. 自动跳转回 Explorer，登录成功

## 60. 约束和限制

### 60.1 约束条件

1. **TDengine 版本**：必须使用 TDengine 3.0.0 或以上版本
2. **浏览器要求**：必须使用现代浏览器，不支持 IE 11 及以下
3. **HTTPS 要求**：生产环境必须启用 HTTPS
4. **OAuth 要求**：IdP 必须支持 OIDC 协议
5. **网络连接**：Explorer 必须能访问 TDengine 服务端 REST API

### 60.2 限制

1. **查询结果**：
  - 单次查询最多显示 10,000 条记录（页面显示）
  - 导出最多 100,000 条记录
  - 建议使用 LIMIT 和分页查询
1. **文件上传**：
  - CSV 文件大小限制：100MB
  - 任务配置文件大小限制：10MB
1. **并发限制**：
  - 单用户并发请求数：10/秒
  - 同时运行的数据接入任务：50 个
1. **SQL 执行**：
  - 单条 SQL 超时时间：30 秒
  - 不支持执行多条 SQL（分号分隔）
1. **数据接入**：
  - OPC UA 节点数限制：1000 个/任务
  - MQTT topic 数限制：100 个/任务

## 61. 常见错误和排查

### 61.1 错误 1：登录失败

**错误信息**：`Connection refused` 或 `Network error`
**原因分析**：
- TDengine 服务未启动
- 网络不可达
- 配置的 TDengine 地址错误
**排查步骤**：
```bash

## 1. 检查 TDengine 服务状态

systemctl status taosd

## 2. 测试 TDengine REST API

curl -u root:taosdata http://localhost:6041/rest/sql -d "SHOW DATABASES"

## 3. 检查 Explorer 配置

cat /etc/taos/explorer.toml | grep -A 5 "\[database\]"

## 4. 查看 Explorer 日志

tail -f logs/explorer.log
```

### 61.2 错误 2：数据接入任务失败

**错误信息**：`Task execution failed` 或 `Connection timeout`
**原因分析**：
- 数据源地址不可达
- 认证信息错误
- 网络防火墙阻断
- 目标表不存在
**排查步骤**：
```bash

## 1. 测试数据源连接（OPC UA 示例）

telnet 192.168.1.100 4840

## 2. 查看任务日志

curl -H "Authorization: Bearer <token>" \
  http://localhost:6060/api/x/tasks/{task_id}/logs

## 3. 检查目标表是否存在

taos> DESCRIBE industrial.sensors;

## 4. 验证数据映射配置

## 62. 确认源字段类型与目标字段类型兼容

```

### 62.1 错误 3：OAuth 登录失败

**错误信息**：`Invalid token` 或 `Authentication failed`
**原因分析**：
- OAuth 配置错误
- Redirect URI 不匹配
- client_secret 错误
- IdP 服务异常
**排查步骤**：
```bash

## 1. 检查 OAuth 配置

cat /etc/taos/explorer.toml | grep -A 10 "\[oauth\]"

## 2. 验证 OIDC 配置

curl https://auth.example.com/realms/tdengine/.well-known/openid-configuration

## 3. 检查 Redirect URI

## 63. 确认 IdP 中配置的 Redirect URI 与 Explorer 中的一致

## 4. 查看详细错误信息

tail -f logs/explorer.log | grep "oauth"
```

### 63.1 错误 4：查询慢或超时

**错误信息**：`Query timeout` 或查询响应很慢
**原因分析**：
- 查询数据量过大
- 未使用索引或时间范围
- TDengine 资源不足
**优化建议**：
```sql
-- 使用时间过滤
SELECT * FROM meters WHERE ts >= NOW - 1h LIMIT 1000;

-- 使用聚合查询减少返回数据量
SELECT _wstart, AVG(current) FROM meters 
WHERE ts >= NOW - 1d 
INTERVAL(10m);

-- 使用 tag 索引
SELECT * FROM meters WHERE groupid = 2 AND ts >= NOW - 1h;

-- COMPACT
COMPACT DATABASE database1;
```

## 64. 可观测性

### 64.1 taos shell

**无影响**。Explorer 与 taos shell 是独立的工具，不会相互影响。

### 64.2 TDinsight

**无直接影响**。但 Explorer 提供了 Grafana 集成指南，可以帮助用户更方便地配置 TDinsight。

### 64.3 taos Explorer

**本文档就是针对 TDengine Explorer 的功能设计**。主要的行为变化和功能均在本文档中描述。
