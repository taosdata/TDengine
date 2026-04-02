# pSpace Plugin 开发指南

## 概述

taosx-pspace 是 taosX 的 Java 外部插件，用于从 pSpace 实时数据库采集数据并写入 TDengine。

支持三种数据采集模式：

- **Query**：一次性历史数据迁移
- **Subscribe**：实时数据订阅同步
- **QuerySync**：历史回填 + 持续轮询同步

详细设计见 [design/pspace-run-modes.md](design/pspace-run-modes.md)

## 前置依赖

| 依赖     | 版本要求 | 说明                                         |
| -------- | -------- | -------------------------------------------- |
| JDK      | 8+       | 编译和运行                                   |
| Maven    | 3.6+     | 构建管理                                     |
| pSpace   | —        | 需要可访问的 pSpace 服务实例                 |
| TDengine | 3.0+     | 数据写入目标（使用 `local_only` 模式可跳过） |
| taosX    | —        | 插件宿主进程（使用 `local_only` 模式可跳过） |

## 构建

```bash
cd plugins/pspace
mvn clean package -DskipTests

# 构建产物（fat jar）
ls target/taosx-pspace-*-jar-with-dependencies.jar

# 复制到插件目录（供 taosX 调用）
cp target/taosx-pspace.jar ./
```

## CLI 命令模式

插件通过 `-m` 参数指定命令模式，`-c` 指定 TOML 配置文件：

```bash
java -jar taosx-pspace.jar -c pspace.toml -m <mode>
```

| 模式     | 说明                                              |
| -------- | ------------------------------------------------- |
| `check`  | 检查连接配置是否有效，输出 JSON 结果到 stdout     |
| `nodes`  | 查询 pSpace 节点树，输出 JSON 到 stdout           |
| `points` | 查询数据点位列表，输出 JSON 到 stdout             |
| `run`    | 执行数据采集任务（Query / Subscribe / QuerySync） |

## 配置文件

插件通过 TOML 配置文件驱动，包含以下配置节：

```toml
# pSpace 连接配置
[connection]
server = "192.168.2.149"
port = 8889
username = "admin"
password = "admin888"

# 节点配置
[nodes]
root = 100001

# 点位配置
[points]
name_filter = "tag_*"           # 可选，按名称过滤
include_data_type = true        # 可选，获取数据类型信息
# point_ids = [150019, 150021]  # 可选，直接指定点位 ID

# 运行模式配置
[run]
mode = "Query"                  # Query | Subscribe | QuerySync
start_time = "2025-01-01T00:00:00Z"
end_time = "2025-06-01T00:00:00Z"
time_window = 86400             # 秒，默认 1 天
time_excursion = 0              # 秒，乱序偏移
query_interval = 10             # 秒，QuerySync 轮询间隔

# 上报配置
[report]
remote = "127.0.0.1:6055"      # taosX IPC 地址
keep_raw_data = false           # 是否保留原始数据到本地文件
local_only = false              # 仅写本地，不连接 taosX
```

各配置项详细说明：

- 运行模式和参数：[design/pspace-run-modes.md](design/pspace-run-modes.md)
- 点位获取规则：[design/pspace-points.md](design/pspace-points.md)
- 数据映射：[design/pspace-data-mapping.md](design/pspace-data-mapping.md)
- 高级功能：[design/pspace-advanced.md](design/pspace-advanced.md)

## 项目结构

```
plugins/pspace/
├── pom.xml                              # Maven 构建配置
├── pspace.toml                          # 示例配置文件
├── src/main/java/com/taosdata/taosx/
│   ├── TaosXpSpaceMain.java            # CLI 入口，模式分发
│   └── pspace/
│       ├── Check.java                   # check 模式：连接检查
│       ├── Nodes.java                   # nodes 模式：节点树查询
│       ├── Points.java                  # points 模式：点位查询
│       ├── Point.java                   # 点位数据结构
│       ├── Node.java                    # 节点数据结构
│       ├── ServerInfo.java              # 服务信息
│       ├── VersionProvider.java         # 版本号提供
│       ├── config/
│       │   ├── Configuration.java       # 顶层配置（TOML 解析）
│       │   ├── CommandMode.java         # CLI 模式枚举
│       │   ├── Connection.java          # [connection] 配置
│       │   ├── NodesConfig.java         # [nodes] 配置
│       │   ├── PointsConfig.java        # [points] 配置
│       │   ├── RunConfig.java           # [run] 配置
│       │   ├── ReportConfig.java        # [report] 配置
│       │   └── TimeUtils.java           # 时间解析工具
│       ├── run/
│       │   ├── QueryTask.java           # Query 模式实现
│       │   ├── QuerySyncTask.java       # QuerySync 模式实现
│       │   └── SubscribeTask.java       # Subscribe 模式实现
│       ├── arrow/
│       │   ├── PSpaceArrowWriter.java   # Arrow IPC 序列化
│       │   └── ArrowInitDto.java        # Arrow Schema 初始化
│       ├── netty/
│       │   ├── PSpaceNettyClient.java   # Netty TCP 客户端
│       │   └── ArrowEncoder.java        # Arrow 编码器
│       └── query/
│           └── PSpaceQueryExecutor.java # 历史查询执行器
├── docs/
│   └── dev/
│       ├── README.md                    # 本文档
│       ├── TEST_QUICKSTART.md           # 测试快速入门
│       └── design/                      # 设计文档
└── target/
```

## 设计文档索引

| 文档                                                    | 内容                                 |
| ------------------------------------------------------- | ------------------------------------ |
| [pspace-run-modes.md](design/pspace-run-modes.md)       | 三种运行模式总览及配置参数           |
| [pspace-query.md](design/pspace-query.md)               | Query / QuerySync SDK 调用与分片算法 |
| [pspace-subscribe.md](design/pspace-subscribe.md)       | Subscribe 实时订阅 SDK 调用          |
| [pspace-data-mapping.md](design/pspace-data-mapping.md) | pSpace → Arrow Schema 字段映射       |
| [pspace-points.md](design/pspace-points.md)             | 点位列表获取规则                     |
| [pspace-advanced.md](design/pspace-advanced.md)         | rawdata 保留、local_only、类型分组   |

## 调试

### 独立测试（不连接 taosX）

在配置中设置 `local_only = true`，插件将跳过 Netty 连接，仅将数据写入本地文件：

```toml
[report]
local_only = true
keep_raw_data = true
keep_raw_data_dir = "./output"
```

### 日志级别

通过 log4j2 配置调整日志级别：

```bash
java -Dlog4j2.level=DEBUG -jar taosx-pspace.jar -c pspace.toml -m run
```

### 常见问题

| 问题                   | 排查方向                                             |
| ---------------------- | ---------------------------------------------------- |
| 连接 pSpace 失败       | 检查 server/port，确认网络可达：`telnet <ip> <port>` |
| 点位查询返回空         | 检查 `nodes.root` 是否正确，`name_filter` 是否过严   |
| 历史查询无数据         | 确认 `start_time`/`end_time` 范围内确实存在数据      |
| Netty 连接 taosX 失败  | 检查 taosX 是否运行，`report.remote` 地址是否正确    |
| 数据写入 TDengine 失败 | 检查 taosX 日志，确认 IPC 通道正常                   |
