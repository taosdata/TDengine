# pSpace Plugin 测试

## 前提条件

- JDK 8+ 已安装
- Maven 3.6+ 已安装
- （集成测试需要）可访问的 pSpace 服务实例

## 1. 构建

```bash
cd plugins/pspace
mvn clean package -DskipTests
```

## 2. 单元测试（无需外部服务）

```bash
mvn test
```

单元测试不依赖 pSpace 服务，覆盖配置解析、数据映射等纯逻辑。

## 3. 集成测试（需要 pSpace 服务）

### 3.1. 打印版本

```bash
java -jar target/taosx-pspace.jar -V
```

预期输出示例：

```
version: 1.11.0 (core-1.11.0 debug)
git: 7ecbefe5e307597640f3a16f1e389733c0b34ed6
build: macos-x86_64 2026-01-20 18:35:19 +08:00
```

### 3.2. 验证连接

```bash
java -jar target/taosx-pspace.jar -m check -c ./example/check.toml
```

预期输出示例：

```JSON
{
  "valid": true,
  "support": true,
  "data_source": "pspace",
  "version": "7.1"
}
```

### 3.3 查询节点

```bash
java -jar target/taosx-pspace.jar -m nodes -c ./example/nodes.toml
```

配置文件：[./example/nodes.toml](../../example/nodes.toml)

预期输出示例：

```JSON
[
  {
    "id": 150016,
    "name": "北京",
    "long_name": "\\北京",
    "is_leaf": false
  },
  {
    "id": 150028,
    "name": "上海",
    "long_name": "\\上海",
    "is_leaf": true
  }
]
```

### 3.4 查询数据点

```bash
java -jar target/taosx-pspace.jar -m points -c ./example/points.toml
```

配置文件：[./example/points.toml](../../example/points.toml)

预期输出示例：

```JSON
[
  {
    "id": 150019,
    "name": "气温",
    "type": "PS_ANALOG",
    "long_name": "\\北京\\朝阳\\气温",
    "desc": ""
  },
  {
    "id": 150021,
    "name": "气温",
    "type": "PS_ANALOG",
    "long_name": "\\北京\\朝阳\\望京\\气温",
    "desc": ""
  },
  {
    "id": 150023,
    "name": "气温",
    "type": "PS_ANALOG",
    "long_name": "\\北京\\朝阳\\酒仙桥\\气温",
    "desc": ""
  }
]
```

### 3.5 历史查询（Query, local_only）

创建配置文件 `pspace-query-local.toml`：

```toml
[connection]
server = "192.168.2.149"
port = 8889
username = "admin"
password = "admin888"

[nodes]
root = 150016

[points]
# name_filter = "\\北京\\朝阳\\*气温*"
include_data_type = true
point_ids = [150019]

[run]
mode = "Query"
start_time = "2026-03-01T00:00:00+08:00"
end_time = "2026-03-01T05:00:00+08:00"
time_window = 3600
time_excursion = 0

[report]
local_only = true

[advanced_options]
log_level = "info"
keep_raw_data = true
keep_raw_data_days = 7
keep_raw_data_dir = "./raw_data"
```

运行：

```bash
java -jar target/taosx-pspace.jar -m run -c pspace-query-local.toml
```

- `local_only = true`，不需要 taosX 运行
- `keep_raw_data = true`，数据写入 `./raw_data/` 目录
- 检查输出文件确认数据格式正确

### 3.6 实时订阅测试（Subscribe, local_only）

创建配置文件 `pspace-subscribe-local.toml`：

```toml
[connection]
server = "192.168.2.149"
port = 8889
username = "admin"
password = "admin888"

[nodes]
root = 150016

[points]
name_filter = "\\北京\\朝阳\\*气温*"
include_data_type = true

[run]
mode = "Subscribe"

[report]
local_only = true

[advanced_options]
log_level = "info"
keep_raw_data = true
keep_raw_data_days = 7
keep_raw_data_dir = "./raw_data"
```

运行：

```bash
java -jar target/taosx-pspace.jar -m run -c pspace-subscribe-local.toml
```

- `local_only = true`，不需要 taosX 运行
- 观察日志中是否持续收到推送数据
- `keep_raw_data = true`，检查 `./raw_data/` 目录确认输出
- `Ctrl+C` 停止

### 3.7 QuerySync 测试（local_only）

创建配置文件 `pspace-querysync-local.toml`：

```toml
[connection]
server = "192.168.2.149"
port = 8889
username = "admin"
password = "admin888"

[nodes]
root = 150016

[points]
# name_filter = "\\北京\\朝阳\\*气温*"
include_data_type = true
point_ids = [150019, 150021, 150023]

[run]
mode = "QuerySync"
start_time = "2026-03-03T00:00:00+08:00"
time_window = 86400
time_excursion = 0
query_interval = 10

[report]
local_only = true

[advanced_options]
log_level = "info"
keep_raw_data = true
keep_raw_data_days = 7
keep_raw_data_dir = "./raw_data"
```

运行：

```bash
java -jar target/taosx-pspace.jar -m run -c pspace-querysync-local.toml
```

- `local_only = true`，不需要 taosX 运行
- Phase 1：历史回填，日志显示窗口查询进度
- Phase 2：持续同步，每 10 秒查询一次新数据
- `keep_raw_data = true`，检查 `./raw_data/` 目录确认输出
- `Ctrl+C` 停止

## 4. 测试矩阵

| 测试级别 | 外部依赖 | 命令                                                            | 耗时     |
| -------- | -------- | --------------------------------------------------------------- | -------- |
| 单元测试 | 无       | `mvn test`                                                      | < 30s    |
| 连接检查 | pSpace   | `-m check -c ./example/check.toml`                              | < 5s     |
| 节点查询 | pSpace   | `-m nodes -c ./example/nodes.toml`                              | < 5s     |
| 点位查询 | pSpace   | `-m points -c ./example/points.toml`                            | < 5s     |
| 历史查询 | pSpace   | `-m run -c pspace-query-local.toml` (Query, local_only)         | 视数据量 |
| 实时订阅 | pSpace   | `-m run -c pspace-subscribe-local.toml` (Subscribe, local_only) | 手动停止 |
| 查询同步 | pSpace   | `-m run -c pspace-querysync-local.toml` (QuerySync, local_only) | 手动停止 |

## 6. 故障排查

| 问题               | 排查方向                                                   |
| ------------------ | ---------------------------------------------------------- |
| `pSpace 连接失败`  | 检查 server/port，`telnet 192.168.2.149 8889`              |
| `认证失败`         | 确认 username/password 正确                                |
| `点位查询返回空`   | 检查 `nodes.root` 是否有效，`name_filter` 匹配范围是否过窄 |
| `查询返回空数据`   | 确认时间范围内有数据，先用 pSpace 客户端工具查看           |
| `rawdata 文件为空` | 确认 `keep_raw_data = true`，检查目录写入权限              |
| `OutOfMemoryError` | 减小 `time_window`，或增大 JVM 堆内存 `-Xmx2g`             |
