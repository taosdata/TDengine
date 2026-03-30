# taosgen 日志路径参数 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-13 | 2026-03-13 | 1.0 | 裴亚明 | 初始版本，支持通过参数指定日志输出路径 |

## 2. 背景

### 2.1 问题描述

当前 taosgen 程序的日志输出固定在工作目录的 `log/taosgen.log` 文件中，存在以下问题：
1. **灵活性不足**：用户无法自定义日志输出位置，在某些部署场景下（如容器化环境、多实例部署）需要将日志输出到特定目录
2. **权限问题**：在某些受限环境中，当前工作目录可能没有写权限，导致日志创建失败
3. **日志管理困难**：无法将日志统一输出到系统日志目录（如 `/var/log`），不便于集中管理和监控

### 2.2 目标

本功能旨在：
1. 支持通过命令行参数 `--log-dir` 和 `--log-file` 指定日志输出路径
2. 支持通过配置文件参数 `log_dir` 和 `log_file` 指定日志输出路径
3. 保持向后兼容，未指定参数时使用默认路径 `./log/taosgen.log`
4. 提供清晰的错误提示，当日志路径无效时及时报错

## 3. 定义

| 术语 | 定义 |
| --- | --- |
| 日志目录 | 存放日志文件的目录路径，可以是相对路径或绝对路径 |
| 日志文件 | 完整的日志文件路径，包括目录和文件名 |
| 控制台日志 | 仅输出到标准输出（stdout）和标准错误（stderr）的日志，不写入文件 |

## 4. 行为说明

### 4.1 命令行参数

新增两个命令行参数用于指定日志输出路径：

#### 4.1.1 --log-dir 参数

**语法：**
```bash
taosgen --log-dir=<directory_path>
taosgen -d <directory_path>
```

**说明：**
- 指定日志输出目录，日志文件名固定为 `taosgen.log`
- 支持相对路径和绝对路径
- 如果目录不存在，程序会自动创建（包括父目录）
- 默认值：`./log`
**示例：**
```bash

## 5. 使用绝对路径

taosgen --log-dir=/var/log/taosgen --config-file=test.yaml

## 6. 使用相对路径

taosgen --log-dir=./logs --config-file=test.yaml

## 7. 使用短参数

taosgen -d /tmp/taosgen-logs -c test.yaml
```

#### 7.0.1 --log-file 参数

**语法：**
```bash
taosgen --log-file=<file_path>
taosgen -f <file_path>
```

**说明：**
- 指定完整的日志文件路径（包括目录和文件名）
- 支持相对路径和绝对路径
- 优先级高于 `--log-dir` 参数
- 如果父目录不存在，程序会自动创建
- 无默认值
**示例：**
```bash

## 8. 指定完整路径

taosgen --log-file=/var/log/taosgen/benchmark.log --config-file=test.yaml

## 9. 使用相对路径

taosgen --log-file=./output/test.log --config-file=test.yaml

## 10. 使用短参数

taosgen -f /tmp/my-test.log -c test.yaml
```

### 10.1 YAML 配置文件参数

全局作用域中，增加参数 log_dir、log_file

#### 10.1.1 log_dir 参数

- log_dir（字符串）：指定日志输出目录，默认值为 `log/`。仅设置 log_dir 时，日志文件将写入 `<log_dir>/taosgen.log`。

#### 10.1.2 log_file 参数

- log_file（字符串）：指定完整的日志文件路径，设置后将覆盖 log_dir。

### 10.2 参数优先级

日志路径的确定遵循以下优先级（从高到低）：
1. `--log-file`（命令行）
2. `--log-dir`（命令行）
3. `log_file`（YAML 配置文件）
4. `log_dir`（YAML 配置文件）
5. 默认值：`log/taosgen.log`
**示例：**
```bash

## 11. 同时指定两个参数，--log-file 优先

taosgen --log-dir=/var/log --log-file=/tmp/test.log -c test.yaml

## 12. 实际日志路径：/tmp/test.log

## 13. 只指定 --log-dir

taosgen --log-dir=/var/log -c test.yaml

## 14. 实际日志路径：/var/log/taosgen.log

## 15. 都不指定

taosgen -c test.yaml

## 16. 实际日志路径：./log/taosgen.log

```

### 16.1 错误处理

#### 16.1.1 无效的日志路径

当指定的日志路径无法创建或写入时，程序会报错并退出。
**错误场景：**
- 父目录无法创建（权限不足）
- 文件路径指向一个目录
- 文件路径指向系统保护的位置（如 `/proc`）
**错误示例：**
```bash
$ taosgen --log-file=/proc/test.log -c test.yaml
2026-03-13 10:00:00.123456 12345 10:00:00 Parsed create-super-table action: meters
2026-03-13 10:00:00.123457 12345 ERROR Failed to initialize log file: Invalid log file path '/proc/test.log': Cannot open log file: /proc/test.log
```

#### 16.1.2 目录创建失败

当日志目录无法创建时，程序会报错并退出。
**错误示例：**
```bash
$ taosgen --log-dir=/root/protected/logs -c test.yaml
2026-03-13 10:00:00.123456 12345 ERROR Failed to initialize log file: Failed to create log directory '/root/protected/logs': Permission denied
```

**错误码：**

| 错误类型 | 异常类型 | 错误信息格式 | 说明 |
| --- | --- | --- | --- |
| 目录创建失败 | `std::runtime_error` | `Failed to create log directory '<path>': <reason>` | 无法创建日志目录 |
| 文件路径无效 | `std::runtime_error` | `Invalid log file path '<path>': <reason>` | 无法打开或写入日志文件 |

## 17. 性能

本功能对性能的影响：**无**
**理由：**
1. **日志初始化**：日志系统的初始化只在程序启动时执行一次，对整体性能无影响
2. **日志写入**：日志写入机制未改变，仍使用 spdlog 的异步日志，性能特性保持不变
3. **参数解析**：新增的参数解析逻辑非常轻量，仅涉及字符串比较和路径拼接，耗时可忽略不计

## 18. 安全

**风险：** 用户可能通过命令行参数指定恶意路径，尝试覆盖系统文件或访问敏感目录。
**防护措施：**
1. **路径验证**：在创建日志文件前，尝试打开文件进行验证，如果失败则报错退出
2. **权限检查**：依赖操作系统的文件权限机制，无权限的路径会自动失败
3. **错误提示**：清晰的错误信息帮助用户快速定位问题，避免误操作

## 19. 兼容性

### 19.1 向后兼容

**完全兼容**
- 未指定 `--log-dir` 或 `--log-file` 参数时，行为与旧版本完全一致，日志输出到 `./log/taosgen.log`
- 所有现有的命令行参数、配置文件格式保持不变
- 日志文件格式保持不变

### 19.2 升级说明

从旧版本升级到新版本：
1. **无需修改配置**：现有的配置文件和启动脚本无需修改
2. **可选升级**：如果需要自定义日志路径，添加 `--log-dir` 或 `--log-file` 参数即可
3. **无数据迁移**：日志文件格式未变，无需迁移历史日志

## 20. 使用场景

### 20.1 场景一：开发环境快速测试

**描述：** 开发人员在本地进行快速测试，使用默认日志路径。
**操作：**
```bash
cd /path/to/taosgen
./taosgen --config-file=test.yaml
```

**结果：**
- 日志输出到 `./log/taosgen.log`
- 控制台同时显示日志

### 20.2 场景二：生产环境部署

**描述：** 在生产服务器上部署 taosgen，将日志输出到系统日志目录。
**操作：**
```bash

## 21. 创建日志目录

sudo mkdir -p /var/log/taosgen
sudo chown taosgen:taosgen /var/log/taosgen

## 22. 运行 taosgen

taosgen --log-dir=/var/log/taosgen --config-file=/etc/taosgen/prod.yaml
```

**结果：**
- 日志输出到 `/var/log/taosgen/taosgen.log`
- 便于集中管理和监控

### 22.1 场景三：容器化部署

**描述：** 在 Docker 容器中运行 taosgen，将日志输出到挂载的卷。
**操作：**
```bash
docker run -v /host/logs:/var/log/taosgen \
           -v /host/config:/etc/taosgen \
           taosgen:latest \
           --log-dir=/var/log/taosgen \
           --config-file=/etc/taosgen/config.yaml
```

**结果：**
- 日志输出到宿主机的 `/host/logs/taosgen.log`
- 容器重启后日志不丢失

## 23. 约束和限制

### 23.1 约束

1. **路径长度**：日志文件路径长度不能超过操作系统限制（Linux 通常为 4096 字符）
2. **文件系统**：日志目录所在的文件系统必须支持文件创建和写入操作
3. **权限要求**：运行 taosgen 的用户必须对日志目录有写权限
4. **磁盘空间**：日志目录所在的磁盘必须有足够的可用空间

### 23.2 限制

1. **不支持远程日志**：`--log-file` 参数不支持网络路径（如 NFS、SMB），只支持本地文件系统
2. **不支持日志分割策略自定义**：日志轮转的大小（5MB）和保留数量（3个）目前是硬编码的，不支持通过参数配置
3. **不支持日志格式自定义**：日志格式固定，不支持通过参数修改
4. **不支持多个日志文件**：一个 taosgen 进程只能输出到一个日志文件
5. **路径中的特殊字符**：建议避免在路径中使用特殊字符（如空格、引号），虽然程序支持，但可能在某些 shell 环境中引起问题

## 24. 常见错误和排查

### 24.1 错误：日志文件创建失败

**错误信息：**
```plaintext
Failed to initialize log file: Failed to create log directory '/var/log/taosgen': Permission denied
```

**原因：**
- 当前用户对指定目录没有写权限
**排查步骤：**
1. 检查目录权限：`ls -ld /var/log/taosgen`
2. 检查当前用户：`whoami`
3. 检查父目录权限：`ls -ld /var/log`
**解决方案：**
```bash

## 25. 方案 1：修改目录权限

sudo chown $USER:$USER /var/log/taosgen

## 26. 方案 2：使用有权限的目录

taosgen --log-dir=/tmp/taosgen --config-file=test.yaml

## 27. 方案 3：使用 sudo 运行（不推荐）

sudo taosgen --log-dir=/var/log/taosgen --config-file=test.yaml
```

### 27.1 错误：日志文件路径无效

**错误信息：**
```plaintext
Failed to initialize log file: Invalid log file path '/proc/test.log': Cannot open log file: /proc/test.log
```

**原因：**
- 指定的路径指向系统保护的位置
- 路径指向一个目录而不是文件
**排查步骤：**
1. 检查路径是否存在：`ls -l /proc/test.log`
2. 检查是否是目录：`file /proc/test.log`
**解决方案：**
```bash

## 28. 使用正确的路径

taosgen --log-file=/tmp/test.log --config-file=test.yaml
```

### 28.1 错误：磁盘空间不足

**错误信息：**
```plaintext
[*** LOG ERROR #0001 ***] [2026-03-13 10:00:00] [taosgen_logger] async flush: No space left on device
```

**原因：**
- 日志目录所在磁盘空间已满
**排查步骤：**
1. 检查磁盘使用情况：`df -h /var/log`
2. 检查日志目录大小：`du -sh /var/log/taosgen`
3. 查找大文件：`find /var/log/taosgen -type f -size +100M`
**解决方案：**
```bash

## 29. 方案 1：清理旧日志

rm /var/log/taosgen/taosgen.log.*

## 30. 方案 2：移动日志到其他磁盘

taosgen --log-dir=/mnt/data/taosgen --config-file=test.yaml

## 31. 方案 3：配置 logrotate

sudo vi /etc/logrotate.d/taosgen
```

## 32. 安装和卸载

不涉及/保持不变

## 33. 文档

### 33.1 企业版文档

**需要修改：** 否

### 33.2 官网文档

**需要修改：** 是
