# taosBackup 与 taosdump 参数对比

## 概要

| 对比项 | taosdump | taosBackup |
| --- | --- | --- |
| 存储格式 | Avro 行存储 | 私有二进制列存（.dat, zstd）/ Parquet 列存（.par） |
| 写入方式 | STMT2 参数化绑定（逐表） | STMT2 多表模式 / STMT1 参数化绑定 |
| 断点续传 | 不支持 | 支持（备份 flag + 恢复 checkpoint） |
| STMT 多表模式 | 不支持（逐表 bind_one） | 支持（STMT2 64 slot 批量） |

## 共同参数

以下参数在两个工具中均存在且语义一致：

| 短选项 | 长选项 | 说明 | taosdump 默认 | taosBackup 默认 | 差异说明 |
| :---: | --- | --- | :---: | :---: | --- |
| `-h` | `--host` | 服务器地址 | localhost | localhost | 一致 |
| `-P` | `--port` | 服务器端口 | 6030 | 6030 | 一致 |
| `-u` | `--user` | 用户名 | root | root | 一致 |
| `-p` | `--password` | 密码 | taosdata | taosdata | 一致 |
| `-D` | `--databases` | 指定数据库列表 | 逗号分隔 | 逗号分隔，上限 64 | 一致 |
| `-s` | `--schemaonly` | 仅备份 Schema | — | — | 一致 |
| `-S` | `--start-time` | 起始时间 | epoch/ISO8601 | epoch ms/ISO8601 | 一致 |
| `-E` | `--end-time` | 截止时间 | epoch/ISO8601 | epoch ms/ISO8601 | 一致 |
| `-B` | `--data-batch` | 批次行数 | 16384 | STMT2: 10000, STMT1: 60000 | 默认值不同 |
| `-T` | `--thread-num` | 线程数 | 8 | 8 | 一致 |
| `-W` | `--rename` | 恢复时重命名 | `db1=new1\|db2=new2` | `db1=new1\|db2=new2`，上限 64 对 | 格式一致 |
| `-X` | `--dsn` | DSN 连接串 | 云服务 DSN | 云服务 DSN | 一致 |
| `-Z` | `--driver` | 连接驱动 | Native/WebSocket | Native/WebSocket | 一致 |
| `-k` | `--retry-count` | 重试次数 | 可配置 | 默认 3 | 一致 |
| `-z` | `--retry-sleep-ms` | 重试间隔 | 可配置 | 默认 1000ms | 一致 |
| `-o` | `--outpath` | 备份输出目录 | 必选 | 默认 `./output` | 一致 |
| `-i` | `--inpath` | 恢复输入目录 | 必选 | 指定即触发恢复 | 一致 |
| `-g` | `--debug` | 调试模式 | — | — | 一致 |
| `-V` | `--version` | 版本信息 | — | — | 一致 |
| — | `--help` | 帮助信息 | — | — | 一致 |

**位置参数**：两者均支持 `dbname [tbname ...]` 位置参数形式。

## 仅 taosdump 支持的参数

| 短选项 | 长选项 | 说明 | taosBackup 替代方案 |
| :---: | --- | --- | --- |
| `-c` | `--config-dir` | TDengine 配置目录 | 无，taosBackup 不读取 taos.cfg |
| `-r` | `--resultFile` | 结果文件路径 | 无，taosBackup 直接输出到 stdout/stderr |
| `-a` | `--allow-sys` | 允许导出系统库 | 无，taosBackup 始终排除系统库 |
| `-A` | `--all-databases` | 备份所有数据库 | 不指定 `-D` 时自动备份所有库 |
| `-e` | `--escape-character` | 转义数据库名 | 无需，taosBackup 内部自动处理 |
| `-N` | `--without-property` | 不导出数据库属性 | 无，taosBackup 始终导出数据库属性 |
| `-d` | `--avro-codec` | Avro 编解码器（null/deflate/snappy/lzma） | 无，taosBackup 使用 zstd 或 Parquet |
| `-I` | `--inspect` | 查看 Avro 文件内容 | 无 |
| `-L` | `--loose-mode` | 宽松模式（仅字母数字） | 无 |
| `-n` | `--no-escape` | 不使用转义字符 | 无 |
| `-Q` | `--dot-replace` | 表名点号替换为下划线 | 无 |
| `-C` | `--cloud` | DSN 别名（等同 `-X`） | `-C` 在 taosBackup 中用于断点续传（含义不同） |
| `-t` | `--timeout` | WebSocket 超时（秒） | 无独立超时参数 |

## 仅 taosBackup 支持的参数

| 短选项 | 长选项 | 说明 | taosdump 对应 |
| :---: | --- | --- | --- |
| `-F` | `--format` | 存储格式（binary/parquet） | 无，taosdump 固定 Avro |
| `-v` | `--stmt-version` | STMT API 版本（1/2） | 无，taosdump 固定使用 STMT2 |
| `-m` | `--tag-thread-num` | 标签备份线程数（默认 2） | 无 |
| `-C` | `--checkpoint` | 断点续传 | 无（taosdump 的 `-C` 是 DSN 别名） |

## 参数名相同但语义/用法有差异

| 短选项 | 参数名 | taosdump 语义 | taosBackup 语义 |
| :---: | --- | --- | --- |
| `-C` | cloud / checkpoint | DSN 连接串别名（等同 `-X`） | 启用断点续传（flag） |
| `-B` | data-batch | 默认 16,384 | STMT2 默认 10,000 / STMT1 默认 60,000 |
| `-A` | all-databases | 需要显式指定 | 不存在，不指定 `-D` 时自动备份所有库 |

## 功能对比汇总

| 功能 | taosdump | taosBackup |
| --- | :---: | :---: |
| 存储格式 | Avro（行存储） | Binary 列存 / Parquet 列存 |
| 压缩算法 | null/deflate/snappy/lzma（Avro 编解码器） | zstd（逐列压缩） |
| 写入 API | STMT2（逐表 bind_one） | STMT1 / STMT2（参数化绑定） |
| 多表批量写入 | 不支持（逐表绑定） | STMT2 多表模式（64 slot 批量） |
| 断点续传 | 不支持 | 备份（flag）+ 恢复（checkpoint 哈希表） |
| Schema 变更适配 | 不支持 | 支持（ColMapping 部分列写入） |
| 虚拟表 | 不支持 | 支持（VTAB/VSTB） |
| DECIMAL 类型 | 不支持 | 支持 |
| 进度显示 | 无 | TTY 1 秒刷新 / 非 TTY 30 秒输出 |
| 信号处理 | 无优雅退出 | SIGINT/SIGTERM 优雅退出 |
| Avro 文件检查 | `-I` 支持 | 不适用 |
| 配置文件 | 支持 `-c` 读取 taos.cfg | 不读取配置文件 |
| 系统库备份 | `-a` 可选 | 始终排除 |
| 结果文件 | `-r` 输出 | 无，直接 stdout |
| 标签独立线程 | 无 | `-m` 独立标签线程 |
| 平台支持 | Linux / macOS / Windows | Linux / macOS |
