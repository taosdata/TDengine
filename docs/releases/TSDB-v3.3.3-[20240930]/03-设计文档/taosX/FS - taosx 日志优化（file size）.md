# FS - taosx 日志优化（file size）

## 1. 背景

taosx 现在仅按“天”拆分日志文件，没有对单个日志文件的大小以及总日志大小做限制，这导致日志文件经常很大，有时甚至将磁盘占满，影响系统正常运行。
基于此问题，提出日志优化的改进需求，可以通过设置单个日志的大小及总日志的大小做限制，参考下方 jira：

TD-30033

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/08/02 | 0.1 | @张元湃 | 初稿 |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |

## 3. 定义

- 日志：taosx 与 taosx-agent 运行过程中产生的例如 /var/log/taos/taosx.log 的日志文件

## 4. 行为说明

### 4.1 taosx 配置文件

<quote-container>
代码路径：taosx/examples/taosx.toml
默认部署路径：/etc/taos/taosx.toml(on linux/macOS) C:\TDengine\cfg\taosx.toml(on windows)
</quote-container>

- 增加以下配置：
```toml

## 5. a maximum size limit to an existing 'Rotation', default 100M(100_000_000)

log_filesize_limit = 100_000_000
```

- 修改以下配置：
```toml

## 6. log keep days

log_keep_days = 30

修改为：

## 7. keeps the last `n` log files on disk

log_keep_files = 30
```

### 7.1 taosx 命令行参数/环境变量

- 新增两个命令行参数，如下所示：
```shell

## 8. 单日志文件限制大小 100M，日志文件最多保留 30 个

taosx serve --log-filesize-limit=100000000 --log-keep-files=30
```

- 新增两个环境变量 LOG_FILESIZE_LIMIT 与 LOG_KEEP_FILES
- 删除命令行参数 --log-keep-days，删除环境变量 LOG_KEEP_DAYS

### 8.1 taosx-agent 配置文件

<quote-container>
代码路径：taosx/taosx-agent/examples/agent.toml
默认部署路径：/etc/taos/agent.toml(on linux/macOS) C:\TDengine\cfg\agent.toml(on windows)
</quote-container>

- 增加以下配置：
```toml

## 9. a maximum size limit to an existing 'Rotation', default 100M(100_000_000)

log_filesize_limit = 100_000_000
```

- 修改以下配置：
```toml

## 10. log keep days

log_keep_days = 30

修改为：

## 11. keeps the last `n` log files on disk

log_keep_files = 30
```

### 11.1 taosx-agent 命令行参数/环境变量

- 新增两个命令行参数，如下所示：
```shell

## 12. 单日志文件限制大小 100M，日志文件最多保留 30 个

taosx-agent --log-filesize-limit=100000000 --log-keep-files=30
```

- 新增两个环境变量 LOG_FILESIZE_LIMIT 与 LOG_KEEP_FILES
- 删除命令行参数 --log-keep-days，删除环境变量 LOG_KEEP_DAYS

### 12.1 日志文件变化

<quote-container>
默认日志路径：/var/log/taos(on linux/macOS) C:\TDengine\log(on windows)
</quote-container>

#### 12.1.1 日志文件名变化

taosx 日志文件名由 taosx.log.yyyy-mm-dd 改为 taosx.log.yyyy-mm-dd.index
taosx-agent 日志文件名由 agent.log.yyyy-mm-dd 改为 agent.log.yyyy-mm-dd.index

#### 12.1.2 日志文件保存时效变化

现状是按“天”拆分日志文件，并且设置保留文件天数，可以明确日志文件保存时效。
修改后，按“天”与“单个文件大小”拆分日志文件，并且设置最大文件个数，如果某一天日志量超大，则有可能冲掉之前几天的文件。

## 13. 性能

估计影响不大，待实现后测试。

## 14. 兼容性

配置文件、命令行、环境变量发生变化，有使用上的影响，但对程序本身无影响（参数有默认值）。

## 15. 运维

无。

## 16. 使用场景

需要限制单个日志文件大小的时候使用。

## 17. 约束和限制

单个文件大小与文件个数的配置，受日志目录所在磁盘大小的限制。

## 18. 常见错误和排查

无。

## 19. 可观测性

日志文件的文件名相比之前有变化。

## 20. 安装和卸载

无。

## 21. 文档

需要修改企业版文档。
不需要修改官网文档。

## 22. 参考文档

无。

## 23. 附录

无。
