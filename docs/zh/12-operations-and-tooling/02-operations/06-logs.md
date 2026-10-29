---
sidebar_label: 日志文件
title: 日志文件位置与获取
description: TDengine 各组件的日志文件在哪里、文件名是什么，如何确认实际日志目录，以及如何收集、打包日志提交给技术支持
toc_max_heading_level: 4
---

本节说明 TDengine 各组件的日志文件在哪里、文件名是什么、如何确认实际日志目录，以及排查问题时如何收集和打包日志并提交给技术支持。日志的写入、滚动和归档机制见 [日志系统](../../15-internals/09-log.md)。

## 默认日志目录

服务端、客户端和大多数组件默认写入同一个日志目录：

| 操作系统 | 默认日志目录 | 默认配置文件目录 |
| --- | --- | --- |
| Linux | `/var/log/taos` | `/etc/taos` |
| macOS | `/var/log/taos` | `/etc/taos` |
| Windows | `C:\TDengine\log` | `C:\TDengine\cfg` |

如果在配置文件中修改过 `logDir`（taosd 和客户端）或 `log.path`（其他组件），以实际配置为准，确认方法见 [确认实际日志目录](#确认实际日志目录)。

使用 Docker 部署时，日志位于容器内的 `/var/log/taos`。建议启动容器时用 `-v` 将该目录映射到宿主机，详见 [Docker 部署](./03-deployment/02-docker.md)。

## 各组件的日志文件

下表以 Linux 默认目录为例；在 Windows 上，将 `/var/log/taos` 替换为 `C:\TDengine\log`。表中 `<实例 ID>` 为组件的 `instanceId` 配置，`<日期>` 格式为 `YYYYMMDD`。

| 组件 | 默认目录 | 文件名 | 由哪个配置决定 |
| --- | --- | --- | --- |
| taosd | `/var/log/taos` | `taosdlog.0`、`taosdlog.1` | `taos.cfg` 中的 `logDir`，见 [taosd 日志相关参数](../03-components/01-taosd.md#日志相关) |
| 客户端 | `/var/log/taos` | `taoslogX.Y` | 客户端 `taos.cfg` 中的 `logDir`，见 [taosc 参考手册](../03-components/02-taosc.md) |
| 慢查询日志 | `/var/log/taos` | `taosslowlog.<年-月-日>` | 客户端 `taos.cfg` 中的 `logDir` |
| UDF | `/var/log/taos` | `taosudfdlog.*`；`taospyudf.log` | `taos.cfg` 中的 `logDir`，见 [UDF 开发](../../10-developer-guide/06-udf.md) |
| MQTT 订阅 | `/var/log/taos` | `taosmqttdlog.*` | `taos.cfg` 中的 `logDir`，见 [MQTT 数据订阅](../../06-data-subscription/03-mqtt.md) |
| Xnode 守护进程 | `/var/log/taos` | `taosxnoded_<实例 ID>_<日期>.log` | `taos.cfg` 中的 `logDir`；见 [数据接入（Xnode）](../../05-tdengine-sql/08-cluster-management/02-xnode.md) |
| taosAdapter | `/var/log/taos` | `taosadapter_<实例 ID>_<日期>.log` | `taosadapter.toml` 中的 `log.path`，见 [taosAdapter 日志配置](../03-components/03-taosadapter.md#日志配置) |
| taosKeeper | `/var/log/taos` | `taoskeeper_<实例 ID>_<日期>.log` | `taoskeeper.toml` 中的 `log.path`，见 [taosKeeper 参考手册](../03-components/05-taoskeeper.md) |
| taosExplorer | `/var/log/taos` | `taosexplorer_<实例 ID>_<日期>.log` | `explorer.toml` 中的 `log.path`，见 [taosExplorer 参考手册](../03-components/04-explorer.md) |
| taosX | `/var/log/taos` | `taosx_<实例 ID>_<日期>.log` | `taosx.toml` 中的 `log.path`，见 [taosX 参考手册](../03-components/06-taosx.md) |
| taosX-Agent | `/var/log/taos` | `taosx_agent_<实例 ID>_<日期>.log` | `agent.toml` 中的 `log.path`，见 [taosX-Agent 配置参考](../03-components/07-taosx-agent/01-configuration.md) |
| TDgpt（taosanode） | `/var/log/taos/taosanode` | `error.log`、`access.log`、`taosanode.app.log` | TDgpt 配置文件，见 [TDgpt 安装部署](../../09-ai-and-advanced-analytics/01-tdgpt/03-management.md) |
| 授权中心（CLS） | `/var/log/taoscls` | `taoscls.log` | CLS 配置文件 `[log]` 段的 `file`，见 [部署与激活](../03-components/09-license-center/01-deploy-and-activate.md) |

日志按天或按大小滚动后，旧文件会带上日期、序号或 `.gz` 后缀，排查时通常只需问题发生前后的几个文件。

## 确认实际日志目录

- **taosd**：在 `taos` shell 中执行 `SHOW DNODES;` 获取 `dnode_id`，再执行 `SHOW DNODE <dnode_id> VARIABLES LIKE 'logDir';`。也可以直接查看服务端配置文件：`grep -i '^logDir' /etc/taos/taos.cfg`。
- **客户端**：在 `taos` shell 中执行 `SHOW LOCAL VARIABLES LIKE 'logDir';`，或查看客户端机器上的 `taos.cfg`。
- **taosAdapter、taosKeeper、taosExplorer、taosX、taosX-Agent**：查看对应 TOML 配置文件 `[log]` 段中的 `path`。部分组件也可通过命令行参数 `--log.path` 或环境变量（如 `TAOS_ADAPTER_LOG_PATH`、`TAOS_KEEPER_LOG_PATH`）指定日志目录，优先级高于配置文件。
- 如果启动 taosd 时使用了 `-o` 参数，日志会输出到该参数指定的位置，见 [taosd 命令行参数](../03-components/01-taosd.md#命令行参数)。

相关 SQL 的完整语法见 [SHOW 命令](../../05-tdengine-sql/09-system-info/03-show.md)。

## 服务未能启动时

如果服务在打开日志文件之前就启动失败，日志目录中可能没有相关记录。此时可查看系统服务日志：

```bash
systemctl status taosd
journalctl -u taosd --since "1 hour ago"
```

其他组件将 `taosd` 替换为对应的服务名，如 `taosadapter`、`taoskeeper`、`taos-explorer`、`taosx`、`taosx-agent`。macOS 上可查看 `launchd.log`。

进程崩溃时生成的 core dump 文件位置见 [Linux 分析调试工具](./13-analysis-and-debug/01-debug-linux.md#core-dump-文件)。

## 收集日志提交技术支持

1. 如果问题可以复现，先提高日志级别。
   - 服务端：执行 `ALTER ALL DNODES 'debugFlag' '135';`，或只修改单个节点 `ALTER DNODE <dnode_id> 'debugFlag' '135';`。也可以在 `taos.cfg` 末尾添加一行 `debugFlag 135` 后重启 `taosd`。
   - 客户端：在 `taos` shell 中执行 `ALTER LOCAL 'debugFlag' '135';`，仅对当前进程有效。
2. 复现问题，并记下问题发生的大致时间。
3. 在每个相关节点上打包日志目录和配置目录：

   ```bash
   tar czf tdengine-$(hostname)-$(date +%Y%m%d%H%M).tar.gz /var/log/taos /etc/taos
   ```

   Windows 上压缩 `C:\TDengine\log` 和 `C:\TDengine\cfg` 两个目录。日志目录很大时，可只打包问题发生前后修改过的文件。
4. 发送前检查配置文件。部分组件的配置文件（如 `taoskeeper.toml`）中包含连接数据库的用户名和密码，请删除或替换为占位符。
5. 附上 TDengine 版本、操作系统、部署方式、复现步骤和问题发生时间，按 [问题反馈](../../16-faq/01-faq.md#问题反馈) 提交。
6. 问题定位后，将日志级别改回 `131`，如 `ALTER ALL DNODES 'debugFlag' '131';`。`135` 会产生大量日志并降低系统性能，不适合长期使用。
