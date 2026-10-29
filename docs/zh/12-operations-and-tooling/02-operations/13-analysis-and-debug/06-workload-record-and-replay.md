---
sidebar_label: 工作负载录制与回放
title: 工作负载录制与回放
toc_max_heading_level: 3
---

工作负载录制在 Native 客户端进程中保存 API 调用，`taosreplay` 可检查录制文件，并在测试集群中重新执行这些调用，用于复现问题。录制仅覆盖 Native 连接，不覆盖 WebSocket 或 REST 连接。每个客户端进程生成一个录制会话；同一目录可以包含多个会话。

:::warning
录制文件包含未经脱敏的 SQL 和参数绑定数据。请限制目录和文件的访问权限，传输、保存及删除时按敏感数据处理。回放可能执行写入和 DDL；务必指定隔离测试集群的地址，并确认目标数据库及账号权限，不要对生产集群执行回放。
:::

## 开启录制

在运行应用的机器上，修改 **客户端**使用的 `taos.cfg`，例如：

```ini
workloadRecord 1
workloadRecordDir /var/log/taos/workload
workloadRecordBufferSize 256
```

重启客户端应用使配置生效，然后执行要复现的业务操作。录制文件会写入 `workloadRecordDir`；如果未设置该目录，默认使用客户端 `logDir` 下的 `workload` 目录。目录中生成以 `wlr-` 开头、以 `.wlr` 结尾的分段文件以及会话清单。建议在操作结束后正常退出客户端，再复制录制文件用于分析。停止录制时，将 `workloadRecord` 改为 `0` 并重启客户端应用。

这三个参数均不支持动态修改，参数含义和取值范围见 [taosc 配置参数](../../03-components/02-taosc.md#workloadrecord)。录制目录不可用或录制写入失败时，客户端业务调用继续执行，但本次录制可能不完整；检查客户端日志中的 `WLR` 信息确认录制状态。

## 检查录制文件

`taosreplay` 是仅在 Linux 源码构建中生成的开发工具，不随产品安装包提供。在源码构建目录的 `build/bin` 中找到 `taosreplay`。先检查录制内容：

```bash
taosreplay inspect --input /var/log/taos/workload
```

`--input` 可以指定包含 `.wlr` 文件的目录或单个 `.wlr` 文件，也可以重复指定。需要查看每条事件时添加 `--verbose`；详细输出可能包含 SQL 文本，注意保护终端输出。

## 在测试集群回放

先在隔离测试集群准备所需的数据库及账号。使用 `--target-host` 将录制中的所有连接指向测试集群；未指定 `--target-port` 时使用端口 `6030`。以下示例通过环境变量提供账号密码，避免密码出现在命令行参数和 shell 历史中：

```bash
export TAOSREPLAY_USER_PASSWORD='root=测试集群密码'
taosreplay replay --input /var/log/taos/workload \
  --target-host test-taos.example.com --target-port 6030
unset TAOSREPLAY_USER_PASSWORD
```

多个录制账号可重复使用 `--user-password user=password` 指定密码；该方式会把密码暴露在进程列表和 shell 历史中，应仅在受控环境使用。若需将不同的原始地址分别映射到测试地址，可重复使用 `--endpoint-map old-host:port=new-host:port`，但不能与 `--target-host` 或 `--target-port` 同时使用。请为每个录制地址提供映射，避免意外连接到原集群。

默认拒绝存在事件缺口的录制文件。确认可接受只回放完整前缀时，才使用 `--allow-incomplete`；仅有尚未结束的调用不属于事件缺口。`--unfinished-timeout` 设置未完成调用的等待上限，默认 300 秒，设为 `0` 可取消该限制。使用 `--report-dir` 可将 `replay-report.json` 写入已存在的目录；不指定时仍会在终端输出回放报告。

## taosreplay 参数

| 序号 | 参数 | 适用命令 | 说明 |
| ---- | ---- | -------- | ---- |
| 1 | `--input <path>` | `inspect`、`replay` | 必填，可重复指定。接受录制目录或单个 `.wlr` 分段文件。 |
| 2 | `--verbose` | `inspect` | 输出每条事件的详细信息，可能包含 SQL；默认只输出汇总。 |
| 3 | `--user-password <user=password>` | `replay` | 提供录制账号在目标集群上的密码，可重复指定。至少需要一个账号密码；若未提供此参数，可使用 `TAOSREPLAY_USER_PASSWORD=user=password` 环境变量提供一个账号密码。命令行参数中的密码可能暴露在进程列表和 shell 历史中。 |
| 4 | `--target-host <host>` | `replay` | 将所有录制连接指向指定目标主机；不能与 `--endpoint-map` 同时使用。建议始终明确指定隔离测试集群的地址。 |
| 5 | `--target-port <port>` | `replay` | 目标端口，范围为 `1`～`65535`；仅可与 `--target-host` 一起使用，省略时默认为 `6030`。 |
| 6 | `--endpoint-map <old-host[:port]=new-host[:port]>` | `replay` | 将一个录制地址映射到目标地址，可重复指定；不能与 `--target-host` 或 `--target-port` 同时使用。 |
| 7 | `--allow-incomplete` | `replay` | 允许回放有事件缺口的录制文件中可验证的前缀；默认拒绝此类输入。 |
| 8 | `--unfinished-timeout <seconds>` | `replay` | 未完成调用的等待上限，默认 `300` 秒；设为 `0` 表示不限制。 |
| 9 | `--report-dir <path>` | `replay` | 将 `replay-report.json` 写入指定的已存在目录；省略时仅在终端输出报告。 |
| 10 | `--help`、`-h` | `inspect`、`replay` | 显示命令用法。 |

退出码 `0` 表示成功，`2` 表示输入或参数错误，`3` 表示凭据或连接错误，`4` 表示回放已结束但存在错误、跳过的调用或不完整输入，`5` 表示内部错误；退出码 `1` 为保留值。
