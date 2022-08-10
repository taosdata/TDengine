---
title: TDengine 命令行(CLI)
sidebar_label: TDengine CLI
description: TDengine CLI 的使用说明和技巧
---

TDengine 命令行程序（以下简称 TDengine CLI）是用户操作 TDengine 实例并与之交互的最简洁最常用的方式。

## 安装

如果在 TDengine 服务器端执行，无需任何安装，已经自动安装好 TDengine CLI。如果要在非 TDengine 服务器端运行，需要安装 TDengine 客户端驱动安装包，具体安装，请参考 [连接器](/reference/connector/)。

## 执行

要进入 TDengine CLI，您只要在 Linux 终端或 Windows 终端执行 `taos` 即可。

```bash
taos
```

如果连接服务成功，将会打印出欢迎消息和版本信息。如果失败，则会打印错误消息。（请参考 [FAQ](/train-faq/faq) 来解决终端连接服务端失败的问题）。TDengine CLI 的提示符号如下：

```cmd
taos>
```

进入 TDengine CLI 后，你可执行各种 SQL 语句，包括插入、查询以及各种管理命令。

## 执行 SQL 脚本

在 TDengine CLI 里可以通过 `source` 命令来运行脚本文件中的多条 SQL 命令。

```sql
taos> source <filename>;
```

## 在线修改显示字符宽度

可以在 TDengine CLI 里使用如下命令调整字符显示宽度

```sql
taos> SET MAX_BINARY_DISPLAY_WIDTH <nn>;
```

如显示的内容后面以 ... 结尾时，表示该内容已被截断，可通过本命令修改显示字符宽度以显示完整的内容。

## 命令行参数

您可通过配置命令行参数来改变 TDengine CLI 的行为。以下为常用的几个命令行参数：

- -h, --host=HOST: 要连接的 TDengine 服务端所在服务器的 FQDN, 默认为连接本地服务
- -P, --port=PORT: 指定服务端所用端口号
- -u, --user=USER: 连接时使用的用户名
- -p, --password=PASSWORD: 连接服务端时使用的密码
- -?, --help: 打印出所有命令行参数

还有更多其他参数：

- -c, --config-dir: 指定配置文件目录，Linux 环境下默认为 `/etc/taos`，该目录下的配置文件默认名称为 `taos.cfg`
- -C, --dump-config: 打印 -c 指定的目录中 `taos.cfg` 的配置参数
- -d, --database=DATABASE: 指定连接到服务端时使用的数据库
- -D, --directory=DIRECTORY: 导入指定路径中的 SQL 脚本文件
- -f, --file=FILE: 以非交互模式执行 SQL 脚本文件。文件中一个 SQL 语句只能占一行
- -k, --check=CHECK: 指定要检查的表
- -l, --pktlen=PKTLEN: 网络测试时使用的测试包大小
- -n, --netrole=NETROLE: 网络连接测试时的测试范围，默认为 `startup`, 可选值为 `client`、`server`、`rpc`、`startup`、`sync`、`speed` 和 `fqdn` 之一
- -r, --raw-time: 将时间输出出无符号 64 位整数类型(即 C 语音中 uint64_t)
- -s, --commands=COMMAND: 以非交互模式执行的 SQL 命令
- -S, --pkttype=PKTTYPE: 指定网络测试所用的包类型，默认为 TCP。只有 netrole 为 `speed` 时既可以指定为 TCP 也可以指定为 UDP
- -T, --thread=THREADNUM: 以多线程模式导入数据时的线程数
- -s, --commands: 在不进入终端的情况下运行 TDengine 命令
- -z, --timezone=TIMEZONE: 指定时区，默认为本地时区
- -V, --version: 打印出当前版本号

示例：

```bash
taos -h h1.taos.com -s "use db; show tables;"
```

## TDengine CLI 小技巧

- 可以使用上下光标键查看历史输入的指令
- 在 TDengine CLI 中使用 `alter user` 命令可以修改用户密码，缺省密码为 `taosdata`
- Ctrl+C 中止正在进行中的查询
- 执行 `RESET QUERY CACHE` 可清除本地表 Schema 的缓存
- 批量执行 SQL 语句。可以将一系列的 TDengine CLI 命令（以英文 ; 结尾，每个 SQL 语句为一行）按行存放在文件里，在 TDengine CLI 里执行命令 `source <file-name>` 自动执行该文件里所有的 SQL 语句
- 输入 `q` 或 `quit` 或 `exit` 回车，可以退出 TDengine CLI
