---
title: TDengine 命令行(CLI)
sidebar_label: TDengine CLI
description: TDengine CLI 的使用说明和技巧
---

TDengine 命令行程序（以下简称 TDengine CLI）是用户操作 TDengine 实例并与之交互的最简洁最常用的方式。

## 安装

如果在 TDengine 服务器端执行，无需任何安装，已经自动安装好 TDengine CLI。如果要在非 TDengine 服务器端运行，需要安装 TDengine 客户端驱动安装包，具体安装，请参考 [连接器](../../connector/)。

## 执行

要进入 TDengine CLI，您只要在终端执行 `taos` 即可。

```bash
taos
```

如果连接服务成功，将会打印出欢迎消息和版本信息。如果失败，则会打印错误消息。（请参考 [FAQ](../../train-faq/faq) 来解决终端连接服务端失败的问题）。TDengine CLI 的提示符号如下：

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

- -h HOST: 要连接的 TDengine 服务端所在服务器的 FQDN, 默认为连接本地服务
- -P PORT: 指定服务端所用端口号
- -u USER: 连接时使用的用户名
- -p PASSWORD: 连接服务端时使用的密码
- -?, --help: 打印出所有命令行参数

还有更多其他参数：

- -a AUTHSTR: 连接服务端的授权信息
- -A: 通过用户名和密码计算授权信息
- -B: 设置 BI 工具显示模式，设置后所有输出都遵循 BI 工具的格式进行输出
- -c CONFIGDIR: 指定配置文件目录，Linux 环境下默认为 `/etc/taos`，该目录下的配置文件默认名称为 `taos.cfg`
- -C: 打印 -c 指定的目录中 `taos.cfg` 的配置参数
- -d DATABASE: 指定连接到服务端时使用的数据库
- -E dsn: 使用 WebSocket DSN 连接云服务或者提供 WebSocket 连接的服务端
- -f FILE: 以非交互模式执行 SQL 脚本文件。文件中一个 SQL 语句只能占一行
- -k: 测试服务端运行状态，0: unavailable，1: network ok，2: service ok，3: service degraded，4: exiting
- -l PKTLEN: 网络测试时使用的测试包大小
- -n NETROLE: 网络连接测试时的测试范围，默认为 `client`, 可选值为 `client`、`server`
- -N PKTNUM: 网络测试时使用的测试包数量
- -r: 将时间输出出无符号 64 位整数类型(即 C 语音中 uint64_t)
- -R: 使用 RESTful 模式连接服务端
- -s COMMAND: 以非交互模式执行的 SQL 命令
- -t: 测试服务端启动状态，状态同-k
- -w DISPLAYWIDTH: 客户端列显示宽度
- -z TIMEZONE: 指定时区，默认为本地时区
- -V: 打印出当前版本号

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

## TDengine CLI 导出查询结果到文件中

- 可以使用符号 “>>” 导出查询结果到某个文件中，语法为： sql 查询语句 >> ‘输出文件名’; 输出文件如果不写路径的话，将输出至当前目录下。如 select * from d0 >> ‘/root/d0.csv’;  将把查询结果输出到 /root/d0.csv 中。

## TDengine CLI 导入文件中的数据到表中

- 可以使用 insert into table_name file '输入文件名'，把上一步中导出的数据文件再导入到指定表中。如 insert into d0 file '/root/d0.csv'; 表示把上面导出的数据全部再导致至 d0 表中。
