---
title: TDengine 命令行(CLI)
sidebar_label: TDengine CLI
description: TDengine CLI 的使用说明和技巧
---

TDengine 命令行 （以下简称 TDengine CLI）是用户操作 TDengine 并与之交互的最简洁最常用的方式。

## 基本使用
请参考[这里](/get-started/)

## 命令行参数

您可通过配置命令行参数来改变 TDengine CLI 的行为。以下为常用的几个命令行参数：

- -c, --config-dir: 指定配置文件目录，默认为 `/etc/taos`，该目录下的配置文件默认名称为 taos.cfg
- -C, --dump-config: 打印 -c 指定的目录中 taos.cfg 的配置参数
- -d, --database=DATABASE: 指定连接到服务端时使用的数据库
- -D, --directory=DIRECTORY: 导入指定路径中的 SQL 脚本文件
- -f, --file=FILE: 以非交互模式执行 SQL 脚本文件
- -h, --host=HOST: 要连接的 TDengine 服务端所在服务器的 FQDN, 默认为连接本地服务
- -k, --check=CHECK: 指定要检查的表
- -l, --pktlen=PKTLEN: 网络测试时使用的测试包大小
- -n, --netrole=NETROLE: 网络连接测试时的测试范围，默认为 startup, 可选值为 client, server, rpc, startup, sync, speed, fqdn
- -p, --password=PASSWORD: 连接服务端时使用的密码
- -P, --port=PORT: 指定服务端所用端口号
- -r, --raw-time: 将时间输出出 uint64_t
- -s, --commands=COMMAND: 以非交互模式执行的 SQL 命令
- -S, --pkttype=PKTTYPE: 指定网络测试所用的包类型，默认为 TCP。只有 netrole 为 speed 时既可以指定为 TCP 也可以指定为 UDP
- -T, --thread=THREADNUM: 以多线程模式导入数据时的线程数
- -u, --user=USER: 连接时使用的用户名
- -s, --commands: 在不进入终端的情况下运行 TDengine 命令
- -z, --timezone=TIMEZONE: 指定时区，默认为本地
- -V, --version: 打印出当前版本号
- -?, --help: 打印出所有命令行参数

示例：

```bash
taos -h h1.taos.com -s "use db; show tables;"
```

## TDengine CLI 小技巧

- 可以使用上下光标键查看历史输入的指令
- 修改用户密码：在 shell 中使用 `alter user` 命令，缺省密码为 taosdata
- ctrl+c 中止正在进行中的查询
- 执行 `RESET QUERY CACHE` 可清除本地缓存的表 schema
- 批量执行 SQL 语句。可以将一系列的 shell 命令（以英文 ; 结尾，每个 SQL 语句为一行）按行存放在文件里，在 shell 里执行命令 `source <file-name>` 自动执行该文件里所有的 SQL 语句
- 输入 q 回车，退出 taos shell
- 在线修改显示字符宽度
    ```sql
    SET MAX_BINARY_DISPLAY_WIDTH <nn>;
    ```
    如显示的内容后面以...结尾时，表示该内容已被截断，可通过本命令修改显示字符宽度以显示完整的内容。