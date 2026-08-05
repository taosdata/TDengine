---
title: taos shell 参考手册
sidebar_label: taos shell
toc_max_heading_level: 4
---

`taos` shell 是操作 TDengine 实例并与之交互最常用的命令行工具。

## 工具获取

`taos` shell 随 TDengine 服务器及客户端安装包默认安装，安装后即可使用，参考 [下载与安装](../../04-quick-start/index.md)。

## 运行

进入交互执行模式，在终端执行：

```bash
taos
```

如果连接服务成功，将会打印出欢迎消息和版本信息。若失败，打印错误消息。

提示符如下：

```shell
taos>
```

进入后可执行各种 SQL 语句，包括插入、查询以及各种管理命令。
退出时执行 `q`、`quit` 或 `exit` 后回车即可。

```shell
taos> quit
```

## 命令行参数

### 基础参数

可通过配置命令行参数来改变 `taos` shell 的行为。以下为常用的几个命令行参数：

- -h HOST：要连接的 TDengine 服务端所在服务器的 FQDN, 默认值：127.0.0.1。
- -P PORT：指定服务端所用端口号，默认值：6030。
- -u USER：连接时使用的用户名，默认值：root。
- -p PASSWORD：连接服务端时使用的密码，特殊字符如 `! & ( ) < > ; |` 需使用字符 `\` 进行转义处理，默认值：taosdata。
  - 如果 `-p` 参数后面不跟密码字符串，则会提示用户输入密码，如：`taos -u root -p`。
  - 如果 `-p` 参数后面直接跟密码字符串，则使用该密码字符串进行连接，如：`taos -u root -ptaosdata`。
- -?, --help：打印出所有命令行参数。
- -s COMMAND：以非交互模式执行的 SQL 命令。

    使用 `-s` 参数可进行非交互式执行 SQL，执行完成后退出，此模式适合在自动化脚本中使用。
    如以下命令连接到服务器 h1.taos.com, 执行 -s 指定的 SQL：

    ```bash
    taos -h my-server -s "use db; show tables;"
    ```

- -c CONFIGDIR：指定配置文件目录。

    Linux 环境下默认为 `/etc/taos`，该目录下的配置文件默认名称为 `taos.cfg`。
    使用 `-c` 参数改变 `taosc` 客户端加载配置文件的位置，客户端配置参数参考 [taosc 参考手册](../03-components/02-taosc.md)。
    以下命令指定了 `taosc` 客户端加载 `/root/cfg/` 下的 `taos.cfg` 配置文件。

    ```bash
    taos -c /root/cfg/
    ```

- -Z：指定连接方式，0 表示采用原生连接方式，1 表示采用 WebSocket 连接方式，默认采用原生连接方式。

### 高级参数

- -a AUTHSTR：连接服务端的授权信息。
- -A：通过用户名和密码计算授权信息。
- -B：设置 BI 工具显示模式，设置后所有输出都遵循 BI 工具的格式进行输出。
- -H, --binary-as-hex：将包含不可打印字符的 BINARY 字符串按十六进制形式显示（`0x...`），默认关闭。
- -C：打印 -c 指定的目录中 `taos.cfg` 的配置参数。
- -d DATABASE：指定连接到服务端时使用的数据库。
- -E dsn：使用 WebSocket DSN 连接云服务或者提供 WebSocket 连接的服务端。
- -f FILE：以非交互模式执行 SQL 脚本文件。文件中一个 SQL 语句只能占一行。
- -k：测试服务端运行状态，0：unavailable、1：network ok、2：service ok、3：service degraded、4：exiting。
- -l PKTLEN：网络测试时使用的测试包大小。
- -n NETROLE：网络连接测试时的测试范围，默认为 `client`，可选值为 `client`、`server`。
- -N PKTNUM：网络测试时使用的测试包数量。
- -q TOKEN：使用 Token 连接服务端（仅企业版支持）。
  - 如果 `-q` 参数后面不跟 Token 字符串，则会提示用户输入 Token，如：`taos -u root -q`。
  - 如果 `-q` 参数后面直接跟 Token 字符串，则使用该 Token 进行连接，如：`taos -u root -q<token_string>`。
- -r：将时间列转化为无符号 64 位整数类型输出 (即 C 语言中 uint64_t)。
- -t：测试服务端启动状态，状态同 -k。
- -w DISPLAYWIDTH：客户端列显示宽度。
- -z TIMEZONE：指定时区，默认为本地时区。
- -V：打印出当前版本号。

## 数据导出/导入

### 数据导出

- 可以使用符号“>>”导出查询结果到某个文件中，语法为：sql 查询语句 >> ‘输出文件名’; 输出文件如果不写路径的话，将输出至当前目录下。如 `select * from d0 >> '/root/d0.csv';`  将把查询结果输出到 /root/d0.csv 中。

### 数据导入

- 可以使用 insert into table_name file '输入文件名'，把上一步中导出的数据文件再导入到指定表中。如 `insert into d0 file '/root/d0.csv';` 表示把上面导出的数据全部再导入至 d0 表中。

## 执行 SQL 脚本

在 `taos` shell 里可以通过 `source` 命令来运行脚本文件中的多条 SQL 命令，脚本文件中多条 SQL 按行书写即可。

```sql
taos> source <filename>;
```

## 数据订阅 {#数据订阅}

`taos` shell 支持在交互模式下通过 `subscribe` 命令订阅 Topic 数据，实时接收并显示推送的消息，便于快速验证数据订阅功能。主题与消费模型的完整说明见 [数据订阅](../../06-data-subscription/index.md)。

### 语法

```sql
taos> subscribe <topic_name> -g <group_id> [options];
```

### 参数说明

| 参数 | 说明 | 是否必填 | 默认值 |
| --- | --- | --- | --- |
| `<topic_name>` | 要订阅的 Topic 名称 | 是 | — |
| `-g <group_id>` | 消费者组 ID | 是 | — |
| `-o <offset>` | 起始消费位置，可选 `earliest` 或 `latest` | 否 | `latest` |
| `-n <count>` | 接收指定条数后自动退出 | 否 | 无限制（持续接收） |
| `-t <timeout_ms>` | 单次 poll 超时时间（毫秒） | 否 | `1000` |
| `-c <client_id>` | 自定义 client ID | 否 | `taos_subscribe_cli` |
| `-h` | 显示帮助信息 | 否 | — |

### 使用示例

前提条件：已创建 Topic，例如：

```sql
taos> CREATE TOPIC topic_meters AS SELECT ts, current, voltage, phase FROM power.meters;
```

示例 1 — 持续订阅，按 Ctrl+C 退出：

```sql
taos> subscribe topic_meters -g my_group -o earliest;
```

示例 2 — 从最早位置消费 10 条后自动退出：

```sql
taos> subscribe topic_meters -g my_group -o earliest -n 10;
```

示例 3 — 自定义 poll 超时为 500ms：

```sql
taos> subscribe topic_meters -g my_group -o earliest -t 500;
```

### 输出格式

订阅成功后，数据以表格形式展示，与 `SELECT` 查询结果格式一致：

```text
Subscribing to topic [topic_meters], group [my_group], offset [earliest] ...
Press Ctrl+C to stop.

           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2023-11-15 06:13:20.000 |              12.6000 |         220 |               0.3100 |
 2023-11-15 06:13:20.001 |              15.3000 |         225 |               0.6200 |
```

退出时显示接收总行数：

```text
Unsubscribed. Total rows received: 10
```

### 注意事项

- `subscribe` 命令仅在交互模式和 `-s` 非交互模式下可用。
- `-o` 参数仅在消费者组**无已提交 offset** 时生效；若该组已有提交记录，则从上次提交位置继续消费。
- 如需在订阅结果中显示子表名，请在创建 Topic 时将 `tbname` 作为查询列，例如：`CREATE TOPIC my_topic AS SELECT tbname, ts, col1 FROM stable1;`。
- 按 Ctrl+C 可随时中止订阅（仅交互模式有效）。

## 使用小技巧

### TAB 键自动补全

- TAB 键前为空命令状态下按 TAB 键，会列出 `taos` shell 支持的所有命令。
- TAB 键前为空格状态下按 TAB 键，会显示此位置可以出现的所有命令词的第一个，再次按 TAB 键切为下一个。
- TAB 键前为字符串，会搜索与此字符串前缀匹配的所有可出现命令词，并显示第一个，再次按 TAB 键切为下一个。
- 输入反斜杠 `\` + TAB 键，会自动补全为列显示模式命令词 `\G;`。

### 设置字符列显示宽度

可以在 `taos` shell 里使用如下命令调整字符串类型字段列显示宽度，默认显示宽度为 30 个字符。
以下命令设置了显示宽度为 120 个字符：

```sql
taos> SET MAX_BINARY_DISPLAY_WIDTH 120;
```

如显示的内容后面以 `...` 结尾时，表示该内容已被截断，可通过本命令修改显示字符宽度以显示完整的内容。

### 其它

- 可以使用上下光标键查看历史输入的指令。
- 在 `taos` shell 中使用 `alter user` 命令可以修改用户密码，缺省密码为 `taosdata`。
- Ctrl+C 中止正在进行中的查询。
- 执行 `RESET QUERY CACHE` 可清除本地表 Schema 的缓存。
- 批量执行 SQL 语句。可以将一系列命令（以英文 `;` 结尾，每个 SQL 语句为一行）按行存放在文件里，在 `taos` shell 里执行 `source <file-name>` 自动执行该文件里所有的 SQL 语句。

## 错误代码表

从 `v3.3.4.8` 起，`taos` shell 在返回错误信息中包含具体错误码，可查阅 [错误码](../../10-developer-guide/09-error-codes.md) 了解原因及处理建议。
