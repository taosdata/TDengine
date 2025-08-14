---
title: TDengine TSDB CLI 参考手册
sidebar_label: TDengine TSDB CLI
toc_max_heading_level: 4
---

TDengine TSDB 命令行程序（以下简称 TDengine TSDB CLI）是用户操作 TDengine TSDB 实例并与之交互最简洁常用工具。 

## 工具获取

TDengine TSDB CLI 是 TDengine TSDB 服务器及客户端安装包中默认安装组件，安装后即可使用，参考 [TDengine TSDB 安装](../../../get-started/)

## 运行

进入 TDengine TSDB CLI 交互执行模式，在终端命令行执行：

```bash
taos
```

如果连接服务成功，将会打印出欢迎消息和版本信息。若失败，打印错误消息。

TDengine TSDB CLI 的提示符号如下：

```shell
taos>
```

进入 TDengine TSDB CLI 后，可执行各种 SQL 语句，包括插入、查询以及各种管理命令。
退出 TDengine TSDB CLI，执行 `q` 或 `quit` 或 `exit` 回车即可。
```shell
taos> quit
```

## 命令行参数

### 基础参数
可通过配置命令行参数来改变 TDengine TSDB CLI 的行为。以下为常用的几个命令行参数：

- -h HOST：要连接的 TDengine TSDB 服务端所在服务器的 FQDN, 默认值：127.0.0.1。
- -P PORT：指定服务端所用端口号，默认值：6030。
- -u USER：连接时使用的用户名，默认值：root。
- -p PASSWORD：连接服务端时使用的密码，特殊字符如 `! & ( ) < > ; |` 需使用字符 `\` 进行转义处理，默认值：taosdata。
- -?, --help：打印出所有命令行参数。
- -s COMMAND：以非交互模式执行的 SQL 命令。

    使用 `-s` 参数可进行非交互式执行 SQL，执行完成后退出，此模式适合在自动化脚本中使用。  
    如以下命令连接到服务器 h1.taos.com, 执行 -s 指定的 SQL：
    ```bash
    taos -h my-server -s "use db; show tables;"
    ```

- -c CONFIGDIR：指定配置文件目录。
 
    Linux 环境下默认为 `/etc/taos`，该目录下的配置文件默认名称为 `taos.cfg`。
    使用 `-c` 参数改变 `taosc` 客户端加载配置文件的位置，客户端配置参数参考 [客户端配置](../../components/taosc)。  
    以下命令指定了 `taosc` 客户端加载 `/root/cfg/` 下的 `taos.cfg` 配置文件。
    ```bash
    taos -c /root/cfg/
    ```

- -Z：指定连接方式，0 表示采用原生连接方式，1 表示采用 WebSocket 连接方式，默认采用原生连接方式。

### 高级参数

- -a AUTHSTR：连接服务端的授权信息。
- -A：通过用户名和密码计算授权信息。
- -B：设置 BI 工具显示模式，设置后所有输出都遵循 BI 工具的格式进行输出。
- -C：打印 -c 指定的目录中 `taos.cfg` 的配置参数。
- -d DATABASE：指定连接到服务端时使用的数据库。
- -E dsn：使用 WebSocket DSN 连接云服务或者提供 WebSocket 连接的服务端。
- -f FILE：以非交互模式执行 SQL 脚本文件。文件中一个 SQL 语句只能占一行。
- -k：测试服务端运行状态，0：unavailable、1：network ok、2：service ok、3：service degraded、4：exiting。
- -l PKTLEN：网络测试时使用的测试包大小。
- -n NETROLE：网络连接测试时的测试范围，默认为 `client`，可选值为 `client`、`server`。
- -N PKTNUM：网络测试时使用的测试包数量。
- -r：将时间列转化为无符号 64 位整数类型输出 (即 C 语言中 uint64_t)。
- -R：使用 RESTful 模式连接服务端。
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

在 TDengine TSDB CLI 里可以通过 `source` 命令来运行脚本文件中的多条 SQL 命令，脚本文件中多条 SQL 按行书写即可
```sql
taos> source <filename>;
```


## 使用小技巧

### TAB 键自动补全

- TAB 键前为空命令状态下按 TAB 键，会列出 TDengine TSDB CLI 支持的所有命令。
- TAB 键前为空格状态下按 TAB 键，会显示此位置可以出现的所有命令词的第一个，再次按 TAB 键切为下一个。
- TAB 键前为字符串，会搜索与此字符串前缀匹配的所有可出现命令词，并显示第一个，再次按 TAB 键切为下一个。
- 输入反斜杠 `\` + TAB 键，会自动补全为列显示模式命令词 `\G;`。

### 设置字符列显示宽度

可以在 TDengine TSDB CLI 里使用如下命令调整字符串类型字段列显示宽度，默认显示宽度为 30 个字符。  
以下命令设置了显示宽度为 120 个字符：
```sql
taos> SET MAX_BINARY_DISPLAY_WIDTH 120;
```

如显示的内容后面以 ... 结尾时，表示该内容已被截断，可通过本命令修改显示字符宽度以显示完整的内容。

### 其它

- 可以使用上下光标键查看历史输入的指令。
- 在 TDengine TSDB CLI 中使用 `alter user` 命令可以修改用户密码，缺省密码为 `taosdata`。
- Ctrl+C 中止正在进行中的查询。
- 执行 `RESET QUERY CACHE` 可清除本地表 Schema 的缓存。
- 批量执行 SQL 语句。可以将一系列的 TDengine TSDB CLI 命令（以英文 `;` 结尾，每个 SQL 语句为一行）按行存放在文件里，在 TDengine TSDB CLI 里执行命令 `source <file-name>` 自动执行该文件里所有的 SQL 语句。

## 错误代码表
在 TDengine TSDB 3.3.4.8 版本后 TDengine TSDB CLI 在返回错误信息中返回了具体错误码，用户可到 TDengine TSDB 官网错误码页面查找具体原因及解决措施，见：[错误码参考表](https://docs.taosdata.com/reference/error-code/)
