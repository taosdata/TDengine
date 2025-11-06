---
title: taosdump 参考手册
sidebar_label: taosdump
toc_max_heading_level: 4
---

taosdump 是为开源用户提供的 TDengine TSDB 数据备份/恢复工具，备份数据文件采用标准 [Apache AVRO](https://avro.apache.org/) 格式，方便与外界生态交换数据。taosdump 提供多种数据备份及恢复选项来满足不同需求，可通过 --help 查看支持的全部选项。

## 工具获取

taosdump 是 TDengine TSDB 服务器及客户端安装包中默认安装组件，安装后即可使用，参考 [TDengine TSDB 安装](../../../get-started/)

## 运行

taosdump 需在命令行终端中运行，运行时必须带参数，指明是备份操作或还原操作，如：

``` bash
taosdump -h my-server -D test -o /root/test/
```

以上命令表示备份主机名为 `my-server` 机器上的 `test` 数据库到 `/root/test/` 目录下。

``` bash
taosdump -h my-server -i /root/test/
```

以上命令表示把 `/root/test/` 目录下之前备份的数据文件恢复到主机名为 `my-server` 的主机上。

## 命令行参数

以下为 taosdump 详细命令行参数列表：

```
Usage: taosdump [OPTION...] dbname [tbname ...]
  or:  taosdump [OPTION...] --databases db1,db2,...
  or:  taosdump [OPTION...] --all-databases
  or:  taosdump [OPTION...] -i inpath
  or:  taosdump [OPTION...] -o outpath

  -h, --host=HOST            Server host from which to dump data. Default is
                             localhost.
  -p, --password             User password to connect to server. Default is
                             taosdata.
  -P, --port=PORT            Port to connect.
  -u, --user=USER            User name used to connect to server. Default is
                             root.
  -c, --config-dir=CONFIG_DIR   Configure directory. Default is /etc/taos.
  -i, --inpath=INPATH        Input file path.
  -o, --outpath=OUTPATH      Output file path.
  -r, --resultFile=RESULTFILE   DumpOut/In Result file path and name.
  -a, --allow-sys            Allow to dump system database (2.0 only).
  -A, --all-databases        Dump all databases.
  -D, --databases=DATABASES  Dump listed databases. Use comma to separate
                             databases names.
  -e, --escape-character     Use escaped character for database name.
  -N, --without-property     Dump database without its properties.
  -s, --schemaonly           Only dump table schemas.
  -d, --avro-codec=snappy    Choose an avro codec among null, deflate, snappy,
                             and lzma(Windows is not currently supported).
  -S, --start-time=START_TIME   Start time to dump. Either epoch or
                             ISO8601/RFC3339 format is acceptable. ISO8601
                             format example: 2017-10-01T00:00:00.000+0800 or
                             2017-10-0100:00:00:000+0800 or '2017-10-01
                             00:00:00.000+0800'.
  -E, --end-time=END_TIME    End time to dump. Either epoch or ISO8601/RFC3339
                             format is acceptable. ISO8601 format example:
                             2017-10-01T00:00:00.000+0800 or
                             2017-10-0100:00:00.000+0800 or '2017-10-01
                             00:00:00.000+0800'.
  -B, --data-batch=DATA_BATCH   Number of data per query/insert statement when
                             backup/restore. Default value is 16384. If you see
                             'error actual dump .. batch ..' when backup or if
                             you see 'WAL size exceeds limit' error when
                             restore, please adjust the value to a smaller one
                             and try. The workable value is related to the
                             length of the row and type of table schema.
  -I, --inspect              inspect avro file content and print on screen.
  -L, --loose-mode           Use loose mode if the table name and column name
                             use letter and number only. Default is NOT.
  -n, --no-escape            No escape char '`'. Default is using it.
  -Q, --dot-replace          Replace dot character with underline character in
                             the table name.
  -T, --thread-num=THREAD_NUM   Number of threads for dump in/out data. Default
                             is 8.
  -W, --rename=RENAME-LIST   Rename database name with new name during
                             importing data.         RENAME-LIST:
                             "db1=newDB1|db2=newDB2" means rename db1 to newDB1
                             and rename db2 to newDB2.
  -C, --cloud=CLOUD_DSN      Alias for the -X/--dsn option.
  -k, --retry-count=VALUE    Set the number of retry attempts for connection or
                             query failures.
  -R, --restful              Use RESTful interface to connect server.
  -t, --timeout=SECONDS      The timeout seconds for websocket to interact.
  -X, --dsn=DSN              The dsn to connect the cloud service.
  -z, --retry-sleep-ms=VALUE Sleep interval between retries, in milliseconds.
  -Z, --driver=DRIVER        Connect driver , value can be "Native" or
                             "WebSocket", default is Native.
  -g, --debug                Print debug info.
  -?, --help                 Give this help list
      --usage                Give a short usage message
  -V, --version              Print program version.

Mandatory or optional arguments to long options are also mandatory or optional
for any corresponding short options.

Report bugs to <support@taosdata.com>.
```

## 常用使用场景

### 备份数据

1. 备份所有数据库：指定 `-A` 或 `--all-databases` 参数。
2. 备份多个指定数据库：使用 `-D db1,db2,...` 参数。
3. 备份指定数据库中某些超级表或普通表：使用 `dbname stbname1 stbname2 tbname1 tbname2 ...` 参数，注意这种输入序列第一个参数为数据库名称，且只支持一个数据库，第二个和之后的参数为该数据库中的超级表或普通表名称，中间以空格分隔。
4. 备份系统 log 库：TDengine TSDB 集群通常会包含一个系统数据库，名为 `log`，这个数据库内的数据为 TDengine TSDB 自我运行的数据，taosdump 默认不会对 log 库进行备份。如果有特定需求对 log 库进行备份，可以使用 `-a` 或 `--allow-sys` 命令行参数。
5. “宽容”模式备份：taosdump 1.4.1 之后的版本提供 `-n` 参数和 `-L` 参数，用于备份数据时不使用转义字符和“宽容”模式，可以在表名、列名、标签名没使用转义字符的情况下减少备份数据时间和备份数据占用空间。如果不确定符合使用 `-n` 和 `-L` 条件时请使用默认参数进行“严格”模式进行备份。转义字符的说明请参考 [官方文档](../../taos-sql/escape)
6. `-o` 参数指定的目录下如果已存在备份文件，为防止数据被覆盖，taosdump 会报错并退出，请更换其它空目录或清空原来数据后再备份。
7. 目前 taosdump 不支持数据断点继备功能，一旦数据备份中断，需要从头开始。如果备份需要很长时间，建议使用（-S -E 选项）指定开始/结束时间进行分段备份的方法。

:::tip

- taosdump 1.4.1 之后的版本提供 `-I` 参数，用于解析 avro 文件 schema 和数据，如果指定 `-s` 参数将只解析 schema。
- taosdump 1.4.2 之后的备份使用 `-B` 参数指定的批次数，默认值为 16384，如果在某些环境下由于网络速度或磁盘性能不足导致 "Error actual dump .. batch .." 可以通过 `-B` 参数调整为更小的值进行尝试。
- taosdump 的导出不支持中断恢复，所以当进程意外终止后，正确的处理方式是删除当前已导出或生成的所有相关文件。
- taosdump 的导入支持中断恢复，但是当进程重新启动时，会收到一些“表已经存在”的提示，可以忽视。

:::

### 恢复数据

- 恢复指定路径下的数据文件：使用 `-i` 参数加上数据文件所在路径。如前面提及，不应该使用同一个目录备份不同数据集合，也不应该在同一路径多次备份同一数据集，否则备份数据会造成覆盖或多次备份。
- taosdump 支持数据恢复至新数据库名下，参数是 -W, 详细见命令行参数说明。
- 支持超级表及普通表 TAG/COLUMN 列有变动时仍可导入未变动列的数据（3.3.6.0 及以上版本及使用新版本导出数据才能支持）。

:::tip
taosdump 内部使用 TDengine TSDB stmt binding API 进行恢复数据的写入，为提高数据恢复性能，目前使用 16384 为一次写入批次。如果备份数据中有较多列数据，可能会导致产生 "WAL size exceeds limit" 错误，此时可以通过使用 `-B` 参数调整为一个更小的值进行尝试。

:::
