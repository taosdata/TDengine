---
title: taosdump
description: "taosdump 是一个支持从运行中的 TDengine 集群备份数据并将备份的数据恢复到相同或另一个运行中的 TDengine 集群中的工具应用程序"
---

## 简介

taosdump 是一个支持从运行中的 TDengine 集群备份数据并将备份的数据恢复到相同或另一个运行中的 TDengine 集群中的工具应用程序。

taosdump 可以用数据库、超级表或普通表作为逻辑数据单元进行备份，也可以对数据库、超级
表和普通表中指定时间段内的数据记录进行备份。使用时可以指定数据备份的目录路径，如果
不指定位置，taosdump 默认会将数据备份到当前目录。

如果指定的位置已经有数据文件，taosdump 会提示用户并立即退出，避免数据被覆盖。这意味着同一路径只能被用于一次备份。
如果看到相关提示，请小心操作。

taosdump 是一个逻辑备份工具，它不应被用于备份任何原始数据、环境设置、
硬件信息、服务端配置或集群的拓扑结构。taosdump 使用
[ Apache AVRO ](https://avro.apache.org/)作为数据文件格式来存储备份数据。

## 安装

taosdump 有两种安装方式:

- 安装 taosTools 官方安装包, 请从[发布历史页面](https://docs.taosdata.com/releases/tools/)页面找到 taosTools 并下载安装。

- 单独编译 taos-tools 并安装, 详情请参考 [taos-tools](https://github.com/taosdata/taos-tools) 仓库。

## 常用使用场景

### taosdump 备份数据

1.  备份所有数据库：指定 `-A` 或 `--all-databases` 参数；
2.  备份多个指定数据库：使用 `-D db1,db2,...` 参数；
3.  备份指定数据库中的某些超级表或普通表：使用 `dbname stbname1 stbname2 tbname1 tbname2 ...` 参数，注意这种输入序列第一个参数为数据库名称，且只支持一个数据库，第二个和之后的参数为该数据库中的超级表或普通表名称，中间以空格分隔；
4.  备份系统 log 库：TDengine 集群通常会包含一个系统数据库，名为 `log`，这个数据库内的数据为 TDengine 自我运行的数据，taosdump 默认不会对 log 库进行备份。如果有特定需求对 log 库进行备份，可以使用 `-a` 或 `--allow-sys` 命令行参数。
5.  “宽容”模式备份：taosdump 1.4.1 之后的版本提供 `-n` 参数和 `-L` 参数，用于备份数据时不使用转义字符和“宽容”模式，可以在表名、列名、标签名没使用转义字符的情况下减少备份数据时间和备份数据占用空间。如果不确定符合使用 `-n` 和 `-L` 条件时请使用默认参数进行“严格”模式进行备份。转义字符的说明请参考[官方文档](/taos-sql/escape)。

:::tip
- taosdump 1.4.1 之后的版本提供 `-I` 参数，用于解析 avro 文件 schema 和数据，如果指定 `-s` 参数将只解析 schema。
- taosdump 1.4.2 之后的备份使用 `-B` 参数指定的批次数，默认值为 16384，如果在某些环境下由于网络速度或磁盘性能不足导致 "Error actual dump .. batch .." 可以通过 `-B` 参数调整为更小的值进行尝试。
- taosdump 的导出不支持中断恢复，所以当进程意外终止后，正确的处理方式是删除当前已导出或生成的所有相关文件。
- taosdump 的导入支持中断恢复，但是当进程重新启动时，会收到一些“表已经存在”的提示，可以忽视。

:::

### taosdump 恢复数据

恢复指定路径下的数据文件：使用 `-i` 参数加上数据文件所在路径。如前面提及，不应该使用同一个目录备份不同数据集合，也不应该在同一路径多次备份同一数据集，否则备份数据会造成覆盖或多次备份。

:::tip
taosdump 内部使用 TDengine stmt binding API 进行恢复数据的写入，为提高数据恢复性能，目前使用 16384 为一次写入批次。如果备份数据中有比较多列数据，可能会导致产生 "WAL size exceeds limit" 错误，此时可以通过使用 `-B` 参数调整为一个更小的值进行尝试。

:::

## 详细命令行参数列表

以下为 taosdump 详细命令行参数列表：

```
Usage: taosdump [OPTION...] dbname [tbname ...]
  or:  taosdump [OPTION...] --databases db1,db2,...
  or:  taosdump [OPTION...] --all-databases
  or:  taosdump [OPTION...] -i inpath
  or:  taosdump [OPTION...] -o outpath

  -h, --host=HOST            Server host dumping data from. Default is
                             localhost.
  -p, --password             User password to connect to server. Default is
                             taosdata.
  -P, --port=PORT            Port to connect
  -u, --user=USER            User name used to connect to server. Default is
                             root.
  -c, --config-dir=CONFIG_DIR   Configure directory. Default is /etc/taos
  -i, --inpath=INPATH        Input file path.
  -o, --outpath=OUTPATH      Output file path.
  -r, --resultFile=RESULTFILE   DumpOut/In Result file path and name.
  -a, --allow-sys            Allow to dump system database
  -A, --all-databases        Dump all databases.
  -D, --databases=DATABASES  Dump inputted databases. Use comma to separate
                             databases' name.
  -e, --escape-character     Use escaped character for database name
  -N, --without-property     Dump database without its properties.
  -s, --schemaonly           Only dump tables' schema.
  -d, --avro-codec=snappy    Choose an avro codec among null, deflate, snappy,
                             and lzma.
  -S, --start-time=START_TIME   Start time to dump. Either epoch or
                             ISO8601/RFC3339 format is acceptable. ISO8601
                             format example: 2017-10-01T00:00:00.000+0800 or
                             2017-10-0100:00:00:000+0800 or '2017-10-01
                             00:00:00.000+0800'
  -E, --end-time=END_TIME    End time to dump. Either epoch or ISO8601/RFC3339
                             format is acceptable. ISO8601 format example:
                             2017-10-01T00:00:00.000+0800 or
                             2017-10-0100:00:00.000+0800 or '2017-10-01
                             00:00:00.000+0800'
  -B, --data-batch=DATA_BATCH   Number of data per query/insert statement when
                             backup/restore. Default value is 16384. If you see
                             'error actual dump .. batch ..' when backup or if
                             you see 'WAL size exceeds limit' error when
                             restore, please adjust the value to a smaller one
                             and try. The workable value is related to the
                             length of the row and type of table schema.
  -I, --inspect              inspect avro file content and print on screen
  -L, --loose-mode           Using loose mode if the table name and column name
                             use letter and number only. Default is NOT.
  -n, --no-escape            No escape char '`'. Default is using it.
  -Q, --dot-replace          Repalce dot character with underline character in
                             the table name.(Version 2.5.3)
  -T, --thread-num=THREAD_NUM   Number of thread for dump in file. Default is
                             8.
  -C, --cloud=CLOUD_DSN      specify a DSN to access TDengine cloud service
  -R, --restful              Use RESTful interface to connect TDengine
  -t, --timeout=SECONDS      The timeout seconds for websocket to interact.
  -g, --debug                Print debug info.
  -?, --help                 Give this help list
      --usage                Give a short usage message
  -V, --version              Print program version
  -W, --rename=RENAME-LIST   Rename database name with new name during
                             importing data. RENAME-LIST: 
                             "db1=newDB1|db2=newDB2" means rename db1 to newDB1
                             and rename db2 to newDB2 (Version 2.5.4)

Mandatory or optional arguments to long options are also mandatory or optional
for any corresponding short options.

Report bugs to <support@taosdata.com>.
```
