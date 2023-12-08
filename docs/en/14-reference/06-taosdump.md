---
title: taosdump
description: This document describes how to use taosdump, a tool for backing up and restoring the data in a TDengine cluster.
---

## Introduction

taosdump is a tool that supports backing up data from a running TDengine cluster and restoring the backed up data to the same, or another running TDengine cluster.

taosdump can back up a database, a super table, or a normal table as a logical data unit or backup data records in the database, super tables, and normal tables. When using taosdump, you can specify the directory path for data backup. If you do not specify a directory, taosdump will back up the data to the current directory by default.

If the specified location already has data files, taosdump will prompt the user and exit immediately to avoid data overwriting. This means that the same path can only be used for one backup.

Please be careful if you see a prompt for this and please ensure that you follow best practices and relevant SOPs for data integrity, backup and data security.

Users should not use taosdump to back up raw data, environment settings, hardware information, server configuration, or cluster topology. taosdump uses [Apache AVRO](https://avro.apache.org/) as the data file format to store backup data.

## Installation

There are two ways to install taosdump:

- Install the taosTools official installer. Please find taosTools from [Release History](https://docs.taosdata.com/releases/tools/) page and download and install it.

- Compile taos-tools separately and install it. Please refer to the [taos-tools](https://github.com/taosdata/taos-tools) repository for details.

## Common usage scenarios

### taosdump backup data

1. backing up all databases: specify `-A` or `-all-databases` parameter.
2. backup multiple specified databases: use `-D db1,db2,... ` parameters;
3. back up some super or normal tables in the specified database: use `dbname stbname1 stbname2 tbname1 tbname2 ... ` parameters. Note that the first parameter of this input sequence is the database name, and only one database is supported. The second and subsequent parameters are the names of super or normal tables in that database, separated by spaces.
4. back up the system log database: TDengine clusters usually contain a system database named `log`. The data in this database is the data that TDengine runs itself, and the taosdump will not back up the log database by default. If users need to back up the log database, users can use the `-a` or `-allow-sys` command-line parameter. 
5. Loose mode backup: taosdump version 1.4.1 onwards provides `-n` and `-L` parameters for backing up data without using escape characters and "loose" mode, which can reduce the number of backups if table names, column names, tag names do not use escape characters. This can also reduce the backup data time and backup data footprint. If you are unsure about using `-n` and `-L` conditions, please use the default parameters for "strict" mode backup. See the [official documentation](../../taos-sql/escape) for a description of escaped characters.

:::tip
- taosdump versions after 1.4.1 provide the `-I` argument for parsing Avro file schema and data. If users specify `-s` then only taosdump will parse schema.
- Backups after taosdump 1.4.2 use the batch count specified by the `-B` parameter. The default value is 16384. If, in some environments, low network speed or disk performance causes "Error actual dump ... batch ...", then try changing the `-B` parameter to a smaller value.
- The export of taosdump does not support resuming from an interruption. Therefore, if the taosdump process terminates unexpectedly, delete all related files that have been exported or generated.
- The import of taosdump supports resuming from an interruption, but when the process resumes, you will receive some "table already exists" messages, which could be ignored.

:::

### taosdump recover data

Restore the data file in the specified path: use the `-i` parameter plus the path to the data file. You should not use the same directory to backup different data sets, and you should not backup the same data set multiple times in the same path. Otherwise, the backup data will cause overwriting or multiple backups.

:::tip
taosdump internally uses TDengine stmt binding API for writing recovery data with a default batch size of 16384 for better data recovery performance. If there are more columns in the backup data, it may cause a "WAL size exceeds limit" error. You can try to adjust the batch size to a smaller value by using the `-B` parameter.

:::

## Detailed command-line parameter list

The following is a detailed list of taosdump command-line arguments.

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
  -P, --port=PORT            Port to connect
  -u, --user=USER            User name used to connect to server. Default is
                             root.
  -c, --config-dir=CONFIG_DIR   Configure directory. Default is /etc/taos
  -i, --inpath=INPATH        Input file path.
  -o, --outpath=OUTPATH      Output file path.
  -r, --resultFile=RESULTFILE   DumpOut/In Result file path and name.
  -a, --allow-sys            Allow to dump system database
  -A, --all-databases        Dump all databases.
  -D, --databases=DATABASES  Dump listed databases. Use comma to separate
                             database names.
  -e, --escape-character     Use escaped character for database name
  -N, --without-property     Dump database without its properties.
  -s, --schemaonly           Only dump table schemas.
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
  -L, --loose-mode           Use loose mode if the table name and column name
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

```
