---
title: taosdump Reference
sidebar_label: taosdump
slug: /tdengine-reference/tools/taosdump
---

`taosdump` is a TDengine data backup/recovery tool provided for open source users, and the backed up data files adopt the standard [Apache AVRO](https://avro.apache.org/)
  Format, convenient for exchanging data with the external ecosystem.
 taosdump provides multiple data backup and recovery options to meet different data needs, and all supported options can be viewed through --help.

## Get

taosdump is the default installation component in the TDengine server and client installation package. It can be used after installation, refer to [TDengine Installation](../../../get-started/)

## Startup

taosdump needs to be run in the command line terminal. It must be run with parameters to indicate backup or restore operations, such as:

``` bash
taosdump -h my-server -D test -o /root/test/
```

The above command means to backup the `test` database on the `my server` machine to the `/root/test/` directory.

``` bash
taosdump -h my-server -i /root/test/
```

The above command means to restore the previously backed up data files in the `/root/test/` directory to the host named `my server`.

## Command Line Parameters

Below is the detailed command line parameters list for taosdump:

```text
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

## Common Use Cases

### Backup Data

1. Backup all databases: specify the `-A` or `--all-databases` parameter.
2. Backup multiple specified databases: use the `-D db1,db2,...` parameter.
3. Backup certain supertables or basic tables in a specified database: use the `dbname stbname1 stbname2 tbname1 tbname2 ...` parameter, note that this input sequence starts with the database name, supports only one database, and the second and subsequent parameters are the names of the supertables or basic tables in that database, separated by spaces.
4. Backup the system log database: TDengine clusters usually include a system database named `log`, which contains data for TDengine's own operation, taosdump does not back up the log database by default. If there is a specific need to back up the log database, you can use the `-a` or `--allow-sys` command line parameter.
5. "Tolerant" mode backup: Versions after taosdump 1.4.1 provide the `-n` and `-L` parameters, used for backing up data without using escape characters and in "tolerant" mode, which can reduce backup data time and space occupied when table names, column names, and label names do not use escape characters. If unsure whether to use `-n` and `-L`, use the default parameters for "strict" mode backup. For an explanation of escape characters, please refer to the [official documentation](../../sql-manual/escape-characters/)
6. If a backup file already exists in the directory specified by the `-o` parameter, to prevent data from being overwritten, taosdump will report an error and exit. Please replace it with another empty directory or clear the original data before backing up.
7. Currently, taosdump does not support data breakpoint backup function. Once the data backup is interrupted, it needs to be started from scratch.
 If the backup takes a long time, it is recommended to use the (-S -E options) method to specify the start/end time for segmented backup.

:::tip

- Versions after taosdump 1.4.1 provide the `-I` parameter, used for parsing avro file schema and data, specifying the `-s` parameter will only parse the schema.
- Backups after taosdump 1.4.2 use the `-B` parameter to specify the number of batches, the default value is 16384. If "Error actual dump .. batch .." occurs due to insufficient network speed or disk performance in some environments, you can try adjusting the `-B` parameter to a smaller value.
- taosdump's export does not support interruption recovery, so the correct way to handle an unexpected termination of the process is to delete all related files that have been exported or generated.
- taosdump's import supports interruption recovery, but when the process restarts, you may receive some "table already exists" prompts, which can be ignored.

:::

### Restore Data

- Restore data files from a specified path: use the `-i` parameter along with the data file path. As mentioned earlier, the same directory should not be used to back up different data sets, nor should the same path be used to back up the same data set multiple times, otherwise, the backup data will cause overwriting or multiple backups.
- taosdump supports data recovery to a new database name with the parameter `-W`, please refer to the command line parameter description for details.
- Supports importing data of unchanged columns when there are changes in the TAG/COLUMN columns of both super tables and ordinary tables (supported in version 3.3.6.0 and above).

:::tip
taosdump internally uses the TDengine stmt binding API to write restored data, currently using 16384 as a batch for writing. If there are many columns in the backup data, it may cause a "WAL size exceeds limit" error, in which case you can try adjusting the `-B` parameter to a smaller value.

:::
