---
title: taosdump Reference
sidebar_label: taosdump
slug: /tdengine-reference/tools/taosdump
---

`taosdump` is a tool that supports backing up data from a running TDengine cluster and restoring the backed-up data to the same or another running TDengine cluster.

`taosdump` can back up data at the logical data unit level, such as databases, supertables, or basic tables, and can also back up data records within specified time periods from databases, supertables, and basic tables. You can specify the directory path for data backups; if no location is specified, `taosdump` will default to backing up data to the current directory.

If the specified location already contains data files, `taosdump` will prompt the user and exit immediately to prevent data overwriting. This means that the same path can only be used for one backup. Please proceed with caution if you see relevant prompts.

`taosdump` is a logical backup tool and should not be used to back up any raw data, environmental settings, hardware information, server configurations, or the topology of the cluster. `taosdump` uses [Apache AVRO](https://avro.apache.org/) as the data file format to store backup data.

## Installation

There are two installation methods for `taosdump`:

- Install the official `taosTools` installation package. You can find and download `taosTools` from the [Release History page](https://docs.taosdata.com/releases/tools/).
- Compile `taos-tools` separately and install it. For details, please refer to the [taos-tools](https://github.com/taosdata/taos-tools) repository.

## Common Use Cases

### Backing Up Data with taosdump

1. Back up all databases: Use the `-A` or `--all-databases` parameter.
2. Back up multiple specified databases: Use the `-D db1,db2,...` parameter.
3. Back up certain supertables or basic tables in a specified database: Use the `dbname stbname1 stbname2 tbname1 tbname2 ...` parameter. Note that the first parameter in this input sequence must be the database name, and it only supports one database. The second and subsequent parameters are the names of the supertables or basic tables in that database, separated by spaces.
4. Back up the system `log` database: The TDengine cluster usually contains a system database named `log`, which holds data for the self-operation of TDengine. By default, `taosdump` will not back up the `log` database. If there is a specific requirement to back up the `log` database, use the `-a` or `--allow-sys` command-line parameter.
5. "Loose" mode backups: Starting from version 1.4.1, `taosdump` provides the `-n` and `-L` parameters for backing up data without using escape characters and "loose" mode, which can reduce backup time and storage space for backup data when table names, column names, and tag names do not use escape characters. If you are unsure whether the conditions for using `-n` and `-L` are met, please use the default parameters for "strict" mode backups. For details about escape characters, refer to the [official documentation](../../sql-manual/escape-characters/).

:::tip

- After version 1.4.1, `taosdump` provides the `-I` parameter for parsing the AVRO file schema and data. If you specify the `-s` parameter, it will only parse the schema.
- After version 1.4.2, backups use the batch size specified by the `-B` parameter, with a default value of 16384. If you encounter "Error actual dump .. batch .." due to network speed or disk performance issues, you can try adjusting it to a smaller value using the `-B` parameter.
- The export process of `taosdump` does not support interrupted recovery. Therefore, if the process unexpectedly terminates, the correct action is to delete all related files that have been exported or generated.
- The import process of `taosdump` supports interrupted recovery; however, when the process is restarted, you may receive some "table already exists" prompts, which can be ignored.

:::

### Restoring Data with taosdump

To restore data files from a specified path, use the `-i` parameter followed by the path to the data file. As previously mentioned, do not use the same directory to back up different data sets, nor should you back up the same data set multiple times in the same path, as this could lead to overwriting or multiple backups of the data.

:::tip

`taosdump` uses the TDengine statement binding API for writing restored data. To improve restoration performance, a batch size of 16384 is currently used for each write operation. If the backup data contains many columns, you might encounter the "WAL size exceeds limit" error. In this case, try using the `-B` parameter to adjust it to a smaller value.

:::

## Detailed Command-Line Parameters List

Below is the detailed list of command-line parameters for `taosdump`:

```text
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
                             databases' names.
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
  -I, --inspect              Inspect avro file content and print on screen
  -L, --loose-mode           Using loose mode if the table name and column name
                             use letter and number only. Default is NOT.
  -n, --no-escape            No escape char '`'. Default is using it.
  -Q, --dot-replace          Replace dot character with underline character in
                             the table name.(Version 2.5.3)
  -T, --thread-num=THREAD_NUM   Number of threads for dump in file. Default is
                             8.
  -C, --cloud=CLOUD_DSN      Specify a DSN to access TDengine cloud service
  -R, --restful              Use RESTful interface to connect to TDengine
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
