---
title: TDengine Command Line Interface (CLI)
sidebar_label: Command Line Interface
description: This document describes how to use the TDengine CLI.
---

The TDengine command-line interface (hereafter referred to as `TDengine CLI`) is the simplest way for users to manipulate and interact with TDengine instances.

## Installation

If executed on the TDengine server-side, there is no need for additional installation steps to install TDengine CLI as it is already included and installed automatically. To run TDengine CLI in an environment where no TDengine server is running, the TDengine client installation package needs to be installed first. For details, please refer to [Install Client Driver](../../client-libraries/#install-client-driver).

## Execution

To access the TDengine CLI, you can execute `taos` command-line utility from a terminal.

```bash
taos
```

TDengine CLI will display a welcome message and version information if it successfully connected to the TDengine service. If it fails, TDengine CLI will print an error message. See [FAQ](../../train-faq/faq) to solve the problem of terminal connection failure to the server. The TDengine CLI prompts as follows:

```cmd
taos>
```

After entering the TDengine CLI, you can execute various SQL commands, including inserts, queries, or administrative commands.

## Execute SQL script file

Run SQL command script file in the TDengine CLI via the `source` command.

```sql
taos> source <filename>;
```

## Adjust display width to show more characters

Users can adjust the display width in TDengine CLI to show more characters with the following command:

```sql
taos> SET MAX_BINARY_DISPLAY_WIDTH <nn>;
```

If the displayed content is followed by `...` you can use this command to change the display width to display the full content.

## Command Line Parameters

You can change the behavior of TDengine CLI by specifying command-line parameters. The following parameters are commonly used.

- -h HOST: FQDN of the server where the TDengine server is to be connected. Default is to connect to the local service
- -P PORT: Specify the port number to be used by the server. Default is `6030`
- -u USER: the user name to use when connecting. Default is `root`
- -p PASSWORD: the password to use when connecting to the server. Default is `taosdata`
- -?, --help: print out all command-line arguments

And many more parameters.

- -a AUTHSTR: Authorization information to connect to the server.
- -A: Obtain authorization information from username and password.
- -B: Set BI mode , all outputs follow the format of BI tools for output if setting
- -c CONFIGDIR: Specify the directory where configuration file exists. The default is `/etc/taos`, and the default name of the configuration file in this directory is `taos.cfg`
- -C: Print the configuration parameters of `taos.cfg` in the default directory or specified by -c
- -d DATABASE: Specify the database to use when connecting to the server
- -E dsn: connect to the TDengine Cloud or a server which provides WebSocket connection
- -f FILE: Execute the SQL script file in non-interactive mode Note that each SQL statement in the script file must be only one line.
- -k: Test the operational status of the server. 0: unavailable; 1: network ok; 2: service ok; 3: service degraded; 4: exiting
- -l PKTLEN: Test package size to be used for network testing
- -n NETROLE: test scope for network connection test, default is `client`. The value can be `client` or `server`.
- -N PKTNUM: Number of packets used for network testing
- -r: output the timestamp format as unsigned 64-bits integer (uint64_t in C language)
- -R: Use RESTful mode when connecting
- -s COMMAND: execute SQL commands in non-interactive mode
- -t: Test the boot status of the server. The statuses of -k apply.
- -w DISPLAYWIDTH: Specify the number of columns of the server display.
- -z TIMEZONE: Specify time zone. Default is the value of current configuration file
- -V: Print out the current version number

For example:

```bash
taos -h h1.taos.com -s "use db; show tables;"
```

## Export query results to a file

- You can use ">>" to export the query results to a file, the syntax is like `select * from table >> file`. If there is only file name without path, the file will be generated under the current working directory of TDegnine CLI.

## Import data from CSV file

- You can use `insert into table_name file 'fileName'` to import the data from the specified file into the specified table. For example, `insert into d0 file '/root/d0.csv';` means importing the data in file "/root/d0.csv" into table "d0". If there is only file name without path, that means the file is located under current working directory of TDengine CLI. 

## TDengine CLI tips

- You can use the up and down keys to iterate the history of commands entered
- Change user password: use `alter user` command in TDengine CLI to change user's password. The default password is `taosdata`.
- use Ctrl+C to stop a query in progress
- Execute `RESET QUERY CACHE` to clear the local cache of the table schema
- Execute SQL statements in batches. You can store a series of shell commands (ending with ;, one line for each SQL command) in a script file and execute the command `source <file-name>` in the TDengine CLI to execute all SQL commands in that file automatically
- Enter `q` to exit TDengine CLI

  
