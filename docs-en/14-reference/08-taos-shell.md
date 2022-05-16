---
title: TDengine Command Line (CLI)
sidebar_label: TDengine CLI
description: Instructions and tips for using the TDengine CLI
---

The TDengine command-line application (hereafter referred to as TDengine CLI) is the cleanest and most common way for users to manipulate and interact with TDengine instances.

## Installation

If executed on the TDengine server-side, there is no need for additional installation as it is already installed automatically. To run on the non-TDengine server-side, the TDengine client driver needs to be installed. For details, please refer to [connector](/reference/connector/).

## Execution

To access the TDengine CLI, you can execute `taos` from a Linux terminal or Windows terminal.

```bash
taos
```
TDengine will display a welcome message and version information if the connection to the service is successful. If it fails, TDengine will print an error message (see [FAQ](/train-faq/faq) to solve the problem of terminal connection failure to the server.) The TDengine CLI prompt symbols are as follows:

```cmd
taos>
```
After entering the CLI, you can execute various SQL statements, including inserts, queries, and administrative commands.

## Execute SQL scripts

Run SQL command scripts in the TDengine CLI via the `source` command.

```sql
taos> source <filename>;
```

## Modify display character width online

Users can adjust the character display width in TDengine CLI with the following command:

```sql
taos> SET MAX_BINARY_DISPLAY_WIDTH <nn>;
```

If the displayed content is followed by `...` you can use this command to change the display width to display the full content.

## Command Line Parameters

You can change the behavior of TDengine CLI by configuring command-line parameters. The following command-line arguments are commonly used.

-h, --host=HOST: FQDN of the server where the TDengine server is to be connected. Default is to connect to the local service
-P, --port=PORT: Specify the port number to be used by the server
-u, --user=USER: the user name to use when connecting
-p, --password=PASSWORD: the password to use when connecting to the server
--?, --help: print out all command-line arguments 

And many more parameters.

-c, --config-dir: Specify the configuration file directory. The default is `/etc/taos`, and the default name of the configuration file in this directory is taos.cfg
-C, --dump-config: Print the configuration parameters of taos.cfg in the directory specified by -c
-d, --database=DATABASE: Specify the database to use when connecting to the server
-D, --directory=DIRECTORY: Import the SQL script file in the specified path
-f, --file=FILE: Execute the SQL script file in non-interactive mode
-k, --check=CHECK: Specify the table to be checked
-l, --pktlen=PKTLEN: Test package size to be used for network testing
-n, --netrole=NETROLE: test scope for network connection test, default is `startup`, The value can be `client`, `server`, `rpc`, `startup`, `sync`, `speed`, or `fqdn`.
-r, --raw-time: output the time to uint64_t
-s, --commands=COMMAND: execute SQL commands in non-interactive mode
-S, --pkttype=PKTTYPE: Specify the packet type used for network testing. The default is TCP. only `netrole` can be specified as either TCP or UDP when speed is specified
-T, --thread=THREADNUM: The number of threads to import data in multi-threaded mode
-s, --commands: Run TDengine commands without entering the terminal
-z, --timezone=TIMEZONE: Specify time zone. Default is local
-V, --version: Print out the current version number

Example.

```bash
taos -h h1.taos.com -s "use db; show tables;"
```
## TDengine CLI tips

- You can use the up and down cursor keys to see the history of commands entered
- Change user password: use `alter user` command in TDengine CLI. The default password is `taosdata`.
- ctrl+c to stop a query in progress
- Execute `RESET QUERY CACHE` to clear the local cache of the table schema
- Execute SQL statements in batches. You can store a series of shell commands (ending with ;, one line for each SQL statement) in a file and execute the command `source <file-name>` in the shell to execute all SQL statements in that file automatically
- Enter `q` to exit taos shell
