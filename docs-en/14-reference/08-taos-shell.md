---
title: TDengine Command Line (CLI)
sidebar_label: TDengine CLI
description: Instructions and tips for using the TDengine CLI
---

The TDengine command-line application (hereafter referred to as `TDengine CLI`) is the simplest way for users to manipulate and interact with TDengine instances.

## Installation

If executed on the TDengine server-side, there is no need for additional installation steps to install TDengine CLI as it is already included and installed automatically. To run TDengine CLI in an environment where no TDengine server is running, the TDengine client installation package needs to be installed first. For details, please refer to [connector](/reference/connector/).

## Execution

To access the TDengine CLI, you can execute `taos` command-line utility from a Linux terminal or Windows terminal.

```bash
taos
```

TDengine CLI will display a welcome message and version information if it successfully connected to the TDengine service. If it fails, TDengine CLI will print an error message. See [FAQ](/train-faq/faq) to solve the problem of terminal connection failure to the server. The TDengine CLI prompts as follows:

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

- -h, --host=HOST: FQDN of the server where the TDengine server is to be connected. Default is to connect to the local service
- -P, --port=PORT: Specify the port number to be used by the server. Default is `6030`
- -u, --user=USER: the user name to use when connecting. Default is `root`
- -p, --password=PASSWORD: the password to use when connecting to the server. Default is `taosdata`
- -?, --help: print out all command-line arguments 

And many more parameters.

- -c, --config-dir: Specify the directory where configuration file exists. The default is `/etc/taos`, and the default name of the configuration file in this directory is `taos.cfg`
- -C, --dump-config: Print the configuration parameters of `taos.cfg` in the default directory or specified by -c
- -d, --database=DATABASE: Specify the database to use when connecting to the server
- -D, --directory=DIRECTORY: Import the SQL script file in the specified path
- -f, --file=FILE: Execute the SQL script file in non-interactive mode
- -k, --check=CHECK: Specify the table to be checked
- -l, --pktlen=PKTLEN: Test package size to be used for network testing
- -n, --netrole=NETROLE: test scope for network connection test, default is `startup`. The value can be `client`, `server`, `rpc`, `startup`, `sync`, `speed`, or `fqdn`.
- -r, --raw-time: output the timestamp format as unsigned 64-bits integer (uint64_t in C language)
- -s, --commands=COMMAND: execute SQL commands in non-interactive mode
- -S, --pkttype=PKTTYPE: Specify the packet type used for network testing. The default is TCP, can be specified as either TCP or UDP when `speed` is specified to `netrole` parameter
- -T, --thread=THREADNUM: The number of threads to import data in multi-threaded mode
- -s, --commands: Run TDengine CLI commands without entering the terminal
- -z, --timezone=TIMEZONE: Specify time zone. Default is the value of current configuration file
- -V, --version: Print out the current version number

Example.

```bash
taos -h h1.taos.com -s "use db; show tables;"
```
## TDengine CLI tips

- You can use the up and down keys to iterate the history of commands entered
- Change user password: use `alter user` command in TDengine CLI to change user's password. The default password is `taosdata`.
- use Ctrl+C to stop a query in progress
- Execute `RESET QUERY CACHE` to clear the local cache of the table schema
- Execute SQL statements in batches. You can store a series of shell commands (ending with ;, one line for each SQL command) in a script file and execute the command `source <file-name>` in the TDengine CLI to execute all SQL commands in that file automatically
- Enter `q` to exit TDengine CLI
