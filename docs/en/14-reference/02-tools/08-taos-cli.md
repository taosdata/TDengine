---
title: TDengine CLI Reference
sidebar_label: TDengine CLI
slug: /tdengine-reference/tools/tdengine-cli
---

The TDengine command line program (hereinafter referred to as TDengine CLI) is the simplest and most commonly used tool for users to operate and interact with TDengine instances. It requires the installation of either the TDengine Server package or the TDengine Client package before use.

## Get

TDengine CLI is the default installation component in the TDengine server and client installation package. It can be used after installation, refer to [TDengine Installation](../../../get-started/)

## Startup

To enter the TDengine CLI, simply execute `taos` in the terminal.

```shell
taos
```

If the connection to the service is successful, a welcome message and version information will be printed. If it fails, an error message will be printed.

The TDengine CLI prompt is as follows:

```shell
taos>
```

After entering the TDengine CLI, you can execute various SQL statements, including insertions, queries, and various management commands.
To exit the TDengine CLI, execute `q`, `quit`, or `exit` and press enter.

```shell
taos> quit
```

## Command Line Parameters

### Basic Parameters

You can change the behavior of the TDengine CLI by configuring command line parameters. Below are some commonly used command line parameters:

- -h HOST: The FQDN of the server where the TDengine service is located, default is to connect to the local service.
- -P PORT: Specifies the port number used by the server.
- -u USER: Username to use when connecting.
- -p PASSWORD: Password to use when connecting to the server.
- -?, --help: Prints out all command line parameters.
- -s COMMAND: SQL command executed in non-interactive mode.  
    Use the `-s` parameter to execute SQL non interactively, and exit after execution. This mode is suitable for use in automated scripts.
    For example, connect to the server h1.taos.com with the following command, and execute the SQL specified by `-s`:

    ```bash
    taos -h my-server -s "use db; show tables;"
    ```

- -c CONFIGDIR: Specify the configuration file directory.  
    In Linux, the default is `/etc/tao`. The default name of the configuration file in this directory is `taos.cfg`.
    Use the `-c` parameter to change the location where the `taosc` client loads the configuration file. For client configuration parameters, refer to [Client Configuration](../../components/taosc).
    The following command specifies the `taos.cfg` configuration file under `/root/cfg/` loaded by the `taosc` client.

    ```bash
    taos -c /root/cfg/
    ```

### Advanced Parameters

- -a AUTHSTR: Authorization information for connecting to the server.
- -A: Calculate authorization information using username and password.
- -B: Set BI tool display mode, after setting, all outputs follow the format of BI tools.
- -C: Print the configuration parameters of `taos.cfg` in the directory specified by -c.
- -d DATABASE: Specifies the database to use when connecting to the server.
- -E dsn: Connect to cloud services or servers providing WebSocket connections using WebSocket DSN.
- -f FILE: Execute SQL script file in non-interactive mode. Each SQL statement in the file must occupy one line.
- -k: Test the running status of the server, 0: unavailable, 1: network ok, 2: service ok, 3: service degraded, 4: exiting.
- -l PKTLEN: Test packet size used during network testing.
- -n NETROLE: Test range during network connection testing, default is `client`, options are `client`, `server`.
- -N PKTNUM: Number of test packets used during network testing.
- -r: Convert time columns to unsigned 64-bit integer type output (i.e., uint64_t in C language).
- -R: Connect to the server using RESTful mode.
- -t: Test the startup status of the server, status same as -k.
- -w DISPLAYWIDTH: Client column display width.
- -z TIMEZONE: Specifies the timezone, default is the local timezone.
- -V: Print the current version number.
- -Z：The connection method, with 0 indicating the use of native connection method, 1 indicating the use of WebSocket connection method, and default to native connection method.

## Data Export/Import

### Data Export To a File

- You can use the symbol `>>` to export query results to a file, the syntax is: sql query statement >> 'output file name'; If no path is written for the output file, it will be output to the current directory. For example, `select * from d0 >> '/root/d0.csv';` will output the query results to /root/d0.csv.

### Data Import From a File

- You can use insert into table_name file 'input file name', to import the data file exported in the previous step back into the specified table. For example, `insert into d0 file '/root/d0.csv';` means to import all the data exported above back into the d0 table.

## Execute SQL Scripts

In the TDengine CLI, you can run multiple SQL commands from a script file using the `source` command, multiple SQL statements in the script file can be written in line.

```sql
taos> source <filename>;
```

## TDengine CLI Tips

### TAB Key Completion

- Pressing the TAB key when no command is present will list all commands supported by TDengine CLI.
- Pressing the TAB key when preceded by a space will display the first of all possible command words at this position, pressing TAB again will switch to the next one.
- If a string precedes the TAB key, it will search for all command words that match the prefix of this string and display the first one, pressing TAB again will switch to the next one.
- Entering a backslash `\` + TAB key, will automatically complete to the column display mode command word `\G;`.

### Modification of Display Character Width

You can adjust the display character width in the TDengine CLI using the following command:

```sql
taos> SET MAX_BINARY_DISPLAY_WIDTH <nn>;
```

If the displayed content ends with ..., it indicates that the content has been truncated. You can modify the display character width with this command to display the full content.

### Other

- You can use the up and down arrow keys to view previously entered commands.
- In TDengine CLI, use the `alter user` command to change user passwords, the default password is `taosdata`.
- Ctrl+C to stop a query that is in progress.
- Execute `RESET QUERY CACHE` to clear the cache of the local table Schema.
- Batch execute SQL statements.
    You can store a series of TDengine CLI commands (ending with a semicolon `;`, each SQL statement on a new line) in a file, and execute the command `source <file-name>` in TDengine CLI to automatically execute all SQL statements in that file.

## Error Code Table

Starting from TDengine version 3.3.4.8, TDengine CLI returns specific error codes in error messages. Users can visit the TDengine official website's error code page to find specific reasons and solutions, see: [Error Code Reference](../../error-codes/)
