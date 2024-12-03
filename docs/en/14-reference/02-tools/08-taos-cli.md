---
title: TDengine CLI Reference
sidebar_label: TDengine CLI
slug: /tdengine-reference/tools/tdengine-cli
---

The TDengine Command-Line Interface (hereinafter referred to as TDengine CLI) is the simplest and most commonly used way for users to operate and interact with TDengine instances. Before use, you need to install the TDengine Server or TDengine Client package.

## Starting

To enter the TDengine CLI, simply execute `taos` in the terminal.

```bash
taos
```

If the connection to the service is successful, a welcome message and version information will be printed. If it fails, an error message will be displayed.

The prompt for TDengine CLI is as follows:

```shell
taos>
```

Once inside the TDengine CLI, you can execute various SQL statements, including inserts, queries, and various management commands.

## Executing SQL Scripts

You can run multiple SQL commands from a script file in the TDengine CLI using the `source` command.

```sql
taos> source <filename>;
```

## Online Modification of Display Character Width

You can adjust the character display width in the TDengine CLI using the following command:

```sql
taos> SET MAX_BINARY_DISPLAY_WIDTH <nn>;
```

If the displayed content ends with ..., it indicates that the content has been truncated. You can modify the display character width with this command to show the full content.

## Command-Line Parameters

You can configure command-line parameters to change the behavior of the TDengine CLI. Here are some commonly used command-line parameters:

- -h HOST: The FQDN of the TDengine server to connect to. Defaults to connecting to the local service.
- -P PORT: Specify the port number used by the server.
- -u USER: The username to use when connecting.
- -p PASSWORD: The password to use when connecting to the server.
- -?, --help: Print all command-line parameters.

Other parameters include:

- -a AUTHSTR: Authorization information for connecting to the server.
- -A: Calculate authorization information using username and password.
- -B: Set the BI tool display mode; all output will follow the BI tool's format.
- -c CONFIGDIR: Specify the configuration file directory, defaulting to `/etc/taos` in Linux, where the default configuration file is `taos.cfg`.
- -C: Print the configuration parameters in the `taos.cfg` specified by -c.
- -d DATABASE: Specify the database to use when connecting to the server.
- -E dsn: Use WebSocket DSN to connect to cloud services or servers providing WebSocket connections.
- -f FILE: Execute SQL script files in non-interactive mode. Each SQL statement must occupy one line.
- -k: Test the server's running status; 0: unavailable, 1: network ok, 2: service ok, 3: service degraded, 4: exiting.
- -l PKTLEN: Packet size used for network testing.
- -n NETROLE: The scope of network connection testing, defaulting to `client`, with optional values `client` and `server`.
- -N PKTNUM: The number of packets used in network testing.
- -r: Output time as an unsigned 64-bit integer type (i.e., uint64_t in C).
- -R: Connect to the server using RESTful mode.
- -s COMMAND: Execute SQL commands in non-interactive mode.
- -t: Test the server's startup status, with the same statuses as -k.
- -w DISPLAYWIDTH: Client column display width.
- -z TIMEZONE: Specify the timezone, defaulting to the local timezone.
- -V: Print the current version number.

Example:

```bash
taos -h h1.taos.com -s "use db; show tables;"
```

## Configuration File

You can also control the behavior of the TDengine CLI through parameters set in the configuration file. For available configuration parameters, refer to [Client Configuration](../../components/taosc).

## Error Codes Reference
After version 3.3.4.8 of TDengine, the TDengine CLI returned error codes in the error message. Users can search for the specific cause and solution on the error code page of the TDengine official website, see [Error Codes Table](https://docs.taosdata.com/reference/error-code)

## TDengine CLI Tips

- Use the up and down arrow keys to view previously entered commands.
- Use the `alter user` command in the TDengine CLI to change user passwords, with the default password being `taosdata`.
- Press Ctrl+C to abort an ongoing query.
- Execute `RESET QUERY CACHE` to clear the local table schema cache.
- Batch execute SQL statements. You can store a series of TDengine CLI commands (ending with a semicolon, with each SQL statement on a new line) in a file, and execute the command `source <file-name>` in the TDengine CLI to automatically run all SQL statements in that file.
- Type `q`, `quit`, or `exit` and press Enter to exit the TDengine CLI.

## Exporting Query Results to Files with TDengine CLI

- You can use the symbol “>>” to export query results to a specific file. The syntax is: SQL query statement >> ‘output file name’; if you don't specify a path, the output will be directed to the current directory. For example, `select * from d0 >> ‘/root/d0.csv’;` will output the query results to `/root/d0.csv`.

## Importing Data from Files into Tables with TDengine CLI

- You can use `insert into table_name file 'input file name'` to import the previously exported data file back into the specified table. For example, `insert into d0 file '/root/d0.csv';` indicates that all previously exported data will be imported into the `d0` table.
