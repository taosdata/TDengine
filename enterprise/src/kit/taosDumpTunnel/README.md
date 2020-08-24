## Introduction
The tool _**taosDumpTunnel**_ is used to dump data from a TDengine server to
another, regardless the version of two servers. For example, you can use the
tool to dump data from version 1.6.X to 2.0.X. The tool uses go driver to read 
data from source server, and use HTTP interface to dump data into the destination
server.

## Build
To build the tool, make sure you have go build environment at first. Please use your
search engine to help to finishe this step.

Then, download the taosdata go driver from [github](https://github.com/taosdata/driver-go).
Use branch 1.6 to build the tool.
```
go build -o taosDumpTunnel taosDumpTunnel.go
```

## Usage
Users can use _-h_ option to check the usage of tool.

```
./taosDumpTunnel -h
```

## Examples
- Example 1: dump a whole database _**db**_ data
    ```
    ./taosDumpTunnel -db=db
    ```
- Example 2: dump data of tables `t0 t1 t2` in database _**db**_
    ```
    ./taosDumpTunnel -db=db t0 t1 t2
    ```
- Example 3: dump a whole database _**db**_ data with batch size 1000
    ```
    ./taosDumpTunnel -db=db -batch=1000
    ```

## Note
1. Destination server must create database before doing dump work.
2. Do _**NOT**_ use super table names as the argument of the tool.