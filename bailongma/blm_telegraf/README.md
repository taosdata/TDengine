# API for Telegraf

This is an API to support Telegraf writing data into TDengine.

## prerequisite

before running the software, you need to install the `golang-1.10` or later version in your environment and install [TDengine][] so the program can use the lib of TDengine.

To use it:

```
go build
```
During the go build process, there maybe some errors arised because of lacking some needed packages. You can use `go get` the package to solve it
```
go get github.com/taosdata/TDengine/src/connector/go/src/taosSql

```
After successful build, there will be a blm_telegraf in the same directory. 

## Running in background

Using following command to run the program in background

```
nohup ./blm_telegraf --host 112.102.3.69:0 --batch-size 200 --http-workers 2 --sql-workers 2 --dbname telegraf --port 1234 > /dev/null 2>&1 &
```
The API url is `http://ipaddress:port/telegraf`

There are several options can be set:

```
--host 
set the host of TDengine, IP:port, for example "192.168.0.1:0"

--batch-size 
set the size of how many records in one SQL cmd line writing into TDengine. There is a limitation that TDengine could only accept SQL line small than 64000 bytes, so usually the batch size should not exceed 800. Default is 10.

--http-workers
set the number of workers who process the HTTP request. default is 10

--sql-workers
set the number of workers who process the database request. default is 10 

--dbname
set the database name in TDengine, if not exists, a database will be created after this dbname. default is "telegraf".

--dbuser
set the user name that have the right to access the TDengine. default is "root"

--dbpassword
set the password of dbuser. default is "taosdata"

--port
set the port that prometheus configuration remote_write. as showed above, in the prometheus.yaml

```

## Configure the Telegraf

To write into blm_telegraf API, you should configure the telegraf as below
In the telegraf configuration file, output plugin part:

1. telegraf out put plugin setup:
Set the url to the blm_telegraf API 
Set the data format as "json"
Set the json timstamp units as "1ms"
```toml
[[outputs.http]]
#   ## URL is the address to send metrics to
url = "http://114.116.124.178:8081/telegraf"

data_format = "json"
json_timestamp_units = "1ms"

```
In the Agent part, the hostname should be unique among all the telegraf which report to the TDengine.

```toml
# Configuration for telegraf agent
[agent]
  ## Default data collection interval for all inputs
  interval = "5s"
...

  ## Override default hostname, if empty use os.Hostname()
  hostname = "testhost1"

```
Then start telegraf:

```sh
 telegraf --config xxx.conf
```
Then you can check the TDengine if there is super table and tables.

## Check the TDengine tables and datas

Use the taos client shell to query the result.
```
Welcome to the TDengine shell from linux, client version:1.6.4.0 server version:1.6.4.0
Copyright (c) 2017 by TAOS Data, Inc. All rights reserved.

This is the trial version and will expire at 2019-12-11 14:25:31.

taos> use prometheus;
Database changed.

taos> show stables;
                              name                              |     created_time     |columns| tags  |  tables   |
====================================================================================================================
system                                                          | 19-11-22 21:48:10.205|      2|      3|         12|
system_str                                                      | 19-11-22 21:48:10.205|      2|      3|          2|
cpu                                                             | 19-11-22 21:48:10.225|      2|      4|        200|
cpu_str                                                         | 19-11-22 21:48:10.226|      2|      4|          0|
processes                                                       | 19-11-22 21:48:10.230|      2|      3|         16|
processes_str                                                   | 19-11-22 21:48:10.230|      2|      3|          0|
disk                                                            | 19-11-22 21:48:10.233|      2|      7|        357|
disk_str                                                        | 19-11-22 21:48:10.234|      2|      7|          0|
diskio                                                          | 19-11-22 21:48:10.247|      2|      4|         72|
diskio_str                                                      | 19-11-22 21:48:10.248|      2|      4|          0|
swap                                                            | 19-11-22 21:48:10.254|      2|      3|          7|
swap_str                                                        | 19-11-22 21:48:10.255|      2|      3|          0|
mem                                                             | 19-11-22 21:48:10.272|      2|      3|         61|
mem_str                                                         | 19-11-22 21:48:10.272|      2|      3|          0|
Query OK, 14 row(s) in set (0.000733s)



taos> select * from mem;

......

 19-11-23 14:19:11.000|              0.000000000|testhost1                                         |1.202.240.226       |huge_pages_free                         |
 19-11-23 14:19:16.000|              0.000000000|testhost1                                         |1.202.240.226       |huge_pages_free                         |
 19-11-23 14:19:21.000|              0.000000000|testhost1                                         |1.202.240.226       |huge_pages_free                         |
 19-11-23 14:19:26.000|              0.000000000|testhost1                                         |1.202.240.226       |huge_pages_free                         |Query OK, 3029 row(s) in set (0.060828s)


```

## Support Kubernates liveness probe
The blm_telegraf support the liveness probe.

When the service is running, GET the url`http://ip:port/health` will return 200 OK response which means the service is running healthy. If no response, means the service is dead and need to restart it.


## Limitations

The TDengine limits the length of super table name, so if the name of Telegraf measurement name  exceeds 60 byte, it will be truncated to first 60 bytes. And the length of tags name is limited within 50 byte.  


[TDengine]:https://www.github.com/Taosdata/TDengine