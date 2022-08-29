# 如何在windows上使用nodejs进行TDengine应用开发

## 环境准备

（1）安装nodejs-10.22.0

下载链接：https://nodejs.org/dist/v10.22.0/node-v10.22.0-win-x64.zip
解压安装，把node配置到环境变量里

cmd启动命令行，查看node的版本

```shell
> node.exe --version
v10.22.0

> npm --version
6.14.6
```



（2）安装python2.7

下载链接：https://www.python.org/ftp/python/2.7.18/python-2.7.18.amd64.msi

查看python版本

```shell
>python --version
Python 2.7.18
```


（3）安装TDengine-client

下载地址：https://www.taosdata.com/cn/all-downloads/，选择一个合适的windows-client下载（client应该尽量与server端的版本保持一致）

使用client的TDengine CLI连接server

```shell
>taos -h node5

Welcome to the TDengine shell from Linux, Client Version:2.0.6.0
Copyright (c) 2017 by TAOS Data, Inc. All rights reserved.

taos> show dnodes;
   id   |           end_point            | vnodes | cores  |    status    |  role   |       create_time       |       offline reason       |
============================================================================================================================================
      1 | node5:6030                     |      7 |      1 | ready        | any     | 2020-10-26 09:45:26.308 |                            |
Query OK, 1 row(s) in set (0.036000s)
```

注意：
* 检查能否在client的机器上ping通server的fqdn
* 如果你的dns server并没有提供到server的域名解析，可以将server的hostname配置到client的hosts文件中


## 应用开发

（1）建立nodejs项目

```
npm init
```

（2）安装windows-build-tools
```
npm install --global --production windows-build-tools
```

（3）安装td2.0-connector驱动

``` tdshell
npm install td2.0-connector
```

（4）nodejs访问tdengine的示例程序

```javascript
const taos = require('td2.0-connector');

var host = null;
var port = 6030;
for (var i = 2; i < global.process.argv.length; i++) {
    var key = global.process.argv[i].split("=")[0];
    var value = global.process.argv[i].split("=")[1];

    if ("host" == key) {
        host = value;
    }
    if ("port" == key) {
        port = value;
    }
}

if (host == null) {
    console.log("Usage: node nodejsChecker.js host=<hostname> port=<port>");
    process.exit(0);
}

// establish connection
var conn = taos.connect({host: host, user: "root", password: "taosdata", port: port});
var cursor = conn.cursor();
// create database
executeSql("create database if not exists testnodejs", 0);
// use db
executeSql("use testnodejs", 0);
// drop table
executeSql("drop table if exists testnodejs.weather", 0);
// create table
executeSql("create table if not exists testnodejs.weather(ts timestamp, temperature float, humidity int)", 0);
// insert
executeSql("insert into testnodejs.weather (ts, temperature, humidity) values(now, 20.5, 34)", 1);
// select
executeQuery("select * from testnodejs.weather");
// close connection
conn.close();

function executeQuery(sql) {
    var start = new Date().getTime();
    var promise = cursor.query(sql, true);
    var end = new Date().getTime();
    promise.then(function (result) {
        printSql(sql, result != null, (end - start));
        result.pretty();
    });
}

function executeSql(sql, affectRows) {
    var start = new Date().getTime();
    var promise = cursor.execute(sql);
    var end = new Date().getTime();
    printSql(sql, promise == affectRows, (end - start));
}

function printSql(sql, succeed, cost) {
    console.log("[ " + (succeed ? "OK" : "ERROR!") + " ] time cost: " + cost + " ms, execute statement ====> " + sql);
}
```

（5）测试nodejs程序

```shell
>node nodejsChecker.js
Usage: node nodejsChecker.js host=<hostname> port=<port>
# 提示指定host

>node nodejsChecker.js host=node5
Successfully connected to TDengine
Query OK, 0 row(s) affected (0.00997610s)
[ OK ] time cost: 14 ms, execute statement ====> create database if not exists testnodejs
Query OK, 0 row(s) affected (0.00235920s)
[ OK ] time cost: 4 ms, execute statement ====> use testnodejs
Query OK, 0 row(s) affected (0.06604280s)
[ OK ] time cost: 67 ms, execute statement ====> drop table if exists testnodejs.weather
Query OK, 0 row(s) affected (0.59403290s)
[ OK ] time cost: 595 ms, execute statement ====> create table if not exists testnodejs.weather(ts timestamp, temperature float, humidity int)
Query OK, 1 row(s) affected (0.01058950s)
[ OK ] time cost: 12 ms, execute statement ====> insert into testnodejs.weather (ts, temperature, humidity) values(now, 20.5, 34)
Query OK, 1 row(s) in set (0.00401490s)
[ OK ] time cost: 10 ms, execute statement ====> select * from testnodejs.weather
Connection is closed

           ts             |       temperature        |  humidity   |
=====================================================================
2020-10-27 18:49:15.547   | 20.5                     | 34          |
```

## 指南

### 如何设置主机名和hosts

在server上查看hostname和fqdn
```shell
查看hostname
# hostname
taos-server

查看fqdn
# hostname -f
taos-server
```

windows下hosts文件位于：
C:\\Windows\System32\drivers\etc\hosts
修改hosts文件，添加server的ip和hostname

```
192.168.56.101  node5
```

> 什么是FQDN？
>
> FQDN（Full qualified domain name）全限定域名，fqdn由2部分组成：hostname+domainname。
>
> 例如，一个邮件服务器的fqdn可能是：mymail.somecollege.edu，其中mymail是hostname（主机名），somcollege.edu是domainname（域名）。本例中，.edu是顶级域名，.somecollege是二级域名。
>
> 当连接服务器时，必须指定fqdn，然后，dns服务器通过查看dns表，将hostname解析为相应的ip地址。如果只指定hostname（不指定domainname），应用程序可能服务解析主机名。因为如果你试图访问不在本地的远程服务器时，本地的dns服务器和可能没有远程服务器的hostname列表。
>
> 参考：https://kb.iu.edu/d/aiuv
